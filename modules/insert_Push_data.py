#!/usr/bin/env python3
"""
insert_Push_data.py

Loads CSV files from a directory into existing MySQL tables.

Key features:
- Uses get_db_connection() from db_utils (fallbacks handled externally by your db_utils).
- Robust schema discovery (information_schema / DESCRIBE).
- Optionally fills missing non-nullable fields with defaults (--fill_defaults).
- Deduplicates by primary key per batch and uses ON DUPLICATE KEY UPDATE to avoid 1062 errors.
- Loads dimension tables first (files starting with 'Dim'), then others.
- Optionally disables foreign key checks during load (--disable_fk_checks).
- Writes skipped rows to `<csv_filename>.skipped.csv` for manual inspection.
- Produces a summary dict printed at end.

Usage:
    python -m modules.insert_Push_data --dir ./Run_Space/Test_Runner --fill_defaults --disable_fk_checks

"""

import os
import sys
import argparse
import logging
import math
import time
import traceback
from typing import List, Tuple, Set, Dict, Any, Optional
from datetime import datetime, date

import pandas as pd
import mysql.connector
from mysql.connector import Error

# Import user-provided get_db_connection (must exist)
from db_utils import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# -------------------------
# Schema helpers
# -------------------------
def get_table_columns_info(cursor, table_name: str) -> Tuple[List[str], Set[str], Dict[str, str]]:
    """
    Returns (columns_in_order, set_of_non_nullable_columns, type_map)
    Attempts multiple information_schema queries to be robust.
    type_map: { column_name: column_type_string }
    """
    attempts = [
        # MySQL: restrict to current database/schema
        (
            """
            SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_TYPE
            FROM information_schema.columns
            WHERE table_name = %s
              AND table_schema = DATABASE()
            ORDER BY ORDINAL_POSITION
            """,
            (table_name,)
        ),
        # fallback: try table_name only
        (
            """
            SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_TYPE
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ORDINAL_POSITION
            """,
            (table_name,)
        ),
    ]

    for query, params in attempts:
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            if rows:
                cols = [r[0] for r in rows]
                non_nullable = {r[0] for r in rows if (r[1] or "").upper() == "NO"}
                type_map = {r[0]: r[2] for r in rows}
                return cols, non_nullable, type_map
        except Exception:
            logging.debug("get_table_columns_info attempt failed: %s", traceback.format_exc())

    # As last resort, try DESCRIBE (MySQL)
    try:
        cursor.execute(f"DESCRIBE `{table_name}`;")
        rows = cursor.fetchall()
        if rows:
            cols = [r[0] for r in rows]
            non_nullable = {r[0] for r in rows if (r[2] or "").upper() == "NO"}
            type_map = {r[0]: r[1] for r in rows}
            return cols, non_nullable, type_map
    except Exception:
        logging.debug("Fallback DESCRIBE failed: %s", traceback.format_exc())

    return [], set(), {}


def get_table_primary_key_columns(cursor, table_name: str) -> List[str]:
    """
    Returns list of primary key columns for the table (order not critical).
    """
    try:
        cursor.execute(f"SHOW KEYS FROM `{table_name}` WHERE Key_name = 'PRIMARY';")
        rows = cursor.fetchall()
        pk_cols = []
        # Typically Column_name at index 4
        for r in rows:
            try:
                col = r[4]
            except Exception:
                # fallback if dict-like
                try:
                    col = r.get("Column_name")
                except Exception:
                    col = None
            if col:
                pk_cols.append(col)
        return pk_cols
    except Exception:
        logging.debug("get_table_primary_key_columns failed: %s", traceback.format_exc())
        return []


# -------------------------
# Utilities for defaults
# -------------------------
def is_numeric_type(col_type: str) -> bool:
    if not col_type:
        return False
    t = col_type.lower()
    return any(x in t for x in ("int", "decimal", "numeric", "float", "double", "real", "tinyint", "smallint", "mediumint", "bigint"))


def is_text_type(col_type: str) -> bool:
    if not col_type:
        return True
    t = col_type.lower()
    return any(x in t for x in ("char", "text", "varchar", "blob", "enum", "set"))


def is_date_type(col_type: str) -> bool:
    if not col_type:
        return False
    t = col_type.lower()
    return any(x in t for x in ("date", "time", "timestamp", "datetime", "year"))


def default_for_column(col_name: str, col_type: str) -> Any:
    """Return a sensible default for the column type."""
    if is_numeric_type(col_type):
        return 0
    if is_date_type(col_type):
        # Use ISO date string
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    # default: short sentinel string
    return "UNKNOWN"


# -------------------------
# Insert helpers
# -------------------------
def dedupe_rows_by_pk(rows: List[Tuple], col_names: List[str], pk_cols: List[str]) -> List[Tuple]:
    """
    Remove duplicate rows by primary key, preserving first occurrence.
    rows: list of tuples matching col_names order.
    """
    if not pk_cols:
        return rows
    try:
        pk_idx = [col_names.index(pk) for pk in pk_cols if pk in col_names]
    except ValueError:
        return rows
    seen = set()
    out = []
    for r in rows:
        key = tuple(r[i] for i in pk_idx)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def insert_rows(conn, cursor, table_name: str, table_columns: List[str], rows: List[Tuple[Any, ...]],
                pk_cols: List[str], col_type_map: Dict[str, str], batch_size: int = 500) -> Dict[str, Any]:
    """
    Insert rows (list of tuples) into table.
    - chunked insertion
    - deduplicate per chunk
    - ON DUPLICATE KEY UPDATE to avoid 1062 duplicate errors (updates non-PK columns)
    Returns dict: {'inserted': n, 'skipped': m, 'error': None or text}
    """
    inserted = 0
    skipped = 0
    error_text = None

    col_names = table_columns
    placeholders = ", ".join(["%s"] * len(col_names))
    cols_sql = ", ".join([f"`{c}`" for c in col_names])

    non_pk_cols = [c for c in col_names if c not in pk_cols]
    if non_pk_cols:
        dup_updates = ", ".join([f"`{c}` = VALUES(`{c}`)" for c in non_pk_cols])
    else:
        dup_updates = ""

    insert_sql = f"INSERT INTO `{table_name}` ({cols_sql}) VALUES ({placeholders})"
    if dup_updates:
        insert_sql += f" ON DUPLICATE KEY UPDATE {dup_updates}"

    try:
        for start in range(0, len(rows), batch_size):
            chunk = rows[start:start + batch_size]
            # dedupe inside chunk
            chunk = dedupe_rows_by_pk(chunk, col_names, pk_cols)
            if not chunk:
                continue
            try:
                cursor.executemany(insert_sql, chunk)
                conn.commit()
                inserted += len(chunk)
            except mysql.connector.errors.IntegrityError as ie:
                # log chunk-level error, fall back to row-by-row to isolate bad rows
                logging.error("Failed inserting chunk into %s: %s", table_name, ie)
                logging.debug("Falling back to row-by-row insert to skip bad rows.")
                for row_tuple in chunk:
                    try:
                        cursor.execute(insert_sql, row_tuple)
                        conn.commit()
                        inserted += 1
                    except mysql.connector.errors.IntegrityError as row_ie:
                        skipped += 1
                        logging.error("Skipping row due to IntegrityError: %s. Row preview: %s", row_ie, dict(zip(col_names, row_tuple)))
                    except Exception as row_e:
                        skipped += 1
                        logging.error("Skipping row due to unexpected error: %s. Row preview: %s", row_e, dict(zip(col_names, row_tuple)))
            except Exception as e:
                logging.error("Unexpected error while inserting chunk into %s: %s", table_name, e)
                logging.error(traceback.format_exc())
                error_text = str(e)
                break
    except Exception as outer_e:
        logging.error("Unhandled error in insert_rows for %s: %s", table_name, outer_e)
        logging.error(traceback.format_exc())
        error_text = str(outer_e)

    return {"inserted": inserted, "skipped": skipped, "error": error_text}


# -------------------------
# CSV loading core
# -------------------------
def _resolve_directory_arg(directory: str) -> str:
    """
    Resolve directory arg robustly: absolute, cwd-relative, project-root-relative.
    """
    if not directory:
        directory = "."
    if os.path.isabs(directory):
        candidate = os.path.abspath(directory)
        if os.path.isdir(candidate):
            return candidate
        raise FileNotFoundError(f"Directory not found: {candidate}")

    cwd_candidate = os.path.abspath(os.path.join(os.getcwd(), directory))
    if os.path.isdir(cwd_candidate):
        return cwd_candidate

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    root_candidate = os.path.abspath(os.path.join(project_root, directory))
    if os.path.isdir(root_candidate):
        return root_candidate

    alt_candidate = os.path.abspath(os.path.join(project_root, "..", directory))
    if os.path.isdir(alt_candidate):
        return alt_candidate

    raise FileNotFoundError(
        f"Directory not found. Tried: cwd={cwd_candidate}, project_root={root_candidate}, alt={alt_candidate}"
    )


def load_csvs_into_db(directory: str,
                      fill_defaults: bool = False,
                      disable_fk_checks: bool = False,
                      batch_size: int = 1000,
                      skip_missing_table: bool = False) -> Dict[str, Any]:
    """
    Main entry: load CSVs (top-level) in directory to DB.
    Returns summary mapping filename -> { inserted, skipped, error }.
    """
    directory = _resolve_directory_arg(directory)
    logging.info("Resolved target directory: %s", directory)

    csv_files = sorted([f for f in os.listdir(directory) if f.lower().endswith(".csv")])
    if not csv_files:
        logging.warning("No CSV files found in %s", directory)
        return {}

    # prefer dimension tables first (filenames starting with 'Dim' or 'dim')
    dims = [f for f in csv_files if os.path.splitext(f)[0].lower().startswith("dim")]
    others = [f for f in csv_files if f not in dims]
    ordered_files = dims + others
    logging.info("Found %d CSV files. Will process in this order: %s", len(csv_files), ordered_files)

    summary: Dict[str, Dict[str, Any]] = {}

    # connect once, reuse cursor
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
    except Exception as e:
        logging.exception("Failed to obtain DB connection or cursor.")
        raise

    # Optionally disable FK checks
    fk_disabled = False
    try:
        if disable_fk_checks:
            try:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                conn.commit()
                fk_disabled = True
                logging.info("Disabled FOREIGN_KEY_CHECKS for bulk load.")
            except Exception:
                logging.warning("Could not disable FOREIGN_KEY_CHECKS: %s", traceback.format_exc())
    except Exception:
        logging.debug("FK checks block error ignored.")

    for csv_file in ordered_files:
        csv_path = os.path.join(directory, csv_file)
        table_name = os.path.splitext(csv_file)[0]
        logging.info("Processing file '%s' -> table '%s'", csv_file, table_name)
        summary_entry = {"inserted": 0, "skipped": 0, "error": None}
        skipped_rows_preview = []

        # read csv into dataframe (all as string first)
        try:
            df = pd.read_csv(csv_path, dtype=str, keep_default_na=True, na_values=["", "NA", "N/A", "nan", "NaN"])
            logging.info("Read CSV '%s' shape=%s", csv_file, df.shape)
        except pd.errors.EmptyDataError:
            logging.warning("CSV is empty: %s. Skipping.", csv_file)
            summary[csv_file] = summary_entry
            continue
        except Exception as e:
            logging.exception("Failed to read CSV '%s': %s", csv_file, e)
            summary_entry["error"] = f"read_error: {e}"
            summary[csv_file] = summary_entry
            continue

        # normalize column names
        df.columns = [c.strip() for c in df.columns]

        # fetch table schema
        try:
            table_columns, non_nullable_cols, type_map = get_table_columns_info(cursor, table_name)
            if not table_columns:
                msg = f"No columns found for table '{table_name}'."
                logging.warning(msg)
                if skip_missing_table:
                    summary_entry["error"] = msg
                    summary[csv_file] = summary_entry
                    continue
                else:
                    # fallback: assume CSV columns are target
                    table_columns = list(df.columns)
                    non_nullable_cols = set()
                    type_map = {c: "" for c in table_columns}
                    logging.info("Fallback to CSV columns for table '%s': %s", table_name, table_columns)
            else:
                logging.info("Table '%s' columns: %s", table_name, table_columns)
                if non_nullable_cols:
                    logging.info("Non-nullable columns for '%s': %s", table_name, sorted(list(non_nullable_cols)))
        except Exception as e:
            logging.exception("Failed to fetch columns for table '%s': %s", table_name, e)
            summary_entry["error"] = f"schema_fetch_error: {e}"
            summary[csv_file] = summary_entry
            continue

        # primary keys for dedupe
        try:
            pk_cols = get_table_primary_key_columns(cursor, table_name)
            logging.debug("Primary key columns for %s: %s", table_name, pk_cols)
        except Exception:
            pk_cols = []

        # align CSV columns to table columns and prepare rows (tuples)
        rows_to_insert: List[Tuple[Any, ...]] = []
        skipped_count = 0
        skipped_rows_details: List[Dict[str, Any]] = []

        for idx, row in enumerate(df.itertuples(index=False, name=None), start=1):
            row_map = dict(zip(df.columns, row))
            row_vals = []
            violated = []
            for col in table_columns:
                if col in df.columns:
                    v = row_map.get(col)
                    # treat pandas NA's
                    if pd.isna(v):
                        val = None
                    else:
                        val = v
                        # strip strings
                        if isinstance(val, str):
                            val = val.strip()
                            if val == "":
                                val = None
                    # if missing and fill_defaults requested and non-nullable -> fill
                    if val is None and fill_defaults and col in non_nullable_cols:
                        val = default_for_column(col, type_map.get(col, ""))
                else:
                    # column not present in CSV
                    val = None
                    if fill_defaults and col in non_nullable_cols:
                        val = default_for_column(col, type_map.get(col, ""))

                if val is None and col in non_nullable_cols:
                    violated.append(col)
                row_vals.append(val)

            if violated:
                skipped_count += 1
                preview = {c: (row_map.get(c) if c in row_map else None) for c in table_columns[:6]}
                logging.warning("Skipping row #%s from file '%s' because non-nullable columns would be NULL: %s. Preview: %s",
                                idx, csv_file, violated, preview)
                skipped_rows_details.append({"row_index": idx, "violated": violated, "preview": preview})
                continue

            rows_to_insert.append(tuple(row_vals))

        # if nothing to insert, continue
        if not rows_to_insert:
            logging.info("No rows to insert for %s (all skipped or empty). Skipped_count=%s", csv_file, skipped_count)
            summary_entry["inserted"] = 0
            summary_entry["skipped"] = skipped_count
            # write skipped rows file if any
            if skipped_rows_details:
                skipped_path = os.path.join(directory, f"{csv_file}.skipped.csv")
                try:
                    pd.DataFrame(skipped_rows_details).to_csv(skipped_path, index=False)
                    logging.info("Wrote skipped-row details to %s", skipped_path)
                except Exception:
                    logging.debug("Failed to write skipped rows: %s", traceback.format_exc())
            summary[csv_file] = summary_entry
            continue

        # call insert_rows
        insert_result = insert_rows(conn, cursor, table_name, table_columns, rows_to_insert,
                                    pk_cols=pk_cols, col_type_map=type_map, batch_size=batch_size)
        # insert_result contains inserted/skipped/error
        inserted = insert_result.get("inserted", 0)
        inserted_skipped = insert_result.get("skipped", 0)
        err = insert_result.get("error")

        total_skipped = skipped_count + inserted_skipped

        logging.info("Inserted %d rows into table '%s'. Skipped %d rows (non-nullable or integrity failures).",
                     inserted, table_name, total_skipped)

        summary_entry["inserted"] = inserted
        summary_entry["skipped"] = total_skipped
        summary_entry["error"] = err

        # write skipped rows details file if any
        if skipped_rows_details:
            skipped_path = os.path.join(directory, f"{csv_file}.skipped.csv")
            try:
                pd.DataFrame(skipped_rows_details).to_csv(skipped_path, index=False)
                logging.info("Wrote skipped-row details to %s", skipped_path)
            except Exception:
                logging.debug("Failed to write skipped rows: %s", traceback.format_exc())

        summary[csv_file] = summary_entry

    # restore FK checks if disabled
    try:
        if fk_disabled:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            conn.commit()
            logging.info("Re-enabled FOREIGN_KEY_CHECKS after bulk load.")
    except Exception:
        logging.warning("Failed to re-enable FOREIGN_KEY_CHECKS: %s", traceback.format_exc())

    # close resources
    try:
        if cursor:
            cursor.close()
    except Exception:
        pass
    try:
        if conn:
            conn.close()
    except Exception:
        pass

    return summary


# -------------------------
# CLI
# -------------------------
def parse_args(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(description="Load CSV files into existing DB tables (filename->table mapping).")
    p.add_argument("--dir", type=str, default=".", help="Directory containing CSV files (top-level only).")
    p.add_argument("--batch_size", type=int, default=1000, help="Batch size for inserts.")
    p.add_argument("--fill_defaults", action="store_true", help="Auto-fill missing non-nullable columns with defaults (strings->'UNKNOWN', numbers->0).")
    p.add_argument("--disable_fk_checks", action="store_true", help="Temporarily disable FOREIGN_KEY_CHECKS during load (use with caution).")
    p.add_argument("--skip_missing_table", action="store_true", help="Skip files with no matching table in DB instead of attempting fallback.")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None):
    args = parse_args(argv)
    # debug prints
    logging.info("Current working directory (cwd): %s", os.getcwd())
    logging.info("Module file location (__file__): %s", os.path.abspath(__file__))

    try:
        result = load_csvs_into_db(directory=args.dir,
                                  fill_defaults=args.fill_defaults,
                                  disable_fk_checks=args.disable_fk_checks,
                                  batch_size=args.batch_size,
                                  skip_missing_table=args.skip_missing_table)
        logging.info("Load summary:")
        for fname, info in result.items():
            logging.info(" %s: %s", fname, info)
    except Exception as e:
        logging.exception("Fatal error during load: %s", e)
        sys.exit(2)


if __name__ == "__main__":
    main()
