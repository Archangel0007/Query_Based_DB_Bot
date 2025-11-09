#!/usr/bin/env python3
"""
insert_Push_data.py

Load top-level CSV files from a directory into existing DB tables (simple filename -> table mapping).

Usage:
    # From project root (recommended)
    python -m modules.insert_Push_data --dir ./Run_Space/Test_Runner

Notes:
    - Uses get_db_connection() from db_utils (must be present and return a DB connection).
    - Reads CSVs with pandas (all columns read as strings first).
    - Maps filename (without .csv) -> table name.
    - Fetches target table columns via information_schema.
    - Aligns CSV columns to table columns, filling missing columns with NULL.
    - Inserts rows in batches using cursor.executemany and commits per chunk.
    - Skips rows that would insert NULL into non-nullable columns; skipped rows are logged and counted.
"""

import os
import argparse
import logging
from typing import List, Tuple, Set

import pandas as pd

from db_utils import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_table_columns_info(cursor, table_name: str) -> Tuple[List[str], Set[str]]:
    """
    Fetch a list of column names for table_name and a set of non-nullable column names.
    Returns (columns_in_order, set_of_non_nullable_columns).
    Tries a few information_schema queries to work with MySQL/Postgres.
    """
    # Try MySQL / MariaDB approach (uses current database)
    try:
        query = """
            SELECT COLUMN_NAME, IS_NULLABLE
            FROM information_schema.columns
            WHERE table_name = %s
            AND table_schema = DATABASE()
            ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query, (table_name,))
        rows = cursor.fetchall()
        if rows:
            cols = [r[0] for r in rows]
            non_nullable = {r[0] for r in rows if (r[1] or "").upper() == "NO"}
            return cols, non_nullable
    except Exception:
        pass

    # Try Postgres (current_schema())
    try:
        query = """
            SELECT column_name, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            AND table_schema = current_schema()
            ORDER BY ordinal_position
        """
        cursor.execute(query, (table_name,))
        rows = cursor.fetchall()
        if rows:
            cols = [r[0] for r in rows]
            non_nullable = {r[0] for r in rows if (r[1] or "").upper() == "NO"}
            return cols, non_nullable
    except Exception:
        pass

    # Last resort: query by table_name only (best-effort)
    try:
        query = """
            SELECT COLUMN_NAME, IS_NULLABLE
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query, (table_name,))
        rows = cursor.fetchall()
        if rows:
            cols = [r[0] for r in rows]
            non_nullable = {r[0] for r in rows if (r[1] or "").upper() == "NO"}
            return cols, non_nullable
    except Exception:
        pass

    # If nothing found, return empty list and empty set
    return [], set()


def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(size):
                chunk.append(next(it))
        except StopIteration:
            if chunk:
                yield chunk
            break
        yield chunk


def insert_rows(conn, cursor, table_name: str, columns: List[str], rows: List[Tuple], batch_size: int = 1000):
    """
    Insert rows into table_name using columns order and parameterized executemany.
    Uses %s parameter placeholders (works with mysql-connector-python / psycopg2).
    """
    if not rows:
        return 0

    # Build column list and placeholders
    col_list = ", ".join([f"`{c}`" for c in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders})"

    total_inserted = 0
    for chunk in chunked_iterable(rows, batch_size):
        try:
            cursor.executemany(insert_sql, chunk)
            conn.commit()
            total_inserted += len(chunk)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logging.exception(f"Failed inserting chunk into {table_name}: {e}")
            raise

    return total_inserted


def _resolve_directory_arg(directory: str) -> str:
    """
    Resolve the provided directory argument robustly:
      - If absolute and exists -> use it
      - Else try cwd-relative
      - Else try project-root-relative (assumes this module is in modules/)
    Raises FileNotFoundError with helpful message if not found.
    """
    # Accept None / empty -> use cwd
    if not directory:
        directory = "."

    # If already absolute path
    if os.path.isabs(directory):
        candidate = os.path.abspath(directory)
        if os.path.isdir(candidate):
            return candidate
        raise FileNotFoundError(f"Directory not found: {candidate}")

    # 1) cwd-relative
    cwd_candidate = os.path.abspath(os.path.join(os.getcwd(), directory))
    if os.path.isdir(cwd_candidate):
        return cwd_candidate

    # 2) project-root-relative (assume this file is in modules/)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    root_candidate = os.path.abspath(os.path.join(project_root, directory))
    if os.path.isdir(root_candidate):
        return root_candidate

    # 3) also try one level up from project_root (in case called from another layout)
    alt_candidate = os.path.abspath(os.path.join(project_root, "..", directory))
    if os.path.isdir(alt_candidate):
        return alt_candidate

    # Not found, raise with attempted paths
    raise FileNotFoundError(
        f"Directory not found. Tried the following locations:\n"
        f" - cwd-relative: {cwd_candidate}\n"
        f" - project-root-relative: {root_candidate}\n"
        f" - alt-relative: {alt_candidate}"
    )


def load_csvs_into_db(
    directory: str,
    allow_extra: bool = False,
    batch_size: int = 1000,
    skip_missing_table: bool = False,
) -> dict:
    """
    Walk the specified directory (non-recursive) and push CSVs into DB tables.
    Returns a summary dict: { csv_filename: { 'inserted': n, 'skipped': m, 'error': '...' } }
    """
    # Resolve directory robustly
    directory = _resolve_directory_arg(directory)
    logging.info(f"Resolved target directory: {directory}")

    csv_files = sorted([f for f in os.listdir(directory) if f.lower().endswith(".csv")])
    logging.info(f"Found {len(csv_files)} CSV files in {directory}")

    summary = {}

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
    except Exception as e:
        logging.exception("Failed to obtain DB connection or cursor.")
        raise

    for csv_file in csv_files:
        table_name = os.path.splitext(csv_file)[0]  # filename without .csv -> table name
        csv_path = os.path.join(directory, csv_file)
        summary_entry = {"inserted": 0, "skipped": 0, "error": None}
        logging.info(f"Processing file '{csv_file}' -> table '{table_name}'")

        # Read CSV safely
        try:
            df = pd.read_csv(csv_path, dtype=str)
            logging.info(f"Read CSV '{csv_file}' shape={df.shape}")
        except pd.errors.EmptyDataError:
            logging.warning(f"CSV is empty: {csv_file}. Skipping.")
            summary_entry["skipped"] = 0
            summary[csv_file] = summary_entry
            continue
        except Exception as e:
            logging.exception(f"Failed to read CSV '{csv_file}': {e}")
            summary_entry["error"] = f"read_error: {e}"
            summary[csv_file] = summary_entry
            continue

        # Normalize column names
        df.columns = [c.strip() for c in df.columns]

        # Get table columns and non-nullable set from DB
        try:
            table_columns, non_nullable_cols = get_table_columns_info(cursor, table_name)
            if not table_columns:
                msg = f"No columns found for table '{table_name}'."
                logging.warning(msg)
                if skip_missing_table:
                    summary_entry["error"] = msg
                    summary[csv_file] = summary_entry
                    continue
                else:
                    # Fall back to CSV columns (best-effort) and assume none required non-nullable
                    table_columns = list(df.columns)
                    non_nullable_cols = set()
            logging.info(f"Table '{table_name}' columns: {table_columns}")
            if non_nullable_cols:
                logging.info(f"Non-nullable columns for '{table_name}': {sorted(list(non_nullable_cols))}")
        except Exception as e:
            logging.exception(f"Failed to fetch columns for table '{table_name}': {e}")
            summary_entry["error"] = f"schema_fetch_error: {e}"
            summary[csv_file] = summary_entry
            continue

        # Prepare rows aligned to table_columns, skipping rows that violate non-nullable constraints
        rows_to_insert = []
        skipped_count = 0
        for idx, row in enumerate(df.itertuples(index=False, name=None), start=1):
            # Build a mapping col->value for convenience (columns in df order)
            row_map = dict(zip(df.columns, row))
            row_values = []
            violate_non_nullable = False
            violated_cols = []

            for col in table_columns:
                if col in df.columns:
                    v = row_map.get(col)
                    if pd.isna(v):
                        value = None
                    else:
                        value = v
                        if isinstance(value, str) and value.strip() == "":
                            value = None
                    # check non-nullable violation
                    if value is None and col in non_nullable_cols:
                        violate_non_nullable = True
                        violated_cols.append(col)
                    row_values.append(value)
                else:
                    # missing column -> None (and potential violation)
                    if col in non_nullable_cols:
                        violate_non_nullable = True
                        violated_cols.append(col)
                    row_values.append(None)

            if violate_non_nullable:
                skipped_count += 1
                # Log the skipped row (index, filename, violated columns, small preview)
                preview = {c: (row_map.get(c) if c in row_map else None) for c in table_columns[:6]}
                logging.warning(
                    f"Skipping row #{idx} from file '{csv_file}' because non-nullable columns would be NULL: {violated_cols}. Preview: {preview}"
                )
                continue

            rows_to_insert.append(tuple(row_values))

        # Insert batches
        try:
            inserted = insert_rows(conn, cursor, table_name, table_columns, rows_to_insert, batch_size=batch_size)
            logging.info(f"Inserted {inserted} rows into table '{table_name}'. Skipped {skipped_count} rows due to non-nullable constraints.")
            summary_entry["inserted"] = inserted
            summary_entry["skipped"] = skipped_count
        except Exception as e:
            logging.exception(f"Error inserting rows into '{table_name}': {e}")
            summary_entry["error"] = str(e)
            # still record skipped count
            summary_entry["skipped"] = skipped_count

        summary[csv_file] = summary_entry

    # Close DB resources
    try:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    except Exception:
        logging.warning("Error closing DB resources", exc_info=True)

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load CSV files into existing DB tables (filename->table mapping).")
    parser.add_argument("--dir", type=str, default=".", help="Directory containing CSV files (top-level only).")
    parser.add_argument("--batch_size", type=int, default=1000, help="Batch size for inserts.")
    parser.add_argument("--skip_missing_table", action="store_true", help="Skip files with no matching table in DB.")
    args = parser.parse_args()

    # Debug prints to help if directory resolution still fails
    logging.info(f"Current working directory (cwd): {os.getcwd()}")
    logging.info(f"Module file location (__file__): {os.path.abspath(__file__)}")

    result = load_csvs_into_db(args.dir, batch_size=args.batch_size, skip_missing_table=args.skip_missing_table)
    logging.info("Load summary:")
    for fname, info in result.items():
        logging.info(f" {fname}: {info}")
