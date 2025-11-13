# sql_to_csv.py
import os
import re
import csv
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")


def parse_create_table_statements(filepath: str) -> Dict[str, List[str]]:
    """Parse CREATE TABLE statements to extract column order per table."""
    if not os.path.exists(filepath):
        logging.info("Schema file not found (skipping): %s", filepath)
        return {}

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    schemas: Dict[str, List[str]] = {}
    create_table_regex = re.compile(
        r"CREATE TABLE (?:public\.)?\"?([\w_]+)\"?\s*\((.*?)\);",
        re.DOTALL | re.IGNORECASE
    )
    column_definition_regex = re.compile(r"\"?([\w_]+)\"?\s+[^,\n]+", re.IGNORECASE)

    for match in create_table_regex.finditer(content):
        table = match.group(1).strip().replace('"', "")
        cols_sql = match.group(2)
        cols = [m.group(1).strip().replace('"', "") for m in column_definition_regex.finditer(cols_sql)]
        if cols:
            schemas[table] = cols
            logging.info("Found CREATE TABLE schema for %s (%d cols).", table, len(cols))

    return schemas


def infer_schema_from_insert_statements(values_filepath: str) -> Dict[str, List[str]]:
    """Infer schemas from INSERT statements that include a column list."""
    if not os.path.exists(values_filepath):
        logging.warning("Values file not found for inference: %s", values_filepath)
        return {}

    with open(values_filepath, "r", encoding="utf-8") as f:
        content = f.read()

    insert_with_cols_regex = re.compile(
        r"INSERT INTO (?:public\.)?\"?([\w_]+)\"?\s*\(([^)]+)\)\s*VALUES",
        re.IGNORECASE
    )

    schemas: Dict[str, List[str]] = {}
    for match in insert_with_cols_regex.finditer(content):
        table = match.group(1).strip().replace('"', "")
        cols_str = match.group(2)
        col_names = [c.strip().replace('"', "") for c in re.findall(r'"[^"]*"|[\w_]+', cols_str)]
        if col_names and table not in schemas:
            schemas[table] = col_names
            logging.info("Inferred schema for %s from INSERT column list.", table)
    return schemas


def split_top_level_tuples(values_block: str) -> List[str]:
    """
    Split a VALUES block into top-level tuple inner strings (without outer parentheses).
    Starts collecting when a top-level '(' is encountered. Handles single-quoted strings
    (including doubled single quotes) so commas inside quotes are ignored.
    Returns list of tuple inner texts (no surrounding parentheses).
    """
    tuples = []
    cur = []
    depth = 0
    in_single = False
    i = 0
    s = values_block
    while i < len(s):
        ch = s[i]

        # handle single-quote and doubled single-quote escapes
        if ch == "'":
            cur.append(ch)
            if i + 1 < len(s) and s[i + 1] == "'":
                # doubled quote -> consume both but remain in the same in_single state
                cur.append("'")
                i += 1
            else:
                # toggle in_single
                in_single = not in_single
        else:
            # only consider parentheses when not inside a single-quoted literal
            if not in_single:
                if ch == "(":
                    # if we're at top-level, start a new collection
                    if depth == 0:
                        cur = []  # reset and start fresh from the '('
                    depth += 1
                    cur.append(ch)
                elif ch == ")":
                    depth -= 1
                    cur.append(ch)
                    # when we've closed a top-level tuple, capture it
                    if depth == 0:
                        tup = "".join(cur).strip()
                        # strip leading commas/spaces, then ensure it still is a parenthesized tuple
                        # (sometimes SQL has a comma before the '(' which we removed by starting
                        # collection only at '(' so this check is extra-safety)
                        if tup.startswith("(") and tup.endswith(")"):
                            tuples.append(tup[1:-1].strip())
                        cur = []
                else:
                    # at top level or nested, just append character
                    # but avoid collecting top-level commas before the next '(' (we only collect
                    # after we've seen '(')
                    if depth > 0:
                        cur.append(ch)
            else:
                # inside single-quoted literal: just append character
                cur.append(ch)
        i += 1

    return [t for t in tuples if t]


_value_split_regex = re.compile(r"""
    (N?'(?:[^']|'')*'   # N'...' or '...' with doubled quotes
    |'(?:[^']|'')*'     # '...'
    |NULL               # NULL literal
    |[^,]+              # unquoted token (numbers, booleans)
    )
    """, re.VERBOSE | re.IGNORECASE)


def split_row_values(row_inner: str) -> List[str]:
    parts = [m.group(0).strip() for m in _value_split_regex.finditer(row_inner)]
    cleaned = []
    for p in parts:
        if p.upper() == "NULL":
            cleaned.append("")
        elif p.startswith("N'") and p.endswith("'"):
            cleaned.append(p[2:-1].replace("''", "'"))
        elif p.startswith("'") and p.endswith("'"):
            cleaned.append(p[1:-1].replace("''", "'"))
        else:
            cleaned.append(p)
    return cleaned


def ensure_csv_header_and_append(csv_path: str, headers: List[str], rows: List[List[str]]):
    """Create CSV with header if missing, then append rows."""
    dirpath = os.path.dirname(csv_path)
    if dirpath and not os.path.exists(dirpath):
        os.makedirs(dirpath, exist_ok=True)

    write_header = not os.path.exists(csv_path)
    mode = "a" if not write_header else "w"
    with open(csv_path, mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(headers)
        writer.writerows(rows)


def parse_and_write_inserts(values_filepath: str, schemas: dict, output_dir: str):
    """
    Robust parsing of INSERT ... VALUES blocks and writing/appending rows to CSVs.

    Improvements over previous version:
    - If an INSERT includes an explicit column list, use that list for mapping values.
      (This fixes cases like PlaylistTrack INSERTs that supply only 2 cols while the table schema
      contains 3 — the missing column will be filled with an empty string.)
    - If INSERT does not include a column list, falls back to the full CREATE TABLE schema.
    - Writes CSVs in append-mode when a CSV already exists (header is written only once).
    - Saves problematic rows to a `bad_rows_<table>.csv` for manual inspection.
    - Uses a robust state-machine splitter to avoid splitting on commas inside quoted literals.
    """
    import os
    import re
    import csv
    import logging

    if not os.path.exists(values_filepath):
        logging.error("Values file not found: %s", values_filepath)
        print(f"❌ ERROR: Values file not found at '{values_filepath}'")
        return

    os.makedirs(output_dir, exist_ok=True)
    logging.info("🔍 Reading data values from: %s", values_filepath)
    with open(values_filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Capture INSERT blocks, and optionally capture the explicit column list
    # Group 1 = table name, Group 2 = optional column list (may be None), Group 3 = values block
    insert_regex = re.compile(
        r"INSERT INTO (?:public\.)?\"?([\w_]+)\"?\s*(?:\(([^)]+)\))?\s*VALUES\s*(.*?);",
        re.DOTALL | re.IGNORECASE,
    )

    # Each row tuple inside VALUES(...),(...)
    row_regex = re.compile(r"\((.*?)\)", re.DOTALL)

    def split_row_values(row_str: str) -> list:
        """Split a tuple-string into top-level values, respecting single quotes and doubled quotes."""
        vals = []
        cur = []
        in_single = False
        i = 0
        s = row_str
        while i < len(s):
            ch = s[i]
            if ch == "'":
                cur.append(ch)
                # handle doubled single-quote escape by consuming the second one too
                if i + 1 < len(s) and s[i + 1] == "'":
                    cur.append("'")
                    i += 1
                else:
                    in_single = not in_single
            elif not in_single and ch == ",":
                vals.append("".join(cur).strip())
                cur = []
            else:
                cur.append(ch)
            i += 1
        vals.append("".join(cur).strip())
        return vals

    def clean_token(tok: str) -> str:
        """Normalize tokens into CSV-ready strings."""
        if tok is None:
            return ""
        t = tok.strip()
        if t == "":
            return ""
        if t.upper() == "NULL":
            return ""
        # N'...' or E'...' or normal quoted
        if (t.startswith("N'") or t.startswith("E'")) and t.endswith("'") and len(t) >= 3:
            return t[2:-1].replace("''", "'")
        if t.startswith("'") and t.endswith("'") and len(t) >= 2:
            return t[1:-1].replace("''", "'")
        return t

    # Helper to ensure CSV header exists; returns True if header already present
    def ensure_csv_header(csv_path: str, headers: list) -> bool:
        if not os.path.exists(csv_path):
            # create file with header
            with open(csv_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow(headers)
            return False
        # file exists - assume header present (could validate but keep fast)
        return True

    total_written = 0

    for match in insert_regex.finditer(content):
        table_name = match.group(1).strip().replace('"', "")
        insert_cols_raw = match.group(2)  # may be None
        values_block = match.group(3)

        # Determine per-insert headers (if INSERT provides column list) else fallback to schema
        if insert_cols_raw:
            # parse the column list tokens (handles quoted and unquoted identifiers)
            insert_cols = [c.strip().replace('"', "") for c in re.findall(r'"[^"]*"|[\w_]+', insert_cols_raw)]
        else:
            insert_cols = None

        # Determine full table headers (from provided CREATE TABLE schema)
        table_schema_headers = schemas.get(table_name)

        if insert_cols is None and table_schema_headers is None:
            logging.warning("No schema found for table '%s' and INSERT has no column list — skipping.", table_name)
            continue

        # Use final_headers: the order we'll write to CSV (prefer full schema if available)
        final_headers = table_schema_headers if table_schema_headers is not None else insert_cols

        csv_filepath = os.path.join(output_dir, f"{table_name}.csv")
        bad_rows_path = os.path.join(output_dir, f"bad_rows_{table_name}.csv")

        # Ensure header exists before appending rows; if no file, write header now
        header_exists = ensure_csv_header(csv_filepath, final_headers)

        parsed_tuples = 0
        rows_to_append = []

        for row_match in row_regex.finditer(values_block):
            parsed_tuples += 1
            row_str = row_match.group(1).strip()
            raw_vals = split_row_values(row_str)
            cleaned = [clean_token(v) for v in raw_vals]

            # If INSERT provided an explicit column list, use that mapping: insert_cols -> cleaned
            if insert_cols:
                expected = len(insert_cols)
                if len(cleaned) != expected:
                    # If too many tokens, attempt conservative merge of trailing tokens until counts match.
                    if len(cleaned) > expected:
                        while len(cleaned) > expected and len(cleaned) >= 2:
                            cleaned[-2] = cleaned[-2] + "," + cleaned[-1]
                            cleaned.pop(-1)
                    # If after merging it still does not match, log and dump to bad_rows
                    if len(cleaned) != expected:
                        logging.warning(
                            "Row length mismatch for '%s' (INSERT columns provided). expected=%d got=%d. Row head: %s",
                            table_name, expected, len(cleaned), ", ".join(cleaned[:6])
                        )
                        # save raw problematic row for inspection
                        with open(bad_rows_path, "a", newline="", encoding="utf-8") as bad_f:
                            writer = csv.writer(bad_f)
                            writer.writerow([row_str])
                        continue

                # Map insert_cols -> cleaned values into final row following final_headers order
                if table_schema_headers:
                    # build mapping then order by table_schema_headers
                    mapping = dict(zip(insert_cols, cleaned))
                    final_row = [mapping.get(h, "") for h in final_headers]
                else:
                    # no full schema; final headers == insert_cols
                    final_row = cleaned

            else:
                # No insert column list; INSERT should provide values for full schema order
                expected = len(final_headers)
                if len(cleaned) != expected:
                    # attempt conservative merge if too many tokens
                    if len(cleaned) > expected:
                        while len(cleaned) > expected and len(cleaned) >= 2:
                            cleaned[-2] = cleaned[-2] + "," + cleaned[-1]
                            cleaned.pop(-1)
                    if len(cleaned) != expected:
                        logging.warning(
                            "Row length mismatch for '%s'. expected=%d got=%d. Row head: %s",
                            table_name, expected, len(cleaned), ", ".join(cleaned[:6])
                        )
                        with open(bad_rows_path, "a", newline="", encoding="utf-8") as bad_f:
                            writer = csv.writer(bad_f)
                            writer.writerow([row_str])
                        continue
                final_row = cleaned

            rows_to_append.append(final_row)

        logging.info("Parsed %d tuple(s) from INSERT for table %s.", parsed_tuples, table_name)

        # Append rows_to_append to CSV
        if rows_to_append:
            try:
                # open in append mode
                with open(csv_filepath, "a", newline="", encoding="utf-8") as csv_file:
                    writer = csv.writer(csv_file)
                    # If header didn't exist previously but file existed (very unlikely), we ensured header above.
                    writer.writerows(rows_to_append)
                logging.info(" - ✅ Appended %d rows to %s", len(rows_to_append), csv_filepath)
                total_written += len(rows_to_append)
            except Exception as e:
                logging.error("Failed to append CSV for table %s: %s", table_name, e)
                print(f"❌ ERROR writing CSV for {table_name}: {e}")
        else:
            logging.info(" - No valid rows parsed for table %s in this INSERT.", table_name)

    logging.info("Finished parsing INSERTs. Total rows appended across tables: %d", total_written)


def create_empty_csv_for_all_tables(schemas: Dict[str, List[str]], output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    for table, hdrs in schemas.items():
        csv_path = os.path.join(output_dir, f"{table}.csv")
        if not os.path.exists(csv_path):
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(hdrs)
            logging.info("Created empty CSV for %s", table)


def main():
    print("🚀 SQL -> CSV converter (handles multi-INSERT blocks)")

    tables_sql_file = "create_schema_tables.sql"      # optional
    values_sql_file = "create_schema_values.sql"      # the file containing your INSERTs
    output_dir = "Chinook_Dataset_CSVs"

    # 1) Try to read explicit CREATE TABLE schemas
    schemas = parse_create_table_statements(tables_sql_file)

    # 2) If missing any schemas, try to infer from INSERTs
    if not schemas:
        logging.info("No CREATE TABLE schemas found; trying to infer from INSERT statements.")
        schemas = infer_schema_from_insert_statements(values_sql_file)

    if schemas:
        logging.info("Schemas available for %d tables.", len(schemas))
        create_empty_csv_for_all_tables(schemas, output_dir)
    else:
        logging.info("No schemas available; insert statements must include column lists to succeed.")

    # 3) Parse INSERTs and write CSVs
    parse_and_write_inserts(values_sql_file, schemas, output_dir)

    print("🏁 Done. CSVs are in:", output_dir)


if __name__ == "__main__":
    main()
