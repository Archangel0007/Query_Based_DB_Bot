# values_parser.py
import os
import re
import csv
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

# regex to split values inside a tuple, keeping quoted strings intact
_value_split_regex = re.compile(r"""
    (N?'(?:[^']|'')*'   # N'...' or '...' including doubled single-quotes
    |'(?:[^']|'')*'     # '...' (redundant with previous but explicit)
    |NULL               # NULL literal
    |[^,]+              # unquoted token (numbers, booleans)
    )
    """, re.VERBOSE | re.IGNORECASE)


def split_top_level_tuples(values_block: str) -> List[str]:
    """
    Split a VALUES block into top-level tuple strings (without outer parentheses).
    Handles parentheses and single-quote strings (including doubled single quotes).
    Returns list of inner tuple contents (content between the outer-most parentheses).
    """
    tuples: List[str] = []
    cur_chars: List[str] = []
    depth = 0
    in_single_quote = False

    i = 0
    while i < len(values_block):
        ch = values_block[i]
        cur_chars.append(ch)

        # If char is single-quote, handle toggling quote state
        if ch == "'":
            # detect doubled single quotes ('') which are escaped quotes inside SQL strings
            # If next char is also a single quote, consume it as part of the string but do not toggle state twice
            if i + 1 < len(values_block) and values_block[i + 1] == "'":
                # append next quote and skip toggling; this is escaped quote, remain inside quote
                cur_chars.append("'")
                i += 1
            else:
                in_single_quote = not in_single_quote

        elif not in_single_quote:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    tup = "".join(cur_chars).strip()
                    # remove outer parentheses
                    if tup.startswith("(") and tup.endswith(")"):
                        tuples.append(tup[1:-1])
                    cur_chars = []
                    # after a tuple, there might be a comma and whitespace; we'll let the loop continue

        i += 1

    # return non-empty trimmed tuples
    return [t.strip() for t in tuples if t and t.strip()]


def parse_values_from_tuple(tuple_inner: str) -> List[str]:
    """
    Parse the inner content of a tuple into individual values.
    Normalizes:
      - NULL -> ''
      - N'...' and '...' -> stripped and unescaped doubled quotes
      - numeric/unquoted tokens kept as-is
    """
    parts = [m.group(0).strip() for m in _value_split_regex.finditer(tuple_inner)]
    cleaned: List[str] = []
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


def ensure_csv_headers(csv_path: str, headers: List[str]):
    """
    Ensure CSV exists and has header row. Create folder if necessary.
    If the file doesn't exist, create it and write header.
    """
    dirpath = os.path.dirname(csv_path)
    if dirpath and not os.path.exists(dirpath):
        os.makedirs(dirpath, exist_ok=True)

    if not os.path.exists(csv_path):
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def parse_and_write_inserts(values_filepath: str, schemas: Dict[str, List[str]], output_dir: str):
    """
    Parse INSERT statements from values_filepath and write rows to CSVs in output_dir.
    Uses per-INSERT column lists if present. Falls back to schemas dict if not.
    Appends rows to existing CSVs (mode='a'). If you want to overwrite, remove files first or alter behavior.
    """
    if not os.path.exists(values_filepath):
        logger.error("Values file not found: %s", values_filepath)
        return

    with open(values_filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Capture optional column list and the values block. Non-greedy up to the first semicolon.
    insert_regex = re.compile(
        r"INSERT INTO (?:public\.)?\"?([\w_]+)\"?\s*(?:\(([^)]+)\))?\s*VALUES\s*(.*?);",
        re.IGNORECASE | re.DOTALL
    )

    os.makedirs(output_dir, exist_ok=True)

    for match in insert_regex.finditer(content):
        table_raw = match.group(1)
        table = table_raw.strip().replace('"', "")
        cols_group = match.group(2)  # may be None
        values_block = match.group(3)

        # Determine headers: prefer columns listed on the INSERT, otherwise fallback to schemas
        if cols_group:
            insert_cols = [c.strip().replace('"', "") for c in re.findall(r'"[^"]*"|[\w_]+', cols_group)]
            headers = insert_cols
            logger.info("INSERT for %s provides columns: %s", table, headers)
        else:
            if table in schemas:
                headers = schemas[table]
            else:
                logger.warning("No columns for table '%s' found (no CREATE TABLE and no INSERT column list). Skipping.", table)
                continue

        csv_path = os.path.join(output_dir, f"{table}.csv")
        ensure_csv_headers(csv_path, headers)

        # Split the values block into tuple contents
        tuple_inners = split_top_level_tuples(values_block)
        rows = []
        for tup in tuple_inners:
            vals = parse_values_from_tuple(tup)
            if len(vals) == len(headers):
                rows.append(vals)
            else:
                logger.warning(
                    "Row column count mismatch for table '%s': expected %d, got %d. Row head: %.120s",
                    table, len(headers), len(vals), tup
                )

        if rows:
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(rows)
            logger.info("Wrote %d rows to %s", len(rows), csv_path)
        else:
            logger.info("No valid rows parsed for table %s in this INSERT.", table)


def parse_create_table_statements(filepath: str) -> Dict[str, List[str]]:
    """
    Parse CREATE TABLE statements to extract column order.
    Returns dict: { table_name: [col1, col2, ...] }
    """
    if not os.path.exists(filepath):
        logger.warning("Schema file not found: %s (skipping)", filepath)
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
            logger.info("Found CREATE TABLE schema for %s (%d columns).", table, len(cols))
    return schemas


def infer_schema_from_insert_statements(values_filepath: str) -> Dict[str, List[str]]:
    """
    Fallback: infer schema from INSERT statements that include a column list.
    """
    if not os.path.exists(values_filepath):
        logger.error("Values file not found for schema inference: %s", values_filepath)
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
            logger.info("Inferred schema for %s from INSERT column list.", table)
    return schemas

# sql_to_csv.py
import os
import logging

# If you prefer, you can import ensure_csv_headers or others too
# But to keep the module interface minimal, we'll only use the main functions above.

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def create_empty_csv_for_all_tables_local(schemas, output_dir):
    """Create CSV files with headers for all tables if they don't already exist."""
    os.makedirs(output_dir, exist_ok=True)
    for t, hdrs in schemas.items():
        csv_path = os.path.join(output_dir, f"{t}.csv")
        if not os.path.exists(csv_path):
            # Use values_parser's ensure_csv_headers indirectly via parse_and_write_inserts,
            # but here we implement a small local header writer to avoid circular import.
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                import csv as _csv
                _csv.writer(f).writerow(hdrs)
            logger.info("Created CSV header for %s", t)


def main():
    print("🚀 Starting SQL -> CSV conversion (split module mode)")

    tables_sql_file = "create_schema_tables.sql"   # optional; can be absent
    values_sql_file = "create_schema_values.sql"   # required (contains INSERTs)
    output_dir = "Chinook_Dataset_CSVs"

    schemas = {}
    # Parse CREATE TABLE if present
    if os.path.exists(tables_sql_file):
        schemas = parse_create_table_statements(tables_sql_file)

    # Infer from INSERT column lists; don't override existing CREATE TABLE schemas
    if os.path.exists(values_sql_file):
        inferred = infer_schema_from_insert_statements(values_sql_file)
        for k, v in inferred.items():
            schemas.setdefault(k, v)
    else:
        logger.error("Values SQL file not found: %s", values_sql_file)
        print("❌ Values SQL file not found. Exiting.")
        return

    # Create empty CSVs for tables with known schemas (optional)
    if schemas:
        create_empty_csv_for_all_tables_local(schemas, output_dir)

    # Now parse INSERTs and write CSVs (this will use per-INSERT columns when present)
    parse_and_write_inserts(values_sql_file, schemas, output_dir)

    print("🏁 Done.")


if __name__ == "__main__":
    main()
