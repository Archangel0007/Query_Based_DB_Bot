import os
import csv
import re
import datetime
import decimal
from typing import Optional, Any, Dict, List
from db_utils import get_db_connection
from mysql.connector import Error
 
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
 
def _serialize_value(v):
    """Convert DB values into JSON-safe types."""
    if v is None:
        return None
    if isinstance(v, (int, float, str, bool)):
        return v
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    return str(v)
 
def _safe_table_name(name: str) -> str:
    if not SAFE_NAME_RE.match(name):
        raise ValueError(f"Unsafe table name: {name!r}")
    return name
 
def _count_csv_rows(path: str, has_header: bool = True) -> int:
    """Count non-empty CSV rows. Skip first row if has_header=True."""
    count = 0
    with open(path, "r", encoding="utf-8", newline='') as fh:
        reader = csv.reader(fh)
        if has_header:
            for row in reader:
                if any(cell.strip() for cell in row):
                    break
        for row in reader:
            if any(cell.strip() for cell in row):
                count += 1
    return count
 
def fetch_tables_with_insert_stats(task_id: str,
                                   runspace_base: str = "../Run_Space",
                                   preview_limit: int = 5,
                                   conn: Optional[Any] = None) -> Dict[str, Dict[str, Any]]:
    """
    Returns a dict keyed by table name:
    {
        "table_name": {
            "preview": {"columns": [...], "rows": [[...], ...], "row_count_preview": N},
            "insert_stats": {"csv_rows": int|None, "db_rows": int|None, "inserted_summary": str}
        },
        ...
    }
    """
    result: Dict[str, Dict[str, Any]] = {}
    close_conn = False
    cursor = None
 
    # Prepare runspace folder
    task_dir = os.path.join(runspace_base, task_id)
    if not os.path.isdir(task_dir):
        return {"_error": f"Run folder not found: {task_dir}"}
 
    # Count CSV rows
    csv_files = [f for f in os.listdir(task_dir) if f.lower().endswith(".csv")]
    insert_info: Dict[str, Dict[str, Optional[int]]] = {}
    for f in csv_files:
        table = os.path.splitext(f)[0]
        path = os.path.join(task_dir, f)
        try:
            rows_in_csv = _count_csv_rows(path)
        except Exception:
            rows_in_csv = None
        insert_info[table] = {"csv_rows": rows_in_csv, "db_rows": None, "inserted_summary": None}
 
    try:
        # Connect to DB if not provided
        if conn is None:
            conn = get_db_connection()
            close_conn = True
        cursor = conn.cursor(dictionary=True)
 
        # Get all tables
        cursor.execute("SHOW TABLES;")
        db_tables = [list(r.values())[0] for r in cursor.fetchall()]
 
        for tbl in db_tables:
            table_entry: Dict[str, Any] = {}
 
            # --- Table Preview ---
            try:
                table_name = _safe_table_name(tbl)
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT %s;", (preview_limit,))
                rows = cursor.fetchall()
                columns = list(rows[0].keys()) if rows else [desc[0] for desc in cursor.description]
                serialized_rows = [[_serialize_value(r.get(c)) for c in columns] for r in rows]
                table_entry["preview"] = {
                    "columns": columns,
                    "rows": serialized_rows,
                    "row_count_preview": len(serialized_rows)
                }
            except Exception as e:
                table_entry["preview"] = {"error": str(e)}
 
            # --- Insert Stats ---
            csv_count = insert_info.get(tbl, {}).get("csv_rows")
            try:
                cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{table_name}`;")
                db_count = cursor.fetchone()["cnt"]
            except Exception:
                db_count = None
 
            if csv_count is None:
                inserted_summary = "CSV missing/unreadable"
            elif db_count is None:
                inserted_summary = "DB count error"
            else:
                pct = (db_count / csv_count * 100) if csv_count > 0 else (100.0 if db_count == 0 else 0.0)
                inserted_summary = f"{db_count}/{csv_count} ({pct:.1f}%)"
 
            table_entry["insert_stats"] = {
                "csv_rows": csv_count,
                "db_rows": db_count,
                "inserted_summary": inserted_summary
            }
 
            result[tbl] = table_entry
 
    finally:
        if cursor:
            cursor.close()
        if close_conn and conn:
            conn.close()
 
    return result
 
 
# Example usage
if __name__ == "__main__":
    import json
    data = fetch_tables_with_insert_stats("Test_Runner",runspace_base="../Run_Space", preview_limit=3)
    print(json.dumps(data, indent=2))
 