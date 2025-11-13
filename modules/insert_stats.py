# modules/insert_stats.py
import os
import csv
import re
from typing import Optional, List, Dict, Any
from mysql.connector import Error
from db_utils import get_db_connection

SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")

import os
from db_utils import get_db_connection

def get_insert_counts(task_id, runspace_base="Run_Space"):
    """
    Returns a dict keyed by table name:
      { "table_name": {"rows_in_db": int|None, "rows_in_csv": int|None } }

    rows_in_csv is None if CSV missing / unreadable.
    rows_in_db is None if table missing or count query failed.
    """
    out = {}
    folder = os.path.join(runspace_base, task_id)
    if not os.path.isdir(folder):
        return {"error": f"Run folder not found: {folder}"}

    # build list of csv files -> table names
    csv_files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]

    # first compute csv counts (fast)
    for csv_file in csv_files:
        table = os.path.splitext(csv_file)[0]
        csv_path = os.path.join(folder, csv_file)
        try:
            # count lines, subtract header if present
            with open(csv_path, "r", encoding="utf-8", errors="ignore") as fh:
                # read first line to check header then simple linecount
                lines = sum(1 for _ in fh)
            csv_count = max(0, lines - 1)  # assume at least one header row; safe fallback
        except Exception:
            csv_count = None
        out[table] = {"rows_in_csv": csv_count, "rows_in_db": None}

    # next, query DB for existing tables listed above
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # fetch the list of tables present in DB to avoid useless COUNT queries
        cursor.execute("SHOW TABLES;")
        db_tables = {list(t.values())[0] if isinstance(t, dict) else t[0] for t in (cursor.fetchall() or [])}

        for table in list(out.keys()):
            if table not in db_tables:
                out[table]["rows_in_db"] = 0  # treat missing table as 0 rows (or choose None)
                continue
            try:
                # protect table name - use backticks; if your connector supports params for identifiers, use that
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`;")
                cnt = cursor.fetchone()
                out[table]["rows_in_db"] = int(cnt[0]) if cnt and cnt[0] is not None else 0
            except Exception:
                out[table]["rows_in_db"] = None

    except Exception as e:
        # If DB connection fails, keep rows_in_db as None
        for k in out:
            out[k]["rows_in_db"] = None
        # optionally return an error key too
        out["_error"] = str(e)
    finally:
        try:
            if cursor: cursor.close()
            if conn: conn.close()
        except Exception:
            pass

    return out


def _safe_table_name(name: str) -> str:
    if not SAFE_NAME_RE.match(name):
        raise ValueError(f"Unsafe table name: {name!r}")
    return name

def _count_csv_rows(path: str, has_header: bool = True) -> int:
    """
    Efficiently count non-empty CSV rows. If has_header=True, the first non-empty
    row is assumed header and excluded from the count.
    """
    count = 0
    try:
        with open(path, "r", encoding="utf-8", newline='') as fh:
            reader = csv.reader(fh)
            if has_header:
                # skip until we find the first non-empty row to be header
                for row in reader:
                    if any(cell.strip() for cell in row):
                        break
                # now count the rest
            # count remaining non-empty rows
            for row in reader:
                if any(cell.strip() for cell in row):
                    count += 1
    except FileNotFoundError:
        raise
    return count

def get_insert_counts(task_id: str,
                      runspace_base: str = "Run_Space",
                      conn: Optional[Any] = None,
                      has_header: bool = True) -> List[Dict[str, Any]]:
    """
    For the given task_id, returns list of per-file/table counts:
      [
        {
          "file": "orders.csv",
          "table": "orders",
          "csv_rows": 830,         # integer or None if file missing
          "db_rows": 830,          # integer or None if table missing/error
          "inserted_summary": "830/830 (100%)"  # helpful summary
        }, ...
      ]

    If `conn` is provided, it uses that DB connection; otherwise it will open
    and close its own connection.
    """
    results = []
    task_dir = os.path.join(runspace_base, task_id)
    close_after = False
    cursor = None

    # gather CSV files in runspace folder
    try:
        files = []
        if os.path.isdir(task_dir):
            for name in os.listdir(task_dir):
                # consider only .csv files (case-insensitive)
                if name.lower().endswith(".csv"):
                    files.append(name)
        else:
            # task dir missing
            return [{"error": f"Run space directory not found: {task_dir}"}]

        # open DB connection if not given
        if conn is None:
            conn = get_db_connection()
            close_after = True

        cursor = conn.cursor()
        # fetch existing tables from DB so we don't query COUNT for missing ones repeatedly
        try:
            cursor.execute("SHOW TABLES;")
            rows = cursor.fetchall()
            db_tables = set()
            for r in rows:
                # mysql connector returns tuples like ('orders',)
                if isinstance(r, (list, tuple)) and len(r) > 0:
                    db_tables.add(str(r[0]))
                else:
                    db_tables.add(str(r))
        except Exception:
            db_tables = set()

        for fname in files:
            file_path = os.path.join(task_dir, fname)
            table_name = os.path.splitext(fname)[0]  # strip .csv
            entry = {"file": fname, "table": table_name, "csv_rows": None, "db_rows": None, "inserted_summary": None}

            # CSV rows
            try:
                csv_count = _count_csv_rows(file_path, has_header=has_header)
                entry["csv_rows"] = csv_count
            except FileNotFoundError:
                entry["csv_rows"] = None
                entry["inserted_summary"] = "CSV file missing"
                results.append(entry)
                continue
            except Exception as e:
                entry["csv_rows"] = None
                entry["inserted_summary"] = f"CSV read error: {e}"
                results.append(entry)
                continue

            # DB rows
            if table_name in db_tables:
                try:
                    safe_name = _safe_table_name(table_name)
                    # cannot parametrize identifiers; we've validated the name
                    cursor.execute(f"SELECT COUNT(*) FROM `{safe_name}`;")
                    cnt = cursor.fetchone()
                    # fetchone may return (count,) or similar
                    db_count = int(cnt[0]) if cnt and len(cnt) > 0 else 0
                    entry["db_rows"] = db_count
                except Error as e:
                    entry["db_rows"] = None
                    entry["inserted_summary"] = f"DB error: {e}"
                    results.append(entry)
                    continue
                except Exception as e:
                    entry["db_rows"] = None
                    entry["inserted_summary"] = f"DB error: {e}"
                    results.append(entry)
                    continue
            else:
                # table not found in DB
                entry["db_rows"] = None
                entry["inserted_summary"] = "Table not found in DB"
                results.append(entry)
                continue

            # Build inserted summary
            if entry["db_rows"] is not None and entry["csv_rows"] is not None:
                db_rows = entry["db_rows"]
                csv_rows = entry["csv_rows"]
                percent = (db_rows / csv_rows * 100) if csv_rows > 0 else (100.0 if db_rows == 0 else 0.0)
                entry["inserted_summary"] = f"{db_rows}/{csv_rows} ({percent:.1f}%)"
            results.append(entry)

    except Exception as e:
        return [{"error": f"Unexpected error: {e}"}]
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if close_after and conn:
                conn.close()
        except Exception:
            pass

    return results
