from db_utils import get_db_connection
from mysql.connector import Error
import re, datetime, decimal

SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")

def _serialize_value(v):
    """Convert DB values (datetime, Decimal, etc.) into JSON-safe types."""
    if v is None:
        return None
    if isinstance(v, (int, float, str, bool)):
        return v
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    return str(v)

def _safe_table_name(name):
    """Ensure table name is alphanumeric or underscore (SQL injection prevention)."""
    if not SAFE_NAME_RE.match(name):
        raise ValueError(f"Unsafe table name: {name!r}")
    return name

def fetch_tables_preview(limit=5, conn=None):
    """
    Fetch top `limit` rows from all tables.
    If `conn` is provided, uses it instead of opening a new one.
    """
    close_after = False
    result = []

    try:
        # Use provided connection or open a new one
        if conn is None:
            conn = get_db_connection()
            close_after = True

        cursor = conn.cursor(dictionary=True)
        cursor.execute("SHOW TABLES;")
        tables = [list(t.values())[0] for t in cursor.fetchall()]

        for tbl in tables:
            try:
                table_name = _safe_table_name(tbl)
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT %s;", (limit,))
                rows = cursor.fetchall()
                columns = list(rows[0].keys()) if rows else [desc[0] for desc in cursor.description]
                serialized_rows = [[_serialize_value(r.get(c)) for c in columns] for r in rows]

                result.append({
                    "table": table_name,
                    "columns": columns,
                    "rows": serialized_rows,
                    "row_count_preview": len(serialized_rows)
                })

            except ValueError as ve:
                result.append({"table": tbl, "error": f"Skipped unsafe table name: {ve}"})
            except Error as e:
                result.append({"table": tbl, "error": f"MySQL error: {e}"})
            except Exception as e:
                result.append({"table": tbl, "error": f"Unexpected error: {e}"})

    except Error as e:
        result.append({"error": f"Database error: {e}"})
    except Exception as e:
        result.append({"error": f"Unexpected error: {e}"})
    finally:
        if cursor:
            cursor.close()
        if close_after and conn:
            conn.close()

    return result



if __name__ == "__main__":
    print(fetch_tables_preview())