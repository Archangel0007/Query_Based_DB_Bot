from db_utils import get_db_connection
import mysql.connector
from mysql.connector import Error

def show_all_first_rows():
    try:
        conn = get_db_connection()
        # Use dictionary cursor if available to print values nicely; otherwise fall back to regular cursor.
        try:
            cursor = conn.cursor(dictionary=True)
            use_dict_cursor = True
        except Exception:
            cursor = conn.cursor()
            use_dict_cursor = False

        # Fetch all table names
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()

        if not tables:
            print("No tables found.")
            return 0

        for tbl in tables:
            # `SHOW TABLES` returns rows like ('table_name',) or dict depending on connector version.
            table_name = tbl[0] if not isinstance(tbl, dict) else list(tbl.values())[0]
            print(f"\n📘 Table: `{table_name}`")

            try:
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 1;")
                row = cursor.fetchone()

                if row is None:
                    print("(empty table)")
                    continue

                # If using dictionary cursor, keys are column names
                if use_dict_cursor and isinstance(row, dict):
                    col_names = list(row.keys())
                    values = [row[col] for col in col_names]
                else:
                    col_names = [desc[0] for desc in cursor.description]
                    values = list(row)

                # Pretty print header and row
                header = " | ".join(col_names)
                print(header)
                print("-" * max(len(header), 10))
                print(" | ".join(str(v) for v in values))

            except Error as e:
                # MySQL-specific error (permissions, broken table, etc.)
                print(f"❌ Error fetching from `{table_name}`: {e}")

        return 0

    except Error as e:
        print("❌ Database error:", e)
        return 2

    except Exception as e:
        print("❌ Unexpected error:", e)
        return 3

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    show_all_first_rows()
