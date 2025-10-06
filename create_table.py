import sqlite3
import os

# Path to your SQLite database file
DB_FILE = os.getenv("DB_FILE", "DataBase.db")

# SQL to create the table
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    price REAL NOT NULL DEFAULT 0.00,
    qty INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def create_table():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()
        print("Table created or already exists.")
        return 0
    except sqlite3.Error as err:
        print("SQLite error:", err)
        return 2
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    raise SystemExit(create_table())
