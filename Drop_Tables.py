import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DB_HOST="metro.proxy.rlwy.net"
DB_USER="root"
DB_PASS="NwrMwVHJoQkwvoEqYHNdupOzolbwBSDo"
DB_NAME="railway"
DB_PORT=16519
DB_CONFIG = {
    'host': DB_HOST,
    'user': DB_USER,
    'password': DB_PASS,
    'database': DB_NAME,
    'port': 16519,
}

def drop_all_tables():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Disable foreign key checks to avoid constraint issues during drop
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

        # Fetch all table names
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()

        if not tables:
            print("No tables found to drop.")
            return 0

        for (table_name,) in tables:
            cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
            print(f"Dropped table: {table_name}")

        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        conn.commit()
        print("✅ All tables dropped successfully.")
        return 0

    except Error as e:
        print("❌ MySQL error:", e)
        return 2

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    raise SystemExit(drop_all_tables())
