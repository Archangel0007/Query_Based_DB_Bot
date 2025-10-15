import os
import sys
import json
import logging
import psycopg2
from query import METADATA_JSON, generate_create_table_sql, topological_sort
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT", "5432")
    )
    return conn

def main():
    if not all(os.getenv(var) for var in ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASS"]):
        logging.error("Please set DB_HOST, DB_NAME, DB_USER, DB_PASS in your .env")
        sys.exit(1)

    metadata = json.loads(METADATA_JSON)

    try:
        sorted_tables = topological_sort(metadata)
        logging.info(f"Table creation order: {', '.join(sorted_tables)}")
    except Exception as e:
        logging.error(f"Failed to sort tables: {e}")
        sys.exit(1)

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Log database and current schema
        with conn.cursor() as temp_cur:
            temp_cur.execute("SELECT current_database();")
            db_name = temp_cur.fetchone()[0]
            temp_cur.execute("SELECT current_schema();")
            schema_name = temp_cur.fetchone()[0]
        logging.info(f"Connected to database: {db_name}, schema: {schema_name}")

        # Drop tables first (reverse order to handle dependencies)
        logging.info("--- Dropping existing tables ---")
        for table_name in reversed(sorted_tables):
            logging.info(f"Dropping table {table_name}...")
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')

        # Create tables
        logging.info("--- Creating tables ---")
        for table_name in sorted_tables:
            sql = generate_create_table_sql(table_name, metadata[table_name])
            cur.execute(sql)
            logging.info(f'Table "{table_name}" created in schema "{schema_name}".')

        conn.commit()

        # Verify tables
        logging.info("--- Verifying created tables ---")
        cur.execute(
            "SELECT table_name, table_schema FROM information_schema.tables WHERE table_schema='public';"
        )
        tables = cur.fetchall()
        for t_name, t_schema in tables:
            logging.info(f"Found table: {t_name}, schema: {t_schema}")

        logging.info("Tables created successfully!")

    except Exception as e:
        logging.error(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
