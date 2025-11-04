import os
import sys
import re
import logging

# Add the project root to the path to allow importing modules.
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Use the get_db_connection from the shared db_utils module
from db_utils import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def translate_postgres_to_mysql(sql_command: str) -> str:
    """
    Performs simple translations of PostgreSQL data types to MySQL equivalents.
    """
    # This is not an exhaustive list, but covers common cases from your SQL file.
    translations = {
        r'\bbpchar\b': 'CHAR',
        r'\bcharacter varying\b': 'VARCHAR',
        r'\bbytea\b': 'BLOB',
        r'\breal\b': 'FLOAT',
        r'\bsmallint\b': 'SMALLINT',
        r'\binteger\b': 'INT'
    }
    for pg_type, mysql_type in translations.items():
        sql_command = re.sub(pg_type, mysql_type, sql_command, flags=re.IGNORECASE)
    return sql_command

def execute_sql_from_file(filepath: str):
    """
    Reads an SQL file, splits it into commands, and executes them against the database.
    """
    if not os.path.exists(filepath):
        logging.error(f"SQL file not found at: {filepath}")
        return

    with open(filepath, 'r', encoding='utf-8') as f:
        # Read the whole file and remove comments
        sql_full_script = f.read()
        sql_full_script = re.sub(r'--.*', '', sql_full_script)

    # Split script into individual statements based on the semicolon
    sql_commands = [cmd.strip() for cmd in sql_full_script.split(';') if cmd.strip()]

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        logging.info(f"Successfully connected to the database. Found {len(sql_commands)} commands to execute.")

        for command in sql_commands:
            # Skip PostgreSQL-specific SET commands
            if command.upper().startswith('SET'):
                logging.info(f"Skipping command: {command[:30]}...")
                continue

            # Translate data types for CREATE TABLE statements
            if command.upper().startswith('CREATE TABLE'):
                command = translate_postgres_to_mysql(command)

            try:
                logging.info(f"Executing: {command[:80]}...")
                cursor.execute(command)
            except Exception as e:
                logging.error(f"Failed to execute command: {command[:80]}...")
                logging.error(f"Error: {e}")
                # Decide if you want to stop on error or continue
                # raise  # Uncomment to stop on the first error

        conn.commit()
        logging.info("✅ All commands executed successfully and committed.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    sql_file_path = "create_schema.sql"
    execute_sql_from_file(sql_file_path)
