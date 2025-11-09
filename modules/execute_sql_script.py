import os
import re
import logging
import traceback

# Use the get_db_connection from the shared db_utils module
from db_utils import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def translate_postgres_to_mysql(sql_command: str) -> str:
    """
    Performs simple translations of PostgreSQL data types to MySQL equivalents.
    """
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
    Improved logging, full tracebacks, and robust connection closing.
    """
    if not os.path.exists(filepath):
        logging.error(f"SQL file not found at: {filepath}")
        return

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            # Read the whole file and remove SQL single-line comments starting with --
            sql_full_script = f.read()
            sql_full_script = re.sub(r'--.*', '', sql_full_script)
    except Exception as e:
        logging.error(f"Failed to read SQL file {filepath}: {e}")
        logging.error(traceback.format_exc())
        return

    # Split script into individual statements based on semicolon
    sql_commands = [cmd.strip() for cmd in sql_full_script.split(';') if cmd.strip()]

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        logging.info(f"Successfully connected to the database. Found {len(sql_commands)} commands to execute.")
    except Exception as e:
        logging.error(f"Failed to obtain DB connection or cursor: {e}")
        logging.error(traceback.format_exc())
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return

    try:
        for idx, command in enumerate(sql_commands, start=1):
            if command.strip() == "":
                continue
            if command.upper().startswith('SET'):
                logging.info(f"Skipping command (SET): {command[:120]}...")
                continue

            exec_command = command
            if command.upper().lstrip().startswith('CREATE TABLE'):
                exec_command = translate_postgres_to_mysql(command)

            try:
                logging.info(f"[{idx}/{len(sql_commands)}] Executing: {exec_command[:200]}...")
                cursor.execute(exec_command)
            except Exception as e:
                logging.error(f"[{idx}/{len(sql_commands)}] Failed to execute command (first 300 chars): {exec_command[:300]}")
                logging.error(f"Error: {e}")
                logging.error(traceback.format_exc())

        try:
            conn.commit()
            logging.info("✅ All commands executed successfully and committed.")
        except Exception as e:
            logging.error(f"Commit failed: {e}")
            logging.error(traceback.format_exc())

    except Exception as e_outer:
        logging.error(f"An unexpected error occurred while executing SQL commands: {e_outer}")
        logging.error(traceback.format_exc())
    finally:
        try:
            if cursor:
                cursor.close()
                logging.info("DB cursor closed.")
        except Exception as e:
            logging.warning(f"Error closing cursor: {e}")
            logging.warning(traceback.format_exc())

        try:
            if conn:
                conn.close()
                logging.info("Database connection closed.")
        except Exception as e:
            logging.warning(f"Error closing connection: {e}")
            logging.warning(traceback.format_exc())


if __name__ == "__main__":
    # 🔧 Hardcoded path for testing
    sql_file_path = r"../Run_Space/Test_Runner/create_schema.sql"

    logging.info(f"Testing SQL execution from file: {sql_file_path}")
    execute_sql_from_file(sql_file_path)
