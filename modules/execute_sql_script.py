import os
import re
import logging
import traceback
import time

from dotenv import load_dotenv
load_dotenv()

# Use the get_db_connection from the shared db_utils module (attempted first)
from db_utils import get_db_connection

import mysql.connector
from mysql.connector import Error

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Helper: simple PostgreSQL -> MySQL translations (kept as you had) ---
def translate_postgres_to_mysql(sql_command: str) -> str:
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

# --- Helper: build DB_CONFIG from env or fallback to the working constants you provided ---
def build_db_config_from_env_or_defaults():
    # defaults (from the working snippet you provided)
    defaults = {
        'host': "metro.proxy.rlwy.net",
        'user': "root",
        'password': "NwrMwVHJoQkwvoEqYHNdupOzolbwBSDo",
        'database': "railway",
        'port': 16519,
    }

    # read env first (if present)
    host = os.getenv("DB_HOST", defaults['host'])
    user = os.getenv("DB_USER", defaults['user'])
    password = os.getenv("DB_PASS", defaults['password'])
    database = os.getenv("DB_NAME", defaults['database'])
    port_env = os.getenv("DB_PORT")
    try:
        port = int(port_env) if port_env else defaults['port']
    except ValueError:
        port = defaults['port']

    config = {
        'host': host,
        'user': user,
        'password': password,
        'database': database,
        'port': port,
        # small timeout to fail fast if not reachable
        'connection_timeout': 10,
    }
    # If your provider requires SSL and you set DB_SSL_CA in .env, include it
    ssl_ca = os.getenv("DB_SSL_CA")
    if ssl_ca:
        config['ssl_ca'] = ssl_ca
        config['ssl_verify_cert'] = True

    return config

# --- Core: execute SQL file with robust connection fallback ---
def execute_sql_from_file(filepath: str, max_retries: int = 1):
    if not os.path.exists(filepath):
        logging.error(f"SQL file not found at: {filepath}")
        return

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            sql_full_script = f.read()
            # remove single-line SQL comments beginning with --
            sql_full_script = re.sub(r'--.*', '', sql_full_script)
    except Exception as e:
        logging.error(f"Failed to read SQL file {filepath}: {e}")
        logging.error(traceback.format_exc())
        return

    sql_commands = [cmd.strip() for cmd in sql_full_script.split(';') if cmd.strip()]
    conn = None
    cursor = None

    # Attempt to obtain a connection: try get_db_connection(), else fallback to mysql.connector with DB_CONFIG
    last_exc = None
    for attempt in range(1, max_retries + 2):  # try get_db_connection once, then fallback attempts
        try:
            if attempt == 1:
                # try the shared helper first
                try:
                    logging.info("Attempting to connect using shared get_db_connection()")
                    conn = get_db_connection()
                    logging.info("Connected using get_db_connection()")
                except Exception as e:
                    logging.warning("get_db_connection() failed: %s", e)
                    # fallthrough to next attempt to try connector fallback
                    raise
            else:
                # fallback path: build DB_CONFIG and connect directly with mysql.connector
                db_config = build_db_config_from_env_or_defaults()
                logging.info("Attempting fallback mysql.connector.connect(**DB_CONFIG) -> %s:%s", db_config['host'], db_config['port'])
                conn = mysql.connector.connect(**db_config)
                if conn.is_connected():
                    logging.info("Connected using fallback mysql.connector.connect")
                else:
                    raise Error("Fallback connector returned but is_connected() is False")

            # If we have a connection, create a cursor and break loop
            cursor = conn.cursor()
            break

        except Exception as e:
            last_exc = e
            logging.error("Connection attempt %d failed: %s", attempt, e)
            logging.error(traceback.format_exc())
            # cleanup and retry/backoff if any further attempts left
            try:
                if cursor:
                    cursor.close()
            except Exception:
                pass
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

            if attempt < (max_retries + 1):
                sleep_for = 1 * attempt
                logging.info("Retrying connection after %s seconds...", sleep_for)
                time.sleep(sleep_for)
            else:
                logging.error("All connection attempts exhausted. Last error: %s", last_exc)
                return

    # At this point we should have conn and cursor, otherwise we've returned above
    try:
        logging.info(f"Successfully connected to the database. Found {len(sql_commands)} commands to execute.")
        for idx, command in enumerate(sql_commands, start=1):
            if not command.strip():
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
                logging.error("Error: %s", e)
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
    # 🔧 Hardcoded path for testing (keep as you had)
    sql_file_path = r"../Run_Space/Test_Runner/create_schema.sql"
    logging.info(f"Testing SQL execution from file: {sql_file_path}")
    execute_sql_from_file(sql_file_path)
