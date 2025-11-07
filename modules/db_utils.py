import os
import time
import logging
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv(dotenv_path='../.env')
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Check if environment variables are loaded correctly
from dotenv import load_dotenv
import os
import mysql.connector
import logging

def get_db_connection():
    """
    Establishes and returns a database connection.
    """
    load_dotenv(dotenv_path='../.env')  # Load the environment variables from the .env file
    
    # Log the values to check if they are loaded
    print("DB_HOST:", os.getenv("DB_HOST"))
    print("DB_USER:", os.getenv("DB_USER"))
    print("DB_PASS:", os.getenv("DB_PASS"))
    print("DB_NAME:", os.getenv("DB_NAME"))
    print("DB_PORT:", os.getenv("DB_PORT"))

    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=int(os.getenv("DB_PORT", 3306))  # Ensure DB_PORT is an integer
        )
        if conn.is_connected():
            logging.info("Successfully connected to the database.")
            return conn
        else:
            raise Exception("Connection failed")
    except Exception as e:
        logging.error(f"Error while connecting to MySQL: {e}")
        raise

def execute_with_retry(conn, sql_query, params=None, retries=3, initial_delay=0.1):
    """
    Executes a given SQL query with retry mechanism on failure.
    Retries the execution in case of errors like deadlocks or connection issues.
    """
    load_dotenv(dotenv_path='../.env')
    delay = initial_delay
    for i in range(retries):
        try:
            with conn.cursor() as cur:
                cur.execute(sql_query, params)
                conn.commit()  # Make sure changes are committed
            return
        except (Error) as e:
            conn.rollback()
            logging.warning(f"Attempt {i+1}/{retries} failed due to {e}. Retrying in {delay:.1f}s...")
            time.sleep(delay)
            delay *= 2
        except Exception as e:
            conn.rollback()
            logging.error(f"Error executing query: {e}")
            raise
    raise Exception(f"Failed to execute query after {retries} attempts.")


def create_and_populate_table(conn, table_name, table_schema, data_to_insert=None, returning_col=None):
    """
    Creates and populates a table with data.
    Drops the table if it exists, creates a new one, and inserts the provided data.
    """
    load_dotenv(dotenv_path='../.env')
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    execute_with_retry(conn, drop_table_sql)
    logging.info(f"Dropped table {table_name}.")

    create_table_sql = table_schema["ddl"]
    execute_with_retry(conn, create_table_sql)
    logging.info(f"Created table {table_name}.")

    if data_to_insert:
        columns = table_schema["columns"]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        
        if returning_col:
            # MySQL doesn't support RETURNING like PostgreSQL; handle this differently.
            insert_sql += f" RETURNING {returning_col}"

        batch_size = 1000
        total_inserted = 0
        returned_ids = []

        with conn.cursor() as cur:
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                if not batch:
                    continue

                cur.executemany(insert_sql, batch)
                if returning_col:
                    cur.execute(insert_sql, batch)
                    returned_ids.extend([row[0] for row in cur.fetchall()])
                else:
                    conn.commit()  # Commit the batch insertion

                total_inserted += len(batch)
                logging.info(f"Inserted {total_inserted}/{len(data_to_insert)} rows into {table_name}.")
        
        return returned_ids
    return []

