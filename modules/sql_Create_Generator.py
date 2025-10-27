import json
import os
from .gemini_Call import api_call
from dotenv import load_dotenv
import mysql.connector
import logging

def get_db_connection():
    """
    Establishes and returns a database connection.
    """
    load_dotenv(dotenv_path='../.env')  
    DB_HOST="metro.proxy.rlwy.net"
    DB_USER="root"
    DB_PASS="NwrMwVHJoQkwvoEqYHNdupOzolbwBSDo"
    DB_NAME="railway"
    DB_PORT=16519
    print("DB_HOST:", os.getenv("DB_HOST"))
    print("DB_USER:", os.getenv("DB_USER"))
    print("DB_PASS:", os.getenv("DB_PASS"))
    print("DB_NAME:", os.getenv("DB_NAME"))
    print("DB_PORT:", os.getenv("DB_PORT"))

    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT  # Ensure DB_PORT is an integer
        )
        if conn.is_connected():
            logging.info("Successfully connected to the database.")
            return conn
        else:
            raise Exception("Connection failed")
    except Exception as e:
        logging.error(f"Error while connecting to MySQL: {e}")
        raise


def generate_create_script(metadata_file, plantuml_file, output_file, model="gemini-2.5-flash"):
    with open(metadata_file, 'r') as file:
        refined_metadata = json.load(file)

    plantuml_code = ""
    if os.path.exists(plantuml_file):
        with open(plantuml_file, 'r') as file:
            plantuml_code = file.read()
    example_metadata = """[
    {
        "file_name": "orders",
        "directory_path": "orders.csv",
        "columns": [
            {
                "column_name": "order_id",
                "data_type": "number",
                "has_duplicates": false
            },"""
    prompt = f"""You are a Python coding assistant and MySQL database expert.

Task:

1. Generate a complete Python script that connects to a MySQL database using mysql.connector.
2. Read metadata from a JSON file that defines table names, columns, data types, primary keys, foreign keys and whether a column has null values or not.
3. Create all tables defined in the metadata with appropriate PRIMARY KEY and FOREIGN KEY constraints.
4. Use DEFERRABLE INITIALLY DEFERRED for all foreign keys (if supported syntactically).
5. Use environment variables DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT.
6. Drop existing tables before creation with CASCADE.
7. Build a dependency graph from foreign keys and perform a topological sort to determine table creation order.
8. Print progress messages for each table being created.
9. Generate a perfect script that runs without any syntax errors.
10. Ensure datatype handling is done properly based on metadata provided.
11. Make sure the Create Table statements are simple and execute cleanly.
12. Do not include comments in the generated code.
13. The metadata JSON is in this format:
{example_metadata}

Python script requirements:

* Load metadata.json.
* Compute table creation order based on dependencies.
* Connect to the database.
* For each table in order, execute its CREATE TABLE SQL.
* Close the connection in a finally block.

Use the following pre-existing function from db_utils:
from db_utils import get_db_connection
conn = get_db_connection()
try:
    # Your DB operations here
finally:
    conn.close()

Here is the table metadata in JSON:
{json.dumps(refined_metadata, indent=2)}

Here is the ER diagram in PlantUML:
{plantuml_code}

Your finale code should be a complete Python script in the format as specified below:
# Example format
import mysql.connector

def create_tables():
    statements = [
        "CREATE TABLE IF NOT EXISTS ...",
        "CREATE TABLE IF NOT EXISTS ..."
    ]
    conn = get_db_connection()
    cur = conn.cursor()
    for sql in statements:
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    create_tables()


Return only a Python script that is fully executable as a single file. Do not include explanations, comments, or markdown. Use only standard libraries and mysql.connector.
"""

    print("⏳ Generating Python script with Gemini...")
    py_code = api_call(prompt, model=model)
    if("```python" in py_code):
        py_code = py_code.split("```python")[1].split("```")[0]
    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(output_file, "w") as f:
        f.write(py_code.strip())
    print(f"✅ Python script generated and saved to: {output_file}")
    
if __name__ == "__main__":
    print("This module provides generate_create_script(metadata_file, plantuml_file, output_file, model). Call from app with explicit paths.")
