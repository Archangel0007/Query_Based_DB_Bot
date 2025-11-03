import json
import os
from .gemini_Call import api_call

def generate_insert_script(metadata_file,
                             plantuml_file,
                             output_file,
                             model="gemini-2.5-flash"):
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

    example_code = """
import mysql.connector
import pandas as pd
from db_utils import get_db_connection

def insert_data_from_csv(table_name, csv_path):
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)

    conn = get_db_connection()
    cursor = conn.cursor()

    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    data = df.values.tolist()
    for row in data:
        cursor.execute(insert_query, row)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    insert_data_from_csv("Station", "station.csv")
"""

    prompt = f"""You are a Python coding assistant and MySQL database expert.

Task:

1. Generate a complete Python script that connects to a MySQL database using mysql.connector.
2. Read CSV files for each table.
3. Insert data into tables in correct dependency order based on foreign key relationships.
4. Include retry logic for OperationalError and deadlocks (exponential backoff: 0.1s, 0.2s, 0.4s; max 3 attempts).
5. Convert datetime columns to ISO 8601 format before insertion.
6. Use environment variables DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT.
7. Do not create any external SQL files; everything must execute in Python.
8. Print progress messages for data insertion.
9. Build a dependency graph from foreign keys and perform a topological sort to determine insertion order.
10. Safely handle NULL values in CSVs.
11. Do not include comments in the generated code.
12. Generate a perfect script that runs without any syntax errors.
13. Ensure datatype handling is done properly as per the metadata provided. Even if the datatype is string but the values are numbers, handle them as numbers. If there is a mistake, then skip that one row and continue with the rest.
14. Make sure the insert statements are perfect and run without any errors.
15. Make sure you are reading the CSV and JSON files correctly. The format of the metadata JSON is as follows:
{example_metadata}
16. If circular dependencies exist between tables (bi-directional or mutual foreign keys), handle them gracefully by temporarily disabling foreign key checks during insertion:
   SET FOREIGN_KEY_CHECKS = 0 before inserts and SET FOREIGN_KEY_CHECKS = 1 after all inserts.
   Do not throw or raise an error for circular dependencies.
17. Ensure the generated code always completes successfully, even when some dependencies are circular, by inserting data in available order and deferring constraint enforcement.

CSV handling:

* Read CSVs with pandas.
* Replace missing values with None.
* Convert datetime columns using: pd.to_datetime(..., errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S").

Python script requirements:

* Define execute_with_retry(conn, sql, params=None) that executes queries with retry logic.
* Define create_table_and_insert_data(conn, table_name, columns_meta, csv_file) that:
  * Inserts data from the CSV into the table.

Main execution:

* Load metadata.json.
* Compute table insertion order based on dependencies.
* Connect to the database.
* For each table in order, call create_table_and_insert_data.
* Close the connection in a finally block.

Use the following pre-existing functions from db_utils to connect to the database:
from db_utils import get_db_connection

Do not declare the above functions again in the generated script.

Here is the table metadata in JSON:
{json.dumps(refined_metadata, indent=2)}

Here is the ER diagram in PlantUML:
{plantuml_code}

Use the following Python code as a format for inserting data from a CSV into a MySQL table:
{example_code}

Return only a Python script that is fully executable as a single file. Do not include explanations, comments, or markdown. Use only standard libraries, pandas, and mysql.connector.
"""

    print("⏳ Generating Python script with Gemini...")
    py_code = api_call(prompt)
    # ensure output directory exists
    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(output_file, "w") as f:
        f.write(py_code.strip())
    print(f"✅ Python script generated and saved to: {output_file}")
    
if __name__ == "__main__":
    print("This module exposes generate_insert_script(metadata_file, plantuml_file, output_file, model) which requires explicit paths.\nCall it from your orchestration code (for example, from flask_app) and do not rely on hard-coded defaults.")
