import json
import os
from gemini_Call import api_call
def generate_create_script(metadata_file="Run_Space/metadata.json",
                             plantuml_file="Run_Space/plantuml_code.puml",
                             output_file="Run_Space/create_Database_Script.py",
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
    prompt = f"""You are a Python coding assistant and MySQL database expert.

Task:

1. Generate a complete Python script that connects to a MySQL database using mysql.connector.
2. Read metadata from a JSON file that defines table names, columns, data types, primary keys, and foreign keys.
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
from db_utils import get_db_connection

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

    with open(output_file, "w") as f:
        f.write(py_code.strip())
    print(f"✅ Python script generated and saved to: {output_file}")
    
if __name__ == "__main__":
    generate_create_script("Run_Space/metadata.json", "Run_Space/relationship_schema.puml", "Run_Space/create_Database_Script.py", model="gemini-2.5-flash")
