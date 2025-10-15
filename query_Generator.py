import json
import os
from dotenv import load_dotenv
import google as genai
from gemini_Call import api_call
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    raise RuntimeError("❌ No GEMINI_API_KEY found in .env file")
genai.configure(api_key=API_KEY)


def generate_supabase_script(metadata_file="Run_Space/metadata.json",
                             plantuml_file="Run_Space/plantuml_code.puml",
                             output_file="supabase_create_insert.py",
                             model="gemini-1.5-flash-latest"):
    with open(metadata_file, 'r') as file:
        refined_metadata = json.load(file)

    plantuml_code = ""
    if os.path.exists(plantuml_file):
        with open(plantuml_file, 'r') as file:
            plantuml_code = file.read()

    prompt = f"""You are a Python coding assistant and PostgreSQL/Supabase database expert.

Task:

1. Generate a complete Python script that connects to a PostgreSQL database (Supabase) using psycopg2.
2. Read CSV files for each table.
3. Create tables with PRIMARY KEY and FOREIGN KEY constraints. Use DEFERRABLE INITIALLY DEFERRED for all foreign keys.
4. Insert data into tables in correct dependency order based on foreign key relationships.
5. Include retry logic for OperationalError and deadlocks (exponential backoff: 0.1s, 0.2s, 0.4s; max 3 attempts).
6. Convert datetime columns to ISO 8601 format before insertion.
7. Use environment variables DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT.
8. Do not create any external SQL files; everything must execute in Python.
9. Print progress messages for table creation and data insertion.
10. Drop existing tables before creation with CASCADE.
11. Build a dependency graph from foreign keys and perform a topological sort to determine creation and insertion order.
12. Safely handle NULL values in CSVs.
13. Use placeholders (%s) for inserts and psycopg2.sql.Identifier for table/column names to avoid SQL injection.
14. Do not include comments in the generated code.

CSV handling:

* Read CSVs with pandas.
* Replace missing values with None.
* Convert datetime columns using: pd.to_datetime(..., errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S").

Python script requirements:

* Define execute_with_retry(conn, sql, params=None) that executes queries with retry logic.
* Define create_table_and_insert_data(conn, table_name, columns_meta, csv_file) that:

  * Creates table with PKs and FKs.
  * Inserts data from the CSV into the table.
* Main execution:

  * Load metadata.json.
  * Compute table creation/insertion order based on dependencies.
  * Connect to the database.
  * For each table in order, call create_table_and_insert_data.
  * Close the connection in a finally block.

Use this database connection pattern for secure, reliable connections, logging, and cleanup:
import os, sys, logging, psycopg2
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

if not all(os.getenv(v) for v in ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASS"]):
logging.error("Please set DB_HOST, DB_NAME, DB_USER, DB_PASS in your .env")
sys.exit(1)

Behavior summary:

* Load .env credentials (DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT).
* Establish connection via psycopg2.connect.
* Validate required environment variables.
* Log database name and schema for verification.
* Use try/except/finally to commit on success, roll back on failure, and always close the connection safely.

Here is the table metadata in JSON:
{json.dumps(refined_metadata, indent=2)}

Here is the ER diagram in PlantUML:
{plantuml_code}

Return only a Python script that is fully executable as a single file. Do not include explanations, comments, or markdown. Use only standard libraries, pandas, and psycopg2.
"""

    print("⏳ Generating Python script with Gemini (REST API)...")
    py_code = api_call(prompt, model=model)

    with open(output_file, "w") as f:
        f.write(py_code.strip())

    print(f"✅ Python script generated and saved as {output_file}")
    return output_file
