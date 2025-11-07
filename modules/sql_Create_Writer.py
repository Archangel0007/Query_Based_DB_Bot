import json
import os
import sys

# To handle running this script directly for testing, add the project root to the path.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from modules.api_Call import api_call

def generate_create_sql_writer_script(metadata_file, plantuml_file, output_file, model=None):
    """
    Generates a Python script that, when run, writes CREATE TABLE SQL statements to a .sql file.
    """
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
            }, ...
        ]
    }, ...
]"""

    example_code = """
import os

def write_create_tables_sql():
    # Determine table creation order based on dependencies
    # (Code for topological sort would be here)
    
    sql_statements = []
    
    # Add DROP statements first
    # table_order = [...]
    # for table_name in reversed(table_order):
    #     sql_statements.append(f"DROP TABLE IF EXISTS `{table_name}` CASCADE;")

    # Add CREATE statements
    # for table_name in table_order:
    #     create_statement = f'''CREATE TABLE `{table_name}` ( ... );'''
    #     sql_statements.append(create_statement)
    
    # Get the directory of the current script to save the output file next to it.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_filename = "create_schema.sql"
    output_path = os.path.join(script_dir, output_filename)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\\n".join(sql_statements))
    
    print(f"SQL create script written to {output_filename}")

if __name__ == '__main__':
    write_create_tables_sql()
"""

    prompt = f"""You are a Python coding assistant and MySQL database expert.

Task:

1.  Generate a complete Python script that defines SQL `CREATE TABLE` statements as strings.
2.  The script should read table definitions from the provided metadata JSON.
3.  The script must determine the correct table creation order by performing a topological sort based on foreign key dependencies. The create statements must respect these dependencies.
4.  The script should first generate `DROP TABLE IF EXISTS ... CASCADE;` statements for each table in reverse dependency order.
5.  Then, it should generate `CREATE TABLE ...;` statements for each table in the correct dependency order.
6.  All generated SQL statements should be combined and written into a single output file named `create_schema.sql`.
7.  The generated Python script should NOT connect to any database. Its only job is to write the `.sql` file.
8.  You have to create a table for each of the entities entites present in the ER Diagram. Do Not Miss any of them. 
9.   Check for all the Relations and the constraints that have to be implemented. make sure all of them are enforced while creating tables. 
10.  Ensure the generated Python script is perfect, runs without errors, and uses only standard Python libraries.
11.  The generated script should save its output .sql file in the same directory where the script itself is located. Do not include any comments in the generated Python code.
12.  The file you are writing into i.e create_schema.sql should be in the same directory as this file.

Here is the table metadata in JSON:
{json.dumps(refined_metadata, indent=2)}

Here is the ER diagram in PlantUML for context on relationships:
{plantuml_code}

Use the following Python code as a template for the script you need to generate:
{example_code}

Return only a Python script that is fully executable as a single file. Do not include explanations or markdown.
"""

    print("⏳ Generating Python script to write SQL file...")
    py_code = api_call(prompt, model=model)

    if "```python" in py_code:
        py_code = py_code.split("```python")[1].split("```")[0]

    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(output_file, "w") as f:
        f.write(py_code.strip())

    print(f"✅ Python script (for writing SQL) generated and saved to: {output_file}")

if __name__ == "__main__":

    print("Running standalone test for sql_Create_Writer using files in Run_Space/Test_Runner...")

    # Define paths for the test runner
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    test_runner_dir = os.path.join(project_root, "Run_Space", "Test_Runner")

    metadata_path = os.path.join(test_runner_dir, "metadata.json")
    puml_path = os.path.join(test_runner_dir, "relationship_schema.puml")
    output_script_path = os.path.join(test_runner_dir, "generated_sql_writer.py")

    # Ensure the required input files exist before running.
    if not os.path.exists(metadata_path):
        print(f"ERROR: Input file not found: {metadata_path}")
        sys.exit(1)
    if not os.path.exists(puml_path):
        print(f"ERROR: Input file not found: {puml_path}")
        sys.exit(1)

    # Run the script generation function
    try:
        generate_create_sql_writer_script(
            metadata_file=metadata_path,
            plantuml_file=puml_path,
            output_file=output_script_path  # Use the specified model for the test run
        )
        print(f"\nStandalone test complete. Check the generated script at: {output_script_path}")
    except Exception as e:
        print(f"\nAn error occurred during the standalone test: {e}")