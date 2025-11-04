import os
import re
import csv
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

def parse_create_table_statements(filepath: str) -> dict:
    """
    Parses a SQL file with CREATE TABLE statements to extract table names and column orders.
    """
    if not os.path.exists(filepath):
        logging.error(f"Schema file not found: {filepath}")
        print(f"❌ ERROR: Schema file not found at '{filepath}'")
        return {}

    print(f"🔍 Reading schema definitions from: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    schemas = {}
    # Regex to find CREATE TABLE blocks and capture table name and columns
    # Make the 'public.' schema prefix optional to handle more dump formats
    # Allow underscores in table names
    create_table_regex = re.compile(r"CREATE TABLE (?:public\.)?\"?([\w_]+)\"?\s*\((.*?)\);", re.DOTALL | re.IGNORECASE)
    # Only match lines that start with a column name followed by a data type, ignoring constraints.
    column_regex = re.compile(r"^\s*(\w+)\s+(?:character varying|bpchar|smallint|integer|text|bytea|date|real|numeric|decimal)", re.MULTILINE | re.IGNORECASE)

    print("Parsing for CREATE TABLE statements...")
    for match in create_table_regex.finditer(content):
        table_name = match.group(1)
        columns_sql = match.group(2)
        
        # Find all column names from the definition
        columns = [col.strip() for col in column_regex.findall(columns_sql)]
        schemas[table_name] = columns
        print(f"  - Found schema for table '{table_name}' with columns: {columns}")

    return schemas

def create_empty_csv_for_all_tables(schemas: dict, output_dir: str):
    """
    Ensures a CSV file with headers is created for every table defined in the schema,
    even if there is no data to insert.
    """
    print("📝 Ensuring CSV files exist for all tables...")
    os.makedirs(output_dir, exist_ok=True)
    for table_name, headers in schemas.items():
        csv_filepath = os.path.join(output_dir, f"{table_name}.csv")
        if not os.path.exists(csv_filepath):
            with open(csv_filepath, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(headers)
            print(f"  - Created empty CSV with headers for '{table_name}'.")

def parse_and_write_inserts(values_filepath: str, schemas: dict, output_dir: str):
    """
    Parses a SQL file with INSERT statements and writes the data to CSV files.
    """
    if not os.path.exists(values_filepath):
        logging.error(f"Values file not found: {values_filepath}")
        print(f"❌ ERROR: Values file not found at '{values_filepath}'")
        return

    os.makedirs(output_dir, exist_ok=True)
    print(f"✅ Output directory '{output_dir}' is ready.")

    print(f"🔍 Reading data values from: {values_filepath}")
    with open(values_filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Regex to find all INSERT INTO blocks
    # Make the 'public.' schema prefix optional
    # Allow underscores in table names
    insert_regex = re.compile(r"INSERT INTO (?:public\.)?\"?([\w_]+)\"?\s*VALUES\s*(.*?);", re.DOTALL | re.IGNORECASE)

    for match in insert_regex.finditer(content):
        table_name = match.group(1)
        values_block = match.group(2)

        if table_name not in schemas:
            print(f"  - ⚠️  Skipping table '{table_name}' as no schema was found for it.")
            continue

        headers = schemas[table_name]
        csv_filepath = os.path.join(output_dir, f"{table_name}.csv")

        # Regex to find individual value tuples like (...)
        row_regex = re.compile(r"\((.*?)\)", re.DOTALL)
        rows_data = []

        for row_match in row_regex.finditer(values_block):
            row_str = row_match.group(1)
            # Use a more robust split for comma-separated values, handling quotes
            # Use the csv module to correctly parse the row, respecting quotes.
            # We wrap the string in a list to make it an iterable for the csv reader.
            reader = csv.reader([row_str], quotechar="'", skipinitialspace=True)
            values = next(reader)

            
            # Clean up values: remove surrounding quotes and handle special cases
            cleaned_values = []
            for val in values:
                if val.startswith("'") and val.endswith("'"):
                    # Remove quotes and un-escape doubled single quotes
                    cleaned_values.append(val[1:-1].replace("''", "'"))
                elif val.upper() == 'NULL':
                    cleaned_values.append('') # Represent NULL as an empty string in CSV
                elif val.lower() == r"'\x'":
                    cleaned_values.append('') # Represent hex bytea as empty
                else:
                    cleaned_values.append(val)
            
            if len(cleaned_values) == len(headers):
                rows_data.append(cleaned_values)
            else:
                print(f"    - ⚠️  Row in '{table_name}' has mismatched column count. Expected {len(headers)}, got {len(cleaned_values)}. Row: {row_str}")

        if not rows_data:
            print(f"  - No data found or parsed for table '{table_name}'.")
            continue

        # Write the collected data to a CSV file
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(headers)
            writer.writerows(rows_data)
        
        print(f"  - ✅ Successfully wrote {len(rows_data)} rows to {csv_filepath}")

def main():
    """Main function to orchestrate the SQL to CSV conversion."""
    print("🚀 Starting SQL to CSV conversion process...")
    tables_sql_file = "create_schema_tables.sql"
    values_sql_file = "create_schema_values.sql"
    output_folder = "Northwind_Dataset"

    table_schemas = parse_create_table_statements(tables_sql_file)
    if table_schemas:
        print(f"\nFound {len(table_schemas)} table schemas. Proceeding to process data...")
        # First, create empty files for all tables to ensure they all exist
        create_empty_csv_for_all_tables(table_schemas, output_folder)
        # Now, parse and populate the ones that have data
        parse_and_write_inserts(values_sql_file, table_schemas, output_folder)
    else:
        print("No table schemas found. Cannot process insert values. Exiting.")
    print("🏁 Conversion process finished.")

if __name__ == "__main__":
    main()