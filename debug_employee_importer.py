import os
import re
import csv
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

def import_employee_data(values_filepath: str, output_csv_path: str):
    """
    Parses the employees INSERT statements from a SQL file and appends the data to a CSV file.
    """
    # 1. --- Check for input files ---
    if not os.path.exists(values_filepath):
        print(f"❌ ERROR: SQL values file not found at '{values_filepath}'")
        return
    if not os.path.exists(output_csv_path):
        print(f"❌ ERROR: Output CSV file not found at '{output_csv_path}'. Please ensure it exists with headers.")
        return

    print(f"🔍 Reading data values from: {values_filepath}")
    with open(values_filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 2. --- Find the specific 'employees' INSERT block ---
    # This regex is more robust to handle newlines and tabs before the VALUES keyword.
    insert_regex = re.compile(
        r"INSERT INTO (?:public\.)?\"?employees\"?\s+VALUES\s*(.*?);",
        re.DOTALL | re.IGNORECASE
    )
    match = insert_regex.search(content)

    if not match:
        print("❌ ERROR: Could not find the 'INSERT INTO public.employees' block in the SQL file.")
        return

    values_block = match.group(1)
    print("✅ Found the 'employees' data block.")

    # 3. --- Parse the rows from the block ---
    # This regex handles multi-line content within the parentheses.
    row_regex = re.compile(r"\((.*?)\)", re.DOTALL)
    rows_data = []

    for row_match in row_regex.finditer(values_block):
        row_str = row_match.group(1)
        
        # Use the csv module to correctly parse the row, respecting single quotes
        reader = csv.reader([row_str], quotechar="'", skipinitialspace=True)
        values = next(reader)
        
        # Clean up values: un-escape doubled single quotes
        cleaned_values = [val.replace("''", "'") for val in values]
        rows_data.append(cleaned_values)

    if not rows_data:
        print("⚠️ No data rows were parsed for the 'employees' table.")
        return

    print(f"  - Parsed {len(rows_data)} rows of employee data.")

    # 4. --- Append the data to the existing CSV file ---
    try:
        with open(output_csv_path, 'a', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(rows_data)
        
        print(f"✅ Successfully appended {len(rows_data)} rows to {output_csv_path}")
    except Exception as e:
        print(f"❌ ERROR: Failed to write to CSV file. Reason: {e}")

def main():
    """Main function to run the employee import."""
    print("🚀 Starting Employee Data Importer...")
    values_sql_file = "create_schema_values.sql"
    output_csv = os.path.join("Northwind_Dataset", "employees.csv")

    import_employee_data(values_sql_file, output_csv)
    print("🏁 Import process finished.")

if __name__ == "__main__":
    main()