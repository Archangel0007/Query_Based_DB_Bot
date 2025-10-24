import os
import pandas as pd
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def convert_json_to_csv(json_file_path: str) -> str:
    """
    Converts a single JSON file to a CSV file.
    The JSON is flattened to handle nested structures.
    The original JSON file is deleted after successful conversion.

    Args:
        json_file_path (str): The absolute path to the JSON file.

    Returns:
        str: The path to the newly created CSV file.
    """
    try:
        # Read and flatten the JSON data using pandas' json_normalize
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        df = pd.json_normalize(data)

        # Create the new CSV file path with the same base name
        base_name = os.path.splitext(json_file_path)[0]
        csv_file_path = base_name + ".csv"

        # Save the DataFrame to a CSV file
        df.to_csv(csv_file_path, index=False, encoding='utf-8')
        logging.info(f"Successfully converted '{os.path.basename(json_file_path)}' to '{os.path.basename(csv_file_path)}'.")

        # Remove the original JSON file
        os.remove(json_file_path)
        logging.info(f"Removed original JSON file: '{os.path.basename(json_file_path)}'.")

        return csv_file_path
    except Exception as e:
        logging.error(f"Failed to convert JSON file '{json_file_path}': {e}")
        raise

def convert_excel_to_csv(excel_file_path: str) -> str:
    """
    Converts a single Excel file (.xls/.xlsx) to a CSV file.
    The original Excel file is deleted after successful conversion.

    Returns the path to the created CSV file.
    """
    try:
        # Read Excel (first sheet)
        df = pd.read_excel(excel_file_path, sheet_name=0)

        base_name = os.path.splitext(excel_file_path)[0]
        csv_file_path = base_name + ".csv"

        df.to_csv(csv_file_path, index=False, encoding='utf-8')
        logging.info(f"Successfully converted '{os.path.basename(excel_file_path)}' to '{os.path.basename(csv_file_path)}'.")

        os.remove(excel_file_path)
        logging.info(f"Removed original Excel file: '{os.path.basename(excel_file_path)}'.")

        return csv_file_path
    except Exception as e:
        logging.error(f"Failed to convert Excel file '{excel_file_path}': {e}")
        raise

#====> other conversions to be added here.

def process_uploaded_files(directory_path: str):
    """
    Iterates over files in a directory and converts any JSON files to CSV.
    """
    converted = []
    for root, _, files in os.walk(directory_path):
        for filename in files:
            filepath = os.path.join(root, filename)
            lower = filename.lower()
            try:
                if lower.endswith('.json'):
                    csvp = convert_json_to_csv(filepath)
                    converted.append(csvp)
                elif lower.endswith('.xls') or lower.endswith('.xlsx'):
                    csvp = convert_excel_to_csv(filepath)
                    converted.append(csvp)
            except Exception:
                # continue converting other files even if one fails
                logging.exception(f"Conversion failed for {filepath}")

    logging.info(f"File conversion process completed for directory: {directory_path}. Converted {len(converted)} files.")
    return converted