import os
import json
import pandas as pd
from urllib.parse import urlparse

def get_csv_files_from_directory(directory_path):
    """Return list of all CSV file paths inside the given directory."""
    csv_files = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.lower().endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files

def read_csv_from_sharepoint(sharepoint_url):
    """Attempt to read a CSV file directly from a SharePoint link."""
    try:
        df = pd.read_csv(sharepoint_url)
        return df
    except Exception as e:
        raise RuntimeError(f"Error reading CSV from SharePoint: {e}")

def infer_data_type(series):
    """Infer a simplified data type for a pandas Series."""
    dtype = str(series.dtype)
    if 'int' in dtype or 'float' in dtype:
        return "number"
    elif 'bool' in dtype:
        return "boolean"
    else:
        if series.isnull().any():
            return "string / null"
        return "string"

def generate_metadata_for_dataframe(file_name, file_path, df):
    """Generate metadata dictionary for a single CSV DataFrame."""
    metadata = {
        "file_name": os.path.splitext(file_name)[0],
        "directory_path": file_path.replace("\\", "/"),
        "columns": []
    }

    for col in df.columns:
        data_type = infer_data_type(df[col])
        has_duplicates = df[col].duplicated().any()
        metadata["columns"].append({
            "column_name": col,
            "data_type": data_type,
            "has_duplicates": bool(has_duplicates)
        })

    return metadata

def generate_metadata(source_dir_or_url, output_path):
    """
    Main callable function.
    Scans a local directory or SharePoint CSV URL and writes metadata.json.
    Returns the collected metadata as a Python object.
    """
    all_metadata = []

    if os.path.isdir(source_dir_or_url):
        csv_files = get_csv_files_from_directory(source_dir_or_url)
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in directory: {source_dir_or_url}")
        for csv_path in csv_files:
            try:
                df = pd.read_csv(csv_path)
                relative_path = os.path.relpath(csv_path, source_dir_or_url)
                file_name = os.path.basename(relative_path)
                metadata = generate_metadata_for_dataframe(file_name, relative_path, df)
                all_metadata.append(metadata)
            except Exception as e:
                raise RuntimeError(f"Error reading {csv_path}: {e}")

    elif source_dir_or_url.startswith("http"):
        df = read_csv_from_sharepoint(source_dir_or_url)
        if df is not None:
            parsed_url = urlparse(source_dir_or_url)
            file_name = os.path.basename(parsed_url.path)
            metadata = generate_metadata_for_dataframe(file_name, source_dir_or_url, df)
            all_metadata.append(metadata)
        else:
            raise RuntimeError("Unable to read CSV from provided SharePoint URL")

    else:
        raise ValueError("Invalid source: must be a directory path or SharePoint CSV URL")

    # Write the metadata to the explicitly provided output_path
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_metadata, f, indent=4)

    return all_metadata

if __name__ == "__main__":
    print("This module provides generate_metadata(source_dir_or_url, output_path). Run from the app and pass explicit paths.")