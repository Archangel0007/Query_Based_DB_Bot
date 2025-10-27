import os
import pandas as pd
import json
import logging
import requests
import re
import csv
import time
import textwrap
import pdfplumber
from docx import Document
from pathlib import Path
import xml.etree.ElementTree as ET
from collections import Counter 
from pathlib import Path
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from dotenv import load_dotenv

# Optional Gemini client
try:
    from google import genai
    _HAS_GENAI = True
except Exception:
    genai = None
    _HAS_GENAI = False

# ---------------- CONFIG ----------------
DEFAULT_MODEL = "gemini-2.5-flash"
CHUNK_APPROX_SIZE = 3000
CHUNK_OVERLAP = 250
MAX_RETRIES = 4
BACKOFF_BASE = 3.0
OUTPUT_DIR = "extracted_csvs"
# ----------------------------------------

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if _HAS_GENAI and GEMINI_API_KEY:
    client = genai.Client(api_key=GEMINI_API_KEY)
else:
    client = None
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def convert_html_to_csv(url: str, output_dir: str = None) -> List[str]:
    """
    Fetches HTML from a URL, converts it to Markdown, extracts structured data via LLM (Gemini),
    and saves the extracted schemas into CSV files.

    Args:
        url (str): The target URL to fetch and process.

    Returns:
        List[str]: Paths to the generated CSV files.

    Raises:
        Exception: If any part of the pipeline fails.
    """
    try:
        # ---------------- Fetch HTML ----------------
        logging.info(f"[+] Fetching URL: {url}")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/117.0 Safari/537.36"
            )
        }
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        html = response.text
        logging.info(f"[+] Successfully fetched {len(html)} bytes")

        # ---------------- Clean & Convert to Markdown ----------------
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "iframe", "form", "input", "header", "footer", "nav"]):
            tag.decompose()
        markdown_text = md(str(soup))
        logging.info("[+] Converted HTML → Markdown")

        # ---------------- Helper functions ----------------
        def paragraph_chunker(text: str, approx_chars: int = CHUNK_APPROX_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
            paras = [p for p in re.split(r"\n{2,}", text) if p.strip()]
            chunks, current = [], ""
            for p in paras:
                if not current:
                    current = p
                elif len(current) + 2 + len(p) <= approx_chars:
                    current = current + "\n\n" + p
                else:
                    chunks.append(current.strip())
                    overlap_text = current[-overlap:] if overlap and len(current) > overlap else current
                    current = (overlap_text + "\n\n" + p).strip()
            if current:
                chunks.append(current.strip())
            return chunks

        def sanitize_filename(s: str) -> str:
            s = s.strip()
            s = re.sub(r"[/:\\<>\"|?*\n\r\t]+", " ", s)
            s = re.sub(r"\s+", "_", s)
            s = s[:120]
            return s or "schema"

        def extract_json_from_text(text: str) -> str:
            m = re.search(r"START_JSON\s*={0,3}(?P<json>.+?)END_JSON", text, flags=re.S | re.I)
            if m:
                return m.group("json").strip()
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1 and end > start:
                return text[start:end + 1]
            return ""

        def build_prompt_for_chunk(chunk: str) -> str:
            return textwrap.dedent(f"""
            You are a data extraction assistant. Extract all tables or structured data from the following MARKDOWN and
            return a JSON object mapping schema_name -> list of rows.

            ===START_JSON===
            {{ "example_schema": [{{"key":"value"}}] }}
            ===END_JSON===

            MARKDOWN:
            {chunk}
            """).strip()

        def call_gemini_with_retry(prompt: str, model: str) -> str:
            if not client:
                raise RuntimeError("Gemini client not configured or GEMINI_API_KEY missing.")
            attempt = 0
            while True:
                attempt += 1
                try:
                    resp = client.models.generate_content(model=model, contents=prompt)
                    if hasattr(resp, "text"):
                        return resp.text
                    return str(resp)
                except Exception as e:
                    if attempt > MAX_RETRIES:
                        raise
                    sleep_for = BACKOFF_BASE * (2 ** (attempt - 1))
                    logging.warning(f"Retry {attempt}/{MAX_RETRIES} after error: {e}. Sleeping {sleep_for:.1f}s")
                    time.sleep(sleep_for)

        def write_schemas_to_csv(schema_map: Dict[str, List[Dict[str, Any]]], out_dir: str):
            outpath = Path(out_dir)
            outpath.mkdir(parents=True, exist_ok=True)
            written_files = []
            for schema_name, rows in schema_map.items():
                if not rows:
                    continue
                keys = list({k for r in rows for k in r.keys()})
                file_path = outpath / (sanitize_filename(schema_name) + ".csv")
                with open(file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=keys)
                    writer.writeheader()
                    for r in rows:
                        writer.writerow({k: r.get(k, "") for k in keys})
                written_files.append(str(file_path))
                logging.info(f"✅ Wrote {len(rows)} rows to {file_path}")
            return written_files

        # ---------------- Chunk Markdown ----------------
        chunks = paragraph_chunker(markdown_text)
        logging.info(f"[+] Split Markdown into {len(chunks)} chunks")

        merged_data: Dict[str, List[Dict[str, Any]]] = {}

        # ---------------- Process Each Chunk ----------------
        for idx, chunk in enumerate(chunks, 1):
            prompt = build_prompt_for_chunk(chunk)
            raw = call_gemini_with_retry(prompt, DEFAULT_MODEL)
            json_text = extract_json_from_text(raw)
            if not json_text:
                logging.warning(f"[Chunk {idx}] No JSON detected.")
                continue
            try:
                parsed_obj = json.loads(json_text)
                if isinstance(parsed_obj, dict):
                    for schema, rows in parsed_obj.items():
                        if isinstance(rows, list):
                            merged_data.setdefault(schema, []).extend(rows)
                else:
                    logging.warning(f"[Chunk {idx}] JSON root is not an object.")
            except Exception as e:
                logging.error(f"Failed to parse JSON from chunk {idx}: {e}")

        if not merged_data:
            raise RuntimeError("No structured data extracted from the URL.")

        # ---------------- Write CSVs ----------------
        use_out = output_dir or OUTPUT_DIR
        written_files = write_schemas_to_csv(merged_data, use_out)
        # Normalize to absolute paths for callers
        abs_written = [str(Path(p).resolve()) for p in written_files]
        logging.info(f"[✅] Completed conversion. Files written: {abs_written}")

        return abs_written

    except Exception as e:
        logging.error(f"Failed to convert HTML to CSV: {e}", exc_info=True)
        raise

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

def convert_xml_to_csv(xml_file_path: str, csv_file_path: str = None, record_tag: str = None) -> str:
    """
    Converts an XML file to a CSV file with nested tag support.
    Automatically flattens nested elements and detects record tags if not specified.
    The original XML file is deleted after successful conversion.

    Args:
        xml_file_path (str): The absolute path to the XML file.
        csv_file_path (str, optional): The output CSV file path. Defaults to same base name as XML.
        record_tag (str, optional): The XML tag to treat as individual records. Auto-detected if omitted.

    Returns:
        str: The absolute path to the created CSV file.

    Raises:
        Exception: If any part of the conversion fails.
    """
    try:
        # ---------- Validate Input ----------
        if not os.path.exists(xml_file_path):
            raise FileNotFoundError(f"XML file not found: {xml_file_path}")

        # ---------- Helper Functions ----------
        def flatten_element(element, parent_key='', sep='_'):
            """Recursively flattens nested XML elements into a flat dict."""
            items = {}
            for child in element:
                key = f"{parent_key}{sep}{child.tag}" if parent_key else child.tag
                if list(child):
                    items.update(flatten_element(child, key, sep=sep))
                else:
                    items[key] = child.text.strip() if child.text else ''
            return items

        def detect_record_tag(root):
            """Finds the most frequent child tag under the root to use as the record tag."""
            tag_counts = Counter([child.tag for child in root])
            if not tag_counts:
                return None
            return tag_counts.most_common(1)[0][0]

        # ---------- Parse XML ----------
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        logging.info(f"Parsed XML root tag: '{root.tag}'")

        # ---------- Detect Record Tag ----------
        if record_tag is None:
            record_tag = detect_record_tag(root)
            if not record_tag:
                raise ValueError("Could not detect a record tag automatically. Please specify one.")
            logging.info(f"Auto-detected record tag: '{record_tag}'")

        records = root.findall(record_tag)
        if not records:
            raise ValueError(f"No records found with tag '{record_tag}' in the XML file.")

        # ---------- Flatten Records ----------
        flattened_records = [flatten_element(r) for r in records]
        all_keys = sorted({k for rec in flattened_records for k in rec.keys()})
        logging.info(f"Flattened {len(flattened_records)} records with {len(all_keys)} fields")

        # ---------- Define Output Path ----------
        if not csv_file_path:
            base_name = os.path.splitext(xml_file_path)[0]
            csv_file_path = base_name + ".csv"

        # ---------- Write CSV ----------
        with open(csv_file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(flattened_records)

        logging.info(f"✅ Successfully converted '{os.path.basename(xml_file_path)}' → '{os.path.basename(csv_file_path)}'")

        # ---------- Delete Original XML ----------
        os.remove(xml_file_path)
        logging.info(f"Removed original XML file: '{os.path.basename(xml_file_path)}'")

        return os.path.abspath(csv_file_path)

    except Exception as e:
        logging.error(f"Failed to convert XML to CSV: {e}", exc_info=True)
        raise


def convert_file_to_csv(file_path: str, csv_path: str = None) -> str:
    """
    Converts a PDF or DOCX file containing tables into a CSV file.
    Automatically detects the file type and extracts table data accordingly.
    The original file is deleted after successful conversion.

    Args:
        file_path (str): Path to the input file (.pdf or .docx).
        csv_path (str, optional): Output CSV file path. Defaults to same base name.

    Returns:
        str: The absolute path to the created CSV file.

    Raises:
        Exception: If conversion fails or file type is unsupported.
    """
    try:
        input_file = Path(file_path)
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

        ext = input_file.suffix.lower()
        if not csv_path:
            csv_path = input_file.with_suffix(".csv")
        csv_path = Path(csv_path)

        all_rows = []

        # ---------------- PDF Handling ----------------
        if ext == ".pdf":
            if not pdfplumber:
                raise ImportError("Missing dependency: pdfplumber. Install via `pip install pdfplumber`.")
            logging.info(f"Extracting tables from PDF: {input_file}")
            with pdfplumber.open(input_file) as pdf:
                for page_idx, page in enumerate(pdf.pages, start=1):
                    tables = page.extract_tables()
                    for t_idx, table in enumerate(tables, start=1):
                        all_rows.extend(table)
                        logging.info(f"Extracted table {t_idx} from page {page_idx} with {len(table)} rows.")

        # ---------------- DOCX Handling ----------------
        elif ext in [".docx", ".doc"]:
            if not Document:
                raise ImportError("Missing dependency: python-docx. Install via `pip install python-docx`.")
            logging.info(f"Extracting tables from DOCX: {input_file}")
            doc = Document(input_file)
            for t_idx, table in enumerate(doc.tables, start=1):
                for row in table.rows:
                    all_rows.append([cell.text.strip() for cell in row.cells])
                logging.info(f"Extracted table {t_idx} with {len(table.rows)} rows.")

        else:
            raise ValueError(f"Unsupported file type: {ext}. Only .pdf and .docx are supported.")

        # ---------------- Write to CSV ----------------
        if not all_rows:
            logging.warning(f"No tables found in '{input_file.name}'. An empty CSV will be created.")

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(all_rows)

        logging.info(f"✅ Successfully converted '{input_file.name}' → '{csv_path.name}'")

        # ---------------- Delete Original ----------------
        os.remove(input_file)
        logging.info(f"Removed original file: {input_file.name}")

        return str(csv_path.resolve())

    except Exception as e:
        logging.error(f"Failed to convert file '{file_path}' to CSV: {e}", exc_info=True)
        raise

def process_uploaded_files(directory_path: str) -> List[str]:
    """
    Iterates over files in a directory and converts supported file types to CSV.
    Supported types: JSON, Excel (.xls/.xlsx), XML, PDF, DOCX.

    Each converter deletes the original file after a successful conversion.

    Args:
        directory_path (str): The path to the directory containing uploaded files.

    Returns:
        List[str]: List of paths to the successfully converted CSV files.
    """
    supported_extensions = {
        '.json': convert_json_to_csv,
        '.xls': convert_excel_to_csv,
        '.xlsx': convert_excel_to_csv,
        '.xml': convert_xml_to_csv,
        '.pdf': convert_file_to_csv,
        '.docx': convert_file_to_csv,
        '.doc': convert_file_to_csv
    }

    converted_files = []
    directory_path = Path(directory_path)

    if not directory_path.exists():
        logging.error(f"Provided directory does not exist: {directory_path}")
        return []

    logging.info(f"🚀 Starting file conversion in: {directory_path}")

    for root, _, files in os.walk(directory_path):
        for filename in files:
            filepath = Path(root) / filename
            ext = filepath.suffix.lower()

            converter = supported_extensions.get(ext)
            if not converter:
                logging.debug(f"Skipping unsupported file type: {filename}")
                continue

            try:
                logging.info(f"Processing file: {filename}")
                csv_path = converter(str(filepath))
                converted_files.append(csv_path)
                logging.info(f"✅ Converted {filename} → {Path(csv_path).name}")
            except Exception as e:
                logging.error(f"❌ Failed to convert {filename}: {e}", exc_info=True)
                # continue to next file

    logging.info(f"🏁 Conversion completed. Total converted files: {len(converted_files)}")
    return converted_files

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