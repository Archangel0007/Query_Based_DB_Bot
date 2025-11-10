import os
import re
import json
import sys
import time 
import stat
import logging
from .api_Call import api_call

import os
import stat
import time
import logging
from datetime import datetime
import tempfile
import shutil

def write_text_safely(output_path: str, content: str) -> str:
    """
    Safely writes text to the specified output_path.
    - Ensures parent directory exists.
    - If a directory already exists at output_path, renames it (adds timestamp) instead of failing.
    - Writes to a temporary file then atomically renames to output_path.
    - Sets permissive chmod at the end.
    """
    logging.info(f"[WRITE] Preparing to write file: {output_path}")

    parent_dir = os.path.dirname(output_path) or "."
    os.makedirs(parent_dir, exist_ok=True)

    # If output_path already exists and is a directory → rename it safely
    if os.path.exists(output_path) and os.path.isdir(output_path):
        backup_dir = output_path + "_bak_" + datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        logging.warning(f"[WRITE] '{output_path}' is a directory — renaming to '{backup_dir}'")
        os.rename(output_path, backup_dir)

    # Write atomically via a temp file in the same directory
    fd, tmp_path = tempfile.mkstemp(dir=parent_dir, prefix=".tmp_write_", text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmpf:
            tmpf.write(content)
            tmpf.flush()
            os.fsync(tmpf.fileno())
        # Replace existing file if any
        if os.path.exists(output_path) and os.path.isfile(output_path):
            os.remove(output_path)
        shutil.move(tmp_path, output_path)
    finally:
        # Clean up temp if still present
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    # Apply permissive file mode (best effort)
    try:
        os.chmod(output_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)  # 0o644
        os.chmod(output_path, 0o666)
        logging.info(f"[WRITE] Set permissive chmod for {output_path}")
    except Exception as e:
        logging.debug(f"[WRITE] chmod failed for {output_path}: {e}")

    time.sleep(0.05)
    logging.info(f"[WRITE] Successfully wrote file to {output_path}")
    return output_path

def strip_triple_backticks(s: str) -> str:
    """Remove leading ```[python] and trailing ``` if present, otherwise return original."""
    if not isinstance(s, str):
        return s
    return re.sub(r'^\s*```(?:python)?\s*|\s*```\s*$', '', s, flags=re.IGNORECASE)

def table_converter(files_path, plantUML_path, metadata_path, output_path, max_embed_chars=20000):
    """
    Generate a table-conversion Python script by sending the raw PlantUML text
    and the metadata JSON directly to the LLM with minimal preprocessing.

    Important behavior change: this function will send the PlantUML text and
    the metadata JSON **to the LLM for context**, and instruct the LLM to
    produce a Python script that **does not read** the PlantUML or metadata at
    runtime. Instead the generated script must embed (hard-code) the mapping/schema
    derived from the provided PlantUML + metadata so it can run independently.
    """

    files_path = os.path.abspath(files_path)
    output_path = os.path.abspath(output_path)

    if not os.path.isdir(files_path):
        raise FileNotFoundError(f"files_path not found or not a directory: {files_path}")
    os.makedirs(output_path, exist_ok=True)

    # collect CSV filenames
    csv_files = sorted([f for f in os.listdir(files_path) if f.lower().endswith('.csv')])

    # read raw PlantUML text (minimal processing — just load)
    plantuml_text = None
    if plantUML_path and os.path.exists(plantUML_path):
        try:
            with open(plantUML_path, 'r', encoding='utf-8') as pf:
                plantuml_text = pf.read()
                # truncate if extremely long
                if len(plantuml_text) > max_embed_chars:
                    plantuml_text = plantuml_text[:max_embed_chars] + "\n\n--TRUNCATED--"
        except Exception as e:
            plantuml_text = f"<<ERROR reading PlantUML: {e}>>"

    # read raw metadata JSON (embed raw JSON text so LLM can use full detail)
    metadata_text = None
    if metadata_path and os.path.exists(metadata_path):
        try:
            with open(metadata_path, 'r', encoding='utf-8') as mf:
                raw = mf.read()
                # try to pretty-print if it's valid JSON, otherwise include raw
                try:
                    parsed = json.loads(raw)
                    metadata_text = json.dumps(parsed, indent=2)
                except Exception:
                    metadata_text = raw
                if len(metadata_text) > max_embed_chars:
                    metadata_text = metadata_text[:max_embed_chars] + "\n\n--TRUNCATED--"
        except Exception as e:
            metadata_text = f"<<ERROR reading metadata: {e}>>"

    # Build a single prompt string. IMPORTANT: explicitly instruct the LLM that the generated
    # script MUST NOT read the PlantUML or metadata files at runtime — it should hardcode
    # the inferred mapping/schema based on the provided PlantUML + metadata content embedded here.
    prompt = f"""Generate a single self-contained Python 3 script file named 'generated_table_converter.py'.

The script must perform the following tasks when run locally:

1. Read all CSV files from the same directory where this file will be saved. 
2. Use the mapping/schema inferred from the PlantUML text and metadata JSON (both embedded below) to:
   - Identify target table/entity names as defined in the PlantUML diagram.
   - Determine which columns belong to each table based on the PlantUML and metadata.
   - Map and rename the existing CSV files to the correct table names.
   IMPORTANT: **The generated script must NOT read the PlantUML file or the metadata JSON at runtime**.
   Instead, it must embed (hard-code) the mapping/schema derived from the PlantUML and metadata inside the generated Python file
   so that it can run without accessing those source files.

3. Create new CSV files named exactly as per the tables/entities defined in the PlantUML file.
4. Populate each new CSV with only the required columns based on the constraints from the PlantUML and metadata.
   - Use metadata for datatypes, nullable constraints, and default values where available.
   - If a column is missing in the source file but required by the PlantUML schema, create it as empty or NULL (as appropriate).
5. Write the new CSV files in the same directory where the original files are located (i.e., create the new files next to the originals).
6.Log all major operations (file reads, transformations, deletions) to stdout, and exit with a non-zero status on fatal errors.
7. A single file can have mutliple tables/entities if the PlantUML indicates so; split accordingly.
8. The produced script will be run in the backend using another script, so ensure it is standalone and does not require any user interaction.
9. The file are present in the same directory as the generated script.
10. The script should Strictly NOT accept (via argparse or top-of-file constants). The Files directory is the same as the directory where the script is located.
11. Constraints like PK and FK are for reference but the concept of duplicates , null values etc must be handled as per the PUML coINSERT INTO employee_territories (employee_id, territory_id)
VALUES (employee_id:int, territory_id:int);nstraints for the columns. Every table that is being split need not have all the rows from the source file. Only the relevant rows and columns as per the PUML constraints must be present in the split files.

Constraints for the generated script:
- Must be a single standalone Python file, runnable as: python generated_table_converter.py
- Allowed third-party dependency: pandas (import pandas as pd). If pandas is not installed, print a clear installation message and exit.
- No network calls or API requests are allowed.
- Must be defensive: check file existence, create directories if needed, and handle empty or malformed CSVs gracefully.
- Always overwrite existing files if they already exist.
- The generated script must contain the inferred mapping/schema (hard-coded structures such as dicts/lists) derived from the PlantUML and metadata embedded below; it must not attempt to open or parse the PlantUML or metadata files at runtime.

Inputs provided for context (embed exactly as shown):
- CSV filenames (FILES_DIR top-level): {csv_files}

- PlantUML text (embed below):
----BEGIN_PLANTUML----
{plantuml_text or '<NO_PLANTUML_PROVIDED>'}
----END_PLANTUML----

- Metadata JSON (embed below):
----BEGIN_METADATA----
{metadata_text or '<NO_METADATA_PROVIDED>'}
----END_METADATA----

Important guidance for the LLM:
- Use the PlantUML text as the authoritative source for table/entity names and relationships.
- Use the metadata JSON to determine columns, datatypes, nullability, and any hints about which CSV contains which data.
- Where mapping from CSV filename -> table is ambiguous, implement sensible heuristics (substring matches, singular/plural, tokens) and include a clear, human-readable mapping dict in the generated script so operators can review/modify it.
- The generated script must operate only on the CSVs in FILES_DIR; it must not perform any network I/O or read external files beyond the CSVs it processes.
- Return ONLY the raw Python script content (no markdown, no explanation)."""

    # Call the api to generate the script
    try:
        llm_response = api_call(prompt)
        # strip wrapping triple backticks or ```python fences if present
        if isinstance(llm_response, str):
            llm_response = re.sub(r'^\s*```(?:python)?\s*', '', llm_response, flags=re.IGNORECASE)
            llm_response = re.sub(r'\s*```\s*$', '', llm_response, flags=re.IGNORECASE)
        if not llm_response or not isinstance(llm_response, str):
            raise RuntimeError("api_call returned no script text")
    except Exception as e:
        raise RuntimeError(f"Failed to generate converter script via api_call: {e}")

    output_path = write_text_safely(output_path, llm_response)
    return output_path

if __name__ == "__main__":
    # Adjust this path if you move the file
    BASE_DIR = '../'
    TEST_RUNNER_DIR = os.path.join(BASE_DIR, "Run_Space", "Test_Runner")

    # Ensure module imports work
    if BASE_DIR not in sys.path:
        sys.path.insert(0, BASE_DIR)

    # Import after path setup

    # --- Define paths for test run ---
    files_path = TEST_RUNNER_DIR  # directory with CSVs
    plantuml_path = os.path.join(TEST_RUNNER_DIR, "relationship_schema.puml")
    metadata_path = os.path.join(TEST_RUNNER_DIR, "metadata.json")
    output_path = TEST_RUNNER_DIR  # save generated script here

    print("\n[INFO] Running table_converter for Test_Runner directory...")
    print(f"Files path       : {files_path}")
    print(f"PlantUML path    : {plantuml_path}")
    print(f"Metadata path    : {metadata_path}")
    print(f"Output directory : {output_path}")
    print("-" * 70)

    try:
        result_file = table_converter(
            files_path=files_path,
            plantUML_path=plantuml_path,
            metadata_path=metadata_path,
            output_path=output_path
        )
        print(f"\n✅ Conversion script generated successfully: {result_file}")
    except Exception as e:
        import traceback
        print(f"\n❌ Error running table_converter: {e}")
        traceback.print_exc()
