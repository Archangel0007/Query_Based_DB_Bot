import os
import re
import json
import sys
from .api_Call import api_call

def strip_triple_backticks(s: str) -> str:
    """Remove leading ```[python] and trailing ``` if present, otherwise return original."""
    if not isinstance(s, str):
        return s
    return re.sub(r'^\s*```(?:python)?\s*|\s*```\s*$', '', s, flags=re.IGNORECASE)

def table_converter(files_path, plantUML_path, metadata_path, output_path, max_embed_chars=20000):
    """
    Generate a table-conversion Python script by sending the raw PlantUML text
    and the metadata JSON directly to the LLM with minimal preprocessing.

    Behavior:
      - Lists CSV filenames in files_path (top-level only) and includes that list in the prompt.
      - Reads the raw PlantUML file contents (if present) and embeds them directly in the prompt.
      - Reads the raw metadata JSON (if present) and embeds it directly in the prompt.
      - Asks the LLM to produce a single self-contained Python script named
        `generated_table_converter.py` which:
          * reads CSVs from FILES_DIR,
          * infers/matches CSVs to PlantUML table/entity names using the embedded PlantUML + metadata,
          * performs light normalization (numeric/date casting, nullable handling) guided by metadata,
          * writes normalized CSVs into an `output_tables` subdirectory inside OUTPUT_DIR,
          * logs progress and raises on fatal errors.
      - Saves the LLM output as output_path/generated_table_converter.py and returns that path.

    This version intentionally performs minimal preprocessing: it does NOT try to guess mappings itself;
    it simply lists filenames and hands the PlantUML + metadata to the LLM to use as the authoritative source.
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

    # Build a concise prompt that hands raw PlantUML and metadata to the model
    prompt_lines = [
        "Generate a single self-contained Python 3 script file named 'generated_table_converter.py'.",
        "The script will perform these tasks when run locally:",
        "  1) Read all CSV files from FILES_DIR (top-level only).",
        "  2) Use the provided PlantUML text and metadata JSON (both embedded below) to map/rename CSV files to the target table names in the PlantUML diagram. The PlantUML is authoritative about desired table/entity names and relationships; use metadata to guide datatype/nullable inference.",
        "  3) Perform light normalization: coerce numeric-looking columns to numeric types where safe, parse date-like strings to ISO dates, handle empty strings as NULLs for nullable columns per metadata, trim whitespace, and ensure output CSVs use UTF-8.",
        "  4) Write the normalized CSVs into an `output_tables` subdirectory inside OUTPUT_DIR, using the target table names from PlantUML as filenames (e.g., customers_dimension.csv).",
        "  5) Log progress and errors to stdout. Exit with non-zero status on fatal errors.",
        "",
        "Constraints for the generated script:",
        "- Single file only; runnable as: `python generated_table_converter.py`.",
        "- The script should accept (via top-of-file constants or argparse): FILES_DIR, OUTPUT_DIR, METADATA_PATH (optional).",
        "- Allowed third-party dependency: pandas (import pandas as pd). If pandas is not installed, print a helpful message and exit.",
        "- Do not make any network calls. Do not call external APIs.",
        "- Be defensive: check file existence, create output dir, handle empty or malformed CSVs gracefully.",
        "",
        "Inputs the script should be aware of (embed exactly as shown):",
        f"- CSV filenames (FILES_DIR top-level): {csv_files}",
        f"- PlantUML text (embed below):\n----BEGIN_PLANTUML----\n{plantuml_text or '<NO_PLANTUML_PROVIDED>'}\n----END_PLANTUML----",
        f"- Metadata JSON (embed below):\n----BEGIN_METADATA----\n{metadata_text or '<NO_METADATA_PROVIDED>'}\n----END_METADATA----",
        "",
        "Important: Use the PlantUML text to identify target table/entity names and structure. If a CSV filename clearly matches a PlantUML table name, use that mapping. If matching is ambiguous, implement sensible heuristics (filename tokens, singular/plural variations) and log the chosen mapping so a human can review output.",
        "",
        "Return ONLY the raw Python script content (no comments about the response)."
    ]
    prompt = "\n".join(prompt_lines)

    # Call the api to generate the script
    try:
        llm_response = api_call(prompt)
        llm_response = strip_triple_backticks(llm_response)
        if not llm_response or not isinstance(llm_response, str):
            raise RuntimeError("api_call returned no script text")
    except Exception as e:
        raise RuntimeError(f"Failed to generate converter script via api_call: {e}")

    # Save the generated script
    out_file = os.path.join(output_path, "generated_table_converter.py")
    with open(out_file, "w", encoding="utf-8") as outf:
        outf.write(llm_response)

    return out_file
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
