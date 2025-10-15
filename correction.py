#!/usr/bin/env python3
"""
correction.py
 
Reads:
 - errors.json      (list/dict of error entries describing issues in the puml)
 - data.puml        (original PlantUML text that may contain errors)
 - query.txt        (requirement/context in natural language)
 
Calls Gemini 2.5 Flash (LLM) with a well-structured prompt to correct
the PlantUML content according to the errors and context, and writes:
 - updated_data.puml
 
USAGE:
  python correction.py --errors errors.json --puml data.puml --query query.txt
"""
 
import json
import argparse
import textwrap
import os
import sys
from typing import Any, Dict, List, Union
from dotenv import load_dotenv
 
# ========== CONFIG ==========
# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    # Use a more descriptive error message
    raise RuntimeError("GEMINI_API_KEY not found. Please set it in your .env file.")
 
# Model name requested
MODEL = "gemini-2.5-flash"
 
# Output filename for the corrected PlantUML
OUTPUT_FILE = "updated_data.puml"
# ===========================
 
# Try to import a Google GenAI python client. Adjust if your environment differs.
try:
    # Preferred style used in some samples:
    # pip install google-genai
    from google import genai
    _HAS_GENAI = True
except Exception:
    _HAS_GENAI = False
 
# Optional: fallback to requests-based call (uncomment & adapt if you prefer raw HTTP)
# import requests
 
 
def load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
 
 
def load_text_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
 
 
def normalize_errors(errors_raw: Any) -> List[Dict[str, Any]]:
    """
    Accepts an errors.json payload that could be:
      - a list of objects
      - a top-level dict with an "errors" key
      - newline separated JSON objects
    Returns a list of dicts.
    """
    if isinstance(errors_raw, list):
        return errors_raw
    if isinstance(errors_raw, dict):
        # If single dict, return single-element list
        # or if it's wrapped in {"errors": [...]}
        if "errors" in errors_raw and isinstance(errors_raw["errors"], list):
            return errors_raw["errors"]
        return [errors_raw]
 
    # If it's something else, attempt to parse fallback
    raise ValueError("Unsupported errors.json format — expected JSON list or object.")
 
 
def build_prompt(errors: List[Dict[str, Any]], puml: str, query_text: str) -> str:
    """
    Construct a strong, unambiguous prompt for the LLM to correct the PlantUML.
    The goal: produce ONLY the corrected PlantUML content with no explanation.
    """
    errors_summary = json.dumps(errors, indent=2, ensure_ascii=False)
    prompt = textwrap.dedent(f"""
    You are an expert at translating requirement specifications and error reports into
    corrected PlantUML (.puml) code. You will be given:
      1) Requirement text (query.txt) describing the domain and intended data model.
      2) The original PlantUML source (data.puml).
      3) A list of detected errors (errors.json) that must be corrected.
 
    TASK:
    - Fix ONLY the issues explicitly described in the errors list, and make conservative
      improvements to ensure correctness of types and FK relationships described there.
    - Preserve entity/relationship names where possible. Do not invent new entities unless
      required to fix referential integrity or naming conflicts.
    - Ensure data types align: foreign key columns must use the exact same type as the referenced primary keys.
    - Convert clearly-date fields that are declared as VARCHAR into DATE/DATETIME types, if stated in errors.
    - For currency/money columns indicated in the errors use NUMERIC/DECIMAL (or DECIMAL(precision,scale)).
    - Provide valid PlantUML syntax; do not include backticks or Markdown formatting.
    - Output ONLY the corrected PlantUML source. No commentary, no JSON wrapper, no extra text.
 
    INPUT - Requirement Context (Run_Space/refined_User_Query.txt):
    -------------------------
    {query_text}
 
    INPUT - Original PlantUML ():
    -------------------------
    {puml}
 
    INPUT - Detected Errors (errors.json):
    -------------------------
    {errors_summary}
 
    NOTE: If multiple plausible fixes exist, prefer simple explicit fixes:
      - change a column's data type to match the referenced PK,
      - change VARCHAR dates to DATE or DATETIME,
      - change DECIMAL/NUMERIC usage for currency,
      - keep attribute order and formatting consistent with the original file,
      - keep comments and non-problematic annotations intact.
 
    Return the corrected PlantUML file contents now.
    """).strip()
    return prompt
 
 
def call_llm_with_genai(prompt: str) -> str:
    """
    Call Gemini using google.genai client if available.
    If you use a different client, adapt this function accordingly.
    """
    if not _HAS_GENAI:
        raise RuntimeError(
            "google.genai client not available. Install `google-genai` or adapt call_llm_with_genai()."
        )
 
    # Configure client with API key
    genai.configure(api_key=API_KEY)
 
    # Some client wrappers accept a simple generate call; adapt if your sdk differs.
    # Using generate_content with a single string content (older examples)
    # The return object shape can vary; handle common possibilities.
    print("📡 Sending prompt to Gemini model (via google.genai client)...")
    response = genai_client.models.generate_content(model=MODEL, contents=prompt)
 
    # Response parsing — many wrappers include .text or .result
    # Try common attributes:
    if hasattr(response, "text"):
        return response.text
    
    # Handle the more complex response structure from the genai client
    try:
        return response.candidates[0].content.parts[0].text
    except (AttributeError, IndexError):
        # Fallback for unexpected structures
        return str(response)
 
 
def save_output(text: str, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"✅ Saved corrected PlantUML to: {path}")
 
 
def main(errors_path: str, puml_path: str, query_path: str, output_path: str):
    # 1) Load files
    if not os.path.exists(errors_path):
        print(f"ERROR: errors file not found: {errors_path}")
        sys.exit(2)
    if not os.path.exists(puml_path):
        print(f"ERROR: puml file not found: {puml_path}")
        sys.exit(2)
    if not os.path.exists(query_path):
        print(f"ERROR: query file not found: {query_path}")
        sys.exit(2)
 
    raw_errors = load_json_file(errors_path)
    errors = normalize_errors(raw_errors)
    puml = load_text_file(puml_path)
    query_text = load_text_file(query_path)
 
    # 2) Build prompt
    prompt = build_prompt(errors, puml, query_text)
 
    # 3) Call LLM
    try:
        corrected = call_llm_with_genai(prompt)
    except Exception as e:
        print("LLM call failed:", str(e))
        print("If you don't have the google-genai client installed, run:")
        print("  pip install google-genai")
        print("Or adapt call_llm_with_genai() to your environment.")
        sys.exit(3)
 
    # 4) Basic sanity: ensure the output looks like PlantUML (should contain @startuml)
    # If user LLM didn't include start/end tags, be conservative and add if original had them.
    corrected = corrected.strip()
    if "@startuml" not in corrected and "@enduml" not in corrected:
        # If original had start/end, add them
        if "@startuml" in puml and "@enduml" in puml:
            corrected = "@startuml\n" + corrected + "\n@enduml"
 
    # 5) Save output
    save_output(corrected, output_path)
 
 
if __name__ == "__main__":
    main("Run_Space/errors.json", "Run_Space/relationship_schema.puml", "Run_Space/refined_User_Query.txt", OUTPUT_FILE)
 