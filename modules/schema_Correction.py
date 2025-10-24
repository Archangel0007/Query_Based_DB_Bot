import json
import argparse
import textwrap
import os
import sys
from typing import Any, Dict, List, Union
from dotenv import load_dotenv
import google.generativeai as genai
import re
from .gemini_Call import api_call


def load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_text_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def normalize_errors(errors_raw: Any) -> List[Dict[str, Any]]:
    if isinstance(errors_raw, list):
        return errors_raw
    if isinstance(errors_raw, dict):

        if "errors" in errors_raw and isinstance(errors_raw["errors"], list):
            return errors_raw["errors"]
        return [errors_raw]

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

    INPUT - Requirement Context (query.txt):
    -------------------------
    {query_text}

    INPUT - Original PlantUML (data.puml):
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

def save_output(text: str, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"✅ Saved corrected PlantUML to: {path}")

def correction(errors_path: str, puml_path: str, query_path: str):

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

    prompt = build_prompt(errors, puml, query_text)

    try:
        corrected = api_call(prompt)
    except Exception as e:
        print("LLM call failed:", str(e))
        print("If you don't have the google-genai client installed, run:")
        print("  pip install google-genai")
        print("Or api_call() to your environment.")
        sys.exit(3)

    corrected = corrected.strip()
    if "@startuml" not in corrected and "@enduml" not in corrected:

        if "@startuml" in puml and "@enduml" in puml:
            corrected = "@startuml\n" + corrected + "\n@enduml"

    save_output(corrected, puml_path)

if __name__ == "__main__":
    correction("Run_Space/errors.json", "Run_Space/relationship_schema.puml", "Run_Space/refined_User_Query.txt")
 