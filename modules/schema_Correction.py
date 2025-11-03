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
    json_structure_example = """
{
  "reasoning": [
    {
      "step": "Description of a correction made",
      "details": "Detailed explanation of why this change was necessary to fix an error."
    }
  ],
  "plantuml_code": "string containing the full corrected PlantU-ML code"
}
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
    - Output ONLY a single JSON object. No commentary, no other text.
    The JSON output must follow this structure:
    {json_structure_example}

    INPUT - Requirement Context (query.txt):
    -------------------------
    {query_text}

    INPUT - Original PlantUML (data.puml):
    -------------------------
    {puml}

    INPUT - Detected Errors (errors.json):
    -------------------------
    {errors_summary}

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
        response_text = api_call(prompt)
        clean_output = re.sub(r"```json|```", "", response_text, flags=re.DOTALL).strip()
        response_data = json.loads(clean_output)

        corrected_puml = response_data.get("plantuml_code")
        reasoning = response_data.get("reasoning")

        if not corrected_puml:
            raise ValueError("'plantuml_code' key missing from LLM response.")

        save_output(corrected_puml, puml_path)
        return reasoning

    except Exception as e:
        print(f"⚠️ Correction failed: {e}. Saving raw output for debugging.")
        with open(puml_path + ".correction-error.txt", "w", encoding="utf-8") as f:
            f.write(response_text)
        raise

if __name__ == "__main__":
    correction("Run_Space/errors.json", "Run_Space/relationship_schema.puml", "Run_Space/refined_User_Query.txt")
 