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

def build_prompt(errors: list[dict[str, any]], puml: str, query_text: str) -> str:
    """
    Build a GPT-4o-optimized prompt to correct PlantUML ERD code.
    The model must fix only the described issues and return one valid JSON object.
    """

    json_structure_example = """
{
  "reasoning": [
    {
      "step": "Describe the correction made",
      "details": "Explain briefly why the change was required to resolve the error."
    }
  ],
  "plantuml_code": "Full corrected PlantUML ER diagram code as a single string"
}
"""

    errors_summary = json.dumps(errors, indent=2, ensure_ascii=False)

    prompt = textwrap.dedent(f"""
    You are a senior data architect and PlantUML ERD specialist.
    Your task is to correct a PlantUML data model based on the provided
    requirements and error report. This current PlantUML diagram has been made for a 3NF normalized relational database schema. Maintain this normalization level and make only the necessary corrections.

    --- OBJECTIVE ---
    1. Review the original PlantUML diagram, the requirement description, and the error list.
    2. Apply only the corrections explicitly mentioned in the errors list.
       - Fix incorrect relationships, data types, missing keys, or referential integrity problems.
       - Preserve entity and attribute names whenever possible.
       - Do not introduce new entities or fields unless necessary for referential integrity.
    3. Ensure the corrected diagram is logically consistent and syntactically valid PlantUML code.
    4. Return ONLY a single JSON object that exactly follows the format below.
       - No markdown, no explanations, no extra text.

    --- REQUIRED JSON FORMAT ---
    {json_structure_example}

    --- INPUT DATA ---

    Requirement Context (query.txt)
    -------------------------
    {query_text}

    Original PlantUML (data.puml)
    -------------------------
    {puml}

    Detected Errors (errors.json)
    -------------------------
    {errors_summary}

    Please produce the corrected output JSON now.
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
 