import google.generativeai as genai
import re
import os
from dotenv import load_dotenv
from .gemini_Call import api_call
import json

def run_phase1(user_query_path, output_path):
    """Generate Phase 1 testcases from a user query and write to output_path.

    Both arguments must be explicit paths.
    """
    if not os.path.exists(user_query_path):
        raise FileNotFoundError(f"❌ Missing file: {user_query_path}")
    with open(user_query_path, "r", encoding="utf-8") as f:
        user_query = f.read().strip()

    # Define the JSON structure outside the f-string to avoid formatting errors
    json_structure_example = """
{
  "reasoning": [
    {
      "step": "Test case description",
      "details": "Why the testcase was chosen and what it tests for."
    }
  ],
  "Test Cases": [
      {
          "serial_number": 1,
          "testcase_description": "...",
          "reasoning": "...",
          "category": "...",
          "severity": "..."
      }
  ]
}
"""

    prompt_phase1 = f"""
You are a senior database QA expert and test-case strategist.
Input:
1) USER QUERY: {user_query}
Task:
- Generate 25-30 possible QA testcases that would validate a relational database schema based on the query.
- Each testcase should be a JSON object with:
  - serial_number: integer (unique)
  - testcase_description: string (what to test)
  - reasoning: string (why this test is important)
  - optional: expected_result (if applicable)
  - optional: category (PrimaryKey, ForeignKey, DataType, Relationship, NullConstraint, Index, UniqueConstraint)
  - optional: severity (Critical, High, Medium, Low)
Guidance for producing the BEST testcases:
1.  Think beyond the literal query: Anticipate all possible database schema issues related to the query.
2.  Edge case focus: Include unusual or tricky scenarios such as self-referencing tables, nullable vs non-nullable inconsistencies, data type mismatches between primary and foreign keys, multi-column foreign keys, many-to-many relationship issues, orphaned records, invalid defaults.
3.  Coverage across categories: ensure PrimaryKey, ForeignKey, DataType, Relationship, NullConstraint, Index, UniqueConstraint are considered where relevant.
4.  Reasoning clarity: Provide reasoning for each testcase.
5.  No repetition: Make every testcase unique.
Output format STRICT: Return ONLY a single JSON object. Do not include any other text, explanations, or markdown.
The JSON output must follow this structure:
{json_structure_example}
"""

    output_text = api_call(prompt_phase1)
    clean_output = re.sub(r"```json|```", "", output_text, flags=re.DOTALL).strip()

    try:
        response_data = json.loads(clean_output)
        test_cases = response_data.get("Test Cases")
        reasoning = response_data.get("reasoning")

        out_dir = os.path.dirname(output_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(test_cases, f, indent=2)

        print(f"✅ Phase 1 done: {output_path} created.")
        return True, reasoning
    except Exception as e:
        print("⚠️ Phase 1 failed.\nOutput:\n", clean_output, "\nError:", e)
        return False, None

def run_phase2(plantuml_code_path, testcases_path, output_dir):
    if not os.path.exists(plantuml_code_path):
        raise FileNotFoundError(f"❌ Missing file: {plantuml_code_path}")
    with open(plantuml_code_path, "r", encoding="utf-8") as f:
        plantuml_code = f.read().strip()
    print("\n⚙️ Running Phase 2 — executing testcases...")
    if not os.path.exists(testcases_path):
        raise FileNotFoundError(f"❌ Missing file from Phase 1: {testcases_path}")

    with open(testcases_path, "r", encoding="utf-8") as f:
        testcases_prompt = f.read()

    json_structure_example = """
{
  "reasoning": [
    {
      "step": "Validation Summary",
      "details": "A brief summary of the validation process, including the number of tests passed and failed."
    }
  ],
  "testcases": [ {"serial_number": 1, "status": "pass/fail", "notes": "..."} ],
  "errors": [ {"testcase_serial_number": 2, "error_description": "..."} ]
}
"""
    prompt_phase2 = f"""
    You are a database QA expert.

    Inputs:
    1) PLANTUML CODE (ER diagram): {plantuml_code}
    2) Testcases to run: {testcases_prompt}

    Task:
    - Execute the testcases on the schema.
    - Provide reasoning for the validation process, summarizing the findings.
    Output format STRICT: Return ONLY a single JSON object. Do not include any other text, explanations, or markdown.
    The JSON output must follow this structure:
    {json_structure_example}
    """

    output_text = api_call(prompt_phase2)
    clean_output = re.sub(r"```json|```", "", output_text, flags=re.DOTALL).strip()

    try:
        response_data = json.loads(clean_output)
        testcases_results = response_data.get("testcases", [])
        errors_found = response_data.get("errors", [])
        reasoning = response_data.get("reasoning")

        # Write outputs to provided output_dir
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "testcases.json"), "w", encoding="utf-8") as f:
                json.dump(testcases_results, f, indent=2)
            with open(os.path.join(output_dir, "errors.json"), "w", encoding="utf-8") as f:
                json.dump(errors_found, f, indent=2)

        print(f"✅ Phase 2 done: testcases.json and errors.json created in {output_dir}")
        return True, reasoning
    except Exception as e:
        print("⚠️ Phase 2 failed.\nOutput:\n", clean_output, "\nError:", e)
        return False, None

# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    user_query_path = "Run_Space/refined_User_Query.txt"
    plantuml_code_path = "Run_Space/relationship_schema.puml"
    # When running as a script, write phase1 output next to the Run_Space files.
    output_path = os.path.join(os.path.dirname(user_query_path), "testcases_prompt.json")
    try:
        phase1_success = run_phase1(user_query_path, output_path)
        if phase1_success:
            run_phase2(plantuml_code_path, output_path, os.path.dirname(output_path))
    except Exception as e:
        print("❌ Error during testing phases:", e)
