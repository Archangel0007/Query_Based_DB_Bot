import google.generativeai as genai
import re
import os
from dotenv import load_dotenv
from .api_Call import api_call
import json

def build_prompt_phase_1(user_query: str) -> str:
    """
    Builds a GPT-4o-optimized prompt for generating QA test cases
    to validate a relational database schema based on a user query.
    """

    json_structure_example = """
{
  "reasoning": [
    {
      "step": "Title for each test case",
      "details": "Reasoning for each test case design"
    },
  ],
  "Test Cases": [
    {
      "serial_number": 1,
      "testcase_description": "Description of the test objective.",
      "reasoning": "Why this specific test is important.",
      "category": "PrimaryKey | ForeignKey | DataType | Relationship | NullConstraint | Index | UniqueConstraint",
      "severity": "Critical | High | Medium | Low",
      "expected_result": "Optional expected outcome if applicable."
    }
  ]
}
"""

    prompt_phase1 = f"""
You are a senior **Database QA Architect**.
Your task is to design **20 detailed QA test cases** that validate a relational database schema . These should test by giving simple natural language descriptions that would create sql queries to fetch data from the database to verify its correctness.
derived from the following user query:

--- USER QUERY ---
{user_query}
--- END QUERY ---

--- OBJECTIVE ---
Generate schema validation test cases that check logical correctness,
referential integrity, data type consistency, and normalization adherence.

--- GUIDELINES ---
1. Each test must be unique and cover a distinct potential schema issue.
2. Include both common and edge cases, such as:
   - Self-referencing tables
   - Mismatched PK–FK data types
   - Nullability or constraint violations
   - Many-to-many relationships
   - Orphaned records or missing FKs
   - Redundant attributes violating 3NF
3. Distribute test cases across categories:
   PrimaryKey, ForeignKey, DataType, Relationship, NullConstraint, Index, UniqueConstraint.
4. For each test case:
   - Provide a concise but precise `testcase_description`.
   - Include a clear `reasoning` explaining why this test matters.
   - Assign an appropriate `category` and `severity`.
   - Add an optional `expected_result` if relevant.
5. For each text case that u are generating give a reasoning block for it.

--- OUTPUT FORMAT ---
It must STRICTLY follow this structure:
{json_structure_example}
"""

    return prompt_phase1

def run_phase1(user_query_path, output_path):
    """Generate Phase 1 testcases from a user query and write to output_path.

    Both arguments must be explicit paths.
    """
    if not os.path.exists(user_query_path):
        raise FileNotFoundError(f"❌ Missing file: {user_query_path}")
    with open(user_query_path, "r", encoding="utf-8") as f:
        user_query = f.read().strip()

    prompt_phase1 = build_prompt_phase_1(user_query)
    print("\n⚙️ Running Phase 1 — generating testcases...")
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
        "testcases": [
            {
            "serial_number": 1,
            "status": "pass/fail",
            "notes": "..."
            }
        ],
        "errors": [
            {
            "testcase_serial_number": 2,
            "error_description": "..."
            }
        ]
        }
        """

    prompt_phase2 = f"""
        SYSTEM INSTRUCTIONS:
        You are a highly accurate and detail-oriented **Database QA Expert**.
        Your job is to validate a database schema (in 3NF) against a set of test cases.

        BEHAVIOR RULES:
        - Respond ONLY with valid JSON.
        - Do NOT include markdown, commentary, natural language text, or explanations outside of the JSON.
        - Follow the output schema exactly as shown.
        - Use concise but clear phrasing in all fields.
        - Ensure the JSON is syntactically valid (no trailing commas, no comments).

        INPUTS:
        1. ER Diagram (PlantUML Code):
        {plantuml_code}

        2. Test Cases to Execute:
        {testcases_prompt}

        TASK:
        1. Evaluate the given schema using the provided test cases.
        2. Summarize your validation reasoning under the "reasoning" key.
        3. For each test case, mark whether it **passes** or **fails**, with a short note explaining why.
        4. If a test case fails, include a corresponding entry in the "errors" list with a clear error description.
        5. Ensure all output fits the following JSON structure exactly:

        {json_structure_example}

        FINAL REQUIREMENT:
        Return ONLY the JSON object — no markdown, preamble, or commentary.
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
