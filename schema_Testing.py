import google.generativeai as genai
import re
import os
from dotenv import load_dotenv
from gemini_Call import api_call

def run_phase1(user_query_path):
    if not os.path.exists(user_query_path):
        raise FileNotFoundError(f"❌ Missing file: {user_query_path}")
    with open(user_query_path, "r", encoding="utf-8") as f:
        user_query = f.read().strip()
 
    """Generate QA testcases using Gemini."""
    print("\n⚙️ Running Phase 1 — generating testcases...")

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
1. **Think beyond the literal query:** Anticipate all possible database schema issues related to the query.
2. **Edge case focus:** Include unusual or tricky scenarios such as:
   - Self-referencing tables
   - Nullable vs. non-nullable inconsistencies
   - Data type mismatches between primary and foreign keys
   - Multi-column foreign keys
   - Many-to-many relationship issues
   - Orphaned records or missing references
   - Invalid default values or constraints
3. **Coverage across all categories:** Ensure testcases include PrimaryKey, ForeignKey, DataType, Relationship, NullConstraint, Index, and UniqueConstraint where relevant.
4. **Reasoning clarity:** Provide a clear reasoning for each testcase explaining why it is necessary and what could go wrong if the issue exists.
5. **Severity & prioritization:** Assign severity for each testcase if relevant, helping prioritize critical issues.
6. **No repetition:** Each testcase must be unique and cover a distinct validation point.
7. **Structured output:** Provide a JSON array only. No markdown, no fences, no explanations.
Output format STRICT: ### testcases_prompt.json [ {{ "serial_number": 1, "testcase_description": "Check if all tables have primary keys", "reasoning": "Every table must have a PK to maintain entity integrity" }} ] Rules: - Only JSON array after the header. No markdown or fences.
"""

    
    output_text = api_call(prompt_phase1)

    clean_output = re.sub(r"```json|```", "", output_text, flags=re.DOTALL).strip()

    try:
        testcases_json = clean_output.split("### testcases_prompt.json")[1].strip()
        testcases_json = re.sub(r"^#+.*json", "", testcases_json).strip()

        os.makedirs("Run_Space", exist_ok=True)
        with open("Run_Space/testcases_prompt.json", "w", encoding="utf-8") as f:
            f.write(testcases_json)

        print("✅ Phase 1 done: Run_Space/testcases_prompt.json created")
        return True
    except Exception as e:
        print("⚠️ Phase 1 failed.\nOutput:\n", clean_output, "\nError:", e)
        return False

def run_phase2(plantuml_code_path):
    if not os.path.exists(plantuml_code_path):
        raise FileNotFoundError(f"❌ Missing file: {plantuml_code_path}")
    with open(plantuml_code_path, "r", encoding="utf-8") as f:
        plantuml_code = f.read().strip()
    print("\n⚙️ Running Phase 2 — executing testcases...")

    testcases_path = "Run_Space/testcases_prompt.json"
    if not os.path.exists(testcases_path):
        raise FileNotFoundError(f"❌ Missing file from Phase 1: {testcases_path}")

    with open(testcases_path, "r", encoding="utf-8") as f:
        testcases_prompt = f.read()

    prompt_phase2 = f"""
    You are a database QA expert.

    Inputs:
    1) PLANTUML CODE (ER diagram): {plantuml_code}
    2) Testcases to run: {testcases_prompt}

    Task:
    - Execute the testcases on the schema.
    Output format STRICT:
    ### testcases.json
    [ ... ]
    ### errors.json
    [ ... ]
    Rules:
    - No markdown or fences, only JSON arrays after each header.
    """

    model = genai.GenerativeModel("gemini-2.5-pro")
    response = model.generate_content(prompt_phase2)
    output_text = response.text.strip()
    clean_output = re.sub(r"```json|```", "", output_text, flags=re.DOTALL).strip()

    try:
        testcases_part, errors_part = clean_output.split("### errors.json")
        testcases_json = testcases_part.split("### testcases.json")[1].strip()
        errors_json = errors_part.strip()

        testcases_json = re.sub(r"^#+.*json", "", testcases_json).strip()
        errors_json = re.sub(r"^#+.*json", "", errors_json).strip()

        os.makedirs("Run_Space", exist_ok=True)
        with open("Run_Space/testcases.json", "w", encoding="utf-8") as f:
            f.write(testcases_json)
        with open("Run_Space/errors.json", "w", encoding="utf-8") as f:
            f.write(errors_json)

        print("✅ Phase 2 done: Run_Space/testcases.json and Run_Space/errors.json created")
        return True
    except Exception as e:
        print("⚠️ Phase 2 failed.\nOutput:\n", clean_output, "\nError:", e)
        return False

# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    user_query_path = "Run_Space/refined_User_Query.txt"
    plantuml_code_path = "Run_Space/relationship_schema.puml"

    try:
        phase1_success = run_phase1(user_query_path)
        if phase1_success:
            run_phase2(plantuml_code_path)
    except Exception as e:
        print("❌ Error during testing phases:", e)
