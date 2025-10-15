import google as genai
import re
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    raise RuntimeError("❌ No GEMINI_API_KEY found in .env file")
genai.configure(api_key=API_KEY)
user_query = "These are the tables in the Chinook database. The Chinook data model represents a digital media store having information about artists, albums, media tracks, invoices and customers."

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
Output format STRICT: ### testcases_prompt.json [ {{ "serial_number": 1, "testcase_description": "Check if all tables have primary keys", "reasoning": "Every table must have a PK to maintain entity integrity" }} ] Rules: - Only JSON array after the header. No markdown or
fences.

"""


model = genai.GenerativeModel("gemini-2.5-pro")
response = model.generate_content(prompt_phase1)
output_text = response.text.strip()

# Clean and save
clean_output = re.sub(r"```json|```", "", output_text, flags=re.DOTALL).strip()
try:
    testcases_json = clean_output.split("### testcases_prompt.json")[1].strip()
    testcases_json = re.sub(r"^#+.*json", "", testcases_json).strip()
    with open("testcases_prompt.json", "w", encoding="utf-8") as f:
        f.write(testcases_json)
    print("✅ Phase 1 done: testcases_prompt.json created")
except Exception as e:
    print("⚠️ Phase 1 failed. Output:\n", clean_output, "\nError:", e)

# ----------------------------
# PHASE 2: Run testcases on PlantUML schema
# ----------------------------

# Read testcases from phase 1
with open("testcases_prompt.json", "r", encoding="utf-8") as f:
    testcases_prompt = f.read()

prompt_phase2 = f"""
You are a database QA expert.

Inputs:
1) PLANTUML CODE (ER diagram): {plantuml_code}
2) Testcases to run: {testcases_prompt}

Task:
- Execute the testcases on the schema.
- For each testcase, return:
  - serial_number
  - testcase_description
  - error: "no-error" or "simple-error"
- For errors, return:
  - serial_number
  - testcase_description
  - error_detail

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

# Clean and save both JSON files
clean_output = re.sub(r"```json|```", "", output_text, flags=re.DOTALL).strip()
try:
    testcases_part, errors_part = clean_output.split("### errors.json")
    testcases_json = testcases_part.split("### testcases.json")[1].strip()
    errors_json = errors_part.strip()
    testcases_json = re.sub(r"^#+.*json", "", testcases_json).strip()
    errors_json = re.sub(r"^#+.*json", "", errors_json).strip()

    with open("testcases.json", "w", encoding="utf-8") as f:
        f.write(testcases_json)
    with open("errors.json", "w", encoding="utf-8") as f:
        f.write(errors_json)

    print("✅ Phase 2 done: testcases.json and errors.json created")

except Exception as e:
    print("⚠️ Phase 2 failed. Output:\n", clean_output, "\nError:", e)
