import os
import json
import logging
from dotenv import load_dotenv
import google as genai
from .gemini_Call import api_call

# ========== PATH CONFIG ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ========== LOGGING ==========
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ========== CORE FUNCTIONS ==========

def load_json_file(path):
    """Load a JSON file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ File not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_text_file(path):
    """Load a text file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ File not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def build_prompt(metadata, user_context):
    """
    Builds a GPT-4o-optimized prompt for creating a 3NF conceptual model
    and classifying Fact and Dimension tables from transactional metadata.
    """

    system_instructions = (
        """You are a senior database architect specializing in data warehousing and normalization.
Your task is to design a **3NF conceptual model** from provided source metadata,
and classify each resulting entity as either a **Fact** or **Dimension** table.

--- OBJECTIVE ---
You will:
1. Analyze the provided table metadata.
2. Normalize the structure conceptually to **Third Normal Form (3NF)**:
   - Eliminate partial and transitive dependencies.
   - Ensure each non-key column depends only on the key.
3. Classify each resulting entity as a **Fact** or **Dimension**:
   - Fact tables: contain measurable events, quantitative metrics, or transactional data.
   - Dimension tables: contain descriptive attributes, categories, or hierarchies.
4. Preserve **all columns from the input metadata**.
   - Do NOT add or remove columns.
   - You may move columns between tables to achieve 3NF, but every column must appear exactly once.
5. Use **standard SQL data types** (INTEGER, VARCHAR, DECIMAL, TIMESTAMP, etc.).
6. The number of resulting tables must be **greater than or equal to** the number of source files present in metadata.
7. Dimensional tables are allowed to have single Column.
8. Create as many tables as you can within Reason. It is IMPORTANT to follow 3NF strictly. Give good weight to the user context while designing the model.
9. Provide clear **reasoning**, describing normalization and classification decisions per table.

--- OUTPUT FORMAT ---
Return ONLY one valid JSON object with the structure below.
Do NOT include markdown, explanations, or any text outside the JSON.

{
  "reasoning": [
    {
      "step": "table name and type",
      "details": "Explain how this table was derived or normalized, and why it is Fact or Dimension."
    }
  ],
  "conceptual_data": {
    "tables": [
      {
        "table_name": "string",
        "table_type": "Fact | Dimension",
        "description": "A short summary of the table purpose.",
        "columns": [
          {
            "column_name": "string",
            "data_type": "string"
          }
        ]
      }
    ]
  }
}
"""
    )

    user_payload = (
        "Here is the source metadata to analyze:\n"
        + json.dumps(metadata, indent=2)
        + "\n\nBusiness context to guide modeling decisions:\n"
        + user_context
        + "\n\nPlease generate the dimensional model strictly following the JSON structure above."
    )

    return system_instructions + "\n\n" + user_payload


def generate_dimensional_model(metadata_file=None, user_context_file=None, output_json=None):
    """Main function to generate and save the dimensional model."""
    if not all([metadata_file, user_context_file, output_json]):
        raise ValueError("All file paths (metadata, context, output) must be provided.")

    logger.info("🔍 Loading source metadata and user context...")
    metadata_obj = load_json_file(metadata_file)
    user_context = load_text_file(user_context_file)

    logger.info("✍️ Building prompt for dimensional modeling...")
    # Pass the Python object directly to build_prompt, which will handle serialization.
    prompt = build_prompt(metadata_obj, user_context)

    logger.info("🤖 Calling Gemini to generate the dimensional model...")
    result_text = api_call(prompt)
    # Clean the response to get only the JSON
    if result_text.startswith("```json"):
        result_text = result_text[7:-3].strip()

    try:
        response_data = json.loads(result_text)
        conceptual_data = response_data.get("conceptual_data")
        reasoning = response_data.get("reasoning")

        if not conceptual_data:
            logger.error("❌ 'conceptual_data' not found in the Gemini response.")
            raise ValueError("'conceptual_data' key missing from LLM response.")

        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(conceptual_data, f, indent=4)
        logger.info(f"✅ Dimensional model saved to: {output_json}")

        return reasoning
    except json.JSONDecodeError:
        logger.error("❌ Failed to parse JSON from Gemini response. Saving raw output for debugging.")
        with open(output_json + ".error.txt", "w", encoding="utf-8") as f:
            f.write(result_text)
        raise

if __name__ == "__main__":
    # Note: Before running, ensure 'Run_Space/metadata.json' and 'Run_Space/user_context.txt' exist.
    run_space = os.path.join(BASE_DIR, "Run_Space")
    generate_dimensional_model(
        metadata_file=os.path.join(run_space, "metadata.json"),
        user_context_file=os.path.join(run_space, "refined_User_Query.txt"),
        output_json=os.path.join(run_space, "dimensional_model.json")
    )
