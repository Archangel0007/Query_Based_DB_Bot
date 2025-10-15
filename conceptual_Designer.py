import os
import json
import logging
from dotenv import load_dotenv
import google as genai
from gemini_Call import api_call
# ========== PATH CONFIG ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RNSPACE_DIR = os.path.join(BASE_DIR, "Run_Space")

METADATA_FILE = os.path.join(RNSPACE_DIR, "metadata.json")
USER_CONTEXT_FILE = os.path.join(RNSPACE_DIR, "refined_User_Query.txt")
OUTPUT_JSON = os.path.join(RNSPACE_DIR, "dimensional_model.json")

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
    """Builds a structured Gemini prompt for dimensional modeling."""
    system_instructions = (
        """You are an expert DataBase architect. Your task is to design a dimensional model based on table metadata from a transactional system.

        1.  **Analyze the Tables**: Review the provided table metadata.
        2.  **Normalize to 3NF**: Conceptually normalize the tables to Third Normal Form (3NF) to identify discrete entities.
        3.  **Identify Fact and Dimension Tables**: Based on the 3NF structure and user context, determine which tables should be facts (containing quantitative measures of business events) and which should be dimensions (containing descriptive attributes).
        4.  **Define the Schema**: Propose a new schema. You may need to create new tables (e.g., for date dimensions) or split existing ones. The number of tables you are creating should be either greater than or equal to, but not less than, the number of source files.
        5.  **Output JSON**: Return ONLY a single JSON object that describes the dimensional model. Do not include any other text, explanations, or markdown.
        6.  **Column Data Types**: Use standard SQL data types (e.g., INTEGER, VARCHAR, DECIMAL, TIMESTAMP) for column definitions.
        7.  **No New Columns**: Do Not create New columns for the tables. Neither Fact nor Dimension tables should have any columns that are not present in the source metadata.
        The JSON output must follow this structure:
        {
          "tables": [
            {
              "table_name": "string",
              "table_type": "Fact" | "Dimension",
              "description": "A brief description of the table's purpose.",
              "columns": [
                {
                  "column_name": "string",
                  "data_type": "string (e.g., INTEGER, VARCHAR, DECIMAL, TIMESTAMP)",
                }
              ]
            }
          ]
        }
        """
    )

    user_payload = (
        "Here is the metadata from the source system:\n"
        + json.dumps(metadata, indent=2)
        + "\n\nHere is the user's business context for the data:\n"
        + user_context
        + "\n\nPlease generate the dimensional model in the specified JSON format."
    )

    return system_instructions + "\n\n" + user_payload

def generate_dimensional_model():
    """Main function to generate and save the dimensional model."""
    logger.info("🔍 Loading source metadata and user context...")
    metadata = load_json_file(METADATA_FILE)
    user_context = load_text_file(USER_CONTEXT_FILE)

    logger.info("✍️ Building prompt for dimensional modeling...")
    prompt = build_prompt(metadata, user_context)

    logger.info("🤖 Calling Gemini to generate the dimensional model...")
    result_text = api_call(prompt)

    logger.info("💾 Saving dimensional model JSON...")
    try:
        # The API is asked for JSON, so we parse to validate and then re-serialize with indentation
        dimensional_model = json.loads(result_text)
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(dimensional_model, f, indent=4)
        logger.info(f"✅ Dimensional model saved to: {OUTPUT_JSON}")
    except json.JSONDecodeError:
        logger.error("❌ Failed to parse JSON from Gemini response. Saving raw output for debugging.")
        with open(OUTPUT_JSON + ".error.txt", "w", encoding="utf-8") as f:
            f.write(result_text)
        raise

if __name__ == "__main__":
    # Note: Before running, ensure 'Run_Space/metadata.json' and 'Run_Space/user_context.txt' exist.
    generate_dimensional_model()
