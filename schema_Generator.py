import os
import json
import logging
from dotenv import load_dotenv
import requests
import subprocess

# ========== PATH CONFIG ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PLANTUML_JAR = os.path.join(BASE_DIR, "plantuml.jar")
RNSPACE_DIR = os.path.join(BASE_DIR, "Run_Space")

METADATA_FILE = os.path.join(RNSPACE_DIR, "metadata.json")
OUTPUT_PUML = os.path.join(RNSPACE_DIR, "relationship_schema.puml")
OUTPUT_PNG = os.path.join(RNSPACE_DIR, "relationship_schema.png")

# ========== LOGGING ==========
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ========== ENV ==========
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    raise RuntimeError("❌ No GEMINI_API_KEY found in .env file")

# ========== CORE FUNCTIONS ==========

def load_metadata(path=METADATA_FILE):
    """Load metadata.json."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ metadata.json not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_prompt(metadata):
    """Builds a structured Gemini prompt from metadata."""
    system_instructions = (
        """You are a precise data architect assistant. 
        Given metadata about database tables and their columns, 
        infer reasonable primary keys, foreign keys, and relationships. 
        Output ONLY a PlantUML ER diagram (no prose). 
        Follow strict PlantUML ER syntax with @startuml ... @enduml. 
        Mark cardinalities (1--N, N--N, 1--1) clearly using PlantUML conventions. 
        If you infer associative (join) tables, include them explicitly."""
    )

    user_payload = (
        "Here is the metadata JSON for the tables:\n"
        + json.dumps(metadata, indent=2)
        + "\n\nOutput only the PlantUML ER diagram code."
    )

    return system_instructions + "\n\n" + user_payload

def call_gemini(prompt, model="gemini-2.5-flash", temperature=0.0, max_output_tokens=4000):
    """Calls Gemini API (REST) and returns the generated text."""
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_output_tokens
        },
    }

    try:
        resp = requests.post(endpoint, params={"key": API_KEY}, json=body, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"Gemini API error {resp.status_code}: {resp.text}")

        data = resp.json()
        candidates = data.get("candidates", [])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            if parts and "text" in parts[0]:
                return parts[0]["text"].strip()
        return json.dumps(data, indent=2)

    except Exception as e:
        raise RuntimeError(f"Gemini API error: {e}")

def save_plantuml(code_text, out_path=OUTPUT_PUML):
    """Saves valid PlantUML code to a .puml file."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    code_trim = code_text.strip()

    if "@startuml" in code_trim and "@enduml" in code_trim:
        start = code_trim.index("@startuml")
        end = code_trim.rindex("@enduml") + len("@enduml")
        plantuml = code_trim[start:end]
    else:
        plantuml = "@startuml\n" + code_trim + "\n@enduml"

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(plantuml)

    logger.info(f"💾 PlantUML ER diagram saved to: {out_path}")

def render_plantuml_to_png(puml_path=OUTPUT_PUML):
    """Render PlantUML .puml file to PNG using plantuml.jar"""
    if not os.path.exists(puml_path):
        raise FileNotFoundError(f"❌ {puml_path} not found")

    cmd = ["java", "-jar", PLANTUML_JAR, "-tpng", puml_path, "-o", os.path.dirname(puml_path)]
    subprocess.run(cmd, check=True)

    if not os.path.exists(OUTPUT_PNG):
        raise RuntimeError("❌ Failed to generate PNG from PlantUML")
    
    logger.info(f"🖼 PNG generated: {OUTPUT_PNG}")
    return OUTPUT_PNG

def generate_schema():
    """Generates a PlantUML ER diagram from metadata.json using Gemini API."""
    logger.info("🔍 Loading metadata...")
    metadata = load_metadata()

    logger.info("✍️ Building prompt...")
    prompt = build_prompt(metadata)

    logger.info("🤖 Calling Gemini model...")
    result_text = call_gemini(prompt)

    logger.info("💾 Saving PlantUML output...")
    save_plantuml(result_text)

    logger.info("🖼 Rendering PNG from PlantUML...")
    png_path = render_plantuml_to_png()

    logger.info("✅ Schema generation complete.")
    return png_path

def schema_correction(user_input):
    """Apply corrections to the current schema based on user input."""
    if not os.path.exists(OUTPUT_PUML):
        raise FileNotFoundError(f"Schema file not found at {OUTPUT_PUML}")

    user_input = user_input.strip()
    if not user_input:
        raise ValueError("User input is empty.")

    first_word = user_input.split()[0].lower()
    
    if first_word == "yes":
        logger.info("✅ Schema confirmed as correct — no changes applied.")
        return "Schema confirmed as correct. No modifications needed."

    elif first_word == "no":
        correction_text = " ".join(user_input.split()[1:]).strip()
        if not correction_text:
            return "You said 'no' but didn’t specify any correction details."

        with open(OUTPUT_PUML, "r", encoding="utf-8") as f:
            current_schema = f.read()

        system_instructions = (
            """You are a precise data modeling assistant.
            You are given an existing PlantUML ER diagram.
            Apply the user's correction request carefully and output ONLY the corrected PlantUML code.
            Preserve valid syntax and @startuml ... @enduml structure.
            Do not add explanations or text outside of the UML code."""
        )

        user_payload = f"""
        Existing Schema:
        {current_schema}

        User correction request:
        {correction_text}

        Please modify the schema accordingly and return the updated PlantUML diagram.
        """

        prompt = system_instructions + "\n\n" + user_payload

        corrected_text = call_gemini(prompt)
        save_plantuml(corrected_text)
        render_plantuml_to_png()
        logger.info("🛠 Schema correction applied.")
        return "Schema corrected successfully."

    else:
        return "Please start your message with 'yes' or 'no' to indicate whether correction is needed."

# ========== ENTRY POINT ==========
if __name__ == "__main__":
    generate_schema()
