import os
import json
import logging
from dotenv import load_dotenv
import subprocess
import google.generativeai as genai
from gemini_Call import api_call

# ========== PATH CONFIG ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PLANTUML_JAR = os.path.join(BASE_DIR, "plantuml.jar")
RNSPACE_DIR = os.path.join(BASE_DIR, "Run_Space")

DIMENSIONAL_MODEL_FILE = os.path.join(RNSPACE_DIR, "dimensional_model.json")
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
genai.configure(api_key=API_KEY)

# ========== CORE FUNCTIONS ==========

def load_dimensional_model(path=DIMENSIONAL_MODEL_FILE):
    """Load dimensional_model.json."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ dimensional_model.json not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_prompt(dimensional_model, schema_context):
    """Builds a structured Gemini prompt from the dimensional model."""
    system_instructions = (
        """You are a precise data architect assistant. 
        Given a suggested Model/ Table information in the form of JSON,from the user provided external context and metadata of the orginal data , infer the relationships (primary keys and foreign keys) between them. 
        Output ONLY a PlantUML ER diagram (no prose). 
        Follow strict PlantUML ER syntax with @startuml ... @enduml. 
        Mark cardinalities (1--N, N--N, 1--1) clearly using PlantUML conventions. 
        Do not include any explanations or text outside of the UML code."""
    )

    context_instructions = f"\n\nUse this additional context provided by the user to guide your schema design:\n---USER CONTEXT---\n{schema_context}\n---END USER CONTEXT---"

    user_payload = (
        "Here is the JSON for the suggested table:\n"
        + json.dumps(dimensional_model, indent=2)
        + context_instructions
        + "\n\n Output only the PlantUML ER diagram code based on the metadata and the provided context."
    )

    return system_instructions + "\n\n" + user_payload

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

def generate_schema(schema_context):
    """Generates a PlantUML ER diagram from dimensional_model.json using Gemini API."""
    logger.info("🔍 Loading dimensional model...")
    dimensional_model = load_dimensional_model()

    logger.info("✍️ Building prompt...")
    prompt = build_prompt(dimensional_model, schema_context)

    logger.info("🤖 Calling Gemini model...")
    result_text = api_call(prompt)

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

        corrected_text = api_call(prompt)
        save_plantuml(corrected_text)
        render_plantuml_to_png()
        logger.info("🛠 Schema correction applied.")
        return "Schema corrected successfully."

    else:
        return "Please start your message with 'yes' or 'no' to indicate whether correction is needed."

# ========== ENTRY POINT ==========
if __name__ == "__main__":
    # Example of running with context. In the app, this is passed from the UI.
    user_context = "The 'orders' table links to 'customers' via customerID. Each order can have multiple 'order_details'."
    generate_schema(schema_context=user_context)
    # generate_schema() # This will now raise an error as schema_context is required.
