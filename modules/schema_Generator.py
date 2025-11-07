import os
import json
import logging
from dotenv import load_dotenv
import subprocess
import requests
import google.generativeai as genai
from .api_Call import api_call

# ========== PATH CONFIG ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PLANTUML_JAR = os.path.join(BASE_DIR, "plantuml.jar")

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

def load_dimensional_model(path):
    """Load dimensional_model.json."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ dimensional_model.json not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_prompt(dimensional_model, schema_context):
    """
    Builds a GPT-4o-optimized prompt for generating a 3NF database schema 
    and PlantUML ER diagram from a dimensional model and contextual metadata.
    """

    system_instructions = (
        """You are a senior data architect AI.
Your goal is to analyze a user-provided JSON model and contextual metadata,
then infer normalized (3NF) relational structures and their relationships.

Output Requirements:
1. You must output a single JSON object — no markdown, no text outside the JSON.
2. Inside the JSON, include:
   - "reasoning": a list of step-by-step explanations (one per table) justifying
     primary keys, foreign keys, and relationship cardinalities. Each item in the list
     should be an object with "step" and "details" keys.
   - "plantuml_code": a string containing a valid PlantUML ER diagram.
3. The PlantUML code must:
   - Start with `@startuml` and end with `@enduml`
   - Use standard ER notation.
   - Label primary keys as <<PK>> and foreign keys as <<FK>>.
   - Explicitly show cardinalities (1--N, 1--1, N--N).
4. Follow 3NF design principles — avoid redundancy and ensure dependency preservation.
5. If there is a common column with same name between tables with the same context then there needs to be a relationship between the tables.
Important:
- Do not include any explanation or markdown outside the JSON.
- The JSON must be syntactically valid.
- Follow this exact structure for the output:
{
  "reasoning": [
    {
      "step": "Describe the correction made",
      "step": "Correction for a specific error",
      "details": "Explain briefly why the change was required to resolve the error."
    }
  ],
  "plantuml_code": "Full corrected PlantUML ER diagram code as a single string"
}
"""
    )

    context_instructions = f"""
Use this external context and metadata to guide normalization and relationship inference:
---USER CONTEXT---
{schema_context}
---END USER CONTEXT---
"""

    user_payload = (
        "Here is the user-provided JSON for the suggested model:\n"
        + json.dumps(dimensional_model, indent=2)
        + "\n\n"
        + context_instructions
        + "\nNow, infer relationships, keys, and design the ER diagram according to the rules above."
    )

    return system_instructions + "\n\n" + user_payload

def save_plantuml(code_text, out_path):
    """Saves valid PlantUML code to the provided out_path."""
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
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

    return out_path

def render_plantuml_to_png(puml_path, output_png_path):
    """Render PlantUML .puml file to PNG using plantuml.jar"""
    # Use the provided paths directly
    if not os.path.exists(puml_path):
        raise FileNotFoundError(f"❌ {puml_path} not found")
    diagnostics = {
        "puml_path": puml_path,
        "output_png_path": output_png_path,
        "plantuml_jar": PLANTUML_JAR,
        "attempts": []
    }

    # Attempt 1: use plantuml.jar if it exists
    if os.path.exists(PLANTUML_JAR):
        puml_dir = os.path.dirname(puml_path) or '.'
        puml_base = os.path.basename(puml_path)
        cmd = ["java", "-jar", PLANTUML_JAR, "-tpng", puml_base]
        logger.info(f"Trying plantuml.jar: {PLANTUML_JAR} (cwd={puml_dir})")
        try:
            proc = subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=puml_dir)
            diagnostics["attempts"].append({"method": "jar", "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr})
        except FileNotFoundError as e:
            diagnostics["attempts"].append({"method": "jar", "error": str(e)})
            logger.warning("Java executable not found when trying plantuml.jar")
        except subprocess.CalledProcessError as e:
            diagnostics["attempts"].append({"method": "jar", "returncode": e.returncode, "stdout": getattr(e, 'stdout', ''), "stderr": getattr(e, 'stderr', '')})
            logger.warning(f"plantuml.jar rendering failed: returncode={getattr(e,'returncode',None)}")
        else:
            generated_file = os.path.join(puml_dir, os.path.splitext(puml_base)[0] + ".png")
            if os.path.exists(generated_file):
                os.replace(generated_file, output_png_path)
                logger.info(f"🖼 PNG generated with plantuml.jar: {output_png_path}")
                return output_png_path

    # Attempt 2: try plantuml CLI if available in PATH
    try:
        puml_dir = os.path.dirname(puml_path) or '.'
        puml_base = os.path.basename(puml_path)
        cmd = ["plantuml", "-tpng", puml_base]
        logger.info(f"Trying plantuml CLI (plantuml in PATH) (cwd={puml_dir})")
        proc = subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=puml_dir)
        diagnostics["attempts"].append({"method": "cli", "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr})
        generated_file = os.path.join(puml_dir, os.path.splitext(puml_base)[0] + ".png")
        if os.path.exists(generated_file):
            os.replace(generated_file, output_png_path)
            logger.info(f"🖼 PNG generated with plantuml CLI: {output_png_path}")
            return output_png_path
    except FileNotFoundError as e:
        diagnostics["attempts"].append({"method": "cli", "error": str(e)})
        logger.debug("plantuml CLI not found in PATH")
    except subprocess.CalledProcessError as e:
        diagnostics["attempts"].append({"method": "cli", "returncode": getattr(e,'returncode',None), "stdout": getattr(e,'stdout',''), "stderr": getattr(e,'stderr','')})
        logger.warning(f"plantuml CLI rendering failed: returncode={getattr(e,'returncode',None)}")

    # Attempt 3: fallback to public PlantUML server via HTTP POST
    try:
        with open(puml_path, 'r', encoding='utf-8') as f:
            plantuml_text = f.read()
        logger.info("Attempting rendering via public PlantUML server...")
        resp = requests.post('http://www.plantuml.com/plantuml/png/', data=plantuml_text.encode('utf-8'), timeout=30)
        diagnostics["attempts"].append({"method": "server", "status_code": resp.status_code, "headers": dict(resp.headers)})
        if resp.status_code == 200:
            out_dir = os.path.dirname(output_png_path)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            with open(output_png_path, 'wb') as outf:
                outf.write(resp.content)
            logger.info(f"🖼 PNG generated via PlantUML server: {output_png_path}")
            return output_png_path
        else:
            # include response text snippet in diagnostics
            diagnostics["attempts"].append({"method": "server", "response_text_snippet": resp.text[:500]})
            logger.warning(f"PlantUML server returned status {resp.status_code}")
    except Exception as e:
        diagnostics["attempts"].append({"method": "server_error", "error": str(e)})
        logger.warning(f"PlantUML server rendering failed: {e}")

    # If we reach here, rendering failed for all methods
    # write diagnostics to logger and raise an informative error
    logger.error(f"PlantUML rendering diagnostics: {json.dumps(diagnostics, indent=2)}")
    raise RuntimeError(
        "❌ Failed to render PlantUML to PNG. Diagnostics have been logged. "
        "Ensure modules/plantuml.jar is present and Java is on PATH, or install plantuml CLI, "
        "or allow outbound HTTP to www.plantuml.com for server rendering."
    )

def generate_schema(dimensional_model_path, output_puml_path, output_png_path, schema_context):
    """Generates a PlantUML ER diagram from dimensional_model.json using Gemini API."""
    logger.info("🔍 Loading dimensional model...")
    dimensional_model = load_dimensional_model(dimensional_model_path)

    logger.info("✍️ Building prompt...")
    prompt = build_prompt(dimensional_model, schema_context)

    logger.info("🤖 Calling Gemini model...")
    result_text = api_call(prompt)
    if result_text.startswith("```plantuml"):
        result_text = result_text[11:-3].strip()
    elif result_text.startswith("```json"):
        result_text = result_text[7:-3].strip()

    try:
        response_data = json.loads(result_text)
        plantuml_code = response_data.get("plantuml_code")
        reasoning = response_data.get("reasoning")

        if not plantuml_code:
            raise ValueError("'plantuml_code' key missing from LLM response.")

        logger.info("💾 Saving PlantUML output...")
        puml_safe = save_plantuml(plantuml_code, out_path=output_puml_path)

        logger.info("🖼 Rendering PNG from PlantUML...")
        png_path = render_plantuml_to_png(puml_path=puml_safe, output_png_path=output_png_path)

        logger.info("✅ Schema generation complete.")
        return png_path, reasoning
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"❌ Failed to parse PlantUML from Gemini response: {e}. Saving raw output for debugging.")
        save_plantuml(result_text, out_path=output_puml_path + ".error.puml")
        raise

def build_correction_prompt(current_schema: str, correction_text: str) -> str:
    """
    Builds a GPT-4o-optimized prompt to correct or update an existing PlantUML ER diagram.
    The model must apply only the requested corrections and return the full corrected code.
    """

    system_instructions = (
        """You are an expert data modeler and PlantUML ERD specialist. Your task is to update an existing PlantUML ER diagram based on a user's correction request.

--- OUTPUT REQUIREMENTS ---
- Return ONLY the corrected PlantUML code.
- The code must:
  - Begin with '@startuml' and end with '@enduml'
  - Preserve all valid existing entities, relationships, and formatting.
  - Apply ONLY the requested corrections; do not invent unrelated changes.
  - Maintain valid syntax and consistent indentation.
- Do NOT include explanations, markdown, reasoning, or commentary outside the code.
- No matter what the correction request should be ALWAYS fulfilled in the output.
"""
    )

    user_payload = f"""
--- EXISTING PLANTUML SCHEMA ---
{current_schema}

--- USER CORRECTION REQUEST ---
{correction_text}

Now apply the correction and return only the fully updated PlantUML diagram.
"""

    prompt = system_instructions + "\n\n" + user_payload
    return prompt

def schema_correction(user_input, puml_path, png_path):
    """Apply corrections to the current schema based on user input."""
    if not os.path.exists(puml_path):
        raise FileNotFoundError(f"Schema file not found at {puml_path}")

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

        with open(puml_path, "r", encoding="utf-8") as f:
            current_schema = f.read()

        prompt = build_correction_prompt(current_schema, correction_text)
        logger.info("🤖 Calling Gemini model for schema correction...")

        corrected_text = api_call(prompt)
        save_plantuml(corrected_text, out_path=puml_path)
        render_plantuml_to_png(puml_path=puml_path, output_png_path=png_path)
        logger.info("🛠 Schema correction applied.")
        return "Schema corrected successfully."

    else:
        return "Please start your message with 'yes' or 'no' to indicate whether correction is needed."

# ========== ENTRY POINT ==========
if __name__ == "__main__":
    # Example of running with context. In the app, this is passed from the UI.
    run_space = os.path.join(BASE_DIR, "Run_Space")
    user_context = "The 'orders' table links to 'customers' via customerID. Each order can have multiple 'order_details'."
    generate_schema(
        dimensional_model_path=os.path.join(run_space, "dimensional_model.json"),
        output_puml_path=os.path.join(run_space, "relationship_schema.puml"),
        output_png_path=os.path.join(run_space, "relationship_schema.png"),
        schema_context=user_context
    )
