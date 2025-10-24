import os
from dotenv import load_dotenv
import google.generativeai as genai
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:

    raise RuntimeError("GEMINI_API_KEY not found. Please set it in your .env file.")

MODEL = "gemini-2.5-flash"
try:

    from google import genai
    _HAS_GENAI = True
except Exception:
    _HAS_GENAI = False

#Imports Complete

def api_call(prompt , model=MODEL, temperature=0.0) -> str:
    if not _HAS_GENAI:
        raise RuntimeError(
            "google.genai client not available. Install `google-genai` or adapt call_llm_with_genai()."
        )

    genai_client = genai.Client(api_key=API_KEY)

    print("📡 Sending prompt to Gemini model (via google.genai client)...")
    response = genai_client.models.generate_content(model=model, contents=prompt)
    return response.text

if __name__ == "__main__":
    test_prompt = "Hello, Gemini! Can you generate a simple JSON object for me?"
    response = api_call(test_prompt)
    print("Response from Gemini:")
    print(response)