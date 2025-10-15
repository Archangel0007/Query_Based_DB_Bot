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

    if hasattr(response, "text"):
        return response.text
    if isinstance(response, dict):

        cand = response.get("candidates")
        if isinstance(cand, list) and len(cand) > 0:
            c0 = cand[0]

            for k in ("output", "content", "message", "text"):
                if k in c0:
                    return c0[k]

        choices = response.get("choices")
        if isinstance(choices, list) and choices:
            if "message" in choices[0] and "content" in choices[0]["message"]:
                return choices[0]["message"]["content"]

    return str(response)

if __name__ == "__main__":
    test_prompt = "Hello, Gemini! Can you generate a simple JSON object for me?"
    response = api_call(test_prompt)
    print("Response from Gemini:")
    print(response)