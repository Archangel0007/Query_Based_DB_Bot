import os
from dotenv import load_dotenv
import google.generativeai as genai

# Conditionally import based on openai library version
try:
    from openai import AzureOpenAI
    _HAS_V1_OPENAI = True
except ImportError:
    import openai
    _HAS_V1_OPENAI = False

load_dotenv()

# Gemini Credentials
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    raise RuntimeError("GEMINI_API_KEY not found. Please set it in your .env file.")

MODEL = "gemini-2.5-flash"

# Azure OpenAI Credentials
GPT_KEY = os.getenv("GPT_KEY")
GPT_ENDPOINT = os.getenv("GPT_ENDPOINT")
DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME")

try:
    from google import genai
    _HAS_GENAI = True
except Exception:
    _HAS_GENAI = False

#Imports Complete

def gemini_api_call(prompt , model=MODEL, temperature=0.0) -> str:
    if not _HAS_GENAI:
        raise RuntimeError(
            "google.genai client not available. Install `google-genai` or adapt call_llm_with_genai()."
        )

    genai_client = genai.Client(api_key=API_KEY)

    print("📡 Sending prompt to Gemini model (via google.genai client)...")
    response = genai_client.models.generate_content(model=model, contents=prompt)
    return response.text

def api_call(prompt, model=None, temperature=0.0) -> str:
    """Makes an API call to an Azure OpenAI endpoint."""
    if not all([GPT_KEY, GPT_ENDPOINT, DEPLOYMENT_NAME]):
        raise RuntimeError(
            "Azure OpenAI credentials (GPT_KEY, GPT_ENDPOINT, DEPLOYMENT_NAME) not found in .env file."
        )

    deployment = model if model else DEPLOYMENT_NAME

    if _HAS_V1_OPENAI:
        # Modern (v1.x) client
        print(f"📡 Sending prompt to Azure OpenAI model (v1.x client, deployment: {deployment})...")
        client = AzureOpenAI(
            api_key=GPT_KEY,
            api_version="2024-02-01",
            azure_endpoint=GPT_ENDPOINT
        )
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
        )
        return response.choices[0].message.content
    else:
        # Legacy (v0.x) client
        print(f"📡 Sending prompt to Azure OpenAI model (v0.x client, deployment: {deployment})...")
        openai.api_type = "azure"
        openai.api_base = GPT_ENDPOINT
        openai.api_version = "2023-07-01-preview" # A common version for the v0.x SDK
        openai.api_key = GPT_KEY

        response = openai.ChatCompletion.create(
            engine=deployment,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
        )
        return response['choices'][0]['message']['content']

if __name__ == "__main__":
    test_prompt = "Hello, OpenAI! Can you generate a simple JSON object for me?"
    try:
        print("\n--- Testing OpenAI Call ---")
        response = api_call(test_prompt)
        print("Response from OpenAI:")
        print(response)
    except Exception as e:
        print(f"OpenAI call failed: {e}")