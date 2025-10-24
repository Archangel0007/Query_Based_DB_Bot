import re
import os
from spellchecker import SpellChecker
import google.generativeai as genai
from gemini_Call import api_call

spell = SpellChecker()


def clean_text(cleaned_query: str) -> str:
    prompt = f"""
You are a highly experienced data engineer and database expert.
Treat the following user query as a precise description of database/data-related requirements. 
Do NOT hallucinate any additional information or invent extra assumptions. 
Your output should strictly reflect what is implied by the query.

Tasks:
1. Identify potential data sources (e.g., tables, CSVs, logs) that would satisfy the query.
2. Identify the data cleaning, merging, and transformation steps needed.
3. Describe how this data can be structured and incorporated into a robust data pipeline.
4. Provide suggestions for analysis or applications relevant to the query.

Guidelines:
- Assume the user is non-technical. Interpret ambiguous wording carefully, but do not invent missing data.
- Keep your explanations concise, structured, and technical, focusing on the database/data engineering perspective.
- Output only factual, actionable database/data engineering instructions.

User Query (cleaned):
\"{cleaned_query}\"
"""
    response = api_call(prompt, model="gemini-2.5-pro")
    # Do not assume a default path here; caller should call save_to_txt explicitly with the task path.
    return response.strip()
    return response.strip()

def save_to_txt(content: str, filename: str):
    """Save cleaned query text to the explicit filename provided by caller."""
    out_dir = os.path.dirname(filename)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"\n✅ Analysis saved to '{filename}'")

    print("\n--- Content of the saved file ---")
    with open(filename, "r", encoding="utf-8") as f:
        print(f.read())

if __name__ == "__main__":
    raw_query ="hey so i was lookin at the salez data but its all over the place like therez missing vlaues and i think some entris are duplicated or maybe just wrongly formated also i got some file from last month that dosnt match wit the current one so can u mayb clean it up and maybe group it by region or custmer segment or somethin and also we want to find trendz for next quartar like what product selling gud and stuff also the csv is like four files combined so u probbly need to merge them or somthin too"


    print("This module provides clean_text() and save_to_txt(content, filename). Run from the app and pass explicit paths.")