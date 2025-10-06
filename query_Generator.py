import os
import re
import json
import logging
from typing import Optional
import requests
from google import genai

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def _build_prompt(user_query: str) -> str:
    logger.info("Building prompt for user query: %s", user_query)
    template = f"""
You are a Python coding assistant. Whenever a user asks a data-related question, return Python code (in code blocks) that reads, processes, or analyzes the relevant CSV file and interacts with SQLite. Return nothing except the code block. You are provided with CSV files representing database tables. Each CSV file contains a header row with column names followed by data rows. The CSV files are as follows:

customers.csv
Dirctory path: `customers.csv`

| Column Name  | Data Type     |
| ------------ | ------------- |
| customerID   | string        |
| companyName  | string        |
| contactName  | string        |
| contactTitle | string        |
| address      | string        |
| city         | string        |
| region       | string / null |
| postalCode   | string / null |
| country      | string        |
| phone        | string        |
| fax          | string / null |

order_details.csv
Dirctory path: `order_details.csv`

| Column Name | Data Type |
| ----------- | --------- |
| orderID     | int       |
| productID   | int       |
| unitPrice   | float     |
| quantity    | int       |
| discount    | float     |

orders.csv
Dirctory path: `orders.csv`

| Column Name    | Data Type     |
| -------------- | ------------- |
| orderID        | int           |
| customerID     | string        |
| employeeID     | int           |
| orderDate      | datetime      |
| requiredDate   | datetime      |
| shippedDate    | datetime      |
| shipVia        | int           |
| freight        | float         |
| shipName       | string        |
| shipAddress    | string        |
| shipCity       | string        |
| shipRegion     | string / null |
| shipPostalCode | string / null |
| shipCountry    | string        |

products.csv
Dirctory path: `products.csv`

| Column Name     | Data Type          |
| --------------- | ------------------ |
| productID       | int                |
| productName     | string             |
| supplierID      | int                |
| categoryID      | int                |
| quantityPerUnit | string             |
| unitPrice       | float              |
| unitsInStock    | int                |
| unitsOnOrder    | int                |
| reorderLevel    | int                |
| discontinued    | int (boolean-like) |

Rules & Behavior:

Generate SQL or PL/SQL queries to perform requested operations.
Provide Python code that executes these SQL queries on SQLite.
For SELECT queries, return code to fetch results. do not create tables from beginning.
For INSERT, UPDATE, DELETE, return code to execute and commit changes.
For JOIN queries, generate SQL JOIN statements and Python code.
To create a table from a CSV file:
Use pandas to read CSV.
Insert NULL for empty values.
Commit first, then call df.to_sql(...).
In case of a create table query build the code around this code     
if __name__ == "__main__":
    tables_to_create = [
        ("customers", "customers.csv", customers_schema, customers_pk),
        ... (other tables) ...
    ]

    for table_tuple in tables_to_create:
        table_name, csv_file, schema, pk = table_tuple[:4]
        fk = table_tuple[4] if len(table_tuple) > 4 else None
        datetime_cols = table_tuple[5] if len(table_tuple) > 5 else None
        create_table_and_insert_data(table_name, csv_file, schema, pk, fk, datetime_cols)

Python & SQLite Requirements:
Always start with:
```
DB_FILE = os.getenv("DB_FILE", "DataBase.db")```


Connect with:
```
conn = sqlite3.connect(DB_FILE, timeout=30, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")```
Wrap database operations in try/except/finally.
Call conn.commit() immediately after CREATE, DROP, INSERT, UPDATE, DELETE.
Always call conn.close() in finally.
On sqlite3.OperationalError with "locked", retry 3 times with exponential backoff (0.1s, 0.2s, 0.4s), then re-raise if still failing.
Never keep multiple connections open at once. Do not reuse connections across tables.
Use only SQLite types: INTEGER, REAL, TEXT, BLOB.
Convert datetime columns using pd.to_datetime(..., errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S") before inserting into SQLite.
Define nullable columns explicitly (TEXT NULL / INTEGER NULL).
Define foreign keys separately:
FOREIGN KEY (col) REFERENCES table(col)
Do **not** to enforce self-referencing FKs immediately. Instead, add DEFERRABLE INITIALLY DEFERRED to the FK constraint.
No trailing commas before closing parentheses.

Restrictions:
Return only executable Python code.
No explanations, comments, or Markdown outside code blocks.
Do not execute shell commands or use external libraries except pandas.
Do not create synthetic data.
If a query requires filesystem access or unsupported libraries:
print("This kind of query cannot be executed.")
   User Query:
 {user_query}

"""
    return template


def _read_gemini_key_from_dotenv(path: str = ".env") -> Optional[str]:
    logger.info("Reading Gemini API key from %s", path)
    if not os.path.exists(path):
        logger.warning("Env file %s does not exist", path)
        return None
    try:
        raw = open(path, "r", encoding="utf-8").read()
    except Exception as e:
        logger.error("Error reading env file: %s", e)
        return None
    m = re.search(r"GEMINI[_-]?API[_-]?KEY\s*[:=]\s*\"?(?P<key>[A-Za-z0-9\-_.]+)\"?", raw)
    if m:
        logger.info("Found Gemini API key via first regex")
        return m.group("key")
    m2 = re.search(r"GEMINI[_-]?API[_-]?KEY\s*[:=]\s*(?P<key>.+)", raw)
    if m2:
        key = m2.group("key").strip().strip('"').strip("'")
        logger.info("Found Gemini API key via second regex")
        return key
    logger.warning("Gemini API key not found in env file")
    return None


def generate_and_send(user_query: str, model: str = "gemini-2.5-flash", temperature: float = 0.2) -> str:
    logger.info("Starting generate_and_send with model=%s and temperature=%s", model, temperature)
    gemini_key = _read_gemini_key_from_dotenv()
    if not gemini_key:
        logger.error("Gemini API key not found")
        raise RuntimeError("Gemini API key not found in .env (searched for GEMINI_API_KEY).")

    model_name = model
    prompt = _build_prompt(user_query)
    logger.debug("Prompt length: %d", len(prompt))

    if genai is not None:
        logger.info("Using Google genai client")
        try:
            client = genai.Client(api_key=gemini_key)
            resp = client.models.generate_content(model=model_name, contents=prompt)
            text = getattr(resp, "text", None) or str(resp)
            logger.info("Received response from genai client (length=%d)", len(text))
            return text
        except Exception as e:
            logger.error("Gemini client error: %s", e)
            raise RuntimeError(f"Gemini client error: {e}")

    endpoint = f"https://generativelanguage.googleapis.com/v1beta2/models/{model_name}:generateText"
    body = {"prompt": {"text": prompt}, "temperature": temperature, "maxOutputTokens": 10000}
    try:
        logger.info("Calling Gemini REST API at endpoint: %s", endpoint)
        resp = requests.post(endpoint, params={"key": gemini_key}, json=body, timeout=30)
        logger.info("Received response with status code %d", resp.status_code)
    except Exception as e:
        logger.error("Error calling Gemini endpoint: %s", e)
        raise RuntimeError(f"Error calling Gemini endpoint: {e}")

    if resp.status_code != 200:
        logger.error("Gemini API error %d: %s", resp.status_code, resp.text)
        raise RuntimeError(f"Gemini API error {resp.status_code}: {resp.text}")

    j = resp.json()
    text = ""
    if isinstance(j, dict):
        candidates = j.get("candidates") or j.get("outputs") or []
        if candidates and isinstance(candidates, list):
            first = candidates[0]
            text = first.get("output") or first.get("content") or first.get("text") or ""
            logger.info("Extracted text from response candidates")
        else:
            text = j.get("output") or j.get("result") or ""
            logger.info("Extracted text from response root keys")
    else:
        logger.warning("Unexpected response format, dumping JSON")
        text = json.dumps(j)

    if not text:
        logger.warning("No text content found in API response, dumping JSON")
        text = json.dumps(j)

    logger.info("Returning generated text (length=%d)", len(text))
    return text


if __name__ == "__main__":
    test_query = "print a hello world program and also describe what it does"
    logger.info("Running main test with query: %s", test_query)
    try:
        response = generate_and_send(test_query)
        print(response)
    except Exception as e:
        logger.error("Error in main: %s", e)
        print("Error:", e)
