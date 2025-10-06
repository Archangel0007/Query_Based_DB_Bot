import json
from typing import List, Dict, Optional

from query_Generator import generate_and_send


def _default_dataset_description() -> str:
    return (
        "The dataset is a classic retail business database with tables such as customers, orders, products, "
        "suppliers, employees, shippers, regions, and categories. Each table contains structured business data. "
        "For example: customers have names and addresses, orders link customers to products, employees manage orders, "
        "and order details include quantities and prices."
    )


def summarize_execution_results(exec_results: List[Dict], blocks: List[Dict], user_query: str,
                                dataset_description: Optional[str] = None,
                                model: str = "gemini-1.5", temperature: float = 0.2) -> str:
    if dataset_description is None:
        dataset_description = _default_dataset_description()

    payload = {
        "user_query": user_query,
        "num_code_blocks": len(blocks or []),
        "code_blocks": blocks or [],
        "exec_results": exec_results or [],
    }

    prompt = (
        f"""You are an expert data explainer. A Python/SQL-based assistant executed some generated queries on a structured dataset. 
        Your job: produce a short, clear, non-technical summary that a normal person (little programming or database background) can understand.
        
        Dataset summary: {dataset_description}
        
        Instructions:
        -If there is any kind of error in the execution context then directly just return "Code Execution Failed, Please check the dataset and modify the query accordingly.".
        -If the execution was flawless then write the result in text format in 2 or 3 lines. Follow this with up to 4 short bullet points of inferences that can be drawn. 
        -If more results are present, you may use up to 4 extra bullet points. 
        -Keep language simple and business-oriented (customers, sales, employees, orders, etc.).
        -If You have a list of values or a table in the output, return it in the same format so that it easily rendered.Like a table of values should be returned as a table.
        -If the output is a single value, return it as a single value.
        -If the output is empty or null, return "No such results found".
        -Do not mention programming, technical terms, or execution details. 
        -Do not mention file names or paths. 
        -No code should be generated as part of the output.
        -The entire output must stay within 500 words.

        Execution context (JSON):
        {json.dumps(payload, indent=2)}
        """
    )

    summary = generate_and_send(prompt, model=model, temperature=temperature)
    return summary


if __name__ == "__main__":
    # quick local test scaffold
    sample_exec = [{"block_index": 0, "result": {"returncode": 0, "stdout": "Top 3 customers by number of orders: ALFKI, ANATR, ANTON", "stderr": ""}}]
    sample_blocks = [{"language": "sql", "code": "SELECT customerID, COUNT(*) FROM orders GROUP BY customerID LIMIT 3;"}]
    print(summarize_execution_results(sample_exec, sample_blocks, "Find top 3 customers by orders"))
