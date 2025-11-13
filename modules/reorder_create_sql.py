#!/usr/bin/env python3
import os
import re
import argparse
import logging
import copy
from typing import List, Dict, Tuple, Set, Optional
from .api_Call import api_call

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CREATE_TABLE_REGEX = re.compile(
    r"(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?[\w_]+`?\.)?[`\"]?([\w_]+)[`\"]?\s*\(.*?\)\s*;)",
    re.IGNORECASE | re.DOTALL,
)

REFERENCES_REGEX = re.compile(
    r"REFERENCES\s+(?:`?[\w_]+`?\.)?[`\"]?([\w_]+)[`\"]?",
    re.IGNORECASE,
)

DROP_TABLE_REGEX = re.compile(
    r"(DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:`?[\w_]+`?\.)?[`\"]?([\w_]+)[`\"]?\s*(?:CASCADE|RESTRICT)?\s*;)",
    re.IGNORECASE | re.DOTALL,
)

def read_sql_file(path: str) -> str:
    if not os.path.exists(path):
        logger.error("Input SQL file not found: %s", path)
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def extract_create_blocks(sql_text: str) -> List[Tuple[str, str]]:
    blocks = []
    for match in CREATE_TABLE_REGEX.finditer(sql_text):
        full = match.group(1).strip()
        name = match.group(2).strip()
        blocks.append((full, name))
    logger.info("Extracted %d CREATE TABLE blocks.", len(blocks))
    return blocks

def extract_drop_blocks(sql_text: str) -> Dict[str, str]:
    drops = {}
    for match in DROP_TABLE_REGEX.finditer(sql_text):
        full = match.group(1).strip()
        name = match.group(2).strip()
        if name not in drops:
            drops[name] = full
    logger.info("Extracted %d DROP TABLE blocks.", len(drops))
    return drops

def extract_references_from_block(full_create_sql: str) -> Set[str]:
    refs = set(m.group(1).strip() for m in REFERENCES_REGEX.finditer(full_create_sql))
    return refs

def build_dependency_graph(blocks: List[Tuple[str, str]]) -> Tuple[Dict[str, Set[str]], Dict[str, str]]:
    graph: Dict[str, Set[str]] = {}
    create_map: Dict[str, str] = {}
    present_tables = {name for _, name in blocks}
    for full, name in blocks:
        create_map[name] = full
        refs = extract_references_from_block(full)
        graph[name] = set(r for r in refs if r != name and r in present_tables)
    return graph, create_map

def topological_sort(input_graph: Dict[str, Set[str]]) -> Tuple[bool, List[str]]:
    graph = copy.deepcopy(input_graph)
    in_degree = {n: 0 for n in graph.keys()}
    for node, deps in graph.items():
        for dep in deps:
            if dep in in_degree:
                in_degree[node] += 1
    queue = [n for n, deg in in_degree.items() if deg == 0]
    ordered = []
    while queue:
        n = queue.pop(0)
        ordered.append(n)
        for m in list(graph.keys()):
            if n in graph[m]:
                graph[m].remove(n)
                if m in in_degree:
                    in_degree[m] -= 1
                    if in_degree[m] == 0:
                        queue.append(m)
    if len(ordered) != len(in_degree):
        return False, ordered
    return True, ordered

def call_llm_for_ordering(create_blocks: List[Tuple[str, str]], drop_map: Dict[str, str] = None) -> Optional[List[str]]:
    drop_map = drop_map or {}
    blocks_text = ""
    for full, name in create_blocks:
        if name in drop_map:
            blocks_text += drop_map[name] + "\n\n"
        blocks_text += full + "\n\n"
    table_list = ", ".join(name for _, name in create_blocks)
    prompt = (
        "You are an assistant that reorders SQL statements so they can be executed sequentially.\n\n"
        "I will provide multiple SQL CREATE TABLE statements (and optional DROP TABLE statements). "
        "Some tables reference others using FOREIGN KEY or REFERENCES clauses. Return ONLY SQL statements "
        "ordered so executing them sequentially will not fail due to missing referenced tables.\n\n"
        "Important:\n"
        " - Do NOT add or remove CREATE TABLE statements. You may produce ALTER TABLE ... ADD CONSTRAINT statements "
        "if necessary to break cycles; include both original CREATE TABLE and the ALTER TABLE statements.\n"
        " - Preserve the original SQL text for CREATE TABLE statements (except for moving them). Do not reformat column "
        "definitions or rename anything.\n"
        " - If you include DROP TABLE statements, keep them before the corresponding CREATE for that table.\n"
        " - Output only the final SQL in a single fenced code block with ```sql ... ``` or raw SQL. No extra commentary.\n\n"
        f"Tables provided: {table_list}\n\n"
        f"SQL:\n{blocks_text}\n\n"
        "Return a single SQL block with the ordered statements."
    )
    try:
        logger.info("Calling external api_call() to reorder %d CREATE statements...", len(create_blocks))
        resp_text = api_call(prompt)
        if not resp_text or not isinstance(resp_text, str):
            logger.error("api_call returned no text.")
            return None
        fenced = re.search(r"```(?:sql\s*)?(.*)```", resp_text, re.DOTALL | re.IGNORECASE)
        if fenced:
            sql_text = fenced.group(1).strip()
        else:
            sql_text = resp_text.strip()
        statements = []
        cur = []
        in_single_quote = False
        for ch in sql_text:
            cur.append(ch)
            if ch == "'":
                if len(cur) >= 2 and cur[-2] == "\\":
                    pass
                else:
                    in_single_quote = not in_single_quote
            if ch == ";" and not in_single_quote:
                stmt = "".join(cur).strip()
                if stmt:
                    statements.append(stmt)
                cur = []
        leftover = "".join(cur).strip()
        if leftover:
            statements.append(leftover)
        logger.info("LLM returned %d statements.", len(statements))
        return statements
    except Exception as e:
        logger.exception("LLM call failed: %s", e)
        return None

def reorder_create_statements(input_sql_path: str) -> Tuple[List[str], Dict[str, str]]:
    raw = read_sql_file(input_sql_path)
    blocks = extract_create_blocks(raw)
    drops = extract_drop_blocks(raw)
    if not blocks:
        logger.error("No CREATE TABLE blocks found in input SQL.")
        return [], {}
    graph, create_map = build_dependency_graph(blocks)
    success, ordered_local = topological_sort(graph)
    if success:
        logger.info("Local topological sort succeeded. Returning %d ordered tables.", len(ordered_local))
        ordered_sql: List[str] = []
        for tbl in ordered_local:
            if tbl in drops:
                ordered_sql.append(drops[tbl])
            ordered_sql.append(create_map[tbl])
        return ordered_sql, create_map
    logger.info("Local topological sort failed (cycle detected). Falling back to LLM ordering.")
    llm_statements = call_llm_for_ordering(blocks, drops)
    if llm_statements:
        return llm_statements, create_map
    logger.warning("LLM ordering failed. Using heuristic fallback order.")
    heuristic_order = sorted(graph.keys(), key=lambda n: len(graph[n]))
    ordered_sql = []
    for tbl in heuristic_order:
        if tbl in drops:
            ordered_sql.append(drops[tbl])
        ordered_sql.append(create_map.get(tbl, ""))
    return ordered_sql, create_map

def write_output_statements(statements: List[str], out_path: str):
    with open(out_path, "w", encoding="utf-8") as f:
        for s in statements:
            s_str = s.strip()
            if s_str and not s_str.endswith(";"):
                s_str += ";"
            f.write(s_str + "\n\n")
    logger.info("Wrote ordered SQL to %s", out_path)

def reorder_create_sql_file(input_path: str, output_path: str) -> List[str]:
    ordered_statements, _ = reorder_create_statements(input_path)
    if not ordered_statements:
        logger.error("No ordered statements produced.")
        return []
    write_output_statements(ordered_statements, output_path)
    logger.info("Done. Ordered SQL written to: %s", output_path)
    return ordered_statements

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Reorder CREATE TABLE statements so they can be executed.")
    p.add_argument("--input", "-i", required=True, help="Path to SQL file with CREATE TABLE statements.")
    p.add_argument("--output", "-o", default="ordered_create_tables.sql", help="Path to write ordered SQL.")
    args = p.parse_args()
    reorder_create_sql_file(args.input, args.output)
