#!/usr/bin/env python3
"""
db.py — Create & populate MySQL DB using schema + insertion code from query.py
 
Place next to query.py and run: python db.py
 
Behavior:
 - Loads query.py INTO A SANDBOX (does not execute its main()).
 - Detects TABLE_SCHEMAS or METADATA_JSON (dynamic detection).
 - Creates tables in MySQL (normalizes basic Postgres bits -> MySQL).
 - If query.py exposes create_and_populate_table or create_table_and_insert_data,
   db.py will call that function for each table, providing a MySQL-compatible
   connection & shims so that psycopg2.sql / extras.execute_values style code
   runs against mysql.connector.
 - Otherwise falls back to internal insertion using CSV or DUMMY_CSV_DATA.
"""
import os
import sys
import io
import json
import csv
import logging
import re
from collections import defaultdict, deque
from urllib.parse import urlparse, unquote
from typing import Dict, Any, Optional, List, Tuple
 
from dotenv import load_dotenv
load_dotenv()
 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
 
# mysql connector
try:
    import mysql.connector
    from mysql.connector import errorcode
except Exception:
    logging.error("Please install mysql-connector-python (pip install mysql-connector-python)")
    raise
 
# -------------------------
# DB config helpers
# -------------------------
def parse_database_url(url: str) -> Dict[str, str]:
    p = urlparse(url)
    return {
        "host": p.hostname,
        "port": str(p.port) if p.port else "3306",
        "database": p.path[1:] if p.path and p.path.startswith("/") else p.path,
        "user": p.username,
        "password": p.password
    }
 
def get_db_config_from_env() -> Dict[str, Optional[str]]:
    for env_name in ("DATABASE_URL", "RAILWAY_DATABASE_URL", "SUPABASE_DB_URL"):
        v = os.getenv(env_name)
        if v:
            logging.info(f"Using DB connection from {env_name}")
            parsed = parse_database_url(v)
            if parsed.get("user"):
                parsed["user"] = unquote(parsed["user"])
            if parsed.get("password"):
                parsed["password"] = unquote(parsed["password"])
            return parsed
    return {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT", "3306"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
    }
 
def validate_db_cfg(cfg: Dict[str, Optional[str]]):
    missing = [k for k,v in cfg.items() if not v]
    if missing:
        raise RuntimeError(f"Missing DB config: {', '.join(missing)} — set DATABASE_URL or DB_HOST/DB_NAME/DB_USER/DB_PASS in .env")
 
def get_mysql_connection():
    cfg = get_db_config_from_env()
    validate_db_cfg(cfg)
    conn = mysql.connector.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 3306)),
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        autocommit=False
    )
    return conn
 
# -------------------------
# Load query.py safely (do NOT run its main)
# -------------------------
def load_query_namespace(path="query.py") -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found in cwd {os.getcwd()}")
    src = open(path, "r", encoding="utf-8").read()
    ns: Dict[str, Any] = {"__name__": "loaded_query_module"}
    exec(compile(src, path, "exec"), ns)
    return ns
 
# -------------------------
# DDL normalization PG -> MySQL (heuristic)
# -------------------------
def normalize_ddl_for_mysql(ddl: str) -> str:
    if not isinstance(ddl, str):
        ddl = str(ddl)
    # strip DEFERRABLE clauses
    ddl = re.sub(r'\bDEFERRABLE\s+INITIALLY\s+\w+\b', '', ddl, flags=re.IGNORECASE)
    # SERIAL -> INT AUTO_INCREMENT
    ddl = re.sub(r'\bSERIAL\b', 'INT AUTO_INCREMENT', ddl, flags=re.IGNORECASE)
    # INTEGER -> INT
    ddl = re.sub(r'\bINTEGER\b', 'INT', ddl, flags=re.IGNORECASE)
    # remove double quotes
    ddl = ddl.replace('"', '')
    # fix accidental concatenations e.g. 'NOT NULLREFERENCES'
    ddl = re.sub(r'NOT\s+NULL\s*REFERENCES', 'NOT NULL REFERENCES', ddl, flags=re.IGNORECASE)
    # trailing semicolon
    ddl = ddl.strip()
    if not ddl.endswith(';'):
        ddl += ';'
    # ensure ENGINE=InnoDB when create table (if absent)
    if re.search(r'CREATE\s+TABLE', ddl, flags=re.IGNORECASE) and 'ENGINE=' not in ddl.upper():
        ddl = ddl.rstrip(';') + ' ENGINE=InnoDB;'
    return ddl
 
# -------------------------
# Fallback table DDL builder from metadata
# -------------------------
def fallback_generate_create_table_sql(table_name: str, columns: list) -> str:
    parts = []
    pks = []
    fks = []
    for col in columns:
        name = col["column"]
        typ = col["sql_type"]
        tupper = typ.upper()
        if tupper == "TIMESTAMP":
            mysql_type = "DATETIME"
        else:
            mysql_type = typ
        parts.append(f"`{name}` {mysql_type}")
        if col.get("is_primary_key"):
            pks.append(f"`{name}`")
        if col.get("is_foreign_key") and col.get("references"):
            ref_table, ref_col = col["references"].split(".")
            fks.append((name, ref_table, ref_col))
    if pks:
        parts.append(f"PRIMARY KEY ({', '.join(pks)})")
    for (n, rt, rc) in fks:
        parts.append(f"FOREIGN KEY (`{n}`) REFERENCES `{rt}`(`{rc}`)")
    ddl = f"CREATE TABLE `{table_name}` (\n  " + ",\n  ".join(parts) + "\n) ENGINE=InnoDB;"
    return ddl
 
# -------------------------
# MySQL insertion helper used by adapter execute_values
# -------------------------
def mysql_execute_values(cursor, table_name: str, columns: List[str], data: List[tuple], page_size: int = 1000):
    if not data:
        return
    cols_sql = ", ".join([f"`{c}`" for c in columns])
    placeholder = "(" + ",".join(["%s"]*len(columns)) + ")"
    insert_sql = f"INSERT INTO `{table_name}` ({cols_sql}) VALUES {placeholder}"
    # Use executemany (cursor is mysql.connector cursor)
    for i in range(0, len(data), page_size):
        batch = data[i:i+page_size]
        # build executemany SQL with single placeholder form
        executemany_sql = f"INSERT INTO `{table_name}` ({cols_sql}) VALUES ({','.join(['%s']*len(columns))})"
        cursor.executemany(executemany_sql, batch)
 
# -------------------------
# Shims so query.py insertion code (psycopg2.sql + extras.execute_values) works
# -------------------------
class SQLShim:
    """A tiny shim replacing psycopg2.sql.SQL / Identifier / Placeholder for string-building."""
    class SQL:
        def __init__(self, s):
            self.s = str(s)
        def __str__(self):
            return self.s
        def format(self, *args, **kwargs):
            # naive format: simply replace, not robust but sufficient for simple usage
            return SQLShim.SQL(self.s.format(*[str(a) for a in args], **{k: str(v) for k,v in kwargs.items()}))
    class Identifier:
        def __init__(self, name):
            self.name = str(name)
        def __str__(self):
            return f"`{self.name}`"
    class Placeholder:
        def __init__(self):
            pass
        def __str__(self):
            return "%s"
 
class ExtrasShim:
    """Shim implementing execute_values using mysql cursor.executemany"""
    @staticmethod
    def execute_values(cur, insert_sql_obj, data, page_size=1000, template=None):
        """
        insert_sql_obj may be a Composed/SQL object or a string.
        The query built by the query.py snippet expects a placeholder %s for each column.
        We'll try to extract table name and columns from the SQL text heuristically.
        """
        # Convert to string if not a string
        q = str(insert_sql_obj)
        # Attempt simple parse: find "INTO <table> (col1, col2) VALUES %s"
        m = re.search(r'INTO\s+[`"]?([A-Za-z0-9_]+)[`"]?\s*\(([^)]+)\)\s+VALUES', q, flags=re.IGNORECASE)
        if not m:
            # fallback: just call executemany with the provided SQL text (mysql expects %s placeholders)
            cur.executemany(q, data)
            return
        table = m.group(1)
        cols = [c.strip().strip('`"') for c in m.group(2).split(",")]
        mysql_execute_values(cur, table, cols, data, page_size=page_size)
 
# -------------------------
# Connection/cursor adapter so query.py's "with conn.cursor() as cur:" works
# -------------------------
class MySQLCursorCtx:
    def __init__(self, cur):
        self._cur = cur
    def __enter__(self):
        return self._cur
    def __exit__(self, exc_type, exc, tb):
        # do not auto-close connection; just close the cursor
        try:
            self._cur.close()
        except Exception:
            pass
        return False
 
class MySQLConnAdapter:
    """
    Wrap mysql.connector connection to provide a .cursor() context manager compatible with:
        with conn.cursor() as cur:
            cur.execute(...)
    Also expose commit()/rollback()/close() so code in query.py can call them.
    """
    def __init__(self, mysql_conn):
        self._conn = mysql_conn
    def cursor(self):
        cur = self._conn.cursor()
        return MySQLCursorCtx(cur)
    def commit(self):
        return self._conn.commit()
    def rollback(self):
        return self._conn.rollback()
    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass
 
# -------------------------
# Insert logic fallback (if query.py functions not present)
# -------------------------
def insert_rows_mysql(conn, table_name: str, cols: List[str], rows: List[tuple], batch_size=1000):
    if not rows:
        return 0
    cur = conn.cursor()
    try:
        cols_sql = ", ".join([f"`{c}`" for c in cols])
        vals_placeholder = ",".join(["%s"]*len(cols))
        executemany_sql = f"INSERT INTO `{table_name}` ({cols_sql}) VALUES ({vals_placeholder})"
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            cur.executemany(executemany_sql, batch)
        conn.commit()
        logging.info(f"Inserted {len(rows)} rows into {table_name}")
        return len(rows)
    except Exception as e:
        conn.rollback()
        logging.exception(f"Insert failed for {table_name}: {e}")
        raise
    finally:
        cur.close()
 
# -------------------------
# Main
# -------------------------
def main():
    logging.info("Loading query.py into a sandbox...")
    ns = load_query_namespace("query.py")
 
    # Detect schemas
    table_schemas: Dict[str, Dict[str, Any]] = {}
    table_order: List[str] = []
 
    if "TABLE_SCHEMAS" in ns and isinstance(ns["TABLE_SCHEMAS"], dict):
        logging.info("Found TABLE_SCHEMAS in query.py — using those DDLs.")
        for t, info in ns["TABLE_SCHEMAS"].items():
            if isinstance(info, str):
                table_schemas[t] = {"ddl": info, "columns": []}
            elif isinstance(info, dict):
                ddl = info.get("ddl") or info.get("create_sql") or info.get("sql") or ""
                cols = info.get("columns") or info.get("cols") or []
                table_schemas[t] = {"ddl": ddl, "columns": cols}
            else:
                table_schemas[t] = {"ddl": str(info), "columns": []}
        table_order = list(table_schemas.keys())
    else:
        # try METADATA_JSON (dynamic name allowed)
        metadata = None
        if "METADATA_JSON" in ns and ns["METADATA_JSON"]:
            try:
                metadata = json.loads(ns["METADATA_JSON"])
                logging.info("Found METADATA_JSON in query.py.")
            except Exception as e:
                logging.warning("Failed to parse METADATA_JSON: %s", e)
                metadata = None
        elif "metadata" in ns:
            metadata = ns["metadata"]
        if not metadata:
            logging.error("No TABLE_SCHEMAS or METADATA_JSON found in query.py. Aborting.")
            sys.exit(1)
        # compute order (topo)
        in_deg = {t: 0 for t in metadata}
        adj = defaultdict(list)
        for t, cols in metadata.items():
            for c in cols:
                if c.get("is_foreign_key") and c.get("references"):
                    ref = c["references"].split(".")[0]
                    if ref in metadata and ref != t:
                        adj[ref].append(t)
                        in_deg[t] += 1
        q = deque([t for t, d in in_deg.items() if d == 0])
        order = []
        while q:
            x = q.popleft()
            order.append(x)
            for n in adj[x]:
                in_deg[n] -= 1
                if in_deg[n] == 0:
                    q.append(n)
        if len(order) != len(metadata):
            logging.warning("Cycle or missing tables; falling back to metadata keys order.")
            table_order = list(metadata.keys())
        else:
            table_order = order
        for t in table_order:
            cols = metadata[t]
            # prefer query.py's generate_create_table_sql if exists
            if "generate_create_table_sql" in ns and callable(ns["generate_create_table_sql"]):
                ddl = ns["generate_create_table_sql"](t, cols)
            else:
                ddl = fallback_generate_create_table_sql(t, cols)
            table_schemas[t] = {"ddl": ddl, "columns": [c["column"] for c in cols]}
 
    # DUMMY_CSV_DATA if present
    dummy_csv_data = ns.get("DUMMY_CSV_DATA") or ns.get("DUMMY_DATA") or ns.get("DUMMY_FILES")
 
    # Prepare mysql connection
    try:
        mysql_conn = get_mysql_connection()
    except Exception as e:
        logging.exception("DB connect failed: %s", e)
        sys.exit(1)
    logging.info("Connected to MySQL")
 
    # Create tables
    cur = mysql_conn.cursor()
    try:
        logging.info("Dropping tables (reverse order if present)...")
        for t in reversed(table_order):
            try:
                logging.info(" - %s", t)
                cur.execute(f"DROP TABLE IF EXISTS `{t}`;")
            except Exception as e:
                logging.warning("Drop table %s failed: %s", t, e)
        mysql_conn.commit()
 
        logging.info("Creating tables...")
        for t in table_order:
            entry = table_schemas.get(t)
            if not entry:
                logging.warning("No schema entry for %s, skip", t)
                continue
            ddl = entry.get("ddl", "")
            # convert to string if Composed-like
            ddl_text = str(ddl)
            ddl_mysql = normalize_ddl_for_mysql(ddl_text)
            logging.info("Creating %s ...", t)
            try:
                cur.execute(ddl_mysql)
                mysql_conn.commit()
                logging.info("Table %s created.", t)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    logging.warning("Table %s already exists, skipping.", t)
                    mysql_conn.rollback()
                else:
                    logging.exception("Error creating table %s: %s", t, err)
                    mysql_conn.rollback()
                    raise
 
        # Prepare shims & adapter for calling query.py insertion functions
        sql_shim = SQLShim
        extras_shim = ExtrasShim
        conn_adapter = MySQLConnAdapter(mysql_conn)
 
        # Inject shims into query.py namespace so its insertion function uses them
        ns['sql'] = sql_shim
        ns['extras'] = extras_shim
 
        # Insertion phase:
        logging.info("Starting insertion phase...")
 
        # If query.py provides create_and_populate_table or create_table_and_insert_data, prefer those
        insert_func = None
        if "create_and_populate_table" in ns and callable(ns["create_and_populate_table"]):
            insert_func = ns["create_and_populate_table"]
            logging.info("Will delegate insertion to query.py:create_and_populate_table")
        elif "create_table_and_insert_data" in ns and callable(ns["create_table_and_insert_data"]):
            insert_func = ns["create_table_and_insert_data"]
            logging.info("Will delegate insertion to query.py:create_table_and_insert_data")
 
        # For each table: collect data_to_insert (prefer top-level variables in query.py if available; else DUMMY_CSV_DATA or disk CSV)
        for t in table_order:
            cols = table_schemas[t].get("columns") or []
            data_list = None
 
            # 1) try to find top-level variable like "<table>_data" or "{table.lower()}_data" in ns
            candidates = [
                f"{t}_data", f"{t.lower()}_data",
                f"{t}Data", f"{t.lower()}Data"
            ]
            found = False
            for name in candidates:
                if name in ns:
                    candidate_val = ns[name]
                    if isinstance(candidate_val, (list, tuple)):
                        data_list = list(candidate_val)
                        found = True
                        logging.info("Using top-level data variable from query.py: %s for table %s", name, t)
                        break
 
            # 2) if not found, check DUMMY_CSV_DATA dict entry (CSV text)
            if not found and dummy_csv_data and isinstance(dummy_csv_data, dict) and t in dummy_csv_data:
                csv_text = dummy_csv_data[t]
                f = io.StringIO(csv_text)
                reader = csv.reader(f)
                header = next(reader)
                if not cols:
                    cols = header
                    table_schemas[t]["columns"] = cols
                rows = []
                for r in reader:
                    vals = [None if (v == "" or (isinstance(v, str) and v.upper() == "NULL")) else v for v in r]
                    rows.append(tuple(vals))
                data_list = rows
                logging.info("Built data_list for %s from DUMMY_CSV_DATA (len=%d).", t, len(rows))
 
            # 3) fallback: on-disk <Table>.csv
            if data_list is None:
                fname = f"{t}.csv"
                if os.path.exists(fname):
                    with open(fname, "r", encoding="utf-8") as f:
                        reader = csv.reader(f)
                        header = next(reader)
                        if not cols:
                            cols = header
                            table_schemas[t]["columns"] = cols
                        rows = []
                        for r in reader:
                            vals = [None if (v == "" or (isinstance(v, str) and v.upper() == "NULL")) else v for v in r]
                            rows.append(tuple(vals))
                        data_list = rows
                        logging.info("Built data_list for %s from %s (len=%d).", t, fname, len(rows))
 
            # 4) If still no data, skip insert
            if not data_list:
                logging.info("No data found for table %s; skipping inserts.", t)
                continue
 
            # If query.py provides insertion function and we have it, call it with adapter
            if insert_func:
                try:
                    # Many implementations accept signature (conn, table_name, table_schema, csv_path_or_data)
                    # We'll prefer calling with (conn_adapter, table_name, table_schema, data_list)
                    logging.info("Delegating insert for %s to query.py function...", t)
                    try:
                        insert_func(conn_adapter, t, table_schemas[t], data_list)
                    except TypeError:
                        # try without passing data_list (some functions read csv inside)
                        try:
                            insert_func(conn_adapter, t, table_schemas[t])
                        except TypeError:
                            # fallback to call with only (conn, table_name)
                            insert_func(conn_adapter, t)
                    logging.info("Delegated insert for %s completed (if function used mysql-compatible ops).", t)
                except Exception as e:
                    logging.exception("create_and_populate_table call failed for %s: %s", t, e)
                    # fallback to internal insertion
                    logging.info("Falling back to internal insert for %s", t)
                    insert_rows_mysql(mysql_conn, t, cols, data_list)
            else:
                # Internal insertion (mysql)
                insert_rows_mysql(mysql_conn, t, cols, data_list)
 
        logging.info("Insertion phase finished.")
    except Exception as e:
        logging.exception("Error during create/insert")
        try:
            mysql_conn.rollback()
            logging.info("Rolled back transaction.")
        except Exception:
            pass
        sys.exit(1)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            mysql_conn.close()
            logging.info("DB connection closed.")
        except Exception:
            pass
 
if __name__ == "__main__":
    main()