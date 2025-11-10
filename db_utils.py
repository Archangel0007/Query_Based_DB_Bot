import os
import time
import logging
import socket
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv(dotenv_path='../.env')
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Check if environment variables are loaded correctly
from dotenv import load_dotenv
import os
import mysql.connector
import logging
def _split_host_and_port(host_raw):
    host_raw = (host_raw or "").strip()
    if not host_raw:
        return None, None
    # bracketed IPv6: [::1]:3306
    if host_raw.startswith("[") and "]:" in host_raw:
        host_part, port_part = host_raw.split("]:", 1)
        host = host_part[1:]
        return host, int(port_part) if port_part.isdigit() else None
    # single ":" likely hostname:port
    if host_raw.count(":") == 1:
        host_part, port_part = host_raw.rsplit(":", 1)
        if port_part.isdigit():
            return host_part, int(port_part)
    return host_raw, None

def get_db_connection(retries: int = 3, backoff: float = 1.0):
    
    """
    Robust connector:
      - Accepts DB_HOST or DB_HOST:PORT (and [ipv6]:port)
      - Resolves host and attempts direct connects to each resolved IP (IPv6/IPv4)
      - Tries both use_pure True/False implementations
      - Supports optional DB_SSL_CA (path to CA file)
    """
    load_dotenv(dotenv_path=".env")
    host_raw = os.getenv("DB_HOST", "")
    host_parsed, host_port = _split_host_and_port(host_raw)
    port_env = os.getenv("DB_PORT")
    port = int(port_env) if port_env and port_env.isdigit() else (host_port or 3306)

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    database = os.getenv("DB_NAME")
    ssl_ca = os.getenv("DB_SSL_CA")  # optional path to CA pem for managed DBs

    logging.info("DB connect params: host=%s port=%s user=%s db=%s ssl_ca=%s", host_parsed, port, user, database, bool(ssl_ca))
    print("DB_HOST:", host_parsed, "DB_PORT:", port, "DB_USER:", user, "DB_NAME:", database, "DB_SSL_CA set:", bool(ssl_ca))

    # Resolve host to addresses
    try:
        addrs = socket.getaddrinfo(host_parsed, port, proto=socket.IPPROTO_TCP)
        # deduplicate (family, ip) tuples preserving order
        resolved = []
        for a in addrs:
            fam = a[0]
            sockaddr = a[4]
            ip = sockaddr[0]
            if (fam, ip) not in resolved:
                resolved.append((fam, ip))
        logging.info("Resolved addresses: %s", resolved)
    except socket.gaierror as e:
        logging.error("DNS resolution failed for host %s:%s -> %s", host_parsed, port, e)
        raise

    last_exc = None
    for attempt in range(1, max(1, retries) + 1):
        # try each resolved IP, and for each try both use_pure implementations
        for fam, ip in resolved:
            for use_pure in (True, False):
                try:
                    logging.info("Attempt %d: trying connect to %s (family=%s) use_pure=%s", attempt, ip, "AF_INET6" if fam==socket.AF_INET6 else "AF_INET", use_pure)
                    connect_kwargs = dict(
                        host=ip,
                        port=port,
                        user=user,
                        password=password,
                        database=database,
                        connection_timeout=8,
                        use_pure=use_pure
                    )
                    if ssl_ca:
                        connect_kwargs.update({"ssl_ca": ssl_ca, "ssl_verify_cert": True})
                    conn = mysql.connector.connect(**connect_kwargs)
                    if conn.is_connected():
                        logging.info("Connected to DB %s (ip=%s use_pure=%s)", database, ip, use_pure)
                        return conn
                    else:
                        raise Error("Connector returned but is_connected() is False")
                except Exception as e:
                    last_exc = e
                    logging.warning("Connect failed to %s (use_pure=%s): %s", ip, use_pure, e)
                    # continue to next ip/use_pure
        # backoff before next attempt
        logging.info("Backoff %s seconds before next attempt", backoff * attempt)
        time.sleep(backoff * attempt)

    logging.error("All connection attempts failed: %s", last_exc)
    raise last_exc
def execute_with_retry(conn, sql_query, params=None, retries=3, initial_delay=0.1):
    """
    Executes a given SQL query with retry mechanism on failure.
    Retries the execution in case of errors like deadlocks or connection issues.
    """
    load_dotenv(dotenv_path='../.env')
    delay = initial_delay
    for i in range(retries):
        try:
            with conn.cursor() as cur:
                cur.execute(sql_query, params)
                conn.commit()  # Make sure changes are committed
            return
        except (Error) as e:
            conn.rollback()
            logging.warning(f"Attempt {i+1}/{retries} failed due to {e}. Retrying in {delay:.1f}s...")
            time.sleep(delay)
            delay *= 2
        except Exception as e:
            conn.rollback()
            logging.error(f"Error executing query: {e}")
            raise
    raise Exception(f"Failed to execute query after {retries} attempts.")


def create_and_populate_table(conn, table_name, table_schema, data_to_insert=None, returning_col=None):
    """
    Creates and populates a table with data.
    Drops the table if it exists, creates a new one, and inserts the provided data.
    """
    load_dotenv(dotenv_path='../.env')
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    execute_with_retry(conn, drop_table_sql)
    logging.info(f"Dropped table {table_name}.")

    create_table_sql = table_schema["ddl"]
    execute_with_retry(conn, create_table_sql)
    logging.info(f"Created table {table_name}.")

    if data_to_insert:
        columns = table_schema["columns"]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        
        if returning_col:
            # MySQL doesn't support RETURNING like PostgreSQL; handle this differently.
            insert_sql += f" RETURNING {returning_col}"

        batch_size = 1000
        total_inserted = 0
        returned_ids = []

        with conn.cursor() as cur:
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                if not batch:
                    continue

                cur.executemany(insert_sql, batch)
                if returning_col:
                    cur.execute(insert_sql, batch)
                    returned_ids.extend([row[0] for row in cur.fetchall()])
                else:
                    conn.commit()  # Commit the batch insertion

                total_inserted += len(batch)
                logging.info(f"Inserted {total_inserted}/{len(data_to_insert)} rows into {table_name}.")
        
        return returned_ids
    return []

