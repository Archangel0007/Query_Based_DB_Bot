import os
import sys
import logging
import json
import time
from datetime import datetime, timedelta
 
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.errors import OperationalError, DeadlockDetected
from psycopg2 import sql, extras
from dotenv import load_dotenv
 
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
 
def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT", "5432")
    )
    return conn
 
if not all(os.getenv(v) for v in ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASS"]):
    logging.error("Please set DB_HOST, DB_NAME, DB_USER, DB_PASS in your .env")
    sys.exit(1)
 
def execute_with_retry(conn, sql_query, params=None, retries=3, initial_delay=0.1):
    delay = initial_delay
    for i in range(retries):
        try:
            with conn.cursor() as cur:
                if isinstance(sql_query, sql.Composed):
                    cur.execute(sql_query, params)
                else:
                    cur.execute(sql_query, params)
            return
        except (OperationalError, DeadlockDetected) as e:
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
    drop_table_sql = sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(table_name))
    execute_with_retry(conn, drop_table_sql)
    logging.info(f"Dropped table {table_name}.")
 
    create_table_sql = sql.SQL(table_schema["ddl"])
    execute_with_retry(conn, create_table_sql)
    logging.info(f"Created table {table_name}.")
 
    if data_to_insert:
        columns = table_schema["columns"]
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        if returning_col:
            insert_sql = sql.SQL("{} RETURNING {}").format(insert_sql, sql.Identifier(returning_col))
 
        batch_size = 1000
        total_inserted = 0
        returned_ids = []
 
        with conn.cursor() as cur:
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                if not batch:
                    continue
 
                if returning_col:
                    cur.executemany(insert_sql, batch)
                    returned_ids.extend([row[0] for row in cur.fetchall()])
                else:
                    extras.execute_values(cur, insert_sql, batch)
               
                total_inserted += len(batch)
                logging.info(f"Inserted {total_inserted}/{len(data_to_insert)} rows into {table_name}.")
        return returned_ids
    return []
 
TABLE_SCHEMAS = {
    "Dim_Customer": {
        "ddl": """
            CREATE TABLE Dim_Customer (
                customer_id INTEGER PRIMARY KEY,
                customer_name VARCHAR(255) NOT NULL,
                customer_email VARCHAR(255) UNIQUE NOT NULL,
                customer_phone VARCHAR(20)
            )
        """,
        "columns": ["customer_id", "customer_name", "customer_email", "customer_phone"]
    },
    "Dim_Product": {
        "ddl": """
            CREATE TABLE Dim_Product (
                product_id INTEGER PRIMARY KEY,
                product_name VARCHAR(255) NOT NULL,
                product_category VARCHAR(100),
                product_price DECIMAL(10, 2)
            )
        """,
        "columns": ["product_id", "product_name", "product_category", "product_price"]
    },
    "Dim_Order_Date": {
        "ddl": """
            CREATE TABLE Dim_Order_Date (
                order_date TIMESTAMP PRIMARY KEY
            )
        """,
        "columns": ["order_date"]
    },
    "Shipping": {
        "ddl": """
            CREATE TABLE Shipping (
                shipping_id SERIAL PRIMARY KEY,
                address VARCHAR(255) NOT NULL,
                city VARCHAR(100) NOT NULL,
                state VARCHAR(100) NOT NULL,
                zip VARCHAR(10) NOT NULL
            )
        """,
        "columns": ["address", "city", "state", "zip"]
    },
    "PromoCodes": {
        "ddl": """
            CREATE TABLE PromoCodes (
                promo_code_id SERIAL PRIMARY KEY,
                code_string VARCHAR(50) UNIQUE NOT NULL,
                discount_percentage DECIMAL(5, 4) NOT NULL,
                start_date TIMESTAMP NOT NULL,
                end_date TIMESTAMP NOT NULL
            )
        """,
        "columns": ["code_string", "discount_percentage", "start_date", "end_date"]
    },
    "TaxRates": {
        "ddl": """
            CREATE TABLE TaxRates (
                tax_rate_id SERIAL PRIMARY KEY,
                region VARCHAR(100) NOT NULL,
                rate DECIMAL(5, 4) NOT NULL,
                start_date TIMESTAMP NOT NULL,
                end_date TIMESTAMP,
                UNIQUE (region, start_date)
            )
        """,
        "columns": ["region", "rate", "start_date", "end_date"]
    },
    "Fact_Order": {
        "ddl": """
            CREATE TABLE Fact_Order (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL DEFERRABLE INITIALLY DEFERRED REFERENCES Dim_Customer(customer_id),
                product_id INTEGER NOT NULL DEFERRABLE INITIALLY DEFERRED REFERENCES Dim_Product(product_id),
                order_date TIMESTAMP NOT NULL DEFERRABLE INITIALLY DEFERRED REFERENCES Dim_Order_Date(order_date),
                shipping_id INTEGER NOT NULL DEFERRABLE INITIALLY DEFERRED REFERENCES Shipping(shipping_id),
                promo_code_id INTEGER DEFERRABLE INITIALLY DEFERRED REFERENCES PromoCodes(promo_code_id),
                tax_rate_id INTEGER DEFERRABLE INITIALLY DEFERRED REFERENCES TaxRates(tax_rate_id),
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                item_price DECIMAL(10, 2) NOT NULL,
                total_price DECIMAL(10, 2) NOT NULL,
                payment_status VARCHAR(50) NOT NULL CHECK (payment_status IN ('Pending', 'Paid', 'Refunded', 'Failed')),
                order_status VARCHAR(50) NOT NULL CHECK (order_status IN ('Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled')),
                discount DECIMAL(10, 2)
            )
        """,
        "columns": [
            "order_id", "customer_id", "product_id", "order_date", "shipping_id",
            "promo_code_id", "tax_rate_id", "quantity", "item_price", "total_price",
            "payment_status", "order_status", "discount"
        ]
    }
}
 
TABLE_CREATION_ORDER = [
    "Dim_Customer",
    "Dim_Product",
    "Dim_Order_Date",
    "Shipping",
    "PromoCodes",
    "TaxRates",
    "Fact_Order"
]
 
def main():
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = False
        logging.info(f"Connected to database: {os.getenv('DB_NAME')} on {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}")
 
        metadata_json = [
          {
            "file_name": "orders",
            "directory_path": "Run_Space\\orders.csv",
            "columns": [
              {
                "column_name": "order_id",
                "data_type": "number",
                "has_duplicates": False
              },
              {
                "column_name": "customer_id",
                "data_type": "number",
                "has_duplicates": False
              },
              {
                "column_name": "customer_name",
                "data_type": "string",
                "has_duplicates": False
              },
              {
                "column_name": "customer_email",
                "data_type": "string",
                "has_duplicates": False
              },
              {
                "column_name": "customer_phone",
                "data_type": "string",
                "has_duplicates": False
              },
              {
                "column_name": "product_id",
                "data_type": "number",
                "has_duplicates": True
              },
              {
                "column_name": "product_name",
                "data_type": "string",
                "has_duplicates": True
              },
              {
                "column_name": "product_category",
                "data_type": "string",
                "has_duplicates": True
              },
              {
                "column_name": "product_price",
                "data_type": "number",
                "has_duplicates": True
              },
              {
                "column_name": "quantity",
                "data_type": "number",
                "has_duplicates": True
              },
              {
                "column_name": "order_date",
                "data_type": "string",
                "has_duplicates": False
              },
              {
                "column_name": "shipping_address",
                "data_type": "string",
                "has_duplicates": False
              },
              {
                "column_name": "shipping_city",
                "data_type": "string",
                "has_duplicates": False
              },
              {
                "column_name": "shipping_state",
                "data_type": "string",
                "has_duplicates": True
              },
              {
                "column_name": "shipping_zip",
                "data_type": "number",
                "has_duplicates": False
              },
              {
                "column_name": "total_price",
                "data_type": "number",
                "has_duplicates": False
              },
              {
                "column_name": "payment_status",
                "data_type": "string",
                "has_duplicates": True
              },
              {
                "column_name": "order_status",
                "data_type": "string",
                "has_duplicates": True
              },
              {
                "column_name": "discount",
                "data_type": "number",
                "has_duplicates": False
              },
              {
                "column_name": "sales_tax",
                "data_type": "number",
                "has_duplicates": False
              },
              {
                "column_name": "promo_code",
                "data_type": "string / null",
                "has_duplicates": True
              }
            ]
          }
        ]
 
        csv_file_path = metadata_json[0]["directory_path"]
       
        logging.info(f"Loading data from {csv_file_path}...")
        orders_df = pd.read_csv(csv_file_path)
        orders_df = orders_df.replace({np.nan: None})
        orders_df["order_date"] = pd.to_datetime(orders_df["order_date"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
 
        logging.info("Preparing dimension data...")
 
        customer_data = orders_df[['customer_id', 'customer_name', 'customer_email', 'customer_phone']].drop_duplicates(subset=['customer_id']).values.tolist()
        create_and_populate_table(conn, "Dim_Customer", TABLE_SCHEMAS["Dim_Customer"], customer_data)
 
        product_data = orders_df[['product_id', 'product_name', 'product_category', 'product_price']].drop_duplicates(subset=['product_id']).values.tolist()
        create_and_populate_table(conn, "Dim_Product", TABLE_SCHEMAS["Dim_Product"], product_data)
 
        order_date_data = orders_df[['order_date']].drop_duplicates(subset=['order_date']).dropna().values.tolist()
        create_and_populate_table(conn, "Dim_Order_Date", TABLE_SCHEMAS["Dim_Order_Date"], order_date_data)
 
        shipping_df = orders_df[['shipping_address', 'shipping_city', 'shipping_state', 'shipping_zip']].drop_duplicates().dropna(how='all')
        shipping_data_for_insert = shipping_df.values.tolist()
        shipping_ids = create_and_populate_table(conn, "Shipping", TABLE_SCHEMAS["Shipping"], shipping_data_for_insert, returning_col="shipping_id")
       
        shipping_map = {}
        for idx, row in enumerate(shipping_df.itertuples(index=False)):
            shipping_map[tuple(row)] = shipping_ids[idx]
 
        promo_codes_series = orders_df['promo_code'].dropna().drop_duplicates()
        promo_code_data = []
        now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        future_str = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%S")
        for code in promo_codes_series:
            promo_code_data.append((code, 0.10, now_str, future_str))
       
        promo_ids = create_and_populate_table(conn, "PromoCodes", TABLE_SCHEMAS["PromoCodes"], promo_code_data, returning_col="promo_code_id")
       
        promo_map = {}
        for idx, code_tuple in enumerate(promo_code_data):
            promo_map[code_tuple[0]] = promo_ids[idx]
 
        tax_rate_data = [("Default Region", 0.05, now_str, None)]
        tax_ids = create_and_populate_table(conn, "TaxRates", TABLE_SCHEMAS["TaxRates"], tax_rate_data, returning_col="tax_rate_id")
        default_tax_rate_id = tax_ids[0] if tax_ids else None
 
        logging.info("Preparing Fact_Order data...")
        fact_order_data = []
        for index, row in orders_df.iterrows():
            shipping_key = (row['shipping_address'], row['shipping_city'], row['shipping_state'], row['shipping_zip'])
            shipping_id = shipping_map.get(shipping_key)
 
            promo_code_id = promo_map.get(row['promo_code'])
 
            tax_rate_id = default_tax_rate_id if row['sales_tax'] is not None and row['sales_tax'] > 0 else None
 
            fact_order_data.append((
                row['order_id'],
                row['customer_id'],
                row['product_id'],
                row['order_date'],
                shipping_id,
                promo_code_id,
                tax_rate_id,
                row['quantity'],
                row['product_price'],
                row['total_price'],
                row['payment_status'],
                row['order_status'],
                row['discount']
            ))
       
        create_and_populate_table(conn, "Fact_Order", TABLE_SCHEMAS["Fact_Order"], fact_order_data)
 
        conn.commit()
        logging.info("All tables created and data inserted successfully!")
 
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        if conn:
            conn.rollback()
            logging.info("Transaction rolled back.")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")
 
if __name__ == "__main__":
    main()
 