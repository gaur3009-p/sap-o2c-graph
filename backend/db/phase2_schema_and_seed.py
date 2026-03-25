import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def get_conn():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


# ✅ RELATIONAL SCHEMA ONLY
RELATIONAL_SCHEMA = """
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS journal_entries CASCADE;
DROP TABLE IF EXISTS billing_docs CASCADE;
DROP TABLE IF EXISTS deliveries CASCADE;
DROP TABLE IF EXISTS sales_order_items CASCADE;
DROP TABLE IF EXISTS sales_orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS addresses CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    full_name TEXT
);

CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    description TEXT
);

CREATE TABLE sales_orders (
    sales_order_id TEXT PRIMARY KEY,
    sold_to_party TEXT REFERENCES customers(customer_id)
);

CREATE TABLE sales_order_items (
    item_id TEXT PRIMARY KEY,
    sales_order_id TEXT REFERENCES sales_orders(sales_order_id),
    material TEXT REFERENCES products(product_id),
    net_amount NUMERIC
);

CREATE TABLE deliveries (
    delivery_id TEXT PRIMARY KEY,
    reference_sales_orders JSONB
);

CREATE TABLE billing_docs (
    billing_doc_id TEXT PRIMARY KEY,
    reference_deliveries JSONB
);

CREATE TABLE journal_entries (
    journal_entry_id TEXT PRIMARY KEY,
    reference_document TEXT
);

CREATE TABLE payments (
    payment_id TEXT PRIMARY KEY,
    clearing_document TEXT
);
"""


def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute(RELATIONAL_SCHEMA)
    conn.commit()


def exec_many(conn, sql, rows):
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)


def seed_data(conn, output_dir):
    nodes = output_dir / "nodes"

    df = pd.read_csv(nodes / "customers.csv")
    rows = [(r.customer_id, r.full_name) for r in df.itertuples()]
    exec_many(conn, "INSERT INTO customers VALUES %s", rows)

    df = pd.read_csv(nodes / "products.csv")
    rows = [(r.product_id, r.description) for r in df.itertuples()]
    exec_many(conn, "INSERT INTO products VALUES %s", rows)

    df = pd.read_csv(nodes / "sales_orders.csv")
    rows = [(r.sales_order_id, r.sold_to_party) for r in df.itertuples()]
    exec_many(conn, "INSERT INTO sales_orders VALUES %s", rows)

    df = pd.read_csv(nodes / "sales_order_items.csv")
    rows = [(r.item_id, r.sales_order_id, r.material, r.net_amount) for r in df.itertuples()]
    exec_many(conn, "INSERT INTO sales_order_items VALUES %s", rows)

    conn.commit()
    log.info("Data seeded")


def run_verification(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customers")
        print("Customers:", cur.fetchone())

        cur.execute("SELECT COUNT(*) FROM sales_orders")
        print("Orders:", cur.fetchone())


def main(output_dir):
    conn = get_conn()

    log.info("Connected to DB")

    create_schema(conn)
    seed_data(conn, output_dir)
    run_verification(conn)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    args = parser.parse_args()

    main(args.output_dir)
