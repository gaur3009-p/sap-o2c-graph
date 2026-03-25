"""
Phase 2: PostgreSQL + Apache AGE — Schema Definition & Graph Seeding
=====================================================================
Reads the node/edge CSVs produced by phase1_ingest.py and loads them
into two layers:

  1. Relational layer  — standard PostgreSQL tables with proper types,
                         constraints, and indexes.  Used for analytics,
                         aggregations, and fallback SQL queries.

  2. Graph layer       — Apache AGE (graph extension) vertices and edges
                         with the same data, enabling Cypher traversals.

Why both?
---------
  • Relational tables are fast for GROUP BY / aggregate queries
    (e.g. "which products appear in the most billing docs").
  • AGE Cypher is expressive for path queries
    (e.g. "trace the full flow from order 740506 to payment").
  • Having both lets the LLM query engine choose the right tool per question.

Prerequisites
-------------
  PostgreSQL 15+ with Apache AGE extension installed.
  See: https://age.apache.org/age-manual/master/intro/setup.html

  pip install psycopg2-binary pandas python-dotenv

Usage
-----
  # set env vars or create a .env file:
  export DB_HOST=localhost
  export DB_PORT=5432
  export DB_NAME=o2c_graph
  export DB_USER=postgres
  export DB_PASSWORD=yourpassword

  python phase2_schema_and_seed.py --output-dir ./output
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

GRAPH_NAME = "o2c"   # AGE graph name — used in every Cypher call


# ── DB connection ─────────────────────────────────────────────────────────────

def get_conn():
    """Return a psycopg2 connection using env vars (or .env via python-dotenv)."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "o2c_graph"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def exec(conn, sql: str, params=None) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, params)


def exec_many(conn, sql: str, rows: list) -> None:
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)


# ── Step 1: Install AGE extension & create graph ──────────────────────────────

BOOTSTRAP_SQL = """
CREATE EXTENSION IF NOT EXISTS age;
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
"""

def bootstrap_age(conn) -> None:
    log.info("Bootstrapping Apache AGE extension …")
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS age;")
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, \"$user\", public;")
        # Create the graph (idempotent)
        cur.execute(
            "SELECT create_graph(%s) WHERE NOT EXISTS "
            "(SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s);",
            (GRAPH_NAME, GRAPH_NAME),
        )
    conn.commit()
    log.info("  AGE graph '%s' ready", GRAPH_NAME)


# ── Step 2: Relational schema ─────────────────────────────────────────────────

RELATIONAL_SCHEMA = """
-- Drop and recreate all tables in dependency order
DROP TABLE IF EXISTS payments             CASCADE;
DROP TABLE IF EXISTS journal_entries      CASCADE;
DROP TABLE IF EXISTS billing_doc_items_flat CASCADE;
DROP TABLE IF EXISTS billing_docs         CASCADE;
DROP TABLE IF EXISTS delivery_items_flat  CASCADE;
DROP TABLE IF EXISTS deliveries           CASCADE;
DROP TABLE IF EXISTS sales_order_items    CASCADE;
DROP TABLE IF EXISTS sales_orders         CASCADE;
DROP TABLE IF EXISTS products             CASCADE;
DROP TABLE IF EXISTS addresses            CASCADE;
DROP TABLE IF EXISTS customers            CASCADE;

-- ── Customers ────────────────────────────────────────────────────────────────
CREATE TABLE customers (
    customer_id         TEXT PRIMARY KEY,
    full_name           TEXT,
    bp_category         TEXT,
    is_blocked          BOOLEAN DEFAULT FALSE,
    is_archived         BOOLEAN DEFAULT FALSE,
    company_code        TEXT,
    currency            TEXT,
    payment_terms       TEXT,
    sales_organization  TEXT,
    creation_date       DATE
);

-- ── Addresses ────────────────────────────────────────────────────────────────
CREATE TABLE addresses (
    address_id      TEXT PRIMARY KEY,
    customer_id     TEXT REFERENCES customers(customer_id),
    street          TEXT,
    city            TEXT,
    postal_code     TEXT,
    region          TEXT,
    country         TEXT,
    timezone        TEXT,
    valid_from      DATE,
    valid_to        DATE    -- NULL means open-ended
);

-- ── Products ─────────────────────────────────────────────────────────────────
CREATE TABLE products (
    product_id      TEXT PRIMARY KEY,
    product_type    TEXT,
    description     TEXT,
    old_product_id  TEXT,
    product_group   TEXT,
    base_unit       TEXT,
    gross_weight    NUMERIC,
    net_weight      NUMERIC,
    weight_unit     TEXT,
    division        TEXT,
    industry_sector TEXT,
    is_deleted      BOOLEAN DEFAULT FALSE,
    creation_date   DATE
);

-- ── Sales Orders ─────────────────────────────────────────────────────────────
CREATE TABLE sales_orders (
    sales_order_id          TEXT PRIMARY KEY,
    order_type              TEXT,
    sold_to_party           TEXT REFERENCES customers(customer_id),
    sales_organization      TEXT,
    distribution_channel    TEXT,
    division                TEXT,
    total_net_amount        NUMERIC,
    currency                TEXT,
    creation_date           DATE,
    pricing_date            DATE,
    requested_delivery_date DATE,
    confirmed_delivery_date DATE,
    delivery_status         TEXT,
    billing_status          TEXT,
    delivery_block          TEXT,
    billing_block           TEXT,
    payment_terms           TEXT,
    incoterms               TEXT
);
CREATE INDEX idx_so_customer   ON sales_orders(sold_to_party);
CREATE INDEX idx_so_created    ON sales_orders(creation_date);
CREATE INDEX idx_so_del_status ON sales_orders(delivery_status);

-- ── Sales Order Items ─────────────────────────────────────────────────────────
CREATE TABLE sales_order_items (
    item_id         TEXT PRIMARY KEY,  -- salesOrder_itemNumber
    sales_order_id  TEXT REFERENCES sales_orders(sales_order_id),
    item_number     TEXT,
    material        TEXT REFERENCES products(product_id),
    item_category   TEXT,
    requested_qty   NUMERIC,
    qty_unit        TEXT,
    net_amount      NUMERIC,
    currency        TEXT,
    material_group  TEXT,
    plant           TEXT,
    storage_location TEXT,
    rejection_reason TEXT,
    billing_block   TEXT
);
CREATE INDEX idx_soi_order    ON sales_order_items(sales_order_id);
CREATE INDEX idx_soi_material ON sales_order_items(material);

-- ── Deliveries ────────────────────────────────────────────────────────────────
CREATE TABLE deliveries (
    delivery_id             TEXT PRIMARY KEY,
    shipping_point          TEXT,
    creation_date           DATE,
    actual_goods_mvmt_date  DATE,
    picking_status          TEXT,
    goods_movement_status   TEXT,
    delivery_block          TEXT,
    billing_block           TEXT,
    incompletion_status     TEXT,
    total_delivery_qty      NUMERIC,
    reference_sales_orders  JSONB    -- array of sales order IDs
);
CREATE INDEX idx_del_created  ON deliveries(creation_date);
CREATE INDEX idx_del_ref_so   ON deliveries USING GIN(reference_sales_orders);

-- ── Billing Documents ─────────────────────────────────────────────────────────
CREATE TABLE billing_docs (
    billing_doc_id      TEXT PRIMARY KEY,
    billing_doc_type    TEXT,
    sold_to_party       TEXT REFERENCES customers(customer_id),
    accounting_document TEXT,
    company_code        TEXT,
    fiscal_year         TEXT,
    total_net_amount    NUMERIC,
    currency            TEXT,
    billing_date        DATE,
    creation_date       DATE,
    is_cancelled        BOOLEAN DEFAULT FALSE,
    total_items         INTEGER,
    reference_deliveries JSONB,   -- array of delivery IDs
    materials           JSONB     -- array of product IDs
);
CREATE INDEX idx_bd_customer     ON billing_docs(sold_to_party);
CREATE INDEX idx_bd_acct_doc     ON billing_docs(accounting_document);
CREATE INDEX idx_bd_cancelled    ON billing_docs(is_cancelled);
CREATE INDEX idx_bd_ref_del      ON billing_docs USING GIN(reference_deliveries);
CREATE INDEX idx_bd_materials    ON billing_docs USING GIN(materials);

-- ── Journal Entries ───────────────────────────────────────────────────────────
CREATE TABLE journal_entries (
    journal_entry_id    TEXT PRIMARY KEY,  -- accountingDocument_item
    accounting_document TEXT,
    reference_document  TEXT,              -- billing doc id
    company_code        TEXT,
    fiscal_year         TEXT,
    gl_account          TEXT,
    customer            TEXT REFERENCES customers(customer_id),
    profit_center       TEXT,
    amount              NUMERIC,
    currency            TEXT,
    posting_date        DATE,
    document_date       DATE,
    doc_type            TEXT,
    clearing_date       DATE,
    clearing_document   TEXT               -- links to payment clearing doc
);
CREATE INDEX idx_je_acct_doc    ON journal_entries(accounting_document);
CREATE INDEX idx_je_ref_doc     ON journal_entries(reference_document);
CREATE INDEX idx_je_customer    ON journal_entries(customer);
CREATE INDEX idx_je_clearing    ON journal_entries(clearing_document);

-- ── Payments ─────────────────────────────────────────────────────────────────
CREATE TABLE payments (
    payment_id          TEXT PRIMARY KEY,
    accounting_document TEXT,
    clearing_document   TEXT,
    company_code        TEXT,
    fiscal_year         TEXT,
    customer            TEXT REFERENCES customers(customer_id),
    gl_account          TEXT,
    amount              NUMERIC,
    currency            TEXT,
    posting_date        DATE,
    clearing_date       DATE,
    document_date       DATE
);
CREATE INDEX idx_pay_customer  ON payments(customer);
CREATE INDEX idx_pay_clearing  ON payments(clearing_document);
"""


def create_relational_schema(conn) -> None:
    log.info("Creating relational schema …")
    with conn.cursor() as cur:
        cur.execute(RELATIONAL_SCHEMA)
    conn.commit()
    log.info("  Relational schema created ✓")


# ── Step 3: Load node CSVs into relational tables ─────────────────────────────

def _none(val):
    """Return None for NaN/empty, else the value."""
    if val is None:
        return None
    if isinstance(val, float):
        import math
        return None if math.isnan(val) else val
    s = str(val).strip()
    return None if s in ("", "nan", "None", "NaT") else s


def _bool(val) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("true", "1", "yes")


def _json(val):
    """Pass JSON strings through; return None for empties."""
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "nan", "None"):
        return None
    return s   # psycopg2 will pass this as text; Postgres casts to JSONB


def seed_relational(conn, output_dir: Path) -> None:
    log.info("Seeding relational tables …")
    nodes = output_dir / "nodes"

    # Order matters — FK dependencies
    inserts = [
        ("customers", nodes / "customers.csv", lambda df: [
            (_none(r.customer_id), _none(r.full_name), _none(r.bp_category),
             _bool(r.is_blocked), _bool(r.is_archived), _none(r.company_code),
             _none(r.currency), _none(r.payment_terms), _none(r.sales_organization),
             _none(r.creation_date))
            for r in df.itertuples()
        ],
         """INSERT INTO customers VALUES %s ON CONFLICT DO NOTHING"""),

        ("addresses", nodes / "addresses.csv", lambda df: [
            (_none(r.address_id), _none(r.customer_id), _none(r.street), _none(r.city),
             _none(r.postal_code), _none(r.region), _none(r.country), _none(r.timezone),
             _none(r.valid_from), _none(r.valid_to))
            for r in df.itertuples()
        ],
         """INSERT INTO addresses VALUES %s ON CONFLICT DO NOTHING"""),

        ("products", nodes / "products.csv", lambda df: [
            (_none(r.product_id), _none(r.product_type), _none(r.description),
             _none(r.old_product_id), _none(r.product_group), _none(r.base_unit),
             _none(r.gross_weight), _none(r.net_weight), _none(r.weight_unit),
             _none(r.division), _none(r.industry_sector), _bool(r.is_deleted),
             _none(r.creation_date))
            for r in df.itertuples()
        ],
         """INSERT INTO products VALUES %s ON CONFLICT DO NOTHING"""),

        ("sales_orders", nodes / "sales_orders.csv", lambda df: [
            (_none(r.sales_order_id), _none(r.order_type), _none(r.sold_to_party),
             _none(r.sales_organization), _none(r.distribution_channel), _none(r.division),
             _none(r.total_net_amount), _none(r.currency), _none(r.creation_date),
             _none(r.pricing_date), _none(r.requested_delivery_date),
             _none(r.confirmed_delivery_date), _none(r.delivery_status),
             _none(r.billing_status), _none(r.delivery_block), _none(r.billing_block),
             _none(r.payment_terms), _none(r.incoterms))
            for r in df.itertuples()
        ],
         """INSERT INTO sales_orders VALUES %s ON CONFLICT DO NOTHING"""),

        ("sales_order_items", nodes / "sales_order_items.csv", lambda df: [
            (_none(r.item_id), _none(r.sales_order_id), _none(r.item_number),
             _none(r.material), _none(r.item_category), _none(r.requested_qty),
             _none(r.qty_unit), _none(r.net_amount), _none(r.currency),
             _none(r.material_group), _none(r.plant), _none(r.storage_location),
             _none(r.rejection_reason), _none(r.billing_block))
            for r in df.itertuples()
        ],
         """INSERT INTO sales_order_items VALUES %s ON CONFLICT DO NOTHING"""),

        ("deliveries", nodes / "deliveries.csv", lambda df: [
            (_none(r.delivery_id), _none(r.shipping_point), _none(r.creation_date),
             _none(r.actual_goods_mvmt_date), _none(r.picking_status),
             _none(r.goods_movement_status), _none(r.delivery_block),
             _none(r.billing_block), _none(r.incompletion_status),
             _none(r.total_delivery_qty), _json(r.reference_sales_orders))
            for r in df.itertuples()
        ],
         """INSERT INTO deliveries VALUES %s ON CONFLICT DO NOTHING"""),

        ("billing_docs", nodes / "billing_docs.csv", lambda df: [
            (_none(r.billing_doc_id), _none(r.billing_doc_type), _none(r.sold_to_party),
             _none(r.accounting_document), _none(r.company_code), _none(r.fiscal_year),
             _none(r.total_net_amount), _none(r.currency), _none(r.billing_date),
             _none(r.creation_date), _bool(r.is_cancelled), _none(r.total_items),
             _json(r.reference_deliveries), _json(r.materials))
            for r in df.itertuples()
        ],
         """INSERT INTO billing_docs VALUES %s ON CONFLICT DO NOTHING"""),

        ("journal_entries", nodes / "journal_entries.csv", lambda df: [
            (_none(r.journal_entry_id), _none(r.accounting_document),
             _none(r.reference_document), _none(r.company_code), _none(r.fiscal_year),
             _none(r.gl_account), _none(r.customer), _none(r.profit_center),
             _none(r.amount), _none(r.currency), _none(r.posting_date),
             _none(r.document_date), _none(r.doc_type), _none(r.clearing_date),
             _none(r.clearing_document))
            for r in df.itertuples()
        ],
         """INSERT INTO journal_entries VALUES %s ON CONFLICT DO NOTHING"""),

        ("payments", nodes / "payments.csv", lambda df: [
            (_none(r.payment_id), _none(r.accounting_document), _none(r.clearing_document),
             _none(r.company_code), _none(r.fiscal_year), _none(r.customer),
             _none(r.gl_account), _none(r.amount), _none(r.currency),
             _none(r.posting_date), _none(r.clearing_date), _none(r.document_date))
            for r in df.itertuples()
        ],
         """INSERT INTO payments VALUES %s ON CONFLICT DO NOTHING"""),
    ]

    for name, csv_path, row_fn, sql in inserts:
        if not csv_path.exists():
            log.warning("  Missing CSV: %s — skipping", csv_path)
            continue
        df = pd.read_csv(csv_path)
        rows = row_fn(df)
        exec_many(conn, sql, rows)
        conn.commit()
        log.info("  Inserted %4d rows → %s", len(rows), name)


# ── Step 4: Build AGE graph vertices ──────────────────────────────────────────

def _cypher_props(d: dict) -> str:
    """Render a dict as a Cypher property map string."""
    parts = []
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, bool):
            parts.append(f"{k}: {str(v).lower()}")
        elif isinstance(v, (int, float)):
            parts.append(f"{k}: {v}")
        else:
            escaped = str(v).replace("\\", "\\\\").replace("'", "\\'")
            parts.append(f"{k}: '{escaped}'")
    return "{" + ", ".join(parts) + "}"


def _run_cypher(conn, cypher: str) -> None:
    """Execute a Cypher statement via AGE's cypher() function."""
    sql = f"""
        SELECT * FROM cypher('{GRAPH_NAME}', $$
            {cypher}
        $$) AS (result agtype);
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def seed_graph_vertices(conn, output_dir: Path) -> None:
    """
    Create AGE vertices from each node CSV.
    We use CREATE (MERGE would be safer but is slower for initial load).
    """
    log.info("Seeding AGE graph vertices …")
    nodes_dir = output_dir / "nodes"

    vertex_configs = [
        ("Customer", "customers.csv",
         lambda r: {"id": r.customer_id, "name": r.full_name,
                    "currency": r.currency, "payment_terms": r.payment_terms}),

        ("Address", "addresses.csv",
         lambda r: {"id": r.address_id, "city": r.city,
                    "country": r.country, "postal_code": r.postal_code}),

        ("Product", "products.csv",
         lambda r: {"id": r.product_id, "description": r.description,
                    "product_group": r.product_group, "base_unit": r.base_unit}),

        ("SalesOrder", "sales_orders.csv",
         lambda r: {"id": r.sales_order_id, "order_type": r.order_type,
                    "total_net_amount": _none(r.total_net_amount),
                    "currency": r.currency, "creation_date": str(r.creation_date),
                    "delivery_status": r.delivery_status}),

        ("SalesOrderItem", "sales_order_items.csv",
         lambda r: {"id": r.item_id, "item_number": r.item_number,
                    "requested_qty": _none(r.requested_qty),
                    "net_amount": _none(r.net_amount), "currency": r.currency}),

        ("OutboundDelivery", "deliveries.csv",
         lambda r: {"id": r.delivery_id, "creation_date": str(r.creation_date),
                    "picking_status": r.picking_status,
                    "goods_movement_status": r.goods_movement_status}),

        ("BillingDocument", "billing_docs.csv",
         lambda r: {"id": r.billing_doc_id, "billing_doc_type": r.billing_doc_type,
                    "total_net_amount": _none(r.total_net_amount),
                    "billing_date": str(r.billing_date),
                    "is_cancelled": _bool(r.is_cancelled)}),

        ("JournalEntry", "journal_entries.csv",
         lambda r: {"id": r.journal_entry_id, "accounting_document": r.accounting_document,
                    "amount": _none(r.amount), "currency": r.currency,
                    "posting_date": str(r.posting_date)}),

        ("Payment", "payments.csv",
         lambda r: {"id": r.payment_id, "amount": _none(r.amount),
                    "currency": r.currency, "clearing_date": str(r.clearing_date)}),
    ]

    for label, csv_name, prop_fn in vertex_configs:
        csv_path = nodes_dir / csv_name
        if not csv_path.exists():
            log.warning("  Missing CSV for %s — skipping", label)
            continue
        df = pd.read_csv(csv_path)
        count = 0
        for row in df.itertuples():
            props = prop_fn(row)
            # Filter None values
            props = {k: v for k, v in props.items() if v is not None
                     and str(v) not in ("nan", "None", "NaT")}
            cypher = f"CREATE (:{label} {_cypher_props(props)})"
            _run_cypher(conn, cypher)
            count += 1
        log.info("  Created %4d  %-20s vertices", count, label)


# ── Step 5: Build AGE graph edges ─────────────────────────────────────────────

def seed_graph_edges(conn, output_dir: Path) -> None:
    log.info("Seeding AGE graph edges …")
    edges_dir = output_dir / "edges"

    # Each entry: (csv_file, src_label, src_id_field, tgt_label, tgt_id_field, rel_type)
    edge_configs = [
        ("customer_placed_order.csv",
         "Customer", "source_id", "SalesOrder", "target_id", "PLACED"),

        ("customer_has_address.csv",
         "Customer", "source_id", "Address", "target_id", "HAS_ADDRESS"),

        ("order_contains_item.csv",
         "SalesOrder", "source_id", "SalesOrderItem", "target_id", "CONTAINS"),

        ("item_references_product.csv",
         "SalesOrderItem", "source_id", "Product", "target_id", "REFERENCES"),

        ("order_has_delivery.csv",
         "SalesOrder", "source_id", "OutboundDelivery", "target_id", "HAS_DELIVERY"),

        ("delivery_billed_in.csv",
         "OutboundDelivery", "source_id", "BillingDocument", "target_id", "BILLED_IN"),

        ("billing_recorded_in_journal.csv",
         "BillingDocument", "source_id", "JournalEntry", "target_id", "RECORDED_IN"),

        ("journal_settled_by_payment.csv",
         "JournalEntry", "source_id", "Payment", "target_id", "SETTLED_BY"),
    ]

    for csv_name, src_lbl, src_col, tgt_lbl, tgt_col, rel in edge_configs:
        csv_path = edges_dir / csv_name
        if not csv_path.exists():
            log.warning("  Missing edge CSV: %s — skipping", csv_name)
            continue
        df = pd.read_csv(csv_path)
        count = 0
        for row in df.itertuples():
            src_id = _none(getattr(row, src_col))
            tgt_id = _none(getattr(row, tgt_col))
            if not src_id or not tgt_id:
                continue
            src_id_e = src_id.replace("'", "\\'")
            tgt_id_e = tgt_id.replace("'", "\\'")
            cypher = (
                f"MATCH (a:{src_lbl} {{id: '{src_id_e}'}}), "
                f"(b:{tgt_lbl} {{id: '{tgt_id_e}'}}) "
                f"CREATE (a)-[:{rel}]->(b)"
            )
            _run_cypher(conn, cypher)
            count += 1
        log.info("  Created %4d  [:%s] edges", count, rel)


# ── Step 6: Verification queries ──────────────────────────────────────────────

VERIFICATION_QUERIES = [
    (
        "1. Full O2C path for one sales order",
        """
        SELECT * FROM cypher('o2c', $$
            MATCH (c:Customer)-[:PLACED]->(so:SalesOrder)-[:HAS_DELIVERY]->(d:OutboundDelivery)
                  -[:BILLED_IN]->(b:BillingDocument)-[:RECORDED_IN]->(j:JournalEntry)
                  -[:SETTLED_BY]->(p:Payment)
            RETURN c.name AS customer,
                   so.id  AS sales_order,
                   d.id   AS delivery,
                   b.id   AS billing_doc,
                   j.id   AS journal_entry,
                   p.id   AS payment
            LIMIT 3
        $$) AS (customer agtype, sales_order agtype, delivery agtype,
                billing_doc agtype, journal_entry agtype, payment agtype);
        """,
    ),
    (
        "2. Products in most billing documents (SQL aggregate)",
        """
        SELECT
            p.product_id,
            p.description,
            COUNT(DISTINCT bd.billing_doc_id) AS billing_doc_count,
            SUM(soi.net_amount)               AS total_revenue
        FROM products p
        JOIN sales_order_items soi ON soi.material = p.product_id
        JOIN deliveries d ON d.reference_sales_orders::jsonb ? soi.sales_order_id
        JOIN billing_docs bd ON bd.reference_deliveries::jsonb ? d.delivery_id
        GROUP BY p.product_id, p.description
        ORDER BY billing_doc_count DESC
        LIMIT 10;
        """,
    ),
    (
        "3. Broken flows — orders delivered but never billed",
        """
        SELECT
            so.sales_order_id,
            c.full_name        AS customer,
            so.creation_date,
            so.total_net_amount,
            d.delivery_id,
            d.actual_goods_mvmt_date
        FROM sales_orders so
        JOIN customers c ON c.customer_id = so.sold_to_party
        JOIN deliveries d
            ON d.reference_sales_orders::jsonb ? so.sales_order_id
        LEFT JOIN billing_docs bd
            ON bd.reference_deliveries::jsonb ? d.delivery_id
        WHERE bd.billing_doc_id IS NULL
        ORDER BY so.creation_date;
        """,
    ),
]


def run_verification(conn) -> None:
    log.info("Running verification queries …")
    for title, sql in VERIFICATION_QUERIES:
        log.info("  ── %s", title)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                if rows:
                    for row in rows[:5]:
                        log.info("    %s", row)
                else:
                    log.info("    (no rows returned)")
        except Exception as exc:
            log.warning("    Query failed: %s", exc)
            conn.rollback()
    log.info("  Verification complete ✓")


# ── Main ──────────────────────────────────────────────────────────────────────

def main(output_dir: Path) -> None:
    log.info("=" * 60)
    log.info("Phase 2 — PostgreSQL + AGE Schema & Seed")
    log.info("Output dir: %s", output_dir)
    log.info("=" * 60)

    conn = get_conn()
    log.info("Connected to PostgreSQL ✓")

    bootstrap_age(conn)
    create_relational_schema(conn)
    seed_relational(conn, output_dir)
    seed_graph_vertices(conn, output_dir)
    seed_graph_edges(conn, output_dir)
    run_verification(conn)

    conn.close()
    log.info("=" * 60)
    log.info("Phase 2 complete.  Database is seeded and verified.")
    log.info("Proceed to Phase 3: LLM Query Engine.")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAP O2C — Phase 2 DB Seed")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Path to the output/ folder produced by phase1_ingest.py",
    )
    args = parser.parse_args()

    if not (args.output_dir / "nodes").exists():
        log.error("Nodes directory not found: %s/nodes", args.output_dir)
        log.error("Run phase1_ingest.py first.")
        sys.exit(1)

    main(args.output_dir)
