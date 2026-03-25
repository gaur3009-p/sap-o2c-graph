"""
backend/api/app.py
───────────────────
FastAPI application. Exposes:

  POST /api/chat          — NL question → grounded answer
  GET  /api/graph/nodes   — all graph nodes for visualisation
  GET  /api/graph/edges   — all graph edges for visualisation
  GET  /api/graph/node/{id} — single node with its neighbours
  GET  /api/health        — database + LLM connectivity check

All endpoints return consistent JSON. Errors never expose raw tracebacks.
"""

import logging
import os
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SAP O2C Graph Intelligence API",
    description=(
        "NL-powered query interface over a SAP Order-to-Cash property graph. "
        "Translates natural language to SQL/Cypher, executes safely, and returns "
        "grounded answers."
    ),
    version="1.0.0",
)

# Allow the Next.js dev server to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        os.getenv("FRONTEND_URL", "http://localhost:3000"),
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request / Response models ─────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=3,
        max_length=1000,
        description="Natural language question about the O2C dataset",
        examples=["Which products appear in the most billing documents?"],
    )
    conversation_id: str | None = Field(
        None,
        description="Optional conversation ID for future multi-turn support",
    )


class ChatResponse(BaseModel):
    answer: str
    query: str | None
    query_type: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    error: str | None = None


class GraphNode(BaseModel):
    id: str
    label: str          # vertex label: Customer, SalesOrder, etc.
    properties: dict[str, Any]


class GraphEdge(BaseModel):
    source: str
    target: str
    relationship: str


class GraphData(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]


class HealthResponse(BaseModel):
    status: str         # "ok" | "degraded" | "error"
    database: bool
    llm_configured: bool
    details: dict[str, str]


# ── Chat endpoint ─────────────────────────────────────────────────────────────

@app.post("/api/chat", response_model=ChatResponse, tags=["Chat"])
async def chat(request: ChatRequest):
    """
    Accepts a natural language question and returns a grounded answer
    backed by a SQL or Cypher query executed against the O2C dataset.

    The pipeline:
    1. Relevance classification (out-of-scope questions are rejected early)
    2. Query type selection (SQL vs Cypher)
    3. Query generation with schema injection
    4. Safety validation
    5. DB execution
    6. Answer synthesis grounded in the returned data
    """
    log.info("Chat request: %s", request.question[:100])

    try:
        from backend.llm.nl2query_engine import answer_question
        result = answer_question(request.question)
        return ChatResponse(**result)
    except Exception as exc:
        log.exception("Chat endpoint error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── Graph data endpoints ───────────────────────────────────────────────────────

@app.get("/api/graph/nodes", response_model=list[GraphNode], tags=["Graph"])
async def get_graph_nodes(
    label: str | None = Query(None, description="Filter by vertex label, e.g. 'Customer'"),
    limit: int = Query(200, ge=1, le=1000, description="Maximum nodes to return"),
):
    """
    Returns all graph nodes (or a filtered subset) for the frontend visualiser.
    Each node has an id, label, and its key properties.
    """
    from backend.db.connection import run_sql

    # Build label filter
    label_filter = f"AND label = '{label}'" if label else ""

    # We read from the relational tables (faster than AGE for bulk reads)
    queries = {
        "Customer": f"""
            SELECT customer_id AS id, 'Customer' AS label,
                   json_build_object('full_name', full_name, 'currency', currency,
                                     'payment_terms', payment_terms, 'company_code', company_code) AS properties
            FROM customers LIMIT {limit}
        """,
        "SalesOrder": f"""
            SELECT sales_order_id AS id, 'SalesOrder' AS label,
                   json_build_object('order_type', order_type, 'total_net_amount', total_net_amount,
                                     'currency', currency, 'creation_date', creation_date,
                                     'delivery_status', delivery_status, 'billing_status', billing_status) AS properties
            FROM sales_orders LIMIT {limit}
        """,
        "Product": f"""
            SELECT product_id AS id, 'Product' AS label,
                   json_build_object('description', description, 'product_group', product_group,
                                     'base_unit', base_unit) AS properties
            FROM products LIMIT {limit}
        """,
        "OutboundDelivery": f"""
            SELECT delivery_id AS id, 'OutboundDelivery' AS label,
                   json_build_object('picking_status', picking_status,
                                     'goods_movement_status', goods_movement_status,
                                     'creation_date', creation_date) AS properties
            FROM deliveries LIMIT {limit}
        """,
        "BillingDocument": f"""
            SELECT billing_doc_id AS id, 'BillingDocument' AS label,
                   json_build_object('billing_doc_type', billing_doc_type,
                                     'total_net_amount', total_net_amount,
                                     'billing_date', billing_date,
                                     'is_cancelled', is_cancelled) AS properties
            FROM billing_docs LIMIT {limit}
        """,
        "JournalEntry": f"""
            SELECT journal_entry_id AS id, 'JournalEntry' AS label,
                   json_build_object('accounting_document', accounting_document,
                                     'amount', amount, 'posting_date', posting_date) AS properties
            FROM journal_entries LIMIT {limit}
        """,
        "Payment": f"""
            SELECT payment_id AS id, 'Payment' AS label,
                   json_build_object('amount', amount, 'currency', currency,
                                     'clearing_date', clearing_date) AS properties
            FROM payments LIMIT {limit}
        """,
    }

    nodes = []
    target_labels = [label] if label and label in queries else list(queries.keys())

    for lbl in target_labels:
        try:
            result = run_sql(queries[lbl])
            for row in result["rows"]:
                node_id, node_label, props = row[0], row[1], row[2]
                # props comes back as a dict from json_build_object
                if isinstance(props, str):
                    import json
                    props = json.loads(props)
                nodes.append(GraphNode(
                    id=str(node_id),
                    label=node_label,
                    properties=props or {},
                ))
        except Exception as exc:
            log.warning("Failed to load %s nodes: %s", lbl, exc)

    return nodes


@app.get("/api/graph/edges", response_model=list[GraphEdge], tags=["Graph"])
async def get_graph_edges(
    relationship: str | None = Query(None, description="Filter by relationship type"),
    limit: int = Query(500, ge=1, le=2000),
):
    """
    Returns all graph edges for the frontend visualiser.
    Reads from the relational tables using the same join logic as Phase 2.
    """
    from backend.db.connection import run_sql
    import json as _json

    edges = []

    edge_queries = {
        "PLACED": """
            SELECT sold_to_party AS source, sales_order_id AS target, 'PLACED' AS rel
            FROM sales_orders WHERE sold_to_party IS NOT NULL
        """,
        "HAS_ADDRESS": """
            SELECT customer_id AS source, address_id AS target, 'HAS_ADDRESS' AS rel
            FROM addresses WHERE customer_id IS NOT NULL
        """,
        "CONTAINS": """
            SELECT sales_order_id AS source, item_id AS target, 'CONTAINS' AS rel
            FROM sales_order_items
        """,
        "REFERENCES": """
            SELECT item_id AS source, material AS target, 'REFERENCES' AS rel
            FROM sales_order_items WHERE material IS NOT NULL
        """,
        "HAS_DELIVERY": """
            SELECT so.sales_order_id AS source, d.delivery_id AS target, 'HAS_DELIVERY' AS rel
            FROM deliveries d, sales_orders so
            WHERE d.reference_sales_orders::jsonb ? so.sales_order_id
        """,
        "BILLED_IN": """
            SELECT d.delivery_id AS source, bd.billing_doc_id AS target, 'BILLED_IN' AS rel
            FROM billing_docs bd, deliveries d
            WHERE bd.reference_deliveries::jsonb ? d.delivery_id
        """,
        "RECORDED_IN": """
            SELECT je.reference_document AS source, je.journal_entry_id AS target, 'RECORDED_IN' AS rel
            FROM journal_entries je WHERE je.reference_document IS NOT NULL
        """,
        "SETTLED_BY": """
            SELECT je.journal_entry_id AS source, p.payment_id AS target, 'SETTLED_BY' AS rel
            FROM journal_entries je
            JOIN payments p ON p.clearing_document = je.clearing_document
        """,
    }

    target_rels = (
        [relationship] if relationship and relationship in edge_queries
        else list(edge_queries.keys())
    )

    for rel in target_rels:
        try:
            result = run_sql(edge_queries[rel] + f" LIMIT {limit}")
            for row in result["rows"]:
                edges.append(GraphEdge(
                    source=str(row[0]),
                    target=str(row[1]),
                    relationship=row[2],
                ))
        except Exception as exc:
            log.warning("Failed to load %s edges: %s", rel, exc)

    return edges


@app.get("/api/graph/node/{node_id}", tags=["Graph"])
async def get_node_detail(node_id: str):
    """
    Returns a single node's full properties plus its immediate neighbours
    (one hop in any direction). Used when the user clicks a node in the UI.
    """
    from backend.db.connection import run_sql

    # Try to find the node across all entity tables
    entity_queries = [
        ("Customer",         f"SELECT * FROM customers WHERE customer_id = '{node_id}'"),
        ("SalesOrder",       f"SELECT * FROM sales_orders WHERE sales_order_id = '{node_id}'"),
        ("Product",          f"SELECT * FROM products WHERE product_id = '{node_id}'"),
        ("OutboundDelivery", f"SELECT * FROM deliveries WHERE delivery_id = '{node_id}'"),
        ("BillingDocument",  f"SELECT * FROM billing_docs WHERE billing_doc_id = '{node_id}'"),
        ("JournalEntry",     f"SELECT * FROM journal_entries WHERE journal_entry_id = '{node_id}'"),
        ("Payment",          f"SELECT * FROM payments WHERE payment_id = '{node_id}'"),
    ]

    node_data = None
    node_label = None

    for label, q in entity_queries:
        try:
            result = run_sql(q)
            if result["rows"]:
                node_label = label
                node_data = dict(zip(result["columns"], result["rows"][0]))
                break
        except Exception:
            continue

    if not node_data:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")

    # Fetch immediate neighbours via the relevant edge queries
    neighbour_sql = f"""
        SELECT 'PLACED_BY'   AS rel, c.customer_id AS id, 'Customer'  AS label, c.full_name AS name
        FROM customers c JOIN sales_orders so ON so.sold_to_party = c.customer_id
        WHERE so.sales_order_id = '{node_id}'
        UNION ALL
        SELECT 'PLACED'      AS rel, so.sales_order_id, 'SalesOrder', so.sales_order_id
        FROM sales_orders so WHERE so.sold_to_party = '{node_id}'
        UNION ALL
        SELECT 'HAS_DELIVERY', d.delivery_id, 'OutboundDelivery', d.delivery_id
        FROM deliveries d WHERE d.reference_sales_orders::jsonb ? '{node_id}'
        UNION ALL
        SELECT 'BILLED_IN',    bd.billing_doc_id, 'BillingDocument', bd.billing_doc_id
        FROM billing_docs bd WHERE bd.reference_deliveries::jsonb ? '{node_id}'
        UNION ALL
        SELECT 'RECORDED_IN',  je.journal_entry_id, 'JournalEntry', je.journal_entry_id
        FROM journal_entries je WHERE je.reference_document = '{node_id}'
        UNION ALL
        SELECT 'SETTLED_BY',   p.payment_id, 'Payment', p.payment_id
        FROM payments p JOIN journal_entries je ON p.clearing_document = je.clearing_document
        WHERE je.journal_entry_id = '{node_id}'
        LIMIT 50
    """

    neighbours = []
    try:
        nb_result = run_sql(neighbour_sql)
        for row in nb_result["rows"]:
            neighbours.append({
                "relationship": row[0],
                "id": str(row[1]),
                "label": row[2],
                "name": str(row[3]),
            })
    except Exception as exc:
        log.warning("Could not fetch neighbours for %s: %s", node_id, exc)

    return {
        "id": node_id,
        "label": node_label,
        "properties": node_data,
        "neighbours": neighbours,
    }


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/api/health", response_model=HealthResponse, tags=["System"])
async def health():
    """Returns system health: database connectivity and LLM configuration."""
    from backend.db.connection import test_connection

    db_ok = False
    db_detail = "unknown"
    try:
        db_ok = test_connection()
        db_detail = "connected" if db_ok else "unreachable"
    except Exception as exc:
        db_detail = str(exc)

    groq_key = os.getenv("GROQ_API_KEY", "")
    llm_ok = bool(groq_key and groq_key != "your_groq_api_key_here")

    status = "ok" if (db_ok and llm_ok) else "degraded"

    return HealthResponse(
        status=status,
        database=db_ok,
        llm_configured=llm_ok,
        details={
            "database": db_detail,
            "llm": "groq configured" if llm_ok else "GROQ_API_KEY not set",
            "model": os.getenv("GROQ_MODEL", "llama-3.1-70b-versatile"),
        },
    )


# ── Root ──────────────────────────────────────────────────────────────────────

@app.get("/", tags=["System"])
async def root():
    return {
        "name": "SAP O2C Graph Intelligence API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/health",
    }
