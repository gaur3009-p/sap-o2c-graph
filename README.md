# SAP O2C Graph Intelligence System

A graph-based data modeling and NL-query system for SAP Order-to-Cash data.
Unifies fragmented SAP entities (Sales Orders, Deliveries, Billing Documents,
Journal Entries, Payments) into a property graph, then exposes a natural-language
chat interface backed by Groq-powered LLM query translation.

```
┌─────────────────────────────────────────────────────────────┐
│  Next.js Frontend                                           │
│  ┌──────────────────────┐  ┌──────────────────────────────┐ │
│  │  Graph Visualisation │  │  NL Chat Interface           │ │
│  │  (React Flow)        │  │  "Which products appear in   │ │
│  │                      │  │   the most billing docs?"    │ │
│  └──────────┬───────────┘  └──────────────┬───────────────┘ │
└─────────────┼──────────────────────────────┼────────────────┘
              │              FastAPI          │
┌─────────────▼──────────────────────────────▼────────────────┐
│  FastAPI Backend                                            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  LangChain + Groq (LLaMA 3.1 70B)                   │   │
│  │  NL → SQL/Cypher → Execute → NL Answer              │   │
│  └──────────────────────────┬───────────────────────────┘   │
└─────────────────────────────┼───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│  PostgreSQL 15 + Apache AGE                                 │
│  ┌─────────────────────┐  ┌──────────────────────────────┐  │
│  │  Relational Tables  │  │  AGE Property Graph          │  │
│  │  (analytics/agg)    │  │  (Cypher path traversals)   │  │
│  └─────────────────────┘  └──────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Table of Contents

1. [Architectural Decisions](#architectural-decisions)
2. [Graph Data Model](#graph-data-model)
3. [Quick Start](#quick-start)
4. [Phase-by-Phase Guide](#phase-by-phase-guide)
   - [Phase 1 — Data Ingestion](#phase-1--data-ingestion)
   - [Phase 2 — Database Setup](#phase-2--database-setup)
5. [Project Structure](#project-structure)
6. [LLM Prompting Strategy](#llm-prompting-strategy)
7. [Key Use Cases](#key-use-cases)

---

## Architectural Decisions

### Database: PostgreSQL 15 + Apache AGE (over Neo4j)

| | PostgreSQL + AGE | Neo4j |
|---|---|---|
| **License** | Open-source, free | Community edition limited; Enterprise paid |
| **Infrastructure** | Single service | Separate graph server |
| **Graph queries** | Cypher via AGE extension | Native Cypher |
| **Relational queries** | Full SQL (aggregates, joins) | Limited (no native SQL) |
| **Maturity** | PG: 35 years; AGE: active Apache project | Very mature |
| **Decision** | ✅ Chosen | ❌ Rejected |

**Rationale**: The O2C use cases require *both* graph traversals (trace a full
document flow) *and* aggregation queries (which products appear most?). A dual
PostgreSQL + AGE setup handles both natively. Neo4j would require a separate
service and lacks native SQL for the aggregate queries.

### LLM: Groq + LLaMA 3.1 70B (over Gemini / OpenAI)

- **Speed**: Groq's custom LPU hardware delivers ~300 tokens/sec — critical for
  a responsive chat UX.
- **Cost**: Free tier is generous (14,400 req/day).
- **Quality**: LLaMA 3.1 70B scores comparably to GPT-4o on SQL/Cypher generation.

### Orchestration: LangChain (over raw API calls)

LangChain provides structured tool use, conversation memory, and a clean
abstraction for swapping LLM providers. For the NL2Query pipeline it lets us
define the "generate query → execute → explain" chain declaratively.

### Graph Visualization: React Flow (over Cytoscape / Force Graph)

React Flow has the best DX for interactive node/edge manipulation, built-in
minimap, zoom/pan, and is the most actively maintained of the three.

---

## Graph Data Model

### Nodes

| Label | Source Table(s) | Key Properties |
|---|---|---|
| `Customer` | business_partners, customer_*_assignments | id, full_name, currency, payment_terms |
| `Address` | business_partner_addresses | id, city, country, postal_code |
| `Product` | products, product_descriptions | id, description, product_group |
| `SalesOrder` | sales_order_headers, schedule_lines | id, order_type, total_net_amount, delivery_status |
| `SalesOrderItem` | sales_order_items | id, material, requested_qty, net_amount |
| `OutboundDelivery` | outbound_delivery_headers + items | id, picking_status, goods_movement_status |
| `BillingDocument` | billing_document_headers + items | id, billing_doc_type, total_net_amount, is_cancelled |
| `JournalEntry` | journal_entry_items_accounts_receivable | id, gl_account, amount, posting_date |
| `Payment` | payments_accounts_receivable | id, amount, clearing_date |

### Edges (the O2C flow)

```
Customer ──[PLACED]──────────► SalesOrder
Customer ──[HAS_ADDRESS]──────► Address
SalesOrder ──[CONTAINS]───────► SalesOrderItem
SalesOrderItem ──[REFERENCES]─► Product
SalesOrder ──[HAS_DELIVERY]───► OutboundDelivery
OutboundDelivery ──[BILLED_IN]► BillingDocument
BillingDocument ──[RECORDED_IN]► JournalEntry
JournalEntry ──[SETTLED_BY]───► Payment
```

The `OutboundDelivery -[BILLED_IN]-> BillingDocument` edge is the critical
link for broken-flow detection: if a delivery exists with no outgoing
`BILLED_IN` edge, that delivery was never invoiced.

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Node.js 18+ (for frontend, Phase 4)
- A free [Groq API key](https://console.groq.com)

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/sap-o2c-graph.git
cd sap-o2c-graph
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env — set GROQ_API_KEY at minimum
```

### 3. Start the database

```bash
docker compose up -d
# Wait ~10 seconds for Postgres to be healthy
docker compose ps   # confirm 'healthy'
```

### 4. Install Python dependencies

```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 5. Run Phase 1 — ingest data

```bash
# Place your sap-o2c-data/ folder in the project root, then:
python backend/ingestion/phase1_ingest.py \
  --data-dir ./sap-o2c-data \
  --output-dir ./output
```

Expected output:
```
Phase 1 complete.  Outputs written to: ./output
  Nodes: 9 files
  Edges: 8 files
```

### 6. Run Phase 2 — seed the database

```bash
python backend/db/phase2_schema_and_seed.py --output-dir ./output
```

Expected output:
```
Phase 2 complete.  Database is seeded and verified.
```

You can now browse the database at **http://localhost:8080** (Adminer).
Use server `postgres`, username/password from your `.env`.

---

## Phase-by-Phase Guide

### Phase 1 — Data Ingestion

**Script**: `backend/ingestion/phase1_ingest.py`

**What it does**:

1. Loads all 16 JSONL entity folders (multi-part files concatenated automatically)
2. Normalises SAP-specific quirks:
   - Composite keys (`salesOrder` + `salesOrderItem` → `item_id`)
   - Nested time objects `{"hours": 11, "minutes": 31}` → ignored (date only)
   - SAP open-ended date sentinel `9999-12-31` → `NULL`
   - Empty strings → `NULL`
3. Builds 9 clean node CSV files and 8 edge CSV files
4. Runs a flow-completeness validation and prints broken-flow candidates

**Output structure**:
```
output/
  nodes/
    customers.csv
    addresses.csv
    products.csv
    sales_orders.csv
    sales_order_items.csv
    deliveries.csv
    billing_docs.csv
    journal_entries.csv
    payments.csv
  edges/
    customer_placed_order.csv
    customer_has_address.csv
    order_contains_item.csv
    item_references_product.csv
    order_has_delivery.csv
    delivery_billed_in.csv
    billing_recorded_in_journal.csv
    journal_settled_by_payment.csv
```

**Key design decisions in Phase 1**:

- `SalesOrderItem` is a **first-class node** (not just an edge property). This
  matters because `billing_document_items.referenceSdDocument` points to
  *delivery IDs*, not order IDs. The item node is the bridge that carries
  the product reference through to the billing chain.

- Billing document cancellations are **flagged but kept**. They are critical
  data for broken-flow detection and compliance reporting.

- `reference_sales_orders` and `reference_deliveries` are stored as **JSONB
  arrays** so a single delivery that references multiple sales orders can be
  queried efficiently with `@>` and `?` operators.

---

### Phase 2 — Database Setup

**Script**: `backend/db/phase2_schema_and_seed.py`

**What it does**:

1. **Boots AGE** — installs the extension and creates the `o2c` graph
2. **Relational schema** — creates 9 typed PostgreSQL tables with FK constraints
   and optimised indexes (GIN indexes on JSONB array columns)
3. **Seeds relational tables** — bulk inserts via `execute_values` (500 rows/batch)
4. **Seeds AGE graph** — creates vertices and edges via Cypher statements
5. **Runs 3 verification queries** (see below)

**Verification queries included**:

```sql
-- 1. Full O2C path trace (Cypher via AGE)
MATCH (c:Customer)-[:PLACED]->(so:SalesOrder)-[:HAS_DELIVERY]->(d:OutboundDelivery)
      -[:BILLED_IN]->(b:BillingDocument)-[:RECORDED_IN]->(j:JournalEntry)
      -[:SETTLED_BY]->(p:Payment)
RETURN c.name, so.id, d.id, b.id, j.id, p.id
LIMIT 3;

-- 2. Products in most billing documents (SQL)
SELECT p.product_id, p.description,
       COUNT(DISTINCT bd.billing_doc_id) AS billing_doc_count
FROM products p
JOIN sales_order_items soi ON soi.material = p.product_id
JOIN deliveries d ON d.reference_sales_orders::jsonb ? soi.sales_order_id
JOIN billing_docs bd ON bd.reference_deliveries::jsonb ? d.delivery_id
GROUP BY p.product_id, p.description
ORDER BY billing_doc_count DESC
LIMIT 10;

-- 3. Broken flows — delivered but never billed (SQL)
SELECT so.sales_order_id, c.full_name, d.delivery_id
FROM sales_orders so
JOIN customers c ON c.customer_id = so.sold_to_party
JOIN deliveries d ON d.reference_sales_orders::jsonb ? so.sales_order_id
LEFT JOIN billing_docs bd ON bd.reference_deliveries::jsonb ? d.delivery_id
WHERE bd.billing_doc_id IS NULL;
```

---

## Project Structure

```
sap-o2c-graph/
├── backend/
│   ├── ingestion/
│   │   └── phase1_ingest.py          # Phase 1: JSONL → Node/Edge CSVs
│   ├── db/
│   │   └── phase2_schema_and_seed.py # Phase 2: PG schema + AGE graph seed
│   ├── api/                          # Phase 3: FastAPI app (coming)
│   └── llm/                          # Phase 3: LangChain NL2Query engine (coming)
├── frontend/                         # Phase 4: Next.js + React Flow (coming)
├── scripts/
│   └── init_db.sql                   # Docker init SQL
├── data/
│   └── sample/                       # Sanitised sample records for testing
├── docs/                             # Architecture diagrams (coming)
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Phase 3 — LLM Query Engine

**Scripts**: `backend/llm/`, `backend/api/app.py`, `main.py`

### Starting the API server

```bash
# Ensure DB is running and .env is configured, then:
python main.py

# Server starts at http://localhost:8000
# Swagger UI at http://localhost:8000/docs
```

### API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/chat` | NL question → grounded answer |
| `GET` | `/api/graph/nodes` | All nodes for visualisation (supports `?label=Customer`) |
| `GET` | `/api/graph/edges` | All edges (supports `?relationship=PLACED`) |
| `GET` | `/api/graph/node/{id}` | Single node + immediate neighbours |
| `GET` | `/api/health` | DB + LLM connectivity status |

### Example chat request

```bash
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "Which products appear in the most billing documents?"}'
```

Response:
```json
{
  "answer": "The product with the most billing documents is S8907367001003 (WB-CG CHARCOAL GANG), appearing in 12 distinct billing documents with total revenue of ₹48,320.50.",
  "query": "SELECT p.description, COUNT(DISTINCT bd.billing_doc_id) AS cnt ...",
  "query_type": "sql",
  "columns": ["description", "cnt", "total_revenue"],
  "rows": [["WB-CG CHARCOAL GANG", 12, 48320.50], ...],
  "row_count": 10,
  "error": null
}
```

### NL2Query Pipeline

```
User question
     │
     ▼
① Relevance gate — is this answerable from the O2C dataset?
  If not → return OUT_OF_SCOPE_RESPONSE immediately (no LLM tokens wasted)
     │
     ▼
② Query type selection — SQL (aggregations) or Cypher (path traversals)?
     │
     ▼
③ Query generation — schema context injected into system prompt
     │
     ▼
④ Safety check — reject any write/DDL keywords before DB touch
     │
     ▼
⑤ DB execution — run_sql() or run_cypher() via connection.py
  On error → one self-correction pass with the error message
     │
     ▼
⑥ Answer synthesis — LLM sees raw rows only, synthesises plain English
     │
     ▼
Structured JSON: { answer, query, query_type, columns, rows, row_count }
```

### Guardrail design

| Layer | Mechanism |
|---|---|
| Relevance | Step ① LLM classifier before any schema is revealed |
| Write prevention | Step ④ regex check: blocks INSERT/UPDATE/DELETE/DROP/TRUNCATE + Cypher CREATE/SET/DELETE/MERGE |
| Hallucination | Step ⑥ system prompt: "Answer ONLY based on the data provided. Never invent data." |
| Empty results | Step ⑥ explicit instruction: if 0 rows, say "No matching records were found" |
| Injection | Query length cap (4000 chars), comment pattern detection (`--`, `/*`) |
| Self-correction | One retry on DB error with the error message fed back to the LLM |

### Running the tests

```bash
pytest tests/ -v
# or without pytest installed:
python tests/test_safety.py
```

34 unit tests covering SQL safety, Cypher safety, dispatcher logic, and schema context — all passing without a DB connection.

---

## Key Use Cases

These are the Phase 5 target queries the system is optimised for:

**1. Products with most billing documents**
```sql
SELECT p.description, COUNT(DISTINCT bd.billing_doc_id) AS billing_docs
FROM products p
JOIN sales_order_items soi ON soi.material = p.product_id
JOIN deliveries d ON d.reference_sales_orders::jsonb ? soi.sales_order_id
JOIN billing_docs bd ON bd.reference_deliveries::jsonb ? d.delivery_id
GROUP BY p.product_id, p.description
ORDER BY billing_docs DESC;
```

**2. Full flow trace for a billing document**
```cypher
MATCH path = (so:SalesOrder)-[:HAS_DELIVERY]->(d:OutboundDelivery)
             -[:BILLED_IN]->(b:BillingDocument {id: '90504248'})
             -[:RECORDED_IN]->(j:JournalEntry)
             -[:SETTLED_BY]->(p:Payment)
RETURN path;
```

**3. Incomplete flow detection**
```sql
-- Delivered but not billed
SELECT so.sales_order_id, d.delivery_id
FROM deliveries d
JOIN sales_orders so ON d.reference_sales_orders::jsonb ? so.sales_order_id
LEFT JOIN billing_docs bd ON bd.reference_deliveries::jsonb ? d.delivery_id
WHERE bd.billing_doc_id IS NULL;
```
