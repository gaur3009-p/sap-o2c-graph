"""
backend/llm/schema_context.py
──────────────────────────────
Single source of truth for the schema description injected into every
LLM prompt. Keeping it here means changes to the DB only require one
edit, not a hunt through prompt strings.

The schema is written from the LLM's point of view — it describes what
each table/relationship IS, not just what columns it has. This phrasing
dramatically improves query generation quality.
"""

# ── Relational schema description ─────────────────────────────────────────────

RELATIONAL_SCHEMA = """
## Relational Tables (use standard SQL)

### customers
Represents a business partner / sold-to customer.
Columns: customer_id (PK, TEXT), full_name, bp_category, is_blocked (BOOLEAN),
         is_archived (BOOLEAN), company_code, currency, payment_terms,
         sales_organization, creation_date (DATE)

### addresses
Physical address for a customer.
Columns: address_id (PK, TEXT), customer_id (FK → customers), street, city,
         postal_code, region, country, timezone, valid_from (DATE), valid_to (DATE)

### products
A sellable material/product in the SAP catalogue.
Columns: product_id (PK, TEXT), product_type, description, old_product_id,
         product_group, base_unit, gross_weight (NUMERIC), net_weight (NUMERIC),
         weight_unit, division, industry_sector, is_deleted (BOOLEAN), creation_date (DATE)

### sales_orders
An SAP sales order header. One order can contain many items.
Columns: sales_order_id (PK, TEXT), order_type, sold_to_party (FK → customers),
         sales_organization, distribution_channel, division,
         total_net_amount (NUMERIC), currency, creation_date (DATE),
         pricing_date (DATE), requested_delivery_date (DATE),
         confirmed_delivery_date (DATE), delivery_status, billing_status,
         delivery_block, billing_block, payment_terms, incoterms

### sales_order_items
A line item within a sales order. Links an order to a product.
Columns: item_id (PK, TEXT, format: "salesOrderId_itemNumber"),
         sales_order_id (FK → sales_orders), item_number,
         material (FK → products), item_category, requested_qty (NUMERIC),
         qty_unit, net_amount (NUMERIC), currency, material_group,
         plant, storage_location, rejection_reason, billing_block

### deliveries
An outbound delivery document. The reference_sales_orders JSONB column
contains an array of sales_order_id values this delivery fulfils.
IMPORTANT: To join deliveries → sales_orders use:
  d.reference_sales_orders::jsonb ? so.sales_order_id
Columns: delivery_id (PK, TEXT), shipping_point, creation_date (DATE),
         actual_goods_mvmt_date (DATE), picking_status, goods_movement_status,
         delivery_block, billing_block, incompletion_status,
         total_delivery_qty (NUMERIC),
         reference_sales_orders (JSONB array of sales_order_id strings)

### billing_docs
An SAP billing document (invoice). The reference_deliveries JSONB column
contains an array of delivery_id values this invoice covers.
IMPORTANT: To join billing_docs → deliveries use:
  bd.reference_deliveries::jsonb ? d.delivery_id
Columns: billing_doc_id (PK, TEXT), billing_doc_type, sold_to_party (FK → customers),
         accounting_document, company_code, fiscal_year,
         total_net_amount (NUMERIC), currency, billing_date (DATE),
         creation_date (DATE), is_cancelled (BOOLEAN), total_items (INTEGER),
         reference_deliveries (JSONB array of delivery_id strings),
         materials (JSONB array of product_id strings)

### journal_entries
An accounting journal entry line item in the AR sub-ledger.
Links to a billing document via reference_document = billing_doc_id.
Links to a payment via clearing_document = payments.clearing_document.
Columns: journal_entry_id (PK, TEXT), accounting_document,
         reference_document (= billing_doc_id), company_code, fiscal_year,
         gl_account, customer (FK → customers), profit_center,
         amount (NUMERIC), currency, posting_date (DATE), document_date (DATE),
         doc_type, clearing_date (DATE), clearing_document

### payments
A payment clearing record in the AR sub-ledger.
Links to a journal entry via clearing_document.
Columns: payment_id (PK, TEXT), accounting_document, clearing_document,
         company_code, fiscal_year, customer (FK → customers),
         gl_account, amount (NUMERIC), currency, posting_date (DATE),
         clearing_date (DATE), document_date (DATE)
"""

# ── Graph schema description ───────────────────────────────────────────────────

GRAPH_SCHEMA = """
## Apache AGE Property Graph (use Cypher)

Graph name: o2c

### Vertex Labels and key properties
- Customer   { id, name, currency, payment_terms }
- Address    { id, city, country, postal_code }
- Product    { id, description, product_group, base_unit }
- SalesOrder { id, order_type, total_net_amount, currency, creation_date, delivery_status }
- SalesOrderItem { id, item_number, requested_qty, net_amount, currency }
- OutboundDelivery { id, creation_date, picking_status, goods_movement_status }
- BillingDocument  { id, billing_doc_type, total_net_amount, billing_date, is_cancelled }
- JournalEntry     { id, accounting_document, amount, currency, posting_date }
- Payment          { id, amount, currency, clearing_date }

### Relationships (edges)
(Customer)-[:PLACED]->(SalesOrder)
(Customer)-[:HAS_ADDRESS]->(Address)
(SalesOrder)-[:CONTAINS]->(SalesOrderItem)
(SalesOrderItem)-[:REFERENCES]->(Product)
(SalesOrder)-[:HAS_DELIVERY]->(OutboundDelivery)
(OutboundDelivery)-[:BILLED_IN]->(BillingDocument)
(BillingDocument)-[:RECORDED_IN]->(JournalEntry)
(JournalEntry)-[:SETTLED_BY]->(Payment)

### Complete O2C path pattern
(c:Customer)-[:PLACED]->(so:SalesOrder)-[:HAS_DELIVERY]->(d:OutboundDelivery)
-[:BILLED_IN]->(b:BillingDocument)-[:RECORDED_IN]->(j:JournalEntry)
-[:SETTLED_BY]->(p:Payment)

### Cypher usage notes
- All vertex and edge properties are accessed with dot notation: so.id, c.name
- String comparison in Cypher: WHERE so.id = '740506'  (single quotes)
- To check a boolean property:  WHERE b.is_cancelled = true
- LIMIT is required on all queries that could return many rows
"""

# ── Join cheat-sheet for LLM ──────────────────────────────────────────────────

JOIN_PATTERNS = """
## Critical Join Patterns

### Sales Order → Delivery (via JSONB containment)
FROM sales_orders so
JOIN deliveries d ON d.reference_sales_orders::jsonb ? so.sales_order_id

### Delivery → Billing Document (via JSONB containment)
FROM deliveries d
JOIN billing_docs bd ON bd.reference_deliveries::jsonb ? d.delivery_id

### Billing Document → Journal Entry (via accounting_document)
FROM billing_docs bd
JOIN journal_entries je ON je.reference_document = bd.billing_doc_id

### Journal Entry → Payment (via clearing_document)
FROM journal_entries je
JOIN payments p ON p.clearing_document = je.clearing_document

### Full O2C chain in SQL (for analytics)
FROM customers c
JOIN sales_orders so ON so.sold_to_party = c.customer_id
JOIN deliveries d ON d.reference_sales_orders::jsonb ? so.sales_order_id
JOIN billing_docs bd ON bd.reference_deliveries::jsonb ? d.delivery_id
JOIN journal_entries je ON je.reference_document = bd.billing_doc_id
JOIN payments p ON p.clearing_document = je.clearing_document
"""

# ── Assembled context strings ──────────────────────────────────────────────────

def get_sql_context() -> str:
    """Full schema context for SQL query generation."""
    return RELATIONAL_SCHEMA + "\n" + JOIN_PATTERNS


def get_cypher_context() -> str:
    """Full schema context for Cypher query generation."""
    return GRAPH_SCHEMA


def get_full_context() -> str:
    """Combined context — used when the LLM must choose which query type."""
    return (
        "# Database Schema\n\n"
        + RELATIONAL_SCHEMA
        + "\n"
        + GRAPH_SCHEMA
        + "\n"
        + JOIN_PATTERNS
    )
