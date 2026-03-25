"""
tests/test_safety.py
─────────────────────
Unit tests for the query safety layer.
These tests run without a database connection.

Run with:  pytest tests/test_safety.py -v
"""

import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.llm.query_safety import check_sql, check_cypher, check_query


# ── SQL safety tests ──────────────────────────────────────────────────────────

class TestSQLSafety:

    def test_valid_select(self):
        result = check_sql("SELECT * FROM customers LIMIT 10")
        assert result.ok

    def test_valid_select_with_cte(self):
        result = check_sql("""
            WITH ranked AS (
                SELECT product_id, COUNT(*) AS cnt FROM billing_docs GROUP BY 1
            )
            SELECT * FROM ranked ORDER BY cnt DESC LIMIT 5
        """)
        assert result.ok

    def test_rejects_insert(self):
        result = check_sql("INSERT INTO customers VALUES ('x', 'y')")
        assert not result.ok
        assert "INSERT" in result.reason

    def test_rejects_update(self):
        result = check_sql("UPDATE customers SET full_name = 'hacked' WHERE 1=1")
        assert not result.ok

    def test_rejects_delete(self):
        result = check_sql("DELETE FROM payments")
        assert not result.ok

    def test_rejects_drop(self):
        result = check_sql("DROP TABLE customers")
        assert not result.ok

    def test_rejects_truncate(self):
        result = check_sql("TRUNCATE TABLE billing_docs")
        assert not result.ok

    def test_rejects_empty(self):
        result = check_sql("")
        assert not result.ok

    def test_rejects_non_select(self):
        result = check_sql("SHOW TABLES")
        assert not result.ok

    def test_rejects_pg_sleep(self):
        result = check_sql("SELECT pg_sleep(10)")
        assert not result.ok

    def test_rejects_overlong_query(self):
        result = check_sql("SELECT " + "x, " * 2000 + "1")
        assert not result.ok

    def test_valid_complex_join(self):
        result = check_sql("""
            SELECT p.description, COUNT(DISTINCT bd.billing_doc_id) AS cnt
            FROM products p
            JOIN sales_order_items soi ON soi.material = p.product_id
            JOIN deliveries d ON d.reference_sales_orders::jsonb ? soi.sales_order_id
            JOIN billing_docs bd ON bd.reference_deliveries::jsonb ? d.delivery_id
            GROUP BY p.description
            ORDER BY cnt DESC
            LIMIT 10
        """)
        assert result.ok


# ── Cypher safety tests ───────────────────────────────────────────────────────

class TestCypherSafety:

    def test_valid_match(self):
        result = check_cypher(
            "MATCH (c:Customer)-[:PLACED]->(so:SalesOrder) RETURN c.name, so.id LIMIT 10"
        )
        assert result.ok

    def test_valid_optional_match(self):
        result = check_cypher(
            "MATCH (so:SalesOrder) OPTIONAL MATCH (so)-[:HAS_DELIVERY]->(d) RETURN so.id, d.id LIMIT 10"
        )
        assert result.ok

    def test_rejects_create(self):
        result = check_cypher("CREATE (c:Customer {id: 'hack'})")
        assert not result.ok

    def test_rejects_delete(self):
        result = check_cypher("MATCH (n) DELETE n")
        assert not result.ok

    def test_rejects_set(self):
        result = check_cypher("MATCH (c:Customer) SET c.name = 'hacked'")
        assert not result.ok

    def test_rejects_merge(self):
        result = check_cypher("MERGE (c:Customer {id: 'x'})")
        assert not result.ok

    def test_rejects_empty(self):
        result = check_cypher("")
        assert not result.ok

    def test_rejects_non_match(self):
        result = check_cypher("RETURN 1")
        assert not result.ok


# ── Dispatcher tests ──────────────────────────────────────────────────────────

class TestDispatcher:

    def test_dispatches_sql(self):
        result = check_query("SELECT 1", "sql")
        assert result.ok

    def test_dispatches_cypher(self):
        result = check_query(
            "MATCH (c:Customer) RETURN c.name LIMIT 5", "cypher"
        )
        assert result.ok

    def test_rejects_sql_write_via_dispatcher(self):
        result = check_query("DROP TABLE customers", "sql")
        assert not result.ok

    def test_rejects_cypher_write_via_dispatcher(self):
        result = check_query("CREATE (n:Evil)", "cypher")
        assert not result.ok

    def test_unknown_type_defaults_to_sql(self):
        # Unknown type falls back to SQL check
        result = check_query("SELECT 1", "unknown_type")
        assert result.ok


# ── Schema context tests ──────────────────────────────────────────────────────

class TestSchemaContext:

    def test_sql_context_has_all_tables(self):
        from backend.llm.schema_context import get_sql_context
        ctx = get_sql_context()
        for table in ["customers", "sales_orders", "deliveries", "billing_docs",
                      "journal_entries", "payments", "products", "sales_order_items"]:
            assert table in ctx, f"Table '{table}' missing from SQL context"

    def test_cypher_context_has_all_labels(self):
        from backend.llm.schema_context import get_cypher_context
        ctx = get_cypher_context()
        for label in ["Customer", "SalesOrder", "OutboundDelivery",
                      "BillingDocument", "JournalEntry", "Payment"]:
            assert label in ctx, f"Label '{label}' missing from Cypher context"

    def test_sql_context_has_jsonb_join_pattern(self):
        from backend.llm.schema_context import get_sql_context
        ctx = get_sql_context()
        assert "reference_sales_orders::jsonb ?" in ctx
        assert "reference_deliveries::jsonb ?" in ctx

    def test_full_context_combines_both(self):
        from backend.llm.schema_context import get_full_context
        ctx = get_full_context()
        assert "customers" in ctx       # relational
        assert "Customer" in ctx        # graph
        assert "MATCH" in ctx           # Cypher hint
        assert "SELECT" in ctx          # SQL hint
