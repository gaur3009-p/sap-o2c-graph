"""
backend/db/connection.py
─────────────────────────
Single source of truth for all database interaction.

Exposes two execution surfaces:
  • run_sql(query)    — standard PostgreSQL via psycopg2
  • run_cypher(query) — Apache AGE Cypher wrapped in PostgreSQL

Both return a uniform dict:
  { "columns": [...], "rows": [[...], ...], "row_count": N }

This uniform shape is what the LLM engine receives — it never touches
psycopg2 directly, keeping the query engine fully DB-agnostic.
"""

import json
import logging
import os
import re
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2 import sql

log = logging.getLogger(__name__)

GRAPH_NAME = "o2c"

# ── Connection pool (simple singleton for single-worker dev) ──────────────────

_conn: psycopg2.extensions.connection | None = None


def _get_conn() -> psycopg2.extensions.connection:
    global _conn
    if _conn is None or _conn.closed:
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

        _conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            dbname=os.getenv("DB_NAME", "o2c_graph"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
        )
        # AGE requires this search_path on every connection
        with _conn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute('SET search_path = ag_catalog, "$user", public;')
        _conn.commit()
        log.info("Database connection established")
    return _conn


@contextmanager
def get_cursor():
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _serialize(val: Any) -> Any:
    """Make psycopg2 result values JSON-serialisable."""
    if val is None:
        return None
    if isinstance(val, (int, float, bool, str)):
        return val
    # AGE returns agtype as string — try to parse it
    if hasattr(val, "isoformat"):          # date / datetime
        return val.isoformat()
    s = str(val)
    # agtype strings often look like: "\"value\""  or  "{key: value}"
    try:
        # Strip outer quotes that AGE adds to string scalars
        stripped = s.strip()
        if stripped.startswith('"') and stripped.endswith('"'):
            return stripped[1:-1]
        parsed = json.loads(stripped)
        return parsed
    except (json.JSONDecodeError, ValueError):
        return s


def _format_result(cur) -> dict:
    """Convert a cursor result into the standard uniform dict."""
    if cur.description is None:
        return {"columns": [], "rows": [], "row_count": 0}

    columns = [desc.name for desc in cur.description]
    raw_rows = cur.fetchall()

    rows = []
    for raw_row in raw_rows:
        if isinstance(raw_row, dict):
            row = [_serialize(raw_row[col]) for col in columns]
        else:
            row = [_serialize(v) for v in raw_row]
        rows.append(row)

    return {"columns": columns, "rows": rows, "row_count": len(rows)}


# ── Public API ────────────────────────────────────────────────────────────────

def run_sql(query: str, params: tuple = ()) -> dict:
    """
    Execute a plain SQL query and return a uniform result dict.
    Params are passed as psycopg2 parameterised values (%s placeholders).
    """
    log.debug("SQL: %s", query[:200])
    with get_cursor() as cur:
        cur.execute(query, params)
        return _format_result(cur)


def run_cypher(cypher: str) -> dict:
    """
    Execute an Apache AGE Cypher query via the cypher() wrapper function.

    AGE requires every column in the RETURN clause to be declared in the
    AS clause of the outer SQL. We parse the RETURN clause to build it
    automatically, falling back to a single generic column if parsing fails.
    """
    log.debug("Cypher: %s", cypher[:200])

    # Parse the RETURN clause to determine column aliases
    return_match = re.search(r'\bRETURN\b(.+?)(?:\bLIMIT\b|\bORDER\b|\bSKIP\b|$)',
                              cypher, re.IGNORECASE | re.DOTALL)
    if return_match:
        return_clause = return_match.group(1).strip()
        # Each item may be:  expr AS alias  |  expr  |  *
        col_aliases = []
        for item in return_clause.split(","):
            item = item.strip()
            if " as " in item.lower():
                alias = re.split(r'\s+as\s+', item, flags=re.IGNORECASE)[-1].strip()
            else:
                # Use last segment of dotted path as alias:  so.id → id
                alias = item.split(".")[-1].strip()
                # Remove function calls:  COUNT(x) → count_x
                alias = re.sub(r'[^a-zA-Z0-9_]', '_', alias).strip("_") or "col"
            col_aliases.append(alias)
    else:
        col_aliases = ["result"]

    # Build the AS clause expected by AGE
    as_clause = ", ".join(f"{alias} agtype" for alias in col_aliases)

    wrapped = f"""
        SELECT * FROM cypher('{GRAPH_NAME}', $$
            {cypher}
        $$) AS ({as_clause});
    """

    with get_cursor() as cur:
        cur.execute(wrapped)
        return _format_result(cur)


def test_connection() -> bool:
    """Ping the DB. Returns True if healthy."""
    try:
        result = run_sql("SELECT 1 AS ok")
        return result["rows"][0][0] == 1
    except Exception as exc:
        log.error("DB health check failed: %s", exc)
        return False
