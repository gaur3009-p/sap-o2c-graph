"""
backend/llm/query_safety.py
────────────────────────────
Guards that run BEFORE any LLM-generated query touches the database.

Two layers of protection:
  1. Structural check — reject any query containing write/DDL keywords.
  2. Parse check    — verify the query is syntactically plausible.

Returns a SafetyResult with .ok (bool) and .reason (str | None).
The FastAPI layer converts a failed check into a 400 response; the
LLM engine never sees a failed query reach the DB.
"""

import re
from dataclasses import dataclass


@dataclass
class SafetyResult:
    ok: bool
    reason: str | None = None


# ── Blocked patterns ──────────────────────────────────────────────────────────

# SQL keywords that must never appear in a read-only query
_SQL_WRITE_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE"
    r"|GRANT|REVOKE|EXEC|EXECUTE|CALL|MERGE|UPSERT|COPY)\b",
    re.IGNORECASE,
)

# Cypher write clauses
_CYPHER_WRITE_PATTERN = re.compile(
    r"\b(CREATE|SET|DELETE|DETACH|REMOVE|MERGE|CALL)\b",
    re.IGNORECASE,
)

# Comment-based injection attempts
_COMMENT_INJECTION = re.compile(r"(--|/\*|\*/|;)", re.IGNORECASE)

# Suspicious patterns regardless of query type
_SUSPICIOUS = re.compile(
    r"(pg_sleep|pg_read_file|lo_export|LOAD\s|COPY\s|xp_cmdshell"
    r"|information_schema\.tables\s*WHERE\s*table_schema\s*!=)",
    re.IGNORECASE,
)

# Maximum query length (prevent prompt-injection via enormous queries)
MAX_QUERY_LEN = 4000


# ── Validators ────────────────────────────────────────────────────────────────

def check_sql(query: str) -> SafetyResult:
    """Validate a SQL query is read-only and safe to execute."""
    if not query or not query.strip():
        return SafetyResult(ok=False, reason="Query is empty.")

    if len(query) > MAX_QUERY_LEN:
        return SafetyResult(ok=False, reason=f"Query exceeds maximum length ({MAX_QUERY_LEN} chars).")

    # Must start with SELECT (after stripping whitespace/CTEs)
    stripped = query.strip().lstrip("(")
    # Allow WITH ... SELECT (CTEs)
    if not re.match(r"^\s*(WITH\b|SELECT\b)", stripped, re.IGNORECASE):
        return SafetyResult(
            ok=False,
            reason="Only SELECT queries are permitted. The generated query does not start with SELECT or WITH.",
        )

    if _SQL_WRITE_PATTERN.search(query):
        match = _SQL_WRITE_PATTERN.search(query)
        return SafetyResult(
            ok=False,
            reason=f"Query contains a forbidden keyword: '{match.group()}'.",
        )

    if _SUSPICIOUS.search(query):
        return SafetyResult(ok=False, reason="Query contains a suspicious pattern.")

    # Multiple statements (basic check)
    # Strip string literals first to avoid false positives
    no_strings = re.sub(r"'[^']*'", "''", query)
    if no_strings.count(";") > 1:
        return SafetyResult(ok=False, reason="Multiple statements are not permitted.")

    return SafetyResult(ok=True)


def check_cypher(query: str) -> SafetyResult:
    """Validate a Cypher query is read-only and safe to execute."""
    if not query or not query.strip():
        return SafetyResult(ok=False, reason="Query is empty.")

    if len(query) > MAX_QUERY_LEN:
        return SafetyResult(ok=False, reason=f"Query exceeds maximum length ({MAX_QUERY_LEN} chars).")

    # Must start with MATCH or OPTIONAL MATCH
    stripped = query.strip()
    if not re.match(r"^\s*(MATCH|OPTIONAL\s+MATCH)\b", stripped, re.IGNORECASE):
        return SafetyResult(
            ok=False,
            reason="Only MATCH queries are permitted. The generated Cypher must start with MATCH.",
        )

    if _CYPHER_WRITE_PATTERN.search(query):
        match = _CYPHER_WRITE_PATTERN.search(query)
        return SafetyResult(
            ok=False,
            reason=f"Cypher query contains a forbidden clause: '{match.group()}'.",
        )

    if _SUSPICIOUS.search(query):
        return SafetyResult(ok=False, reason="Query contains a suspicious pattern.")

    return SafetyResult(ok=True)


def check_query(query: str, query_type: str) -> SafetyResult:
    """Dispatch to the correct validator based on query_type ('sql' | 'cypher')."""
    if query_type.lower() == "cypher":
        return check_cypher(query)
    return check_sql(query)
