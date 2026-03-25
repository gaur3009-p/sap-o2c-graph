"""
backend/llm/nl2query_engine.py
────────────────────────────────
The heart of the system. Translates a natural language question into a
SQL or Cypher query, executes it safely, then returns a grounded answer.

Pipeline
────────
  User question
       │
       ▼
  [Step 1] Classify: is this question answerable from the O2C dataset?
           If not → return the standard "out of scope" message immediately.
       │
       ▼
  [Step 2] Choose query type: SQL (aggregations, counts, joins)
           or Cypher (path traversals, relationship chains).
       │
       ▼
  [Step 3] Generate the query, injecting the full schema context.
       │
       ▼
  [Step 4] Safety check — reject if write/DDL detected.
       │
       ▼
  [Step 5] Execute against the database.
       │
       ▼
  [Step 6] Pass raw results + original question back to LLM.
           LLM synthesises a plain-English answer grounded ONLY in the data.
       │
       ▼
  Structured response: { answer, query, query_type, row_count, columns, rows }

Guardrails
──────────
  • System prompts explicitly forbid general knowledge answers.
  • The LLM must return only JSON in Steps 2/3 — no prose.
  • Safety check runs before any query reaches the DB.
  • If the DB returns 0 rows the LLM is told: "no data was found".
  • Errors surface as structured { error: "..." } — never raw tracebacks.
"""

import json
import logging
import os
import re
from typing import Any

log = logging.getLogger(__name__)

# ── Import Groq via LangChain ─────────────────────────────────────────────────
try:
    from langchain_groq import ChatGroq
    from langchain.schema import HumanMessage, SystemMessage
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    log.warning("langchain-groq not installed — LLM calls will be stubbed")

from backend.db.connection import run_sql
from backend.llm.schema_context import get_sql_context
from backend.llm.query_safety import check_query

# ── LLM initialisation ────────────────────────────────────────────────────────

def _get_llm():
    """Initialise the Groq ChatGroq client. Called lazily on first use."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    api_key = os.getenv("GROQ_API_KEY")
    model   = os.getenv("GROQ_MODEL", "llama-3.1-70b-versatile")

    if not api_key:
        raise ValueError(
            "GROQ_API_KEY is not set. Add it to your .env file. "
            "Get a free key at https://console.groq.com"
        )

    return ChatGroq(
        api_key=api_key,
        model=model,
        temperature=0,          # Deterministic — critical for query generation
        max_tokens=1024,
    )


# ── Prompt templates ──────────────────────────────────────────────────────────

# ① Relevance gate — classify before doing any work
RELEVANCE_SYSTEM = """You are a strict data relevance classifier for a SAP Order-to-Cash (O2C) database system.

Your ONLY job is to decide if a user's question can be answered using data from this specific SAP O2C dataset.

The dataset contains: Sales Orders, Deliveries, Billing Documents, Journal Entries, Payments, Customers, Products, Addresses.

Respond with ONLY a JSON object — no explanation, no markdown:
{"relevant": true}   — if the question can be answered from this dataset
{"relevant": false}  — if the question requires general knowledge, opinions, creative writing, or data not in this dataset

Examples of RELEVANT questions:
- "Which products are in the most billing documents?"
- "Show me orders that were never billed"
- "What is the total payment amount for customer 310000108?"
- "Trace billing document 90504248 through the full O2C flow"

Examples of NOT RELEVANT questions:
- "What is the capital of France?"
- "Write me a poem"
- "How does SAP work in general?"
- "What will the stock market do tomorrow?"
"""

# ② Query type selector
QUERY_TYPE_SYSTEM = """You are a database query type selector for a SAP O2C system with two query interfaces:
1. SQL   — best for: aggregations (COUNT, SUM, AVG), filtering, ranking, comparisons, detecting missing records
2. Cypher — best for: tracing document flows/paths, finding connected nodes, graph traversals

Respond with ONLY a JSON object — no explanation, no markdown:
{"query_type": "sql"}    or    {"query_type": "cypher"}

Choose Cypher ONLY when the question explicitly asks to "trace", "follow the path", "show the full flow", or "find connected" nodes.
For everything else (counts, totals, rankings, missing records), choose SQL.
"""

# ③ SQL generator
SQL_GENERATION_SYSTEM = """You are an expert PostgreSQL query writer for a SAP Order-to-Cash database.

{schema}

RULES — follow every one precisely:
1. Return ONLY a JSON object with a single "query" key — no explanation, no markdown, no code fences.
   Example: {{"query": "SELECT customer_id FROM customers LIMIT 10;"}}
2. Write only SELECT queries. Never use INSERT, UPDATE, DELETE, DROP, or any DDL.
3. Use the JSONB join patterns exactly as documented — do not invent join conditions.
4. Always include a LIMIT clause (default LIMIT 50 unless the question implies a specific count).
5. Use table aliases (c for customers, so for sales_orders, d for deliveries, bd for billing_docs, je for journal_entries, p for payments, pr for products, soi for sales_order_items).
6. If the question cannot be answered from the available data, return: {{"query": null, "reason": "explanation"}}

CRITICAL JSONB JOIN PATTERN — memorise this, never deviate:
- deliveries → sales_orders:  d.reference_sales_orders::jsonb ? so.sales_order_id
- billing_docs → deliveries:  bd.reference_deliveries::jsonb ? d.delivery_id
"""

# ④ Cypher generator
CYPHER_GENERATION_SYSTEM = """You are an expert Apache AGE Cypher query writer for a SAP Order-to-Cash graph database.

{schema}

RULES — follow every one precisely:
1. Return ONLY a JSON object with a single "query" key — no explanation, no markdown, no code fences.
   Example: {{"query": "MATCH (c:Customer)-[:PLACED]->(so:SalesOrder) RETURN c.name, so.id LIMIT 10"}}
2. Write only MATCH queries. Never use CREATE, SET, DELETE, MERGE, or any write clause.
3. Always include a LIMIT clause (default LIMIT 25 unless specified).
4. Vertex and relationship labels are case-sensitive: Customer, SalesOrder, OutboundDelivery, BillingDocument, JournalEntry, Payment, Product, SalesOrderItem, Address.
5. All property values in WHERE clauses must use single quotes: WHERE so.id = '740506'
6. If the question cannot be answered from the graph, return: {{"query": null, "reason": "explanation"}}
"""

# ⑤ Answer synthesiser — grounded, never hallucinates
ANSWER_SYSTEM = """You are a data analyst answering business questions about a SAP Order-to-Cash dataset.

You have been given:
- The user's original question
- The database query that was executed
- The raw data results

STRICT RULES:
1. Answer ONLY based on the data provided. Do not use general knowledge.
2. If the data is empty (0 rows), say clearly: "No matching records were found in the dataset for this query."
3. If the data partially answers the question, say what you found and what is missing.
4. Format numbers with commas for readability. Use the currency field when present.
5. Keep answers concise and business-focused — 2-5 sentences maximum unless a table is needed.
6. For list results with more than 5 items, show the top 5 and note how many total were found.
7. Never invent data, never speculate beyond what the rows show.
"""

# ── Out-of-scope fallback ─────────────────────────────────────────────────────

OUT_OF_SCOPE_RESPONSE = (
    "This system is designed to answer questions related to the provided "
    "SAP Order-to-Cash dataset only. Your question appears to be outside "
    "the scope of the available data (Sales Orders, Deliveries, Billing "
    "Documents, Journal Entries, Payments, Customers, and Products). "
    "Please ask a question about the O2C dataset."
)

# ── Helper: call LLM and parse JSON response ──────────────────────────────────

def _llm_json(llm, system: str, user: str) -> dict:
    """
    Call the LLM with a system + user message, expecting a JSON response.
    Strips markdown code fences if the model adds them despite instructions.
    Returns the parsed dict, or {"error": "..."} on failure.
    """
    messages = [
        SystemMessage(content=system),
        HumanMessage(content=user),
    ]
    response = llm.invoke(messages)
    raw = response.content.strip()

    # Strip markdown fences the model sometimes adds despite instructions
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Try extracting a JSON object from the middle of the text
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        log.warning("LLM returned non-JSON: %s", raw[:300])
        return {"error": f"LLM returned non-JSON response: {raw[:200]}"}


# ── Result formatting ─────────────────────────────────────────────────────────

def _format_rows_for_llm(columns: list, rows: list, max_rows: int = 30) -> str:
    """
    Format DB results as a compact string for the answer-synthesis prompt.
    Truncates to max_rows to stay within context limits.
    """
    if not rows:
        return "No rows returned."

    truncated = rows[:max_rows]
    lines = ["| " + " | ".join(str(c) for c in columns) + " |"]
    lines.append("|" + "|".join("---" for _ in columns) + "|")
    for row in truncated:
        lines.append("| " + " | ".join(str(v) if v is not None else "NULL" for v in row) + " |")

    result = "\n".join(lines)
    if len(rows) > max_rows:
        result += f"\n\n(Showing {max_rows} of {len(rows)} total rows)"
    return result


# ── Main pipeline ─────────────────────────────────────────────────────────────

def answer_question(question: str) -> dict[str, Any]:
    """
    Full NL2Query pipeline. Returns a dict:
    {
      "answer":     str,        # plain-English answer
      "query":      str | None, # the SQL or Cypher that was executed
      "query_type": str,        # "sql" | "cypher" | "none"
      "columns":    list,       # column names from the DB result
      "rows":       list,       # raw result rows (for frontend table display)
      "row_count":  int,
      "error":      str | None  # set only on failure
    }
    """
    result = {
        "answer": "",
        "query": None,
        "query_type": "none",
        "columns": [],
        "rows": [],
        "row_count": 0,
        "error": None,
    }

    try:
        llm = _get_llm()

        # ── Step 1: Relevance gate ────────────────────────────────────────────
        log.info("Step 1: Checking relevance for: %s", question[:100])
        relevance = _llm_json(llm, RELEVANCE_SYSTEM, question)

        if not relevance.get("relevant", False):
            log.info("Question classified as out of scope")
            result["answer"] = OUT_OF_SCOPE_RESPONSE
            return result

        # ── Step 2: Choose query type ─────────────────────────────────────────
        log.info("Step 2: Choosing query type")
        query_type = "sql"
        result["query_type"] = "sql"
        log.info("Query type chosen: %s", query_type)

        # ── Step 3: Generate query ────────────────────────────────────────────
        log.info("Step 3: Generating %s query", query_type)
        system_prompt = SQL_GENERATION_SYSTEM.format(schema=get_sql_context())
        gen_result = _llm_json(llm, system_prompt, question)

        if gen_result.get("query") is None:
            reason = gen_result.get("reason", "The question cannot be answered from the available data.")
            log.info("LLM could not generate a query: %s", reason)
            result["answer"] = f"I was unable to find data to answer this question. {reason}"
            return result

        query = gen_result["query"]
        result["query"] = query
        log.info("Generated query: %s", query[:200])

        # ── Step 4: Safety check ──────────────────────────────────────────────
        log.info("Step 4: Running safety check")
        safety = check_query(query, "sql")
        if not safety.ok:
            log.warning("Safety check failed: %s", safety.reason)
            result["error"] = f"The generated query failed the safety check: {safety.reason}"
            result["answer"] = "I was unable to safely execute this query. Please rephrase your question."
            return result

        # ── Step 5: Execute query ─────────────────────────────────────────────
        log.info("Step 5: Executing query")
        try:
            db_result = run_sql(query)
        except Exception as db_err:
            log.error("DB execution error: %s", db_err)
            # Attempt one self-correction pass
            log.info("Attempting query self-correction …")
            correction_prompt = (
                f"The following {query_type.upper()} query produced this error:\n\n"
                f"Query: {query}\n\n"
                f"Error: {str(db_err)}\n\n"
                f"Please fix the query and return a corrected version as JSON: "
                f'{{\"query\": \"corrected query here\"}}'
            )
            correction = _llm_json(llm, system_prompt, correction_prompt)
            corrected_query = correction.get("query")

            if corrected_query:
                safety2 = check_query(corrected_query, "sql")
                if safety2.ok:
                    try:
                        db_result = run_sql(query)
                        result["query"] = corrected_query
                        log.info("Self-correction succeeded")
                    except Exception as db_err2:
                        result["error"] = str(db_err2)
                        result["answer"] = "I encountered a database error while executing the query."
                        return result
                else:
                    result["error"] = str(db_err)
                    result["answer"] = "I encountered a database error while executing the query."
                    return result
            else:
                result["error"] = str(db_err)
                result["answer"] = "I encountered a database error while executing the query."
                return result

        result["columns"]  = db_result["columns"]
        result["rows"]     = db_result["rows"]
        result["row_count"] = db_result["row_count"]
        log.info("Query returned %d rows", db_result["row_count"])

        # ── Step 6: Synthesise natural language answer ────────────────────────
        log.info("Step 6: Synthesising answer")
        data_summary = _format_rows_for_llm(db_result["columns"], db_result["rows"])

        answer_prompt = (
            f"User question: {question}\n\n"
            f"Query executed ({query_type.upper()}):\n{query}\n\n"
            f"Results ({db_result['row_count']} rows):\n{data_summary}"
        )

        answer_messages = [
            SystemMessage(content=ANSWER_SYSTEM),
            HumanMessage(content=answer_prompt),
        ]
        answer_response = llm.invoke(answer_messages)
        result["answer"] = answer_response.content.strip()

        log.info("Pipeline complete. Answer: %s", result["answer"][:100])
        return result

    except ValueError as ve:
        # Config errors (missing API key, etc.)
        log.error("Configuration error: %s", ve)
        result["error"] = str(ve)
        result["answer"] = f"Configuration error: {ve}"
        return result

    except Exception as exc:
        log.exception("Unexpected error in NL2Query pipeline")
        result["error"] = str(exc)
        result["answer"] = "An unexpected error occurred while processing your question."
        return result
