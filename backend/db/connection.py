import json
import logging
import os
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

_conn = None


def _get_conn():
    global _conn
    if _conn is None or _conn.closed:
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

        _conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", 5432)),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )

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
    if val is None:
        return None
    if isinstance(val, (int, float, bool, str)):
        return val
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


def _format_result(cur):
    if cur.description is None:
        return {"columns": [], "rows": [], "row_count": 0}

    columns = [desc.name for desc in cur.description]
    raw_rows = cur.fetchall()

    rows = []
    for raw_row in raw_rows:
        row = [_serialize(raw_row[col]) for col in columns]
        rows.append(row)

    return {"columns": columns, "rows": rows, "row_count": len(rows)}


# ✅ ONLY SQL NOW (NO CYPHER)

def run_sql(query: str, params: tuple = ()) -> dict:
    log.debug("SQL: %s", query[:200])
    with get_cursor() as cur:
        cur.execute(query, params)
        return _format_result(cur)


def test_connection():
    try:
        result = run_sql("SELECT 1 AS ok")
        return result["rows"][0][0] == 1
    except Exception as exc:
        log.error("DB health check failed: %s", exc)
        return False
