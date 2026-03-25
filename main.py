"""
main.py
────────
Entry point for the FastAPI server.

Usage:
    python main.py
    # or with auto-reload for development:
    uvicorn main:app --reload --host 0.0.0.0 --port 8000
"""

import logging
import os

import uvicorn

# Configure logging before importing the app
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)

from backend.api.app import app  # noqa: E402  (import after logging setup)

if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", 8000))
    reload = os.getenv("ENV", "development") == "development"

    print(f"\n{'='*55}")
    print("  SAP O2C Graph Intelligence API")
    print(f"  http://{host}:{port}")
    print(f"  Swagger UI: http://localhost:{port}/docs")
    print(f"  Health:     http://localhost:{port}/api/health")
    print(f"{'='*55}\n")

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )
