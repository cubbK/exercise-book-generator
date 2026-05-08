"""
FastAPI application entrypoint.

Run locally:
  fastapi dev api/main.py

Environment variables required:
  DATABASE_URL  — PostgreSQL connection string
"""

from dotenv import load_dotenv

load_dotenv()  # loads api/.env (or project-root .env) if present

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import text

from api.db import engine
from api.routers import books

app = FastAPI(
    title="Exercise Book API",
    description="Serves the gold layer written by Dagster into the app's own Postgres DB.",
    version="0.1.0",
)


@app.on_event("startup")
def check_db():
    """Verify the DB is reachable. Schema is managed by Alembic, not here."""
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


# Allow the Vite dev server (and any other origin in dev).
# Tighten this to your production domain before deploying.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(books.router)


@app.get("/health")
def health():
    return {"status": "ok"}
