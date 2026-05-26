"""
FastAPI application entrypoint.

Run locally:
  fastapi dev api/main.py

Environment variables required:
  DATABASE_URL  — PostgreSQL connection string
"""

import os

from dotenv import load_dotenv

load_dotenv()  # loads api/.env (or project-root .env) if present — must run before api.* imports

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import text
from typing import Annotated

from api.db import engine
from api.routers import books

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


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


# Allow the Vite dev server during local development.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(books.router, prefix="/api")


@app.get("/health")
def health():
    return {"status": "ok"}


# Serve the built frontend (populated by the Docker multi-stage build).
_static = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.isdir(_static):
    app.mount("/", StaticFiles(directory=_static, html=True), name="frontend")
