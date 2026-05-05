"""
FastAPI application entrypoint.

Run locally:
  uvicorn api.main:app --reload --port 8000

Environment variables required:
  GCP_PROJECT  — GCP project id, e.g. "dan-learning-0929"

Authentication:
  Uses Google Application Default Credentials.
  Run `gcloud auth application-default login` before starting.
"""

from dotenv import load_dotenv

load_dotenv()  # loads api/.env (or project-root .env) if present

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import books

app = FastAPI(
    title="Exercise Book API",
    description="Serves the dbt gold layer from BigQuery to the frontend.",
    version="0.1.0",
)

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
