"""
Database engine and session factory.

Connects to PostgreSQL using the DATABASE_URL environment variable.
Dagster reverse-ETL writes the gold layer into this database.

Example DATABASE_URL:
  postgresql://user:password@localhost:5432/exercise_book
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.environ["DATABASE_URL"]

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_db():
    """FastAPI dependency that yields a SQLAlchemy session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
