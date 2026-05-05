"""
SQLAlchemy ORM models for the PostgreSQL serving database.

Dagster reverse-ETL writes the dbt gold layer into this database.
The primary serving table is `gold_chapter_enriched` — one row per
chapter, fully denormalised with book metadata and all LLM outputs.
"""

from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class ChapterEnriched(Base):
    """
    Maps to: exercise_book_gold.gold_chapter_enriched

    Materialised by dbt model `gold_chapter_enriched`. This is the main
    serving table consumed by the API.  One row per chapter, fully
    denormalised with book metadata and all LLM outputs.
    """

    __tablename__ = "gold_chapter_enriched"
    __table_args__ = {"schema": "gold"}  # PostgreSQL schema written by Dagster

    # --- keys ---
    chapter_id = Column(String, primary_key=True, nullable=False)
    book_id = Column(String, nullable=False, index=True)

    # --- book metadata ---
    book_title = Column(String)

    # --- chapter metadata ---
    chapter_order = Column(Integer)
    source_language = Column(String)
    category = Column(String)

    # --- text content ---
    raw_text = Column(Text)

    # --- LLM gold outputs ---
    a2_text = Column(Text)
    a2_summary = Column(Text)
    b1b2_text = Column(Text)
    swedish_text = Column(Text)
