"""
Pydantic response schemas — the data contracts exposed by the API.

These are intentionally separate from the SQLAlchemy ORM models so the
API shape can evolve independently of the DB schema.
"""

from typing import Optional
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Books
# ---------------------------------------------------------------------------


class BookSummary(BaseModel):
    """Lightweight book card for the listing page."""

    book_id: str
    book_title: str
    chapter_count: int

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Chapters
# ---------------------------------------------------------------------------


class ChapterSummary(BaseModel):
    """Chapter row shown in the book's table of contents."""

    chapter_id: str
    chapter_order: int
    category: Optional[str] = None
    source_language: Optional[str] = None

    model_config = {"from_attributes": True}


class ChapterDetail(BaseModel):
    """Full chapter with all gold-layer LLM outputs."""

    chapter_id: str
    book_id: str
    book_title: Optional[str] = None
    chapter_order: int
    source_language: Optional[str] = None
    category: Optional[str] = None

    # text variants
    raw_text: Optional[str] = None
    a2_text: Optional[str] = None
    a2_summary: Optional[str] = None
    b1b2_text: Optional[str] = None
    swedish_text: Optional[str] = None

    model_config = {"from_attributes": True}
