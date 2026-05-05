from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct

from api.db import get_db
from api.models import ChapterEnriched
from api.schemas import BookSummary, ChapterSummary, ChapterDetail

router = APIRouter(prefix="/books", tags=["books"])


@router.get("", response_model=list[BookSummary])
def list_books(db: Session = Depends(get_db)):
    """
    Return all books with chapter counts.
    Queries the gold_chapter_enriched table — one row per chapter —
    and aggregates to one row per book.
    """
    rows = (
        db.query(
            ChapterEnriched.book_id,
            ChapterEnriched.book_title,
            func.count(ChapterEnriched.chapter_id).label("chapter_count"),
        )
        .group_by(ChapterEnriched.book_id, ChapterEnriched.book_title)
        .order_by(ChapterEnriched.book_title)
        .all()
    )
    return [
        BookSummary(
            book_id=r.book_id, book_title=r.book_title, chapter_count=r.chapter_count
        )
        for r in rows
    ]


@router.get("/{book_id}/chapters", response_model=list[ChapterSummary])
def list_chapters(book_id: str, db: Session = Depends(get_db)):
    """
    Return the table of contents for a book: chapter stubs in reading order.
    """
    rows = (
        db.query(
            ChapterEnriched.chapter_id,
            ChapterEnriched.chapter_order,
            ChapterEnriched.category,
            ChapterEnriched.source_language,
        )
        .filter(ChapterEnriched.book_id == book_id)
        .order_by(ChapterEnriched.chapter_order)
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Book not found or has no chapters")
    return [ChapterSummary.model_validate(r) for r in rows]


@router.get("/{book_id}/chapters/{chapter_id}", response_model=ChapterDetail)
def get_chapter(book_id: str, chapter_id: str, db: Session = Depends(get_db)):
    """
    Return a single chapter with all gold-layer text variants.
    """
    row = (
        db.query(ChapterEnriched)
        .filter(
            ChapterEnriched.book_id == book_id,
            ChapterEnriched.chapter_id == chapter_id,
        )
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Chapter not found")
    return ChapterDetail.model_validate(row)
