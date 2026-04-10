"""
Silver layer — parsed EPUB data: books and chapters.

Asset graph:
    epub_registry (bronze)
        └─ parse_epub_silver  ─┬─ silver_books
                               └─ silver_chapters

``parse_epub_silver`` downloads each new EPUB from GCS exactly once
and writes both ``silver.books`` and ``silver.chapters`` in a single pass.

BigQuery tables created (if absent):
    silver.books    — one row per EPUB
    silver.chapters — one row per chapter (with raw_text)

Idempotency:
    Assets skip rows that already exist (keyed on file_id) so
    re-running produces no duplicate data.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import polars as pl
from dagster import (
    AssetSpec,
    get_dagster_logger,
    multi_asset,
)

from dagster_project.lib.epub_parser import parse_epub
from dagster_project.resources.object_store import GCSObjectStoreResource
from dagster_project.resources.storage import BigQueryStorage

SILVER_DATASET = "exercise_book_silver"

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

_CREATE_BOOKS = """
CREATE TABLE IF NOT EXISTS `{project}.exercise_book_silver.books` (
    book_id          STRING    NOT NULL,
    file_id          STRING    NOT NULL,
    title            STRING    NOT NULL,
    author           STRING,
    language         STRING,
    isbn             STRING,
    publication_year INT64,
    storage_path     STRING    NOT NULL,
    parsed_at        TIMESTAMP NOT NULL
)
"""

_CREATE_CHAPTERS = """
CREATE TABLE IF NOT EXISTS `{project}.exercise_book_silver.chapters` (
    chapter_id      STRING    NOT NULL,
    book_id         STRING    NOT NULL,
    chapter_order   INT64     NOT NULL,
    title           STRING,
    raw_text        STRING,
    parsed_at       TIMESTAMP NOT NULL
)
"""


def _ensure_silver_tables(storage: BigQueryStorage) -> None:
    with storage.get_client() as bq:
        for ddl in (_CREATE_BOOKS, _CREATE_CHAPTERS):
            bq.query(ddl.format(project=storage.project)).result()


# ---------------------------------------------------------------------------
# parse_epub_silver  ─  multi-asset writing books + chapters
# ---------------------------------------------------------------------------


@multi_asset(
    group_name="silver",
    specs=[
        AssetSpec(
            "silver_books",
            deps=["epub_registry"],
            description="One row per EPUB: title, author, language, ISBN, year, path.",
        ),
        AssetSpec(
            "silver_chapters",
            deps=["epub_registry"],
            description="One row per chapter: ordered title + full raw text.",
        ),
    ],
    description=(
        "Downloads each new EPUB from GCS, parses it with ebooklib, "
        "and writes book metadata + chapters to BigQuery silver tables."
    ),
)
def parse_epub_silver(
    storage: BigQueryStorage,
    gcs: GCSObjectStoreResource,
):
    logger = get_dagster_logger()
    _ensure_silver_tables(storage)

    # Fetch only EPUBs not yet present in silver.books (single anti-join query)
    registered = storage.execute(
        f"""
        SELECT r.file_id, r.filename, r.storage_path
        FROM `{storage.project}.{storage.dataset}.epub_registry` AS r
        LEFT JOIN `{storage.project}.{SILVER_DATASET}.books` AS b
          USING (file_id)
        WHERE b.file_id IS NULL
        """
    )
    logger.info(f"{len(registered)} EPUB(s) pending silver parse")

    if not registered:
        logger.info("Nothing to parse — silver tables already up to date")
        return

    book_rows: list[dict] = []
    chapter_rows: list[dict] = []

    for entry in registered:
        file_id: str = entry["file_id"]
        storage_path: str = entry["storage_path"]

        # Derive blob name: gs://bucket/blob_name → blob_name
        blob_name = "/".join(storage_path.split("/")[3:])
        logger.info(f"Parsing {entry['filename']!r} from {storage_path}")

        try:
            data = gcs.download_bytes(blob_name)
        except Exception as exc:
            logger.error(f"Failed to download {blob_name!r}: {exc}")
            continue

        try:
            parsed = parse_epub(data)
        except Exception as exc:
            logger.error(f"Failed to parse {entry['filename']!r}: {exc}")
            continue

        book_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        book_rows.append(
            {
                "book_id": book_id,
                "file_id": file_id,
                "title": parsed.title,
                "author": parsed.author,
                "language": parsed.language,
                "isbn": parsed.isbn,
                "publication_year": parsed.publication_year,
                "storage_path": storage_path,
                "parsed_at": now,
            }
        )

        for chapter in parsed.chapters:
            chapter_rows.append(
                {
                    "chapter_id": str(uuid.uuid4()),
                    "book_id": book_id,
                    "chapter_order": chapter.order,
                    "title": chapter.title,
                    "raw_text": chapter.raw_text,
                    "parsed_at": now,
                }
            )

        logger.info(f"Parsed {entry['filename']!r}: {len(parsed.chapters)} chapter(s)")

    if book_rows:
        books_df = pl.DataFrame(book_rows)
        storage.write_df(f"{SILVER_DATASET}.books", books_df.to_pandas())
        logger.info(f"Wrote {len(book_rows)} book(s) to {SILVER_DATASET}.books")

    if chapter_rows:
        chapters_df = pl.DataFrame(chapter_rows)
        storage.write_df(f"{SILVER_DATASET}.chapters", chapters_df.to_pandas())
        logger.info(
            f"Wrote {len(chapter_rows)} chapter(s) to {SILVER_DATASET}.chapters"
        )
