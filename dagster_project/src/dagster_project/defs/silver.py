"""
Silver layer — parsed EPUB data: books and chapters.

Asset graph:
    epub_registry (bronze)
        └─ parse_epub_silver  ─┬─ silver_books
                               └─ silver_chapters

``parse_epub_silver`` downloads the EPUB for the current book partition
from GCS, parses it, and writes both ``silver.books`` and
``silver.chapters`` for that book.

BigQuery tables created (if absent):
    silver.books    — one row per EPUB
    silver.chapters — one row per chapter (with raw_text)

Partitioning:
    Partitioned by ``book_id`` (DynamicPartitionsDefinition ``"books"``).
    New partitions are registered by ``bronze_epub_registry_raw`` (bronze) when a
    new EPUB is detected.  Re-materialising a partition deletes and
    rewrites the book + its chapters cleanly.
"""

from datetime import datetime, timezone

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetSpec,
    get_dagster_logger,
    multi_asset,
)

from dagster_project.defs.partitions import book_partitions
from dagster_project.libs.epub_parser import parse_epub
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
    chapter_key     STRING,
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
    partitions_def=book_partitions,
    specs=[
        AssetSpec(
            "silver_books",
            deps=["bronze_epub_registry"],
            description="One row per EPUB: title, author, language, ISBN, year, path.",
        ),
        AssetSpec(
            "silver_chapters",
            deps=["bronze_epub_registry"],
            description="One row per chapter: ordered title + full raw text.",
        ),
    ],
    description=(
        "Downloads the EPUB for the current book partition from GCS, parses it "
        "with ebooklib, and writes book metadata + chapters to BigQuery silver tables."
    ),
)
def parse_epub_silver(
    context: AssetExecutionContext,
    storage: BigQueryStorage,
    gcs: GCSObjectStoreResource,
):
    logger = get_dagster_logger()
    book_id = context.partition_key
    _ensure_silver_tables(storage)

    # Fetch the epub_registry row for this book
    registered = storage.execute(
        f"""
        SELECT file_id, filename, storage_path
        FROM `{storage.project}.exercise_book_bronze.bronze_epub_registry`
        WHERE book_id = '{book_id}'
        LIMIT 1
        """
    )
    if not registered:
        logger.warning(f"No epub_registry row found for book_id {book_id!r} — skipping")
        return

    entry = registered[0]
    file_id: str = entry["file_id"]
    storage_path: str = entry["storage_path"]
    blob_name = "/".join(storage_path.split("/")[3:])
    logger.info(f"Parsing {entry['filename']!r} from {storage_path}")

    try:
        data = gcs.download_bytes(blob_name)
    except Exception as exc:
        logger.error(f"Failed to download {blob_name!r}: {exc}")
        return

    try:
        parsed = parse_epub(data)
    except Exception as exc:
        logger.error(f"Failed to parse {entry['filename']!r}: {exc}")
        return

    now = datetime.now(timezone.utc)

    # Delete existing rows for this book so re-runs are idempotent
    for table in (f"{SILVER_DATASET}.books", f"{SILVER_DATASET}.chapters"):
        storage.execute(
            f"DELETE FROM `{storage.project}.{table}` WHERE book_id = '{book_id}'"
        )

    book_row = {
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
    storage.write_df(f"{SILVER_DATASET}.books", pl.DataFrame([book_row]).to_pandas())
    logger.info(f"Wrote book {book_id!r} to {SILVER_DATASET}.books")

    chapter_rows = [
        {
            "chapter_id": f"{book_id}-{chapter.order}",
            "book_id": book_id,
            "chapter_order": chapter.order,
            "chapter_key": chapter.chapter_key,
            "title": chapter.title,
            "raw_text": chapter.raw_text,
            "parsed_at": now,
        }
        for chapter in parsed.chapters
    ]
    if chapter_rows:
        storage.write_df(
            f"{SILVER_DATASET}.chapters",
            pl.DataFrame(chapter_rows).to_pandas(),
        )
    logger.info(f"Wrote {len(chapter_rows)} chapter(s) to {SILVER_DATASET}.chapters")
