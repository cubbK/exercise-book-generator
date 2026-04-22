"""
Gold layer — Language detection for book chapters.

Asset graph:
    silver_chapters
        └─ gold_chapter_language

``gold_chapter_language`` runs each chapter through a LangGraph
detect → judge loop (Vertex AI) and writes the results to
``exercise_book_gold.chapter_language``.

BigQuery table created (if absent):
    gold.chapter_language — one row per chapter

Partitioning:
    Partitioned by ``book_id`` (DynamicPartitionsDefinition ``"books"``).
    New partitions are registered by ``parse_epub_silver`` when a new
    book is parsed. Re-materialising a partition deletes and rewrites
    all rows for that book.
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset, get_dagster_logger

from dagster_project.defs.partitions import book_partitions
from dagster_project.libs.language_detector import (
    build_detector_graph,
    detect_language,
)
from dagster_project.resources.storage import BigQueryStorage
from dagster_project.resources.vertex_ai import VertexAIResource

GOLD_DATASET = "exercise_book_gold"
SILVER_DATASET = "exercise_book_silver"

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

_CREATE_CHAPTER_LANGUAGE = """
CREATE TABLE IF NOT EXISTS `{project}.exercise_book_gold.chapter_language` (
    chapter_id          STRING    NOT NULL,
    book_id             STRING    NOT NULL,
    detected_language   STRING    NOT NULL,
    attempts            INT64     NOT NULL,
    detected_at         TIMESTAMP NOT NULL
)
"""


def _ensure_gold_tables(storage: BigQueryStorage) -> None:
    with storage.get_client() as bq:
        bq.query(_CREATE_CHAPTER_LANGUAGE.format(project=storage.project)).result()


# ---------------------------------------------------------------------------
# gold_chapter_language asset
# ---------------------------------------------------------------------------


@asset(
    name="gold_chapter_language",
    group_name="gold",
    deps=["silver_chapters", "gold_chapter_categories"],
    partitions_def=book_partitions,
    description=(
        "Detects the language (ISO 639-1) of each chapter in silver.chapters "
        "using a LangGraph detect→judge loop backed by Vertex AI. Writes results "
        "to exercise_book_gold.chapter_language. Partitioned by book_id."
    ),
)
def gold_chapter_language(
    context: AssetExecutionContext,
    storage: BigQueryStorage,
    vertex_ai: VertexAIResource,
) -> MaterializeResult:
    logger = get_dagster_logger()
    book_id = context.partition_key
    _ensure_gold_tables(storage)

    # Fetch only chapters categorised as "chapter" (exclude "other")
    pending = storage.execute(
        f"""
        SELECT c.chapter_id, c.title, c.raw_text
        FROM `{storage.project}.{SILVER_DATASET}.chapters` c
        INNER JOIN `{storage.project}.{GOLD_DATASET}.chapter_categories` cc
            ON cc.chapter_id = c.chapter_id AND cc.book_id = c.book_id
        WHERE c.book_id = '{book_id}'
          AND cc.category = 'chapter'
        """
    )
    logger.info(f"{len(pending)} chapter(s) found for book {book_id!r}")

    if not pending:
        logger.info(f"No chapters found for book {book_id!r} — skipping")
        return MaterializeResult(metadata={"chapters_detected": 0})

    graph = build_detector_graph(vertex_ai.get_llm())
    now = datetime.now(timezone.utc)
    rows: list[dict] = []

    for chapter in pending:
        chapter_id: str = chapter["chapter_id"]
        title: str = chapter["title"] or ""
        raw_text: str = chapter["raw_text"] or ""

        try:
            state = detect_language(graph, chapter_id, title, raw_text)
            rows.append(
                {
                    "chapter_id": chapter_id,
                    "book_id": book_id,
                    "detected_language": state["detected_language"],
                    "attempts": state["attempts"],
                    "detected_at": now,
                }
            )
            logger.info(
                f"chapter {chapter_id!r} → {state['detected_language']!r} "
                f"(attempts={state['attempts']}, judgment={state['judgment']!r})"
            )
        except Exception as exc:
            logger.error(f"Failed to detect language for chapter {chapter_id!r}: {exc}")

    if rows:
        # Delete previous results for this book so re-runs are idempotent
        storage.execute(
            f"DELETE FROM `{storage.project}.{GOLD_DATASET}.chapter_language` "
            f"WHERE book_id = '{book_id}'"
        )
        df = pd.DataFrame(rows)
        storage.write_df(f"{GOLD_DATASET}.chapter_language", df)
        logger.info(f"Wrote {len(rows)} row(s) to {GOLD_DATASET}.chapter_language")

    return MaterializeResult(metadata={"chapters_detected": len(rows)})
