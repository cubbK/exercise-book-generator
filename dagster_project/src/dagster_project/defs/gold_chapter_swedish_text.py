"""
Gold layer — Swedish text for book chapters.

Asset graph:
    silver_chapters
        └─ gold_chapter_language
             └─ gold_chapter_swedish_text

``gold_chapter_swedish_text`` reads each chapter from ``silver.chapters``
together with its detected language from ``gold.chapter_language``.

- Chapters already in Swedish: raw_text is copied directly, ``was_translated=false``.
- All other chapters: translated to fluent Swedish via a LangGraph
  translate → judge loop (Vertex AI), ``was_translated=true``.

BigQuery table created (if absent):
    gold.chapter_swedish_text — one row per chapter

Partitioning:
    Partitioned by ``book_id`` (DynamicPartitionsDefinition ``"books"``).
    Re-materialising a partition deletes and rewrites all rows for that book.
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset, get_dagster_logger

from dagster_project.defs.partitions import book_partitions
from dagster_project.libs.chapter_translator import (
    MODEL_NAME as TRANSLATOR_MODEL,
    build_translator_graph,
    translate_chapter,
)
from dagster_project.resources.storage import BigQueryStorage
from dagster_project.resources.vertex_ai import VertexAIResource

GOLD_DATASET = "exercise_book_gold"
SILVER_DATASET = "exercise_book_silver"

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

_CREATE_CHAPTER_SWEDISH_TEXT = """
CREATE TABLE IF NOT EXISTS `{project}.exercise_book_gold.chapter_swedish_text` (
    chapter_id      STRING    NOT NULL,
    book_id         STRING    NOT NULL,
    source_language STRING    NOT NULL,
    swedish_text    STRING    NOT NULL,
    was_translated  BOOL      NOT NULL,
    attempts        INT64     NOT NULL,
    translated_at   TIMESTAMP NOT NULL
)
"""


def _ensure_gold_tables(storage: BigQueryStorage) -> None:
    with storage.get_client() as bq:
        bq.query(_CREATE_CHAPTER_SWEDISH_TEXT.format(project=storage.project)).result()


# ---------------------------------------------------------------------------
# gold_chapter_swedish_text asset
# ---------------------------------------------------------------------------


@asset(
    name="gold_chapter_swedish_text",
    group_name="gold",
    deps=["gold_chapter_language"],
    partitions_def=book_partitions,
    description=(
        "Produces fluent Swedish text for every chapter in silver.chapters. "
        "Already-Swedish chapters are passed through unchanged (was_translated=false). "
        "All other chapters are translated via a LangGraph translate→judge loop "
        "backed by Vertex AI. Writes results to exercise_book_gold.chapter_swedish_text. "
        "Partitioned by book_id."
    ),
)
def gold_chapter_swedish_text(
    context: AssetExecutionContext,
    storage: BigQueryStorage,
    vertex_ai: VertexAIResource,
) -> MaterializeResult:
    logger = get_dagster_logger()
    book_id = context.partition_key
    _ensure_gold_tables(storage)

    # Fetch chapters that have a detected language (inner join keeps only those
    # that completed Phase 1 successfully)
    pending = storage.execute(
        f"""
        SELECT c.chapter_id, c.title, c.raw_text, cl.detected_language
        FROM `{storage.project}.{SILVER_DATASET}.chapters` c
        INNER JOIN `{storage.project}.{GOLD_DATASET}.chapter_language` cl
            ON cl.chapter_id = c.chapter_id AND cl.book_id = c.book_id
        WHERE c.book_id = '{book_id}'
        """
    )
    logger.info(f"{len(pending)} chapter(s) found for book {book_id!r}")

    if not pending:
        logger.info(f"No chapters found for book {book_id!r} — skipping")
        return MaterializeResult(
            metadata={"chapters_processed": 0, "chapters_translated": 0}
        )

    graph = build_translator_graph(vertex_ai.get_llm(TRANSLATOR_MODEL))
    now = datetime.now(timezone.utc)
    rows: list[dict] = []
    translated_count = 0

    for chapter in pending:
        chapter_id: str = chapter["chapter_id"]
        title: str = chapter["title"] or ""
        raw_text: str = chapter["raw_text"] or ""
        source_language: str = chapter["detected_language"]

        if source_language == "sv":
            # Already Swedish — copy through without an LLM call
            rows.append(
                {
                    "chapter_id": chapter_id,
                    "book_id": book_id,
                    "source_language": source_language,
                    "swedish_text": raw_text,
                    "was_translated": False,
                    "attempts": 0,
                    "translated_at": now,
                }
            )
            logger.info(f"chapter {chapter_id!r} → already Swedish, copied directly")
        else:
            try:
                state = translate_chapter(
                    graph, chapter_id, title, raw_text, source_language
                )
                rows.append(
                    {
                        "chapter_id": chapter_id,
                        "book_id": book_id,
                        "source_language": source_language,
                        "swedish_text": state["swedish_text"],
                        "was_translated": True,
                        "attempts": state["attempts"],
                        "translated_at": now,
                    }
                )
                translated_count += 1
                logger.info(
                    f"chapter {chapter_id!r} ({source_language!r}) → translated "
                    f"(attempts={state['attempts']}, judgment={state['judgment']!r})"
                )
            except Exception as exc:
                logger.error(f"Failed to translate chapter {chapter_id!r}: {exc}")

    if rows:
        # Delete previous results for this book so re-runs are idempotent
        storage.execute(
            f"DELETE FROM `{storage.project}.{GOLD_DATASET}.chapter_swedish_text` "
            f"WHERE book_id = '{book_id}'"
        )
        df = pd.DataFrame(rows)
        storage.write_df(f"{GOLD_DATASET}.chapter_swedish_text", df)
        logger.info(f"Wrote {len(rows)} row(s) to {GOLD_DATASET}.chapter_swedish_text")

    return MaterializeResult(
        metadata={
            "chapters_processed": len(rows),
            "chapters_translated": translated_count,
        }
    )
