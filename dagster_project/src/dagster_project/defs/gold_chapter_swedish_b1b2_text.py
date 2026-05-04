"""
Gold layer — B1–B2 level Swedish rewrite of book chapters.

Asset graph:
    silver_chapters
        └─ gold_chapter_language
             └─ gold_chapter_swedish_text
                  └─ gold_chapter_swedish_b1b2_text

``gold_chapter_swedish_b1b2_text`` reads each chapter's fluent Swedish text from
``gold.chapter_swedish_text`` and rewrites it at CEFR B1–B2 level using a
LangGraph rewrite → judge loop (Vertex AI).

Unlike the A2 simplification, the B1–B2 rewrite preserves the full narrative
structure and voice of the original chapter.  The result reads as the chapter
itself — with its story, rhythm, and flow intact — rather than as a condensed
third-person summary.

BigQuery table created (if absent):
    gold.chapter_swedish_b1b2_text — one row per chapter

Partitioning:
    Partitioned by ``book_id`` (DynamicPartitionsDefinition ``"books"``).
    Re-materialising a partition deletes and rewrites all rows for that book.
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset, get_dagster_logger

from dagster_project.defs.partitions import book_partitions
from dagster_project.libs.chapter_b1b2_rewriter import (
    MODEL_NAME as REWRITER_MODEL,
    build_b1b2_rewriter_graph,
    rewrite_chapter,
)
from dagster_project.resources.storage import BigQueryStorage
from dagster_project.resources.vertex_ai import VertexAIResource

GOLD_DATASET = "exercise_book_gold"

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

_CREATE_CHAPTER_SWEDISH_B1B2_TEXT = """
CREATE TABLE IF NOT EXISTS `{project}.exercise_book_gold.chapter_swedish_b1b2_text` (
    chapter_id      STRING    NOT NULL,
    book_id         STRING    NOT NULL,
    b1b2_text       STRING    NOT NULL,
    attempts        INT64     NOT NULL,
    rewritten_at    TIMESTAMP NOT NULL
)
"""


def _ensure_gold_tables(storage: BigQueryStorage) -> None:
    with storage.get_client() as bq:
        bq.query(
            _CREATE_CHAPTER_SWEDISH_B1B2_TEXT.format(project=storage.project)
        ).result()


# ---------------------------------------------------------------------------
# gold_chapter_swedish_b1b2_text asset
# ---------------------------------------------------------------------------


@asset(
    name="gold_chapter_swedish_b1b2_text",
    group_name="gold",
    deps=["gold_chapter_swedish_text"],
    partitions_def=book_partitions,
    description=(
        "Produces a B1–B2 level Swedish rewrite for every chapter, preserving the "
        "full narrative structure and voice of the original. "
        "Reads fluent Swedish from gold.chapter_swedish_text. "
        "Uses a LangGraph rewrite→judge loop backed by Vertex AI. "
        "Writes results to exercise_book_gold.chapter_swedish_b1b2_text. "
        "Partitioned by book_id."
    ),
)
def gold_chapter_swedish_b1b2_text(
    context: AssetExecutionContext,
    storage: BigQueryStorage,
    vertex_ai: VertexAIResource,
) -> MaterializeResult:
    logger = get_dagster_logger()
    book_id = context.partition_key
    _ensure_gold_tables(storage)

    # Fetch fluent Swedish text produced by the previous asset
    pending = storage.execute(
        f"""
        SELECT st.chapter_id, c.title, st.swedish_text
        FROM `{storage.project}.{GOLD_DATASET}.chapter_swedish_text` st
        INNER JOIN `{storage.project}.exercise_book_silver.chapters` c
            ON c.chapter_id = st.chapter_id AND c.book_id = st.book_id
        WHERE st.book_id = '{book_id}'
        """
    )
    logger.info(f"{len(pending)} chapter(s) found for book {book_id!r}")

    if not pending:
        logger.info(f"No chapters found for book {book_id!r} — skipping")
        return MaterializeResult(metadata={"chapters_processed": 0})

    graph = build_b1b2_rewriter_graph(vertex_ai.get_llm(REWRITER_MODEL))
    now = datetime.now(timezone.utc)
    rows: list[dict] = []

    for chapter in pending:
        chapter_id: str = chapter["chapter_id"]
        title: str = chapter["title"] or ""
        swedish_text: str = chapter["swedish_text"] or ""

        try:
            state = rewrite_chapter(graph, chapter_id, title, swedish_text)
            rows.append(
                {
                    "chapter_id": chapter_id,
                    "book_id": book_id,
                    "b1b2_text": state["b1b2_text"],
                    "attempts": state["attempts"],
                    "rewritten_at": now,
                }
            )
            logger.info(
                f"chapter {chapter_id!r} → rewritten B1–B2 "
                f"(attempts={state['attempts']}, judgment={state['judgment']!r})"
            )
        except Exception as exc:
            logger.error(f"Failed to rewrite chapter {chapter_id!r}: {exc}")

    if rows:
        # Delete previous results for this book so re-runs are idempotent
        storage.execute(
            f"DELETE FROM `{storage.project}.{GOLD_DATASET}.chapter_swedish_b1b2_text` "
            f"WHERE book_id = '{book_id}'"
        )
        df = pd.DataFrame(rows)
        storage.write_df(f"{GOLD_DATASET}.chapter_swedish_b1b2_text", df)
        logger.info(
            f"Wrote {len(rows)} row(s) to {GOLD_DATASET}.chapter_swedish_b1b2_text"
        )

    return MaterializeResult(metadata={"chapters_processed": len(rows)})
