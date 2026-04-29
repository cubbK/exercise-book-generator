"""
Gold layer — A2-level chapter summary in Swedish.

Asset graph:
    silver_chapters
        └─ gold_chapter_language
             └─ gold_chapter_swedish_text
                  └─ gold_chapter_swedish_a2_text
                       └─ gold_chapter_swedish_a2_summary

``gold_chapter_swedish_a2_summary`` reads each chapter's A2-level Swedish text
from ``gold.chapter_swedish_a2_text`` and produces a short 3–5 sentence summary
at the same A2 level using a LangGraph summarise → judge loop (Vertex AI).

BigQuery table created (if absent):
    gold.chapter_swedish_a2_summary — one row per chapter

Partitioning:
    Partitioned by ``book_id`` (DynamicPartitionsDefinition ``"books"``).
    Re-materialising a partition deletes and rewrites all rows for that book.
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset, get_dagster_logger

from dagster_project.defs.partitions import book_partitions
from dagster_project.libs.chapter_summariser import (
    MODEL_NAME as SUMMARISER_MODEL,
    build_summariser_graph,
    summarise_chapter,
)
from dagster_project.resources.storage import BigQueryStorage
from dagster_project.resources.vertex_ai import VertexAIResource

GOLD_DATASET = "exercise_book_gold"

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

_CREATE_CHAPTER_SWEDISH_A2_SUMMARY = """
CREATE TABLE IF NOT EXISTS `{project}.exercise_book_gold.chapter_swedish_a2_summary` (
    chapter_id      STRING    NOT NULL,
    book_id         STRING    NOT NULL,
    summary         STRING    NOT NULL,
    attempts        INT64     NOT NULL,
    summarised_at   TIMESTAMP NOT NULL
)
"""


def _ensure_gold_tables(storage: BigQueryStorage) -> None:
    with storage.get_client() as bq:
        bq.query(
            _CREATE_CHAPTER_SWEDISH_A2_SUMMARY.format(project=storage.project)
        ).result()


# ---------------------------------------------------------------------------
# gold_chapter_swedish_a2_summary asset
# ---------------------------------------------------------------------------


@asset(
    name="gold_chapter_swedish_a2_summary",
    group_name="gold",
    deps=["gold_chapter_swedish_a2_text"],
    partitions_def=book_partitions,
    description=(
        "Produces a short A2-level Swedish summary (3–5 sentences) for every chapter "
        "by summarising the text from gold.chapter_swedish_a2_text. "
        "Uses a LangGraph summarise→judge loop backed by Vertex AI. "
        "Writes results to exercise_book_gold.chapter_swedish_a2_summary. "
        "Partitioned by book_id."
    ),
)
def gold_chapter_swedish_a2_summary(
    context: AssetExecutionContext,
    storage: BigQueryStorage,
    vertex_ai: VertexAIResource,
) -> MaterializeResult:
    logger = get_dagster_logger()
    book_id = context.partition_key
    _ensure_gold_tables(storage)

    # Fetch A2 text joined with chapter titles
    pending = storage.execute(
        f"""
        SELECT a2.chapter_id, c.title, a2.a2_text
        FROM `{storage.project}.{GOLD_DATASET}.chapter_swedish_a2_text` a2
        INNER JOIN `{storage.project}.exercise_book_silver.chapters` c
            ON c.chapter_id = a2.chapter_id AND c.book_id = a2.book_id
        WHERE a2.book_id = '{book_id}'
        """
    )
    logger.info(f"{len(pending)} chapter(s) found for book {book_id!r}")

    if not pending:
        logger.info(f"No chapters found for book {book_id!r} — skipping")
        return MaterializeResult(metadata={"chapters_processed": 0})

    graph = build_summariser_graph(vertex_ai.get_llm(SUMMARISER_MODEL))
    now = datetime.now(timezone.utc)
    rows: list[dict] = []

    for chapter in pending:
        chapter_id: str = chapter["chapter_id"]
        title: str = chapter["title"] or ""
        a2_text: str = chapter["a2_text"] or ""

        try:
            state = summarise_chapter(graph, chapter_id, title, a2_text)
            rows.append(
                {
                    "chapter_id": chapter_id,
                    "book_id": book_id,
                    "summary": state["summary"],
                    "attempts": state["attempts"],
                    "summarised_at": now,
                }
            )
            logger.info(
                f"chapter {chapter_id!r} → summarised "
                f"(attempts={state['attempts']}, judgment={state['judgment']!r})"
            )
        except Exception as exc:
            logger.error(f"Failed to summarise chapter {chapter_id!r}: {exc}")

    if rows:
        # Delete previous results for this book so re-runs are idempotent
        storage.execute(
            f"DELETE FROM `{storage.project}.{GOLD_DATASET}.chapter_swedish_a2_summary` "
            f"WHERE book_id = '{book_id}'"
        )
        df = pd.DataFrame(rows)
        storage.write_df(f"{GOLD_DATASET}.chapter_swedish_a2_summary", df)
        logger.info(
            f"Wrote {len(rows)} row(s) to {GOLD_DATASET}.chapter_swedish_a2_summary"
        )

    return MaterializeResult(metadata={"chapters_processed": len(rows)})
