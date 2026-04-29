"""
Gold layer — A2-level simplified Swedish text for book chapters.

Asset graph:
    silver_chapters
        └─ gold_chapter_language
             └─ gold_chapter_swedish_text
                  └─ gold_chapter_swedish_a2_text

``gold_chapter_swedish_a2_text`` reads each chapter's fluent Swedish text from
``gold.chapter_swedish_text`` and rewrites it at CEFR A2 level using a LangGraph
simplify → judge loop (Vertex AI).

BigQuery table created (if absent):
    gold.chapter_swedish_a2_text — one row per chapter

Partitioning:
    Partitioned by ``book_id`` (DynamicPartitionsDefinition ``"books"``).
    Re-materialising a partition deletes and rewrites all rows for that book.
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset, get_dagster_logger

from dagster_project.defs.partitions import book_partitions
from dagster_project.libs.chapter_simplifier import (
    MODEL_NAME as SIMPLIFIER_MODEL,
    build_simplifier_graph,
    simplify_chapter,
)
from dagster_project.resources.storage import BigQueryStorage
from dagster_project.resources.vertex_ai import VertexAIResource

GOLD_DATASET = "exercise_book_gold"

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

_CREATE_CHAPTER_SWEDISH_A2_TEXT = """
CREATE TABLE IF NOT EXISTS `{project}.exercise_book_gold.chapter_swedish_a2_text` (
    chapter_id      STRING    NOT NULL,
    book_id         STRING    NOT NULL,
    a2_text         STRING    NOT NULL,
    attempts        INT64     NOT NULL,
    simplified_at   TIMESTAMP NOT NULL
)
"""


def _ensure_gold_tables(storage: BigQueryStorage) -> None:
    with storage.get_client() as bq:
        bq.query(
            _CREATE_CHAPTER_SWEDISH_A2_TEXT.format(project=storage.project)
        ).result()


# ---------------------------------------------------------------------------
# gold_chapter_swedish_a2_text asset
# ---------------------------------------------------------------------------


@asset(
    name="gold_chapter_swedish_a2_text",
    group_name="gold",
    deps=["gold_chapter_swedish_text"],
    partitions_def=book_partitions,
    description=(
        "Produces A2-level simplified Swedish text for every chapter by rewriting "
        "the fluent Swedish from gold.chapter_swedish_text. "
        "Uses a LangGraph simplify→judge loop backed by Vertex AI. "
        "Writes results to exercise_book_gold.chapter_swedish_a2_text. "
        "Partitioned by book_id."
    ),
)
def gold_chapter_swedish_a2_text(
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

    graph = build_simplifier_graph(vertex_ai.get_llm(SIMPLIFIER_MODEL))
    now = datetime.now(timezone.utc)
    rows: list[dict] = []

    for chapter in pending:
        chapter_id: str = chapter["chapter_id"]
        title: str = chapter["title"] or ""
        swedish_text: str = chapter["swedish_text"] or ""

        try:
            state = simplify_chapter(graph, chapter_id, title, swedish_text)
            rows.append(
                {
                    "chapter_id": chapter_id,
                    "book_id": book_id,
                    "a2_text": state["a2_text"],
                    "attempts": state["attempts"],
                    "simplified_at": now,
                }
            )
            logger.info(
                f"chapter {chapter_id!r} → simplified "
                f"(attempts={state['attempts']}, judgment={state['judgment']!r})"
            )
        except Exception as exc:
            logger.error(f"Failed to simplify chapter {chapter_id!r}: {exc}")

    if rows:
        # Delete previous results for this book so re-runs are idempotent
        storage.execute(
            f"DELETE FROM `{storage.project}.{GOLD_DATASET}.chapter_swedish_a2_text` "
            f"WHERE book_id = '{book_id}'"
        )
        df = pd.DataFrame(rows)
        storage.write_df(f"{GOLD_DATASET}.chapter_swedish_a2_text", df)
        logger.info(
            f"Wrote {len(rows)} row(s) to {GOLD_DATASET}.chapter_swedish_a2_text"
        )

    return MaterializeResult(metadata={"chapters_processed": len(rows)})
