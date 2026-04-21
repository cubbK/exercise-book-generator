"""
Gold layer — LLM-enriched chapter categorisation.

Asset graph:
    silver_chapters
        └─ gold_chapter_categories

``gold_chapter_categories`` runs each uncategorised chapter through a
LangGraph categorise → judge loop (Vertex AI) and writes the results
to ``exercise_book_gold.chapter_categories``.

BigQuery table created (if absent):
    gold.chapter_categories — one row per chapter

Partitioning:
    Partitioned by ``book_id`` (DynamicPartitionsDefinition ``"books"``).
    New partitions are registered by ``parse_epub_silver`` when a new
    book is parsed.  Re-materialising a partition deletes and rewrites
    all rows for that book, so prompt/logic changes can be re-applied
    cleanly per book.
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset, get_dagster_logger

from dagster_project.defs.partitions import book_partitions

from dagster_project.libs.chapter_categorizer import (
    build_categorizer_graph,
    categorize_chapter,
)
from dagster_project.resources.storage import BigQueryStorage
from dagster_project.resources.vertex_ai import VertexAIResource

GOLD_DATASET = "exercise_book_gold"
SILVER_DATASET = "exercise_book_silver"

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

_CREATE_CHAPTER_CATEGORIES = """
CREATE TABLE IF NOT EXISTS `{project}.exercise_book_gold.chapter_categories` (
    chapter_id      STRING    NOT NULL,
    book_id         STRING    NOT NULL,
    category        STRING    NOT NULL,
    attempts        INT64     NOT NULL,
    categorized_at  TIMESTAMP NOT NULL
)
"""


def _ensure_gold_tables(storage: BigQueryStorage) -> None:
    with storage.get_client() as bq:
        bq.query(_CREATE_CHAPTER_CATEGORIES.format(project=storage.project)).result()


# ---------------------------------------------------------------------------
# chapter_categories asset
# ---------------------------------------------------------------------------


@asset(
    name="gold_chapter_categories",
    group_name="gold",
    deps=["silver_chapters"],
    partitions_def=book_partitions,
    description=(
        "Categorises each chapter in silver.chapters using a LangGraph "
        "categorise→judge loop backed by Vertex AI. Writes results to "
        "exercise_book_gold.chapter_categories. Partitioned by book_id."
    ),
)
def gold_chapter_categories(
    context: AssetExecutionContext,
    storage: BigQueryStorage,
    vertex_ai: VertexAIResource,
) -> MaterializeResult:
    logger = get_dagster_logger()
    book_id = context.partition_key
    _ensure_gold_tables(storage)

    # Fetch all chapters for this book
    pending = storage.execute(
        f"""
        SELECT chapter_id, title, raw_text
        FROM `{storage.project}.{SILVER_DATASET}.chapters`
        WHERE book_id = '{book_id}'
        """
    )
    logger.info(f"{len(pending)} chapter(s) found for book {book_id!r}")

    if not pending:
        logger.info(f"No chapters found for book {book_id!r} — skipping")
        return MaterializeResult(metadata={"chapters_categorized": 0})

    graph = build_categorizer_graph(vertex_ai.get_llm())
    now = datetime.now(timezone.utc)
    rows: list[dict] = []

    for chapter in pending:
        chapter_id: str = chapter["chapter_id"]
        title: str = chapter["title"] or ""
        raw_text: str = chapter["raw_text"] or ""

        try:
            state = categorize_chapter(graph, chapter_id, title, raw_text)
            rows.append(
                {
                    "chapter_id": chapter_id,
                    "book_id": book_id,
                    "category": state["category"],
                    "attempts": state["attempts"],
                    "categorized_at": now,
                }
            )
            logger.info(
                f"chapter {chapter_id!r} → {state['category']!r} "
                f"(attempts={state['attempts']}, judgment={state['judgment']!r})"
            )
        except Exception as exc:
            logger.error(f"Failed to categorise chapter {chapter_id!r}: {exc}")

    if rows:
        # Delete previous results for this book so re-runs are idempotent
        storage.execute(
            f"DELETE FROM `{storage.project}.{GOLD_DATASET}.chapter_categories` "
            f"WHERE book_id = '{book_id}'"
        )
        df = pd.DataFrame(rows)
        storage.write_df(f"{GOLD_DATASET}.chapter_categories", df)
        logger.info(f"Wrote {len(rows)} row(s) to {GOLD_DATASET}.chapter_categories")

    return MaterializeResult(metadata={"chapters_categorized": len(rows)})
