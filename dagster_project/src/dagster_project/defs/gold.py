"""
Gold layer — chapter type classification.

Asset graph:
    silver_chapters
        └─ classify_chapters  ─── gold_chapters

``classify_chapters`` reads any silver chapters not yet present in the
gold table and enriches them with an LLM-inferred ``chapter_type``.
Each classification is traced individually in Langfuse (trace ID ==
chapter_id) for full observability.

BigQuery table created (if absent):
    gold.chapters — silver columns + chapter_type + type_reasoning
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from dagster import AssetSpec, asset, get_dagster_logger

from dagster_project.lib.chapter_judge import infer_chapter_type
from dagster_project.resources.langfuse import LangfuseResource
from dagster_project.resources.llm import LLMResource
from dagster_project.resources.storage import BigQueryStorage

GOLD_DATASET = "exercise_book_gold"

_CREATE_GOLD_CHAPTERS = """
CREATE TABLE IF NOT EXISTS `{project}.exercise_book_gold.chapters` (
    chapter_id      STRING    NOT NULL,
    book_id         STRING    NOT NULL,
    chapter_order   INT64     NOT NULL,
    title           STRING,
    raw_text        STRING,
    chapter_type    STRING    NOT NULL,
    type_reasoning  STRING,
    classified_at   TIMESTAMP NOT NULL
)
"""


@asset(
    group_name="gold",
    deps=[AssetSpec("silver_chapters")],
    description=(
        "Enriches silver chapters with an LLM-inferred chapter_type. "
        "Skips chapters already classified (idempotent). "
        "Each LLM call is traced in Langfuse with full prompt/response detail."
    ),
)
def classify_chapters(
    storage: BigQueryStorage,
    langfuse: LangfuseResource,
    llm: LLMResource,
) -> None:
    logger = get_dagster_logger()

    # Ensure destination table exists.
    with storage.get_client() as bq:
        bq.query(_CREATE_GOLD_CHAPTERS.format(project=storage.project)).result()

    # Fetch only chapters not yet classified (anti-join on chapter_id).
    pending = storage.execute(
        f"""
        SELECT s.chapter_id, s.book_id, s.chapter_order, s.title, s.raw_text
        FROM `{storage.project}.exercise_book_silver.chapters` AS s
        LEFT JOIN `{storage.project}.{GOLD_DATASET}.chapters` AS g
          USING (chapter_id)
        WHERE g.chapter_id IS NULL
        ORDER BY s.book_id, s.chapter_order
        """
    )
    logger.info(f"{len(pending)} chapter(s) pending classification")
    if not pending:
        return

    lf_client = langfuse.get_client()
    openai_client = llm.get_client()
    now = datetime.now(timezone.utc)

    rows: list[dict] = []
    for ch in pending:
        chapter_id: str = ch["chapter_id"]
        title: str = ch["title"] or ""
        raw_text: str = ch["raw_text"] or ""
        logger.info(f"Classifying chapter {chapter_id!r} ({title!r})")

        try:
            classification = infer_chapter_type(
                chapter_id=chapter_id,
                title=title,
                text_preview=raw_text,
                openai_client=openai_client,
                langfuse_client=lf_client,
                model=llm.model,
            )
        except Exception as exc:
            logger.error(f"Classification failed for {chapter_id!r}: {exc}")
            continue

        rows.append(
            {
                "chapter_id": chapter_id,
                "book_id": ch["book_id"],
                "chapter_order": ch["chapter_order"],
                "title": title,
                "raw_text": raw_text,
                "chapter_type": classification.chapter_type,
                "type_reasoning": classification.reasoning,
                "classified_at": now,
            }
        )

    if rows:
        df = pd.DataFrame(rows)
        storage.write_df(f"{GOLD_DATASET}.chapters", df)
        logger.info(f"Wrote {len(rows)} classified chapter(s) to gold.chapters")
