"""
Reverse-ETL: BigQuery gold layer → PostgreSQL serving database.

Add new tables by appending a SyncTable entry to SYNC_TABLES.
The factory generates one Dagster asset per entry — no other code changes needed.

Required env var: DATABASE_URL  (e.g. postgresql://user:pw@host/db)
"""

from __future__ import annotations

from dataclasses import dataclass

from dagster import (
    AssetsDefinition,
    AssetKey,
    ConfigurableResource,
    asset,
)
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.sql.elements import TextClause

from dagster_project.resources.storage import BigQueryStorage

_CHUNK_SIZE = 500


# ---------------------------------------------------------------------------
# Table registry — add new gold tables here
# ---------------------------------------------------------------------------


@dataclass
class SyncTable:
    """Declares one BigQuery → Postgres sync."""

    bq_dataset: str  # BigQuery dataset name
    bq_table: str  # BigQuery table name (= dbt model name)
    pg_schema: str  # Postgres destination schema
    pg_table: str  # Postgres destination table
    columns: list[str]  # columns to sync (must exist in both)
    pk: str  # primary key column used for upsert conflict


SYNC_TABLES: list[SyncTable] = [
    SyncTable(
        bq_dataset="exercise_book_gold",
        bq_table="gold_chapter_enriched",
        pg_schema="exercise_book_gold",
        pg_table="gold_chapter_enriched",
        columns=[
            "chapter_id",
            "book_id",
            "book_title",
            "chapter_order",
            "source_language",
            "category",
            "raw_text",
            "a2_text",
            "a2_summary",
            "b1b2_text",
            "swedish_text",
        ],
        pk="chapter_id",
    ),
    # Add future gold tables here, e.g.:
    # SyncTable(
    #     bq_dataset="exercise_book_gold",
    #     bq_table="gold_exercise_generated",
    #     pg_schema="exercise_book_gold",
    #     pg_table="gold_exercise_generated",
    #     columns=["exercise_id", "chapter_id", "exercise_text", ...],
    #     pk="exercise_id",
    # ),
]


# ---------------------------------------------------------------------------
# Resource
# ---------------------------------------------------------------------------


class PostgresResource(ConfigurableResource):
    """Thin wrapper so DATABASE_URL can be injected via Dagster config / EnvVar."""

    database_url: str
    _engine: Engine | None = None

    def get_engine(self) -> Engine:
        if self._engine is None:
            object.__setattr__(
                self, "_engine", create_engine(self.database_url, pool_pre_ping=True)
            )
        assert self._engine is not None
        return self._engine


# ---------------------------------------------------------------------------
# Asset factory
# ---------------------------------------------------------------------------


def _build_upsert_sql(t: SyncTable) -> TextClause:
    cols = ", ".join(t.columns)
    vals = ", ".join(f":{c}" for c in t.columns)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in t.columns if c != t.pk)
    return text(f"""
        INSERT INTO {t.pg_schema}.{t.pg_table} ({cols})
        VALUES ({vals})
        ON CONFLICT ({t.pk}) DO UPDATE SET {updates}
    """)


def _make_sync_asset(t: SyncTable) -> AssetsDefinition:
    upsert_sql = _build_upsert_sql(t)
    bq_ref = f"`{{project}}.{t.bq_dataset}.{t.bq_table}`"
    cols = ", ".join(t.columns)

    @asset(
        name=f"pg_{t.pg_table}",
        deps=[AssetKey([t.bq_table])],
        group_name="reverse_etl",
        description=f"Upserts {t.bq_dataset}.{t.bq_table} from BigQuery into {t.pg_schema}.{t.pg_table} in Postgres.",
    )
    def _sync(
        context,
        storage: BigQueryStorage,
        postgres: PostgresResource,
    ) -> None:
        full_bq = bq_ref.format(project=storage.project)
        rows = storage.execute(f"SELECT {cols} FROM {full_bq}")
        context.log.info(f"Fetched {len(rows)} rows from {full_bq}")

        engine = postgres.get_engine()
        upserted = 0
        with engine.begin() as conn:
            for i in range(0, len(rows), _CHUNK_SIZE):
                chunk = rows[i : i + _CHUNK_SIZE]
                conn.execute(upsert_sql, chunk)
                upserted += len(chunk)
                context.log.info(f"Upserted {upserted}/{len(rows)} rows…")

        context.log.info(f"Done — {upserted} rows in {t.pg_schema}.{t.pg_table}")

    return _sync


# One asset per registered table — Dagster picks these up automatically.
sync_assets = [_make_sync_asset(t) for t in SYNC_TABLES]
