"""
Storage abstraction layer.

Phase 1: DuckDBStorage — local .duckdb file.
Phase 2: BigQueryStorage — swap in without changing assets.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from dagster import ConfigurableResource


class DuckDBStorage(ConfigurableResource):
    """Local DuckDB storage backend for Phase 1."""

    duckdb_path: str = "data/exercise_book.duckdb"

    def _connect(self):
        import duckdb

        Path(self.duckdb_path).parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(self.duckdb_path)

    def execute(self, sql: str) -> list:
        """Run a SQL statement and return all rows as a list of tuples."""
        with self._connect() as con:
            return con.execute(sql).fetchall()

    def write_df(self, table: str, df: pd.DataFrame) -> None:
        """Append a DataFrame to a DuckDB table, creating it if it doesn't exist."""
        with self._connect() as con:
            con.register("_staging", df)
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM _staging LIMIT 0"
            )
            con.execute(f"INSERT INTO {table} SELECT * FROM _staging")


class BigQueryStorage(ConfigurableResource):
    """Phase 2 stub — BigQuery storage backend. Not yet implemented."""

    project: str = ""
    dataset: str = ""

    def execute(self, sql: str) -> list:
        raise NotImplementedError(
            "BigQueryStorage is a Phase 2 stub. "
            "Set ENVIRONMENT=local to use DuckDBStorage."
        )

    def write_df(self, table: str, df: pd.DataFrame) -> None:
        raise NotImplementedError(
            "BigQueryStorage is a Phase 2 stub. "
            "Set ENVIRONMENT=local to use DuckDBStorage."
        )
