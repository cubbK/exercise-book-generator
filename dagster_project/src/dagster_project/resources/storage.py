"""
Storage layer — BigQuery backend.
"""

from __future__ import annotations

import pandas as pd
from dagster import ConfigurableResource
from google.cloud import bigquery


class BigQueryStorage(ConfigurableResource):
    """BigQuery storage backend."""

    project: str
    dataset: str = "exercise_book"

    def _client(self) -> bigquery.Client:
        return bigquery.Client(project=self.project)

    def execute(self, sql: str) -> list[dict]:
        """Run a SQL query and return all rows as a list of dicts."""
        client = self._client()
        result = client.query(sql).result()
        return [dict(row) for row in result]

    def write_df(self, table: str, df: pd.DataFrame) -> None:
        """Append a DataFrame to a BigQuery table (dataset.table format or bare name)."""
        client = self._client()
        destination = table if "." in table else f"{self.dataset}.{table}"
        full_ref = f"{self.project}.{destination}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )
        client.load_table_from_dataframe(df, full_ref, job_config=job_config).result()
