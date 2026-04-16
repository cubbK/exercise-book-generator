"""
Storage layer — BigQuery backend.

Extends ``dagster_gcp.BigQueryResource`` with a ``dataset`` field and
convenience helpers (``execute`` / ``write_df``) so the rest of the
pipeline can work with table names instead of full BigQuery refs.
"""

from __future__ import annotations

import pandas as pd
from dagster_gcp import BigQueryResource
from google.cloud import bigquery


class BigQueryStorage(BigQueryResource):
    """BigQuery storage backend with per-dataset helpers.

    Inherits ``project``, ``gcp_credentials``, ``location``, and
    ``get_client()`` from :class:`dagster_gcp.BigQueryResource`.
    """

    dataset: str = "exercise_book"

    def execute(self, sql: str) -> list[dict]:
        """Run a SQL query and return all rows as a list of dicts."""
        with self.get_client() as client:
            result = client.query(sql).result()
            return [dict(row) for row in result]

    def write_df(
        self,
        table: str,
        df: pd.DataFrame,
        schema: list[bigquery.SchemaField] | None = None,
    ) -> None:
        """Append a DataFrame to a BigQuery table (dataset.table or bare name)."""
        destination = table if "." in table else f"{self.dataset}.{table}"
        dataset_id = destination.split(".")[0]
        full_ref = f"{self.project}.{destination}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
            autodetect=schema is None,
        )
        with self.get_client() as client:
            ds = bigquery.Dataset(f"{self.project}.{dataset_id}")
            ds.location = self.location
            client.create_dataset(ds, exists_ok=True)
            client.load_table_from_dataframe(
                df, full_ref, job_config=job_config
            ).result()
