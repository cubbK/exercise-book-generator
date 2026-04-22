"""
Bronze layer — raw EPUB file registry staging table.

Polls a GCS bucket for EPUB files and appends all rows (no deduplication)
to BigQuery `bronze_raw.epub_registry_raw`. Deduplication is handled
downstream by the dbt `epub_registry` incremental model.

Required environment variables:
    GCP_PROJECT       — Google Cloud project ID (via GCSObjectStoreResource / BigQueryStorage)
    GCS_EPUB_BUCKET   — GCS bucket name (via GCSObjectStoreResource)
    BIGQUERY_DATASET  — BigQuery dataset name (optional, default: ``exercise_book``)
"""

from dotenv import load_dotenv


import hashlib
import uuid
from datetime import datetime, timezone

import pandas as pd
from dagster import AssetDep, AssetExecutionContext, AssetKey, asset, get_dagster_logger
from google.cloud import bigquery

from dagster_project.resources.object_store import GCSObjectStoreResource
from dagster_project.resources.storage import BigQueryStorage


load_dotenv()  # reads variables from a .env file and sets them in os.environ


@asset(
    name="bronze_epub_registry_raw",
    group_name="bronze",
    description=(
        "Polls GCS for EPUB files and writes all metadata rows to "
        "BigQuery bronze_raw.epub_registry_raw. No deduplication — "
        "that is handled by the downstream dbt bronze_epub_registry model."
    ),
)
def bronze_epub_registry_raw(
    context: AssetExecutionContext,
    storage: BigQueryStorage,
    gcs: GCSObjectStoreResource,
) -> None:
    logger = get_dagster_logger()

    epub_blobs = gcs.list_epubs()
    logger.info(f"Found {len(epub_blobs)} EPUB(s) in gs://{gcs.bucket}")

    rows = []
    for blob in epub_blobs:
        data = gcs.download_bytes(blob.name)  # type: ignore
        rows.append(
            {
                "book_id": str(uuid.uuid4()),
                "file_id": str(uuid.uuid4()),
                "filename": blob.name.split("/")[-1],  # type: ignore
                "storage_path": f"gs://{gcs.bucket}/{blob.name}",
                "sha256_hash": hashlib.sha256(data).hexdigest(),
                "uploaded_at": datetime.now(timezone.utc),
            }
        )

    EPUB_REGISTRY_RAW_SCHEMA = [
        bigquery.SchemaField("book_id", "STRING"),
        bigquery.SchemaField("file_id", "STRING"),
        bigquery.SchemaField("filename", "STRING"),
        bigquery.SchemaField("storage_path", "STRING"),
        bigquery.SchemaField("sha256_hash", "STRING"),
        bigquery.SchemaField("uploaded_at", "TIMESTAMP"),
    ]

    if rows:
        storage.write_df(
            "epub_registry_raw", pd.DataFrame(rows), schema=EPUB_REGISTRY_RAW_SCHEMA
        )
        logger.info(f"Wrote {len(rows)} row(s) to epub_registry_raw")
    else:
        logger.info("No EPUBs found — nothing to write")


@asset(
    name="bronze_register_book_partitions",
    group_name="bronze",
    deps=[AssetDep(AssetKey(["bronze", "bronze_epub_registry"]))],
    description=(
        "Reads unique book_ids from dbt bronze_epub_registry and registers "
        "them as dynamic 'books' partitions for downstream silver/gold assets."
    ),
)
def register_book_partitions(
    context: AssetExecutionContext,
    storage: BigQueryStorage,
) -> None:
    logger = get_dagster_logger()
    rows = storage.execute(
        f"SELECT book_id FROM `{storage.project}.exercise_book_bronze.bronze_epub_registry`"
    )
    book_ids = [row["book_id"] for row in rows]
    if book_ids:
        context.instance.add_dynamic_partitions("books", book_ids)
        logger.info(f"Registered {len(book_ids)} book partition(s): {book_ids}")
    else:
        logger.info("No books found in bronze_epub_registry — nothing to register")
