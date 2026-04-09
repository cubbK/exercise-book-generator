"""
Bronze layer — raw EPUB file registry.

Polls a GCS bucket for EPUB files and registers new ones in BigQuery
`bronze.epub_registry`. Uses SHA-256 hashing for idempotent deduplication:
re-uploading the same file produces no new rows.

Required environment variables:
    GCS_EPUB_BUCKET   — name of the GCS bucket containing raw EPUBs
    GCP_PROJECT       — Google Cloud project ID (used by BigQueryStorage)
"""

from __future__ import annotations
from dotenv import load_dotenv


import hashlib
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
from dagster import asset, get_dagster_logger

from dagster_project.resources.storage import BigQueryStorage


load_dotenv()  # reads variables from a .env file and sets them in os.environ

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.epub_registry` (
    file_id       STRING    NOT NULL,
    filename      STRING    NOT NULL,
    storage_path  STRING    NOT NULL,
    sha256_hash   STRING    NOT NULL,
    uploaded_at   TIMESTAMP NOT NULL,
    status        STRING    NOT NULL
)
"""

_EXISTING_HASHES = """
SELECT sha256_hash
FROM `{project}.{dataset}.epub_registry`
"""


@asset(
    group_name="bronze",
    description=(
        "Polls GCS for new EPUB files and registers metadata in "
        "BigQuery bronze.epub_registry. Idempotent via SHA-256 deduplication."
    ),
)
def epub_registry(storage: BigQueryStorage) -> None:
    from google.cloud import storage as gcs

    logger = get_dagster_logger()
    bucket_name = os.environ["GCS_EPUB_BUCKET"]

    bq = storage._client()

    # Ensure destination table exists
    bq.query(
        _CREATE_TABLE.format(project=storage.project, dataset=storage.dataset)
    ).result()

    # Fetch hashes already registered so we can skip duplicates
    existing_hashes: set[str] = {
        row["sha256_hash"]
        for row in storage.execute(
            _EXISTING_HASHES.format(project=storage.project, dataset=storage.dataset)
        )
    }
    logger.info(f"{len(existing_hashes)} EPUB(s) already registered")

    gcs_client = gcs.Client(project=storage.project)
    bucket = gcs_client.bucket(bucket_name)
    epub_blobs = [b for b in bucket.list_blobs() if b.name.lower().endswith(".epub")]
    logger.info(f"Found {len(epub_blobs)} EPUB(s) in gs://{bucket_name}")

    new_rows: list[dict] = []
    for blob in epub_blobs:
        data = blob.download_as_bytes()
        sha256 = hashlib.sha256(data).hexdigest()

        if sha256 in existing_hashes:
            logger.info(f"Skipping {blob.name!r} — already registered")
            continue

        new_rows.append(
            {
                "file_id": str(uuid.uuid4()),
                "filename": blob.name.split("/")[-1],
                "storage_path": f"gs://{bucket_name}/{blob.name}",
                "sha256_hash": sha256,
                "uploaded_at": datetime.now(timezone.utc),
                "status": "registered",
            }
        )
        logger.info(f"Queued for registration: {blob.name!r}")

    if new_rows:
        storage.write_df("epub_registry", pd.DataFrame(new_rows))
        logger.info(f"Registered {len(new_rows)} new EPUB(s)")
    else:
        logger.info("No new EPUBs — nothing to register")
