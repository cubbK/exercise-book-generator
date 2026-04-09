"""
Object storage layer — GCS backend.

Extends ``dagster_gcp.GCSResource`` with a ``bucket`` field and
convenience helpers for the EPUB ingestion pipeline.
"""

from __future__ import annotations

from dagster_gcp import GCSResource
from google.cloud.storage import Blob


class GCSObjectStoreResource(GCSResource):
    """GCS resource scoped to a single bucket.

    Inherits ``project``, ``gcp_credentials``, and ``get_client()``
    from :class:`dagster_gcp.GCSResource`.
    """

    bucket: str

    def list_epubs(self) -> list[Blob]:
        """Return all ``.epub`` blobs in the configured bucket."""
        client = self.get_client()
        return [
            blob
            for blob in client.bucket(self.bucket).list_blobs()
            if blob.name.lower().endswith(".epub")
        ]

    def download_bytes(self, blob_name: str) -> bytes:
        """Download a blob by name and return its raw bytes."""
        client = self.get_client()
        return client.bucket(self.bucket).blob(blob_name).download_as_bytes()
