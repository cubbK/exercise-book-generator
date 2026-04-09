import os
from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder

from dagster_project.resources.object_store import GCSObjectStoreResource
from dagster_project.resources.storage import BigQueryStorage


@definitions
def defs():
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)

    storage = BigQueryStorage(
        project=os.environ["GCP_PROJECT"],
        dataset=os.getenv("BIGQUERY_DATASET", "exercise_book"),
    )

    gcs = GCSObjectStoreResource(
        project=os.environ["GCP_PROJECT"],
        bucket=os.environ["GCS_EPUB_BUCKET"],
    )

    return Definitions.merge(
        loaded,
        Definitions(resources={"storage": storage, "gcs": gcs}),
    )
