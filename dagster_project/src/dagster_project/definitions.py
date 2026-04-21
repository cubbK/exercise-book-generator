import os
from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder
from dagster_dbt import DbtCliResource

from dagster_project.defs.dbt import dbt_project
from dagster_project.resources.object_store import GCSObjectStoreResource
from dagster_project.resources.storage import BigQueryStorage
from dagster_project.resources.vertex_ai import VertexAIResource


@definitions
def defs():
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)

    storage = BigQueryStorage(
        project=os.environ["GCP_PROJECT"],
        dataset=os.getenv("BIGQUERY_DATASET", "exercise_book"),
        location=os.getenv("BIGQUERY_LOCATION", "EU"),
    )

    gcs = GCSObjectStoreResource(
        project=os.environ["GCP_PROJECT"],
        bucket=os.environ["GCS_EPUB_BUCKET"],
    )

    dbt = DbtCliResource(project_dir=os.fspath(dbt_project.project_dir))

    vertex_ai = VertexAIResource(
        project=os.environ["GCP_PROJECT"],
        location=os.getenv("VERTEX_AI_LOCATION", os.getenv("BIGQUERY_LOCATION", "EU")),
    )

    return Definitions.merge(
        loaded,
        Definitions(resources={"storage": storage, "gcs": gcs, "dbt": dbt, "vertex_ai": vertex_ai}),
    )
