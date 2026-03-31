import os
from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder


@definitions
def defs():
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)

    environment = os.getenv("ENVIRONMENT", "local")

    if environment == "local":
        from dagster_project.resources.storage import DuckDBStorage

        storage = DuckDBStorage(
            duckdb_path=os.getenv("DUCKDB_PATH", "data/exercise_book.duckdb")
        )
    else:
        from dagster_project.resources.storage import BigQueryStorage

        storage = BigQueryStorage(
            project=os.getenv("GCP_PROJECT", ""),
            dataset=os.getenv("BIGQUERY_DATASET", "exercise_book"),
        )

    return Definitions.merge(
        loaded,
        Definitions(resources={"storage": storage}),
    )
