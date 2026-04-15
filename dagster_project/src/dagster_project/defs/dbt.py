from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

dbt_project = DbtProject(
    project_dir=Path(__file__).parent.parent.parent.parent / "dbt",
)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> None:
    yield from dbt.cli(["build"], context=context).stream()
