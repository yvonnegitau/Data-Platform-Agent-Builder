from pathlib import Path
from dagster_dbt import (
    DbtCliResource,
    DbtProject,
    dbt_assets,
)
import dagster as dg
import os

dbt_project_dir = "/opt/dbt/app"
# Define your dbt project
dbt_project = DbtProject(project_dir=Path("/opt/dbt/app"))

dbt_manifest_path = f"{dbt_project_dir}/target/manifest.json"


@dbt_assets(
    project=dbt_project,
    manifest=dbt_manifest_path,
    name="f1_bronze_staging_assets",
    select="staging",
)
def dbt_staging_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):

    # Ensure manifest exists by running compile first
    if not os.path.exists(dbt_manifest_path):
        context.log.info("Manifest not found, compiling dbt project first...")
        yield from dbt.cli(["compile"], context=context).stream()
    yield from dbt.cli(["build"], context=context).stream()
