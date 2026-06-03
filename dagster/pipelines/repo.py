from datetime import datetime
from ingestion import f1_assets
import dbt_assets as asset
from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
)
from dagster_dlt import DagsterDltResource
import dagster as dg
from dagster import (
    DefaultScheduleStatus,
)
from dagster_dbt import DbtCliResource

all_assets = load_assets_from_modules([f1_assets, asset])
current_year = datetime.now().year


yearly_partitions_def = dg.TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 1 *",  # January 1st at midnight each year
    fmt="%Y-%m-%d",
    start="1950-01-01",
    end_offset=1,  # Include the current, uncompleted year
)

# Define dbt resource
bronze_dbt_resource = DbtCliResource(
    project_dir="/opt/dbt/app",
    profiles_dir="/opt/dbt/app",
    profile_name="data_platform_f1",  # Explicit profile name
    target="dev",
)

silver_dbt_resource = DbtCliResource(
    project_dir="/opt/dbt/app",
    profiles_dir="/opt/dbt/app",
    profile_name="data_platform_f1",  # Explicit profile name
    target="duckdb_cross",
)

f1_static_job = define_asset_job(
    "f1_bronze_static_job",
    selection=AssetSelection.groups("f1_bronze_static"),
    description="Extract F1 static data",
)

f1_yearly_job = define_asset_job(
    "f1_bronze_yearly_job",
    selection=AssetSelection.groups("f1_bronze_yearly"),
    description="Yearly refresh of F1 static data",
    partitions_def=yearly_partitions_def,
)

f1_race_details_job = define_asset_job(
    "f1_bronze_race_details_job",
    selection=AssetSelection.groups("f1_bronze_race_details"),
    description="Monthly refresh of Race details data",
    partitions_def=yearly_partitions_def,
)

f1_dbt_staging_job = define_asset_job(
    "f1_bronze_dbt_staging_job",
    selection=AssetSelection.assets(asset.dbt_staging_assets),
    description="DBT staging assets for F1 bronze data",
)

f1_dbt_silver_job = define_asset_job(
    "f1_dbt_silver_job",
    selection=AssetSelection.assets(asset.dbt_silver_assets),
    description="DBT silver assets for F1 bronze data",
)


@dg.schedule(
    cron_schedule="0 7 * 3-12 1",
    job=f1_race_details_job,
    default_status=DefaultScheduleStatus.RUNNING,
)  # Every Monday at midnight
def weekly_yearly_schedule(context):
    # Get the current year partition
    current_partition = yearly_partitions_def.get_partition_keys(
        context.scheduled_execution_time
    )
    current_partition_key = current_partition[-1]
    return dg.RunRequest(
        partition_key=current_partition_key,
        tags={
            "schedule": "weekly_race_details",
            "execution_time": context.scheduled_execution_time.isoformat(),
        },
    )


@dg.schedule(
    cron_schedule="0 1 1 3 *",
    job=f1_yearly_job,
    default_status=DefaultScheduleStatus.RUNNING,
)  # Every March 1st at 1 AM
def yearly_schedule(context):
    # Get the current year partition
    current_partition = yearly_partitions_def.get_partition_keys(
        context.scheduled_execution_time
    )
    current_partition_key = current_partition[-1]
    return dg.RunRequest(
        partition_key=current_partition_key,
        tags={
            "schedule": "yearly_refresh",
            "execution_time": context.scheduled_execution_time.isoformat(),
        },
    )


@dg.schedule(
    cron_schedule="0 1 1 3 *",
    job=f1_static_job,
    default_status=DefaultScheduleStatus.RUNNING,
)  # Every March 1st at 1 AM
def static_yearly_schedule(context):
    # Get the current year partition
    current_partition = yearly_partitions_def.get_partition_keys(
        context.scheduled_execution_time
    )
    current_partition_key = current_partition[-1]
    return dg.RunRequest(
        partition_key=current_partition_key,
        tags={
            "schedule": "static_refresh",
            "execution_time": context.scheduled_execution_time.isoformat(),
        },
    )


defs = Definitions(
    assets=all_assets,
    jobs=[
        f1_static_job,
        f1_race_details_job,
        f1_yearly_job,
        f1_dbt_staging_job,
        f1_dbt_silver_job,
    ],
    resources={
        "dlt": DagsterDltResource(),
        "io_manager": dg.fs_io_manager,
        "dbt": bronze_dbt_resource,
        "dbt_silver": silver_dbt_resource,
    },
    executor=dg.in_process_executor,
    schedules=[
        yearly_schedule,
        static_yearly_schedule,
        weekly_yearly_schedule,
    ],
)
