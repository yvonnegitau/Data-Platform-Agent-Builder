from dlt_ingestion.assets import f1_assets
from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
)
from dagster_dlt import DagsterDltResource
import dagster as dg
from dagster import (
    build_schedule_from_partitioned_job,
    DefaultScheduleStatus,
    ScheduleDefinition,
)

all_assets = load_assets_from_modules([f1_assets])


f1_static_job = define_asset_job(
    "f1_bronze_static_job",
    selection=AssetSelection.groups("f1_bronze_static"),
    description="Extract F1 static data",
)

f1_yearly_job = define_asset_job(
    "f1_bronze_yearly_job",
    selection=AssetSelection.groups("f1_bronze_yearly"),
    description="Yearly refresh of F1 static data",
)

f1_race_details_job = define_asset_job(
    "f1_bronze_race_details_job",
    selection=AssetSelection.groups("f1_bronze_race_details"),
    description="Monthly refresh of Race details data",
)

f1_race_season_schedule = ScheduleDefinition(
    job=f1_race_details_job,
    cron_schedule="0 7 * 3-12 1",
    default_status=DefaultScheduleStatus.RUNNING,
)

f1_static_yearly_schedule = ScheduleDefinition(
    job=f1_static_job,
    cron_schedule="0 1 1 3 *",  # First day of March at 1 AM
    default_status=DefaultScheduleStatus.RUNNING,
)

f1_yearly_schedule = ScheduleDefinition(
    job=f1_yearly_job,
    cron_schedule="0 1 1 3 *",  # First day of March at 1 AM
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=all_assets,
    jobs=[f1_static_job, f1_race_details_job, f1_yearly_job],
    resources={"dlt": DagsterDltResource(), "io_manager": dg.fs_io_manager},
    executor=dg.in_process_executor,
    schedules=[
        f1_race_season_schedule,
        f1_static_yearly_schedule,
        f1_yearly_schedule,
    ],
)
