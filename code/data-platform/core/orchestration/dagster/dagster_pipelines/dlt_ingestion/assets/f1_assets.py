from datetime import datetime
import logging
import os
import queue
import threading
from typing import Any, Dict, List, Optional
from dagster import (
    Config,
    AssetExecutionContext,
    asset,
    MetadataValue,
    MonthlyPartitionsDefinition,
    TimeWindowPartitionsDefinition,
    BackfillPolicy,
    StaticPartitionsDefinition,
    AssetMaterialization,
    AssetIn,
    SensorResult,
    RunRequest,
    SensorEvaluationContext,
    DefaultSensorStatus,
    sensor,
)

from dateutil.relativedelta import relativedelta
import dlt
import uuid
import time
import fcntl


from dlt_ingestion.sources.f1_source import (
    f1_api_source,
)


# Define partition startegies
yearly_partitions = TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 1 *",  # Yearly: Jan 1st at midnight
    start="1950-01-01",
    fmt="%Y-%m-%d",
)
monthly_partitions = MonthlyPartitionsDefinition(
    start_date="1950-01-01"  # Optional end date
)

# For reference data that doesn't change often
reference_partitions = StaticPartitionsDefinition(["current"])

# Define backfill policies
single_run_backfill = BackfillPolicy.single_run()
multi_run_backfill = BackfillPolicy.multi_run(max_partitions_per_run=1)


def get_partition_key_for_metadata(context: AssetExecutionContext) -> str:
    """Get partition key for metadata, handling both single and range partitions"""
    if hasattr(context, "partition_key_range") and context.partition_key_range:
        return (
            f"{context.partition_key_range.start} to {context.partition_key_range.end}"
        )
    elif hasattr(context, "partition_key") and context.partition_key:
        return context.partition_key
    else:
        return "N/A"


class F1BronzeConfig(Config):
    start_year: int = 1950
    months_to_run: Optional[int] = None
    simulation_date: Optional[str] = None
    full_refresh: bool = False


bronze_pipeline = dlt.pipeline(
    pipeline_name="f1_bronze",
    destination="postgres",
    dataset_name="f1_bronze",
    progress="log",
)


def get_simulation_date_from_partition(
    context: AssetExecutionContext, config: F1BronzeConfig
) -> datetime:
    """Get simulation date from the current partition."""

    # Debug logging
    context.log.info("=== PARTITION DEBUG ===")

    # For single partition runs
    if hasattr(context, "partition_key") and context.partition_key:
        partition_key = context.partition_key
        context.log.info(f"Using partition key: {partition_key}")

        # Handle static partition "current"
        if partition_key == "current":
            context.log.info("Using current date for static partition")
            return datetime.now()

        # Parse date string
        try:
            simulation_date = datetime.strptime(partition_key, "%Y-%m-%d")
            context.log.info(f"Parsed simulation date: {simulation_date}")
            return simulation_date
        except ValueError:
            context.log.error(f"Could not parse partition key as date: {partition_key}")

    # For multi-partition backfills
    if hasattr(context, "partition_key_range") and context.partition_key_range:
        # Use the start of the range
        start_date = context.partition_key_range.start
        context.log.info(f"Using start date from range: {start_date}")

        try:
            simulation_date = datetime.strptime(start_date, "%Y-%m-%d")
            context.log.info(f"Parsed simulation date: {simulation_date}")
            return simulation_date
        except ValueError:
            context.log.error(f"Could not parse range start date: {start_date}")

    # Ultimate fallback
    context.log.info("No valid partition info found, using config fallback")
    return get_simulation_date(config)


def get_simulation_date(config: F1BronzeConfig) -> datetime:
    if config.simulation_date:
        return datetime.strptime(config.simulation_date, "%Y-%m-%d")
    elif config.months_to_run:
        return datetime(config.start_year, 1, 1) + relativedelta(
            months=config.months_to_run
        )
    else:
        return datetime.now()


@asset(
    compute_kind="dlt",
    description="F1 race data from Ergast API",
    group_name="f1_bronze_race_details",
    partitions_def=yearly_partitions,
    backfill_policy=multi_run_backfill,
    io_manager_key="io_manager",
)
def f1_races(
    context: AssetExecutionContext,
    config: F1BronzeConfig,
) -> List[Dict[str, Any]]:
    simulation_date = get_simulation_date_from_partition(context, config)

    context.log.info(
        f"Extracting Races for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )

    source = f1_api_source(years=[simulation_date.year])
    try:
        bronze_pipeline.run(source.resources["races"])

        race_metadata = get_race_metadata_from_source(source, simulation_date.year)

        # context.log.info(f"Returning race metadata: {race_metadata}")

        # Return the properly typed race metadata
        return race_metadata
    except Exception as e:
        context.log.error(
            f"Error running DLT pipeline for races on {simulation_date.strftime('%Y-%m-%d')}: {e}"
        )
        raise e


def get_race_metadata_from_source(source, year: int) -> List[Dict[str, Any]]:
    """
    Extract race metadata directly from the source data instead of the database.
    This ensures we have the metadata even if the database write fails.
    """
    try:
        race_metadata = []

        # Get the race data from the source
        for race in source.races:
            race_info = {
                "season": race.get("season", str(year)),
                "round": race.get("round"),
                "race_name": race.get("raceName"),
                "date": race.get("date"),
                "circuit_id": (
                    race.get("Circuit", {}).get("circuitId")
                    if isinstance(race.get("Circuit"), dict)
                    else None
                ),
                "completed": (
                    True if race.get("date") else False
                ),  # You might want to improve this logic
            }
            race_metadata.append(race_info)

        return race_metadata

    except Exception as e:
        logging.error(f"Error extracting race metadata from source: {e}")
        # Return empty list with at least the year info
        return [
            {
                "season": str(year),
                "round": None,
                "race_name": "Unknown",
                "date": None,
                "completed": False,
            }
        ]


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_race_details",
    description="F1 race results data from Ergast API",
    partitions_def=yearly_partitions,
    backfill_policy=multi_run_backfill,
    deps=["f1_races"],
)
def f1_results(context: AssetExecutionContext, config: F1BronzeConfig) -> None:
    simulation_date = get_simulation_date_from_partition(context, config)

    context.log.info(
        f"Extracting results for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )

    source = f1_api_source(years=[simulation_date.year])

    bronze_pipeline.run(source.resources["results"])
    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_yearly",
    description="F1 drivers data from Ergast API",
    partitions_def=yearly_partitions,
    backfill_policy=single_run_backfill,
)
def f1_drivers(context: AssetExecutionContext, config: F1BronzeConfig) -> None:
    """Extract F1 drivers data."""
    # Get partition keys (handles both individual and range runs)
    partition_keys = []
    if hasattr(context, "partition_key_range") and context.partition_key_range:
        # Extract start and end years from range
        start_year = context.partition_key_range.start
        end_year = context.partition_key_range.end
        context.log.info(f"Processing partition range: {start_year} to {end_year}")

        # Generate all years in the range
        partition_keys = context.partition_keys
    elif hasattr(context, "partition_key") and context.partition_key:
        partition_keys = [context.partition_key]
        context.log.info(f"Processing single partition: {context.partition_key}")

    context.log.info(f"Will process these partitions: {partition_keys}")

    years = []

    # Process each partition
    for key in partition_keys:
        # Parse the key to get simulation date
        simulation_date = datetime.strptime(key, "%Y-%m-%d") if key else datetime.now()

        context.log.info(
            f"Extracting drivers for {key} (date: {simulation_date.strftime('%Y-%m-%d')})"
        )
        years.append(simulation_date.year)

    # write_disposition = "replace" if config.full_refresh else "merge"
    # Extract Data
    source = f1_api_source(years=years)

    bronze_pipeline.run(source.resources["drivers"])

    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_yearly",
    description="F1 constructors data from Ergast API",
    partitions_def=yearly_partitions,
    backfill_policy=single_run_backfill,  # Changed to match f1_drivers
)
def f1_constructors(context: AssetExecutionContext, config: F1BronzeConfig) -> None:
    """Extract F1 constructors data."""
    # Get partition keys (handles both individual and range runs)
    partition_keys = []
    if hasattr(context, "partition_key_range") and context.partition_key_range:
        # Extract start and end years from range
        start_year = context.partition_key_range.start
        end_year = context.partition_key_range.end
        context.log.info(f"Processing partition range: {start_year} to {end_year}")

        # Generate all years in the range
        partition_keys = context.partition_keys
    elif hasattr(context, "partition_key") and context.partition_key:
        partition_keys = [context.partition_key]
        context.log.info(f"Processing single partition: {context.partition_key}")

    context.log.info(f"Will process these partitions: {partition_keys}")

    years = []

    # Process each partition
    for key in partition_keys:
        # Parse the key to get simulation date
        simulation_date = datetime.strptime(key, "%Y-%m-%d") if key else datetime.now()

        years.append(simulation_date.year)

    context.log.info(f"Extracting constructors for years: {years}")
    bronze_pipeline.run(f1_api_source(years=years).resources["constructors"])

    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_race_details",
    description="F1 driver standings data from Ergast API",
    partitions_def=yearly_partitions,
    backfill_policy=multi_run_backfill,
    deps=["f1_results"],  # Depends on results being available
)
def f1_driver_standings(context: AssetExecutionContext, config: F1BronzeConfig) -> None:
    """Extract F1 driver standings data."""
    simulation_date = get_simulation_date_from_partition(context, config)
    context.log.info(
        f"Extracting driver standings for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )
    source = f1_api_source(years=[simulation_date.year])
    bronze_pipeline.run(source.resources["driver_standings"])

    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_race_details",
    description="F1 constructor standings data from Ergast API",
    partitions_def=yearly_partitions,
    backfill_policy=multi_run_backfill,
    deps=["f1_results"],
)
def f1_constructor_standings(
    context: AssetExecutionContext, config: F1BronzeConfig
) -> None:
    """Extract F1 constructor standings data."""
    simulation_date = get_simulation_date_from_partition(context, config)
    context.log.info(
        f"Extracting constructor standings for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )

    source = f1_api_source(years=[simulation_date.year])
    bronze_pipeline.run(source.resources["constructor_standings"])

    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_static",
    description="F1 seasons reference data from Ergast API",
    partitions_def=reference_partitions,
    backfill_policy=single_run_backfill,
)
def f1_seasons(context: AssetExecutionContext) -> None:
    """Extract F1 seasons reference data."""

    context.log.info("Extracting F1 seasons reference data")

    source = f1_api_source()

    bronze_pipeline.run(source.resources["seasons"])


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_static",
    description="F1 circuits reference data from Ergast API",
    partitions_def=reference_partitions,
    backfill_policy=single_run_backfill,
)
def f1_circuits(context: AssetExecutionContext) -> None:
    """Extract F1 circuits reference data."""

    context.log.info("Extracting F1 circuits reference data")

    source = f1_api_source()

    bronze_pipeline.run(source.resources["circuits"])


@asset(
    compute_kind="dlt",
    description="F1 laps data from Ergast API",
    group_name="f1_bronze_race_details",
    partitions_def=yearly_partitions,
    backfill_policy=multi_run_backfill,
    deps=["f1_races"],
    ins={"race_metadata": AssetIn("f1_races")},
)
def f1_laps(
    context: AssetExecutionContext,
    config: F1BronzeConfig,
    race_metadata: List[Dict[str, Any]],
) -> None:
    """Extract all F1 laps data for a given simulation date."""

    simulation_date = get_simulation_date_from_partition(context, config)
    context.log.info(
        f"Extracting comprehensive F1 data for: {simulation_date.strftime('%Y-%m-%d')}"
    )
    context.log.info(f"Received race metadata: {race_metadata}")

    # Collect ALL rounds and determine the year
    rounds_to_process = []
    year = simulation_date.year  # Default to simulation date year

    for item in race_metadata:
        if "round" not in item or "season" not in item:
            context.log.warning(f"Skipping item without 'round' or 'season': {item}")
            continue

        round_num = int(item["round"])
        season_year = int(item["season"])

        rounds_to_process.append(round_num)
        year = season_year  # Update year (should be consistent across all items)

        context.log.info(f"Added round {round_num} for season {season_year}")

    if not rounds_to_process:
        context.log.warning(
            "No valid rounds found in race metadata, skipping laps extraction."
        )
        return

    # Remove duplicates and sort
    rounds_to_process = sorted(list(set(rounds_to_process)))

    context.log.info(
        f"Extracting laps for {len(rounds_to_process)} rounds: {rounds_to_process} in season {year}"
    )

    # Process all rounds at once
    source = f1_api_source(years=[year], rounds=rounds_to_process)
    bronze_pipeline.run(source.resources["laps"])

    context.add_output_metadata(
        {
            "rounds_processed": len(rounds_to_process),
            "rounds_list": str(rounds_to_process),
            "season": year,
        }
    )

    return None


# @sensor(
#     job_name="f1_bronze_race_job",
#     minimum_interval_seconds=60,  # Every minute
#     description="Sensor to trigger F1_laps after F1_races",
#     default_status=DefaultSensorStatus.RUNNING,
# )
# def f1_laps_sensor(context: SensorEvaluationContext) -> Optional[SensorResult]:
#     """Sensor to trigger f1_laps after F1_races asset is materialized."""

#     # get latest materialization for f1_races
#     latest_materialization = context.instance.get_latest_materialization(
#         asset_key=["f1_races"]
#     )

#     if not latest_materialization:
#         context.log.info(
#             "No materialization found for f1_races, skipping f1_laps sensor."
#         )
#         return None
#     partition_key = latest_materialization.partition_key
#     if not partition_key:
#         context.log.info(
#             "No partition key found in latest materialization, skipping f1_laps sensor."
#         )
#         return None
#     # check if f1_laps has already been materialized for this partition
#     laps_materialization = context.instance.get_latest_materialization(
#         asset_key=["f1_laps"]
#     )
#     # If f1_laps has already been materialized for this partition, skip
#     if (
#         laps_materialization
#         and laps_materialization.partition_key == partition_key
#         and laps_materialization.timestamp > latest_materialization.timestamp
#     ):
#         context.log.info(
#             f"f1_laps already materialized for partition {partition_key}, skipping."
#         )
#         return None
#     # check if this is a new materialization we have not seen
#     current_timestamp = float(context.cursor) if context.cursor else 0.0
#     if latest_materialization.timestamp < current_timestamp:
#         context.log.info(
#             f"Latest materialization timestamp {latest_materialization.timestamp} is not newer than cursor {current_timestamp}, skipping."
#         )
#         return None

#     run_request = RunRequest(
#         run_key=f"f1_laps_{partition_key}_{latest_materialization.timestamp}",
#         partition_key=partition_key,
#         tags={
#             "triggered_by": "f1_laps_sensor",
#             "partition_key": partition_key,
#             "timestamp": str(latest_materialization.timestamp),
#         },
#     )
#     context.log.info(
#         f"Triggering f1_laps for partition {partition_key} with run key {run_request.run_key}"
#     )
#     return SensorResult(
#         run_requests=[run_request],
#         cursor=str(
#             latest_materialization.timestamp
#         ),  # Update cursor to latest timestamp
#     )


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_race_details",
    description="F1 qualyfying data from Ergast API",
    partitions_def=yearly_partitions,
    backfill_policy=multi_run_backfill,
    deps=["f1_races"],
)
def f1_qualifying(
    context: AssetExecutionContext,
    config: F1BronzeConfig,
) -> None:
    """Extract F1 qualifying data for a given simulation date."""

    simulation_date = get_simulation_date_from_partition(context, config)
    context.log.info(
        f"Extracting F1 qualifying data for: {simulation_date.strftime('%Y-%m-%d')}"
    )

    source = f1_api_source(years=[simulation_date.year])
    bronze_pipeline.run(source.resources["qualifying"])

    return None
