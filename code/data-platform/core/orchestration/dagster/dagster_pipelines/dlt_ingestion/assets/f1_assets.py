from datetime import datetime
from typing import Any, Dict, List, Optional
from dagster import Config, AssetExecutionContext, asset, MetadataValue

from dateutil.relativedelta import relativedelta
import dlt


from dlt_ingestion.api_clients.F1Client import ErgastClient
from dlt_ingestion.sources.f1_source import (
    extract_driver_standings,
    extract_constructor_standings,
)


class F1BronzeConfig(Config):
    start_year: int = 1950
    months_to_run: Optional[int] = None
    simulation_date: Optional[str] = None
    full_refresh: bool = False


def get_simulation_date(config: F1BronzeConfig) -> datetime:
    if config.simulation_date:
        return datetime.strptime(config.simulation_date, "%Y-%m-%d")
    elif config.months_to_run:
        return datetime(config.start_year, 1, 1) + relativedelta(
            months=config.months_to_run
        )
    else:
        return datetime.now()


def run_dlt_pipeline(
    data: List[Dict[str, Any]],
    table_name: str,
    primary_key: List[str],
):
    pipeline = dlt.pipeline(
        pipeline_name=f"f1_bronze_{table_name}",
        destination="duckdb",
        dataset_name=f"f1_bronze_{table_name}",
    )

    @dlt.resource(name=table_name, primary_key=primary_key)
    def resource():
        return data

    info = pipeline.run([resource()])
    return {
        "records_loaded": len(data),
        "load_info": info.load_package.resource_counts,
        "runtime_seconds": (info.end_time - info.start_time).total_seconds(),
    }


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_incremental",
    description="F1 race data from Ergast API",
)
def f1_races(
    context: AssetExecutionContext,
    config: F1BronzeConfig,
) -> List[Dict[str, Any]]:
    simulation_date = get_simulation_date(config)

    context.log.info(
        f"Extracting races for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )
    client = ErgastClient()
    races_data = client.get_races_incremental(simulation_date)

    pipeline_result = run_dlt_pipeline(races_data, "races", ["season", "round"])
    context.add_output_metadata(
        {
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "records_extracted": MetadataValue.int(len(races_data)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
        }
    )
    return races_data


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_incremental",
    description="F1 race results data from Ergast API",
    deps=["f1_races"],  # Depends on races being extracted first
)
def f1_results(
    context: AssetExecutionContext, config: F1BronzeConfig
) -> List[Dict[str, Any]]:
    simulation_date = get_simulation_date(config)

    context.log.info(
        f"Extracting results for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )
    client = ErgastClient()
    results_data = client.get_results_incremental(simulation_date)

    pipeline_result = run_dlt_pipeline(
        results_data, "results", ["season", "round", "driverId"]
    )
    context.add_output_metadata(
        {
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "records_extracted": MetadataValue.int(len(results_data)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
        }
    )
    return results_data


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_incremental",
    description="F1 drivers data from Ergast API",
)
def f1_drivers(
    context: AssetExecutionContext, config: F1BronzeConfig
) -> List[Dict[str, Any]]:
    """Extract F1 drivers data."""
    simulation_date = get_simulation_date(config)
    context.log.info(
        f"Extracting drivers for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )

    client = ErgastClient()
    drivers_data = client.get_drivers_incremental(simulation_date)

    # Run DLT pipeline
    pipeline_result = run_dlt_pipeline(drivers_data, "drivers", ["driverId"])

    context.add_output_metadata(
        {
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "records_extracted": MetadataValue.int(len(drivers_data)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
        }
    )

    return drivers_data


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_incremental",
    description="F1 constructors data from Ergast API",
)
def f1_constructors(
    context: AssetExecutionContext, config: F1BronzeConfig
) -> List[Dict[str, Any]]:
    """Extract F1 constructors data."""
    simulation_date = get_simulation_date(config)
    context.log.info(
        f"Extracting constructors for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )

    client = ErgastClient()
    constructors_data = client.get_constructors_incremental(simulation_date)

    # Run DLT pipeline
    pipeline_result = run_dlt_pipeline(
        constructors_data, "constructors", ["constructorId"]
    )

    context.add_output_metadata(
        {
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "records_extracted": MetadataValue.int(len(constructors_data)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
        }
    )

    return constructors_data


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_incremental",
    description="F1 driver standings data from Ergast API",
    deps=["f1_results"],  # Depends on results being available
)
def f1_driver_standings(
    context: AssetExecutionContext, config: F1BronzeConfig
) -> List[Dict[str, Any]]:
    """Extract F1 driver standings data."""
    simulation_date = get_simulation_date(config)
    context.log.info(
        f"Extracting driver standings for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )

    client = ErgastClient()
    standings_data = client.get_standings_incremental(simulation_date)

    # Extract driver standings
    driver_standings = extract_driver_standings(standings_data)

    # Run DLT pipeline
    pipeline_result = run_dlt_pipeline(
        driver_standings, "driver_standings", ["season", "round", "driverId"]
    )

    context.add_output_metadata(
        {
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "records_extracted": MetadataValue.int(len(driver_standings)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
        }
    )

    return driver_standings


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_incremental",
    description="F1 constructor standings data from Ergast API",
    deps=["f1_results"],  # Depends on results being available
)
def f1_constructor_standings(
    context: AssetExecutionContext, config: F1BronzeConfig
) -> List[Dict[str, Any]]:
    """Extract F1 constructor standings data."""
    simulation_date = get_simulation_date(config)
    context.log.info(
        f"Extracting constructor standings for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )

    client = ErgastClient()
    standings_data = client.get_standings_incremental(simulation_date)

    # Extract constructor standings
    constructor_standings = extract_constructor_standings(standings_data)

    # Run DLT pipeline
    pipeline_result = run_dlt_pipeline(
        constructor_standings,
        "constructor_standings",
        ["season", "round", "constructorId"],
    )

    context.add_output_metadata(
        {
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "records_extracted": MetadataValue.int(len(constructor_standings)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
        }
    )

    return constructor_standings


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_reference",
    description="F1 seasons reference data from Ergast API",
)
def f1_seasons(
    context: AssetExecutionContext, config: F1BronzeConfig
) -> List[Dict[str, Any]]:
    """Extract F1 seasons reference data."""

    client = ErgastClient()
    seasons_data = client.get_seasons()

    # Run DLT pipeline
    pipeline_result = run_dlt_pipeline(seasons_data, "seasons", ["season"])

    context.add_output_metadata(
        {
            "records_extracted": MetadataValue.int(len(seasons_data)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "first_season": MetadataValue.text(min(s["season"] for s in seasons_data)),
            "last_season": MetadataValue.text(max(s["season"] for s in seasons_data)),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
        }
    )

    return seasons_data


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_reference",
    description="F1 circuits reference data from Ergast API",
)
def f1_circuits(
    context: AssetExecutionContext, config: F1BronzeConfig
) -> List[Dict[str, Any]]:
    """Extract F1 circuits reference data."""

    client = ErgastClient()
    circuits_data = client.get_circuits()

    # Run DLT pipeline
    pipeline_result = run_dlt_pipeline(circuits_data, "circuits", ["circuitId"])

    context.add_output_metadata(
        {
            "records_extracted": MetadataValue.int(len(circuits_data)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
        }
    )

    return circuits_data
