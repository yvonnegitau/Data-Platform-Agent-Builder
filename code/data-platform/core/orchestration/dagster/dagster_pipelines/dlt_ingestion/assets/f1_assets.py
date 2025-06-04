from datetime import datetime
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
)

from dateutil.relativedelta import relativedelta
import dlt
import uuid
import time
import fcntl


from dlt_ingestion.api_clients.F1Client import ErgastClient
from dlt_ingestion.sources.f1_source import (
    extract_driver_standings,
    extract_constructor_standings,
)

DB_WRITE_QUEUE = queue.Queue()
QUEUE_PROCESSOR_STARTED = False


def start_queue_processor():
    """Start the background queue processor if not already started"""
    global QUEUE_PROCESSOR_STARTED
    if not QUEUE_PROCESSOR_STARTED:
        processor_thread = threading.Thread(target=process_db_queue, daemon=True)
        processor_thread.start()
        QUEUE_PROCESSOR_STARTED = True


def process_db_queue():
    """Background thread that processes database writes sequentially"""
    while True:
        try:
            task = DB_WRITE_QUEUE.get(timeout=1)
            if task is None:  # Poison pill to stop
                break

            # Execute the database write task
            result = task["func"](*task["args"], **task["kwargs"])
            task["result_queue"].put({"success": True, "result": result})

        except queue.Empty:
            continue
        except Exception as e:
            task["result_queue"].put({"success": False, "error": str(e)})
        finally:
            DB_WRITE_QUEUE.task_done()


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


def run_dlt_pipeline(
    data: List[Dict[str, Any]],
    table_name: str,
    primary_key: List[str],
    layer: str = "bronze",
    write_disposition: str = "merge",
    context: Optional[AssetExecutionContext] = None,
):
    """Queue DLT pipeline execution to prevent database conflicts"""

    # Start the queue processor if needed
    start_queue_processor()

    # Create result queue for this specific task
    result_queue = queue.Queue()

    # Get partition key for unique naming
    partition_key = get_partition_key_for_metadata(context) if context else "unknown"

    # Create task for the queue
    task = {
        "func": _execute_dlt_pipeline,
        "args": (
            data,
            table_name,
            primary_key,
            partition_key,
            layer,
            write_disposition,
            context,
        ),
        "kwargs": {},
        "result_queue": result_queue,
    }

    if context:
        context.log.info(
            f"Queuing DLT pipeline for table {table_name}, partition {partition_key}"
        )

    # Add task to queue
    DB_WRITE_QUEUE.put(task)

    # Wait for result (10 minute timeout)
    try:
        result = result_queue.get(timeout=200)
        if result["success"]:
            return result["result"]
        else:
            raise Exception(f"Database write failed: {result['error']}")
    except queue.Empty:
        raise TimeoutError("Database write timed out after 200 seconds")


def _execute_dlt_pipeline(
    data: List[Dict[str, Any]],
    table_name: str,
    primary_key: List[str],
    partition_key: str,
    layer: str = "bronze",
    write_disposition: str = "merge",
    context: Optional[AssetExecutionContext] = None,
):
    """Internal function to execute DLT pipeline - called by queue processor"""

    if context:
        context.log.info(f"=== STARTING DLT PIPELINE EXECUTION ===")
        context.log.info(f"Table: {table_name}")
        context.log.info(f"Records to load: {len(data)}")
        context.log.info(f"Partition: {partition_key}")
        context.log.info(f"Write disposition: {write_disposition}")

    # Ensure the directory exists
    os.makedirs("/data/medallion", exist_ok=True)
    if context:
        context.log.info(f"✅ Created/verified /data/medallion directory")
        context.log.info(f"Directory exists: {os.path.exists('/data/medallion')}")
        context.log.info(
            f"Directory contents: {os.listdir('/data/medallion') if os.path.exists('/data/medallion') else 'Not found'}"
        )

    # Create unique pipeline name to avoid conflicts
    timestamp = int(time.time())
    unique_id = str(uuid.uuid4())[:8]
    safe_partition = partition_key.replace("-", "_").replace(":", "_").replace(" ", "_")
    pipeline_name = f"f1_{layer}_{table_name}_{safe_partition}_{timestamp}_{unique_id}"

    # Use absolute path that works regardless of working directory
    db_path = "/data/medallion/f1_data.duckdb"
    pipeline_dir = f"/tmp/dlt_pipelines/{layer}/{pipeline_name}"

    if context:
        context.log.info(f"Pipeline name: {pipeline_name}")
        context.log.info(f"Database path: {db_path}")
        context.log.info(f"Pipeline directory: {pipeline_dir}")
        context.log.info(f"Current working directory: {os.getcwd()}")

    try:
        if context:
            context.log.info("Creating DLT pipeline...")

        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="duckdb",
            dataset_name=f"f1_{layer}",
            pipelines_dir=pipeline_dir,
        )

        if context:
            context.log.info("✅ DLT pipeline object created")

        pipeline.destination = dlt.destinations.duckdb(
            credentials=f"duckdb:///{db_path}",
        )

        if context:
            context.log.info(f"✅ DLT destination configured: duckdb:///{db_path}")

        @dlt.resource(
            name=table_name,
            primary_key=primary_key,
            write_disposition=write_disposition,
        )
        def resource():
            if context:
                context.log.info(f"DLT resource yielding {len(data)} records...")
            for record in data:
                yield record

        if context:
            context.log.info("✅ DLT resource function created")

        # Execute pipeline with retry logic instead of file locks
        max_retries = 3
        retry_delay = 2  # seconds
        success = False

        for attempt in range(1, max_retries + 1):
            try:
                if context:
                    context.log.info(
                        f"Pipeline execution attempt {attempt}/{max_retries}"
                    )

                start_time = datetime.now()
                info = pipeline.run([resource()])
                end_time = datetime.now()

                if context:
                    context.log.info(f"✅ Pipeline run successful on attempt {attempt}")

                success = True
                break

            except Exception as e:
                if context:
                    context.log.warning(f"Pipeline attempt {attempt} failed: {str(e)}")

                if attempt < max_retries:
                    wait_time = retry_delay * (
                        2 ** (attempt - 1)
                    )  # Exponential backoff
                    if context:
                        context.log.info(f"Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    if context:
                        context.log.error(
                            f"Pipeline failed after {max_retries} attempts"
                        )
                    raise

        if not success:
            raise Exception("Pipeline execution failed after all retries")

        # Final file check
        if context:
            context.log.info(f"=== POST-PIPELINE FILE CHECK ===")
            context.log.info(f"DuckDB file path: {db_path}")
            file_exists = os.path.exists(db_path)
            context.log.info(f"DuckDB file exists: {file_exists}")

            if file_exists:
                file_size = os.path.getsize(db_path)
                context.log.info(f"DuckDB file size: {file_size} bytes")
            else:
                context.log.error(f"❌ DuckDB file was NOT created at {db_path}")

        result = {
            "records_loaded": len(data),
            "load_info": {"status": "completed", "info_type": str(type(info))},
            "runtime_seconds": (end_time - start_time).total_seconds(),
            "database_path": db_path,
            "schema": f"f1_{layer}",
            "table": table_name,
            "full_table_name": f"f1_{layer}.{table_name}",
            "pipeline_name": pipeline_name,
        }

        if context:
            context.log.info(f"=== DLT PIPELINE EXECUTION COMPLETED ===")
            context.log.info(f"Result: {result}")

        return result

    except Exception as e:
        if context:
            context.log.error(f"❌ DLT pipeline execution failed: {str(e)}")
            import traceback

            context.log.error(f"Full traceback: {traceback.format_exc()}")
        raise

    finally:
        # Clean up temporary pipeline directory
        try:
            import shutil

            if os.path.exists(pipeline_dir):
                shutil.rmtree(pipeline_dir)
                if context:
                    context.log.info(
                        f"✅ Cleaned up pipeline directory: {pipeline_dir}"
                    )
        except Exception as e:
            if context:
                context.log.warning(
                    f"Failed to clean up pipeline directory {pipeline_dir}: {e}"
                )


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_incremental",
    description="F1 race data from Ergast API",
    partitions_def=yearly_partitions,
    backfill_policy=multi_run_backfill,
)
def f1_races(
    context: AssetExecutionContext,
    config: F1BronzeConfig,
) -> None:
    simulation_date = get_simulation_date_from_partition(context, config)

    context.log.info(
        f"Extracting races for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )
    client = ErgastClient()
    races_data = client.get_races_incremental(simulation_date)

    if races_data is None:
        context.log.warning("No race data returned from API, using empty list")
        races_data = []

    context.log.info(f"Retrieved {len(races_data)} race records")
    if not races_data:
        context.log.info("No race data to process, skipping pipeline execution")
        context.add_output_metadata(
            {
                "partition_key": MetadataValue.text(
                    get_partition_key_for_metadata(context)
                ),
                "simulation_date": MetadataValue.text(
                    simulation_date.strftime("%Y-%m-%d")
                ),
                "records_extracted": MetadataValue.int(0),
                "records_loaded": MetadataValue.int(0),
                "runtime_seconds": MetadataValue.float(0.0),
                "status": MetadataValue.text("skipped - no data"),
            }
        )
        return None
    write_disposition = "replace" if config.full_refresh else "merge"  # Fixed typo
    pipeline_result = run_dlt_pipeline(
        races_data,
        "races",
        ["season", "round"],
        layer="bronze",
        write_disposition=write_disposition,
        context=context,
    )
    context.add_output_metadata(
        {
            "partition_key": MetadataValue.text(
                get_partition_key_for_metadata(context)
            ),
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "records_extracted": MetadataValue.int(len(races_data)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
            "database_path": MetadataValue.text(pipeline_result["database_path"]),
            "schema": MetadataValue.text(pipeline_result["schema"]),
            "full_table_name": MetadataValue.text(pipeline_result["full_table_name"]),
        }
    )
    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_incremental",
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
    client = ErgastClient()
    results_data = client.get_results_incremental(simulation_date)
    write_disposition = "replace" if config.full_refresh else "merge"  # Fixed typo

    pipeline_result = run_dlt_pipeline(
        results_data,
        "results",
        ["season", "round", "driver__driver_id"],
        layer="bronze",
        write_disposition=write_disposition,  # Fixed typo
        context=context,
    )
    context.add_output_metadata(
        {
            "partition_key": MetadataValue.text(
                get_partition_key_for_metadata(context)
            ),
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "records_extracted": MetadataValue.int(len(results_data)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
            "database_path": MetadataValue.text(pipeline_result["database_path"]),
            "schema": MetadataValue.text(pipeline_result["schema"]),
            "full_table_name": MetadataValue.text(pipeline_result["full_table_name"]),
        }
    )
    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_reference",
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

    all_drivers = []
    all_results = []

    # Process each partition
    for key in partition_keys:
        # Parse the key to get simulation date
        simulation_date = datetime.strptime(key, "%Y-%m-%d") if key else datetime.now()

        context.log.info(
            f"Extracting drivers for {key} (date: {simulation_date.strftime('%Y-%m-%d')})"
        )

        # Extract data
        client = ErgastClient()
        drivers_data = client.get_drivers_incremental(simulation_date)

        # Add year field if not present
        if drivers_data:
            for driver in drivers_data:
                if "year" not in driver:
                    driver["year"] = simulation_date.year

            all_drivers.extend(drivers_data)

            # Save per-partition metadata
            all_results.append(
                {
                    "partition": key,
                    "year": simulation_date.year,
                    "records": len(drivers_data),
                }
            )

            context.log.info(
                f"✅ Found {len(drivers_data)} drivers for {simulation_date.year}"
            )

    # Skip if no data
    if not all_drivers:
        context.log.info("No driver data found across all partitions")
        context.add_output_metadata(
            {
                "partitions_processed": MetadataValue.int(len(partition_keys)),
                "records_extracted": MetadataValue.int(0),
                "status": MetadataValue.text("no data found"),
            }
        )
        return None

    write_disposition = "replace" if config.full_refresh else "merge"

    pipeline_result = run_dlt_pipeline(
        all_drivers,
        "drivers",
        ["driverId", "year"],  # Use API field names
        layer="bronze",
        write_disposition=write_disposition,
        context=context,
    )

    context.add_output_metadata(
        {
            "partition_key": MetadataValue.text(
                get_partition_key_for_metadata(context)
            ),
            "partitions_processed": MetadataValue.int(len(partition_keys)),
            "partitions_with_data": MetadataValue.int(len(all_results)),
            "results_by_partition": MetadataValue.json(all_results),
            "records_extracted": MetadataValue.int(len(all_drivers)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
            "database_path": MetadataValue.text(pipeline_result["database_path"]),
            "schema": MetadataValue.text(pipeline_result["schema"]),
            "full_table_name": MetadataValue.text(pipeline_result["full_table_name"]),
        }
    )

    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_reference",
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

    all_constructors = []
    all_results = []

    # Process each partition
    for key in partition_keys:
        # Parse the key to get simulation date
        simulation_date = datetime.strptime(key, "%Y-%m-%d") if key else datetime.now()

        context.log.info(
            f"Extracting constructors for {key} (date: {simulation_date.strftime('%Y-%m-%d')})"
        )

        # Extract data
        client = ErgastClient()
        constructors_data = client.get_constructors_incremental(simulation_date)

        # Add year field if not present
        if constructors_data:
            for constructor in constructors_data:
                if "year" not in constructor:
                    constructor["year"] = simulation_date.year

            all_constructors.extend(constructors_data)

            # Save per-partition metadata
            all_results.append(
                {
                    "partition": key,
                    "year": simulation_date.year,
                    "records": len(constructors_data),
                }
            )

            context.log.info(
                f"✅ Found {len(constructors_data)} constructors for {simulation_date.year}"
            )

    # Skip if no data
    if not all_constructors:
        context.log.info("No constructor data found across all partitions")
        context.add_output_metadata(
            {
                "partitions_processed": MetadataValue.int(len(partition_keys)),
                "records_extracted": MetadataValue.int(0),
                "status": MetadataValue.text("no data found"),
            }
        )
        return None

    write_disposition = "replace" if config.full_refresh else "merge"

    pipeline_result = run_dlt_pipeline(
        all_constructors,
        "constructors",
        ["constructorId", "year"],
        layer="bronze",
        write_disposition=write_disposition,
        context=context,
    )

    context.add_output_metadata(
        {
            "partition_key": MetadataValue.text(
                get_partition_key_for_metadata(context)
            ),
            "partitions_processed": MetadataValue.int(len(partition_keys)),
            "partitions_with_data": MetadataValue.int(len(all_results)),
            "results_by_partition": MetadataValue.json(all_results),
            "records_extracted": MetadataValue.int(len(all_constructors)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
            "database_path": MetadataValue.text(pipeline_result["database_path"]),
            "schema": MetadataValue.text(pipeline_result["schema"]),
            "full_table_name": MetadataValue.text(pipeline_result["full_table_name"]),
        }
    )

    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_incremental",
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

    client = ErgastClient()
    driver_standings = client.get_driver_standings_incremental(simulation_date)

    # Extract driver standings
    # driver_standings = extract_driver_standings(standings_data)
    write_disposition = "replace" if config.full_refresh else "merge"

    # Run DLT pipeline
    pipeline_result = run_dlt_pipeline(
        driver_standings,
        "driver_standings",
        ["season", "round", "driver__driver_id"],
        layer="bronze",
        write_disposition=write_disposition,
        context=context,
    )

    context.add_output_metadata(
        {
            "partition_key": MetadataValue.text(
                get_partition_key_for_metadata(context)
            ),
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "records_extracted": MetadataValue.int(len(driver_standings)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
            "database_path": MetadataValue.text(pipeline_result["database_path"]),
            "schema": MetadataValue.text(pipeline_result["schema"]),
            "full_table_name": MetadataValue.text(pipeline_result["full_table_name"]),
        }
    )

    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_incremental",
    description="F1 constructor standings data from Ergast API",
    partitions_def=yearly_partitions,
    backfill_policy=multi_run_backfill,
)
def f1_constructor_standings(
    context: AssetExecutionContext, config: F1BronzeConfig
) -> None:
    """Extract F1 constructor standings data."""
    simulation_date = get_simulation_date_from_partition(context, config)
    context.log.info(
        f"Extracting constructor standings for simulation date: {simulation_date.strftime('%Y-%m-%d')}"
    )

    client = ErgastClient()
    constructor_standings = client.get_constructor_standings_incremental(
        simulation_date
    )
    write_disposition = "replace" if config.full_refresh else "merge"  # Fixed typo

    # Extract constructor standings
    # constructor_standings = extract_constructor_standings(standings_data)

    # Run DLT pipeline
    pipeline_result = run_dlt_pipeline(
        constructor_standings,
        "constructor_standings",
        ["season", "round", "constructor__constructor_id"],
        layer="bronze",
        write_disposition=write_disposition,  # Fixed typo
        context=context,
    )

    context.add_output_metadata(
        {
            "partition_key": MetadataValue.text(
                get_partition_key_for_metadata(context)
            ),
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "records_extracted": MetadataValue.int(len(constructor_standings)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
            "database_path": MetadataValue.text(pipeline_result["database_path"]),
            "schema": MetadataValue.text(pipeline_result["schema"]),
            "full_table_name": MetadataValue.text(pipeline_result["full_table_name"]),
        }
    )

    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_static",
    description="F1 seasons reference data from Ergast API",
    partitions_def=reference_partitions,
    backfill_policy=single_run_backfill,
)
def f1_seasons(context: AssetExecutionContext, config: F1BronzeConfig) -> None:
    """Extract F1 seasons reference data."""

    simulation_date = (
        datetime.now()
        if not config.simulation_date
        else datetime.strptime(config.simulation_date, "%Y-%m-%d")
    )

    context.log.info(f"Extracting seasons for simulation date: {simulation_date}")

    client = ErgastClient()
    seasons_data = client.get_seasons()
    write_disposition = "replace" if config.full_refresh else "merge"

    # Filter seasons up to the current simulation date if needed
    filtered_seasons = [
        season
        for season in seasons_data
        if int(season["season"]) <= simulation_date.year
    ]

    context.log.info(
        f"Found {len(filtered_seasons)} seasons (up to {simulation_date.year})"
    )

    if not filtered_seasons:
        context.log.warning("No season data found, skipping pipeline execution")
        context.add_output_metadata(
            {
                "records_extracted": MetadataValue.int(0),
                "records_loaded": MetadataValue.int(0),
                "status": MetadataValue.text("skipped - no data"),
            }
        )
        return None

    pipeline_result = run_dlt_pipeline(
        filtered_seasons,
        "seasons",
        ["season"],
        layer="bronze",
        write_disposition=write_disposition,
        context=context,
    )

    context.add_output_metadata(
        {
            "partition_key": MetadataValue.text(
                get_partition_key_for_metadata(context)
            ),
            "simulation_date": MetadataValue.text(simulation_date.strftime("%Y-%m-%d")),
            "first_season": MetadataValue.text(
                min(s["season"] for s in filtered_seasons)
            ),
            "last_season": MetadataValue.text(
                max(s["season"] for s in filtered_seasons)
            ),
            "records_extracted": MetadataValue.int(len(filtered_seasons)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
            "database_path": MetadataValue.text(pipeline_result["database_path"]),
            "schema": MetadataValue.text(pipeline_result["schema"]),
            "full_table_name": MetadataValue.text(pipeline_result["full_table_name"]),
        }
    )

    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_static",
    description="F1 circuits reference data from Ergast API",
    partitions_def=reference_partitions,
    backfill_policy=single_run_backfill,
)
def f1_circuits(context: AssetExecutionContext, config: F1BronzeConfig) -> None:
    """Extract F1 circuits reference data."""

    simulation_date = (
        datetime.now()
        if not config.simulation_date
        else datetime.strptime(config.simulation_date, "%Y-%m-%d")
    )
    context.log.info(f"Extracting circuits for simulation date: {simulation_date}")

    client = ErgastClient()
    circuits_data = client.get_circuits()
    write_disposition = "replace" if config.full_refresh else "merge"  # Fixed typo

    # Filter circuits based on the simulation date

    pipeline_result = run_dlt_pipeline(
        circuits_data,
        "circuits",
        ["circuitId"],
        layer="bronze",
        write_disposition=write_disposition,  # Fixed typo
        context=context,
    )

    context.add_output_metadata(
        {
            "records_extracted": MetadataValue.int(len(circuits_data)),
            "records_loaded": MetadataValue.int(pipeline_result["records_loaded"]),
            "runtime_seconds": MetadataValue.float(pipeline_result["runtime_seconds"]),
            "database_path": MetadataValue.text(pipeline_result["database_path"]),
            "schema": MetadataValue.text(pipeline_result["schema"]),
            "full_table_name": MetadataValue.text(pipeline_result["full_table_name"]),
        }
    )

    return None


@asset(
    compute_kind="dlt",
    group_name="f1_bronze_comprehensive",
    description="Comprehensive F1 data using separate standings endpoints",
    partitions_def=monthly_partitions,
    backfill_policy=multi_run_backfill,
)
def f1_monthly_comprehensive(
    context: AssetExecutionContext,
    config: F1BronzeConfig,
) -> None:
    """Extract all F1 data using separate standings endpoints."""

    simulation_date = get_simulation_date_from_partition(context, config)
    context.log.info(
        f"Extracting comprehensive F1 data for: {simulation_date.strftime('%Y-%m-%d')}"
    )

    client = ErgastClient()
    write_disposition = "replace" if config.full_refresh else "merge"

    # Get all data in one call
    all_data = client.get_simulated_month_data(simulation_date)

    total_records = 0
    tables_processed = []

    # Define the processing order and table configurations
    data_configs = [
        {
            "data_key": "races",
            "table_name": "races",
            "primary_key": ["season", "round"],
            "required": True,
        },
        {
            "data_key": "results",
            "table_name": "results",
            "primary_key": ["season", "round", "driverId"],
            "required": False,
        },
        {
            "data_key": "driver_standings",
            "table_name": "driver_standings",
            "primary_key": ["season", "round", "driverId"],
            "required": False,
        },
        {
            "data_key": "constructor_standings",
            "table_name": "constructor_standings",
            "primary_key": ["season", "round", "constructorId"],
            "required": False,
        },
    ]

    try:
        # Process each data type
        for config_item in data_configs:
            data_key = config_item["data_key"]
            table_name = config_item["table_name"]
            primary_key = config_item["primary_key"]
            required = config_item["required"]

            data = all_data.get(data_key, [])

            if not data:
                if required:
                    context.log.warning(
                        f"No {data_key} data found - this is required, skipping all"
                    )
                    context.add_output_metadata(
                        {
                            "status": MetadataValue.text(f"skipped - no {data_key}"),
                            "tables_processed": MetadataValue.json([]),
                        }
                    )
                    return None
                else:
                    context.log.info(f"No {data_key} data found - skipping")
                    continue

            # Process the data
            context.log.info(f"Processing {data_key}: {len(data)} records")

            result = run_dlt_pipeline(
                data,
                table_name,
                primary_key,
                layer="bronze",
                write_disposition=write_disposition,
                context=context,
            )

            total_records += result["records_loaded"]
            tables_processed.append(table_name)
            context.log.info(
                f"✅ {data_key}: {result['records_loaded']} records loaded"
            )

        # Final success metadata
        context.add_output_metadata(
            {
                "partition_key": MetadataValue.text(
                    get_partition_key_for_metadata(context)
                ),
                "simulation_date": MetadataValue.text(
                    simulation_date.strftime("%Y-%m-%d")
                ),
                "tables_processed": MetadataValue.json(tables_processed),
                "total_records": MetadataValue.int(total_records),
                "status": MetadataValue.text("completed"),
            }
        )

        context.log.info(f"🏁 Comprehensive F1 data extraction completed!")
        context.log.info(f"📊 Tables: {', '.join(tables_processed)}")
        context.log.info(f"📈 Total records: {total_records}")

    except Exception as e:
        context.log.error(f"❌ Comprehensive extraction failed: {str(e)}")
        context.add_output_metadata(
            {
                "status": MetadataValue.text(f"failed - {str(e)}"),
                "tables_processed": MetadataValue.json(tables_processed),
            }
        )
        raise

    return None
