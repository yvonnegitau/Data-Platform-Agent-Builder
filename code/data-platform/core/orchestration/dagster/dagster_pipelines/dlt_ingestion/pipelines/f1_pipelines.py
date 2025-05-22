from datetime import datetime
import logging
from typing import Optional
import dlt


from dlt_ingestion.sources.f1_source import f1_bronze_data

logger = logging.getLogger(__name__)


def run_f1_bronze_pipeline(
    simulation_date: Optional[datetime] = None,
    full_refresh: bool = False,
    destination_table_prefix: str = "",
):

    if simulation_date is None:
        simulation_date = datetime.now()

    logger.info(f"Running F1 bronze pipeline for simulation date: {simulation_date}")

    # create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name=f"{destination_table_prefix}f1_bronze",
        destination="duckdb",
        dataset_name=f"{destination_table_prefix}f1_bronze",
        full_refresh=full_refresh,
    )

    info = pipeline.run(f1_bronze_data(simulation_date=simulation_date))

    logger.info(f"Pipeline completed. Loaded {info.load_package.count} resources.")
    logger.info(f"Resources loaded: {info.load_package.resource_counts}")

    return info
