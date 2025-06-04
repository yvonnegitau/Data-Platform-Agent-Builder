from dlt_ingestion.assets import f1_assets
from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
)


all_assets = load_assets_from_modules([f1_assets])

f1_bronze_job = define_asset_job(
    "f1_bronze_reference_job",
    selection=AssetSelection.groups("f1_bronze_reference"),
    description="Job to run the F1 bronze pipeline",
)

f1_reference_job = define_asset_job(
    "f1_bronze_comprehensive_job",
    selection=AssetSelection.groups("f1_bronze_incremental"),
    description="Extract F1 reference data",
)

f1_static_job = define_asset_job(
    "f1_bronze_static_job",
    selection=AssetSelection.groups("f1_bronze_static"),
    description="Extract F1 static data",
)


f1_full_job = define_asset_job(
    "f1_full_job",
    selection=AssetSelection.groups("f1_full"),
    description="Extract all F1 Data",
)

defs = Definitions(
    assets=all_assets,
    jobs=[f1_bronze_job, f1_reference_job, f1_full_job, f1_static_job],
)
