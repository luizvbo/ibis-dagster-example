"""Dagster definitions.

Environment selection follows the standard Dagster pattern: a
`resources_by_deployment` dict keyed on DAGSTER_DEPLOYMENT_NAME (set
automatically by Dagster+; export it yourself for `dagster dev`).

    DAGSTER_DEPLOYMENT_NAME=local   dagster dev   # duckdb + csv files
    DAGSTER_DEPLOYMENT_NAME=polars  dagster dev   # polars (in-memory) + csvs
    DAGSTER_DEPLOYMENT_NAME=prod    dagster dev   # pyspark + lake parquet
"""

import os

import dagster as dg

from .assets import (
    SOURCE_SPECS,
    category_revenue,
    cleaned_events,
    daily_active_users,
    raw_events,
    raw_products,
)
from .checks import ALL_CHECKS
from .io_manager import IbisIOManager
from .resources import IbisResource

all_assets = [
    raw_events,
    raw_products,
    cleaned_events,
    daily_active_users,
    category_revenue,
]

ibis_etl_job = dg.define_asset_job(
    "ibis_etl", selection=dg.AssetSelection.assets(*all_assets)
)

# The `dbt run` cron analog — starts stopped; toggle on in the UI or via
# default_status=dg.DefaultStatus.RUNNING. (Declarative alternative for a
# real deployment: AutoMaterializePolicy on the assets themselves.)
daily_schedule = dg.ScheduleDefinition(
    job=ibis_etl_job, cron_schedule="0 6 * * *"
)

CSV_SOURCES = {
    "events_csv": {"format": "csv", "path": "data/raw_events.csv"},
    "products_csv": {"format": "csv", "path": "data/raw_products.csv"},
}

# production sources live in the data lake; ${DATA_LAKE} expands at run time.
# swap "parquet" for {"format": "table", "name": "landing.events"} to read
# existing hive-metastore tables instead.
LAKE_SOURCES = {
    "events_csv": {"format": "parquet", "path": "${DATA_LAKE}/landing/events/"},
    "products_csv": {
        "format": "parquet",
        "path": "${DATA_LAKE}/landing/products/",
    },
}

def _deployment(ibis: IbisResource, sources: dict) -> dict:
    # bind the same IbisResource instance to both keys: the io manager uses it
    # (nested resource) and asset checks use it directly (top-level resource)
    return {"ibis": ibis, "io_manager": IbisIOManager(ibis=ibis, sources=sources)}


resources_by_deployment = {
    # laptop: duckdb file, local csvs
    "local": _deployment(
        IbisResource(
            backend="duckdb",
            duckdb_path=dg.EnvVar("IBIS_DUCKDB_PATH").get_value("warehouse.duckdb"),
        ),
        CSV_SOURCES,
    ),
    # lightweight/ephemeral: polars in-memory engine, same local csvs
    "polars": _deployment(IbisResource(backend="polars"), CSV_SOURCES),
    # production: spark cluster, parquet in the lake, managed tables out
    "prod": _deployment(
        IbisResource(
            backend="pyspark",
            spark_master=dg.EnvVar("SPARK_MASTER").get_value("local[*]"),
            spark_warehouse_dir=dg.EnvVar("SPARK_WAREHOUSE_DIR").get_value(
                "./spark-warehouse"
            ),
        ),
        LAKE_SOURCES,
    ),
}

deployment_name = os.getenv("DAGSTER_DEPLOYMENT_NAME", "local")
if deployment_name not in resources_by_deployment:
    raise ValueError(
        f"Unknown DAGSTER_DEPLOYMENT_NAME {deployment_name!r}; "
        f"expected one of {sorted(resources_by_deployment)}"
    )

defs = dg.Definitions(
    assets=[*all_assets, *SOURCE_SPECS],
    asset_checks=ALL_CHECKS,
    jobs=[ibis_etl_job],
    schedules=[daily_schedule],
    # In-process execution: all steps share one Ibis connection. Required for
    # the polars backend (tables live in the connection) and avoids duckdb
    # file-lock contention between concurrent step processes.
    executor=dg.in_process_executor,
    resources=resources_by_deployment[deployment_name],
)
