"""Dagster definitions: assets + the Ibis resource that picks a backend.

Which engine executes the pipeline is decided entirely by configuration:

    IBIS_BACKEND=duckdb   dagster dev     # local dev / small data
    IBIS_BACKEND=polars   dagster dev     # e.g. lightweight CI or edge
    IBIS_BACKEND=pyspark  dagster dev     # production cluster

Because `IbisResource` is a ConfigurableResource, every field can also be
overridden per-run in the Dagster Launchpad.
"""

import dagster as dg

from .assets import (
    category_revenue,
    cleaned_events,
    daily_active_users,
    raw_events,
    raw_products,
)
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

defs = dg.Definitions(
    assets=all_assets,
    jobs=[ibis_etl_job],
    # In-process execution: all assets share one Ibis connection. That is
    # required for the polars backend (its tables live in the connection) and
    # avoids duckdb file-lock contention between concurrent step processes.
    executor=dg.in_process_executor,
    resources={
        "ibis": IbisResource(
            backend=dg.EnvVar("IBIS_BACKEND").get_value("duckdb"),
            duckdb_path=dg.EnvVar("IBIS_DUCKDB_PATH").get_value("warehouse.duckdb"),
            spark_master=dg.EnvVar("SPARK_MASTER").get_value("local[*]"),
            spark_warehouse_dir=dg.EnvVar("SPARK_WAREHOUSE_DIR").get_value(
                "./spark-warehouse"
            ),
        )
    },
)
