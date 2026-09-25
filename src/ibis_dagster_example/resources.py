"""Dagster resource that gives assets an Ibis backend connection.

The pipeline code never touches a backend-specific API: assets ask this
resource for an `ibis` connection and write portable Ibis expressions.
Switching engines is pure configuration (`backend` field).
"""

import dagster as dg
import ibis
from ibis.backends import BaseBackend
from functools import lru_cache


class IbisResource(dg.ConfigurableResource):
    """Connects to one of the Ibis backends at run time.

    Attributes
    ----------
    backend
        Which Ibis backend to execute on: "duckdb", "polars", or "pyspark".
    duckdb_path
        Path of the DuckDB database file. Use ":memory:" for ephemeral runs.
    spark_master
        Spark master URL, e.g. "local[*]" or "spark://host:7077" (or a
        Spark Connect URL via spark.remote).
    spark_warehouse_dir
        `spark.sql.warehouse.dir` — where Spark persists managed tables.
    """

    backend: str = "duckdb"

    duckdb_path: str = "warehouse.duckdb"
    spark_master: str = "local[*]"
    spark_warehouse_dir: str = "./spark-warehouse"

    def connect(self) -> BaseBackend:
        """Return the process-wide Ibis connection for this configuration.

        Dagster may hand different resource instances to assets, the io
        manager, and checks — so the connection is cached by config, not by
        instance. One connection per process is required anyway: polars
        registers tables per-connection and a duckdb file locks once.
        """
        return _connection(
            self.backend,
            self.duckdb_path,
            self.spark_master,
            self.spark_warehouse_dir,
        )


@lru_cache(maxsize=8)
def _connection(
    backend: str,
    duckdb_path: str,
    spark_master: str,
    spark_warehouse_dir: str,
) -> BaseBackend:
    if backend == "duckdb":
        return ibis.duckdb.connect(duckdb_path)
    if backend == "polars":
        return ibis.polars.connect()
    if backend == "pyspark":
        from pyspark.sql import SparkSession

        session = (
            SparkSession.builder.master(spark_master)
            .config("spark.sql.warehouse.dir", spark_warehouse_dir)
            .getOrCreate()
        )
        return ibis.pyspark.connect(session)
    raise ValueError(
        f"Unsupported ibis backend {backend!r}. "
        "Expected one of: 'duckdb', 'polars', 'pyspark'."
    )
