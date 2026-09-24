"""Dagster resource that gives assets an Ibis backend connection.

The pipeline code never touches a backend-specific API: assets ask this
resource for an `ibis` connection and write portable Ibis expressions.
Switching engines is pure configuration (`backend` field).
"""

import dagster as dg
import ibis
from ibis.backends import BaseBackend
from pydantic import PrivateAttr


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

    # One connection per run: polars registers tables per-connection, and a
    # duckdb file can only be locked once, so all assets share this handle.
    _con: BaseBackend | None = PrivateAttr(default=None)

    def connect(self) -> BaseBackend:
        """Return the run's Ibis connection, opening it on first use."""
        if self._con is None:
            self._con = self._connect()
        return self._con

    def _connect(self) -> BaseBackend:
        if self.backend == "duckdb":
            return ibis.duckdb.connect(self.duckdb_path)
        if self.backend == "polars":
            return ibis.polars.connect()
        if self.backend == "pyspark":
            from pyspark.sql import SparkSession

            session = (
                SparkSession.builder.master(self.spark_master)
                .config("spark.sql.warehouse.dir", self.spark_warehouse_dir)
                .getOrCreate()
            )
            return ibis.pyspark.connect(session)
        raise ValueError(
            f"Unsupported ibis backend {self.backend!r}. "
            "Expected one of: 'duckdb', 'polars', 'pyspark'."
        )
