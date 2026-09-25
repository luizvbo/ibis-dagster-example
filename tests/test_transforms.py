"""Run the pipeline transforms and I/O manager against every available Ibis
backend and assert identical results — the portability contract.
"""

import dagster as dg
import ibis
import pytest

from ibis_dagster_example import data, transforms
from ibis_dagster_example.assets import (
    SOURCE_SPECS,
    category_revenue,
    cleaned_events,
    daily_active_users,
    raw_events,
    raw_products,
)
from ibis_dagster_example.checks import ALL_CHECKS
from ibis_dagster_example.io_manager import IbisIOManager
from ibis_dagster_example.resources import IbisResource

ALL_ASSETS = [
    *SOURCE_SPECS,
    raw_events,
    raw_products,
    cleaned_events,
    daily_active_users,
    category_revenue,
]

CSV_SOURCES = {
    "events_csv": {"format": "csv", "path": "data/raw_events.csv"},
    "products_csv": {"format": "csv", "path": "data/raw_products.csv"},
}


def _connect(backend: str):
    if backend == "duckdb":
        return ibis.duckdb.connect()
    if backend == "polars":
        return ibis.polars.connect()
    if backend == "pyspark":
        pytest.importorskip("pyspark")
        try:
            from pyspark.sql import SparkSession

            session = (
                SparkSession.builder.master("local[2]")
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
                .getOrCreate()
            )
        except Exception:
            pytest.skip("pyspark installed but no JVM available")
        return ibis.pyspark.connect(session)
    raise ValueError(backend)


@pytest.fixture(params=["duckdb", "polars", "pyspark"])
def con(request):
    return _connect(request.param)


@pytest.fixture
def cleaned(con):
    raw = con.create_table("raw_events", data.raw_events, overwrite=True)
    return con.create_table(
        "cleaned_events", transforms.clean_events(raw), overwrite=True
    )


def test_clean_events_removes_duplicates_and_nulls(con, cleaned):
    assert cleaned.count().execute() == 12  # 14 raw - 1 dupe - 1 null user_id
    df = cleaned.execute()
    assert df["event_type"].str.islower().all()
    assert df["amount"].isna().sum() == 0


def test_daily_active_users(con, cleaned):
    df = (
        transforms.daily_active_users(cleaned)
        .execute()
        .sort_values("date")
        .reset_index(drop=True)
    )
    assert list(df["n_active_users"]) == [3, 4]
    assert list(df["n_purchases"]) == [3, 2]


def test_category_revenue(con, cleaned):
    products = con.create_table("raw_products", data.raw_products, overwrite=True)
    df = (
        transforms.category_revenue(cleaned, products)
        .execute()
        .set_index("category")
    )
    assert df.loc["electronics", "n_orders"] == 3
    assert df.loc["electronics", "revenue"] == pytest.approx(198.80)
    assert df.loc["office", "revenue"] == pytest.approx(5.75)


def test_full_pipeline_on_duckdb(tmp_path):
    """Materialize the whole asset graph through the IO manager."""
    db = tmp_path / "warehouse.duckdb"
    result = dg.materialize(
        ALL_ASSETS,
        resources={
            "io_manager": IbisIOManager(
                ibis=IbisResource(backend="duckdb", duckdb_path=str(db)),
                sources=CSV_SOURCES,
            )
        },
    )
    assert result.success
    check = ibis.duckdb.connect(str(db))
    assert check.table("category_revenue").count().execute() == 3
    assert check.table("cleaned_events").count().execute() == 12


def test_full_pipeline_on_polars():
    """Same graph on the in-memory polars backend."""
    result = dg.materialize(
        ALL_ASSETS,
        resources={
            "io_manager": IbisIOManager(
                ibis=IbisResource(backend="polars"), sources=CSV_SOURCES
            )
        },
    )
    assert result.success
    mat = result.asset_materializations_for_node("daily_active_users")[0]
    assert mat.metadata["row_count"].value == 2
    assert mat.metadata["ibis_backend"].value == "polars"


def test_asset_checks_all_pass(tmp_path):
    """dbt-test parity: every asset check evaluates green, on the backend."""
    ibis_res = IbisResource(backend="duckdb", duckdb_path=str(tmp_path / "w.duckdb"))
    result = dg.materialize(
        [*ALL_ASSETS, *ALL_CHECKS],
        resources={
            "ibis": ibis_res,
            "io_manager": IbisIOManager(ibis=ibis_res, sources=CSV_SOURCES),
        },
    )
    assert result.success
    evals = result.get_asset_check_evaluations()
    assert len(evals) == len(ALL_CHECKS)
    assert all(e.passed for e in evals)


def test_pipeline_with_prod_style_parquet_sources(tmp_path):
    """Same asset code, different environment: parquet lake sources on duckdb
    (stands in for the prod deployment, which uses pyspark)."""
    lake = tmp_path / "lake"
    (lake / "landing").mkdir(parents=True)
    seed = ibis.duckdb.connect()
    seed.read_csv("data/raw_events.csv").to_parquet(str(lake / "landing/events"))
    seed.read_csv("data/raw_products.csv").to_parquet(
        str(lake / "landing/products")
    )

    db = tmp_path / "prod.duckdb"
    result = dg.materialize(
        ALL_ASSETS,
        resources={
            "io_manager": IbisIOManager(
                ibis=IbisResource(backend="duckdb", duckdb_path=str(db)),
                sources={
                    "events_csv": {
                        "format": "parquet",
                        "path": str(lake / "landing/events"),
                    },
                    "products_csv": {
                        "format": "parquet",
                        "path": str(lake / "landing/products"),
                    },
                },
            )
        },
    )
    assert result.success
    check = ibis.duckdb.connect(str(db))
    assert check.table("cleaned_events").count().execute() == 12
