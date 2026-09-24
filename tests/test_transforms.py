"""Run the pipeline transforms against every available Ibis backend and
assert identical results — this is the portability contract of the project.
"""

import ibis
import pytest

from ibis_dagster_example import data, transforms


def _connect(backend: str):
    if backend == "duckdb":
        return ibis.duckdb.connect()
    if backend == "polars":
        return ibis.polars.connect()
    if backend == "pyspark":
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        session = (
            SparkSession.builder.master("local[2]")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
            .getOrCreate()
        )
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
