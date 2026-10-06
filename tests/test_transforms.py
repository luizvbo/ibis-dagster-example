"""Run the pipeline transforms and I/O manager against every available Ibis
backend and assert identical results: the portability contract.
"""

import shutil

import dagster as dg
import ibis
import ibis.common.exceptions
import pytest
from ibis import _

from ibis_dagster_example import data, transforms
from ibis_dagster_example.assets import (
    SOURCE_SPECS,
    category_revenue,
    cleaned_events,
    daily_active_users,
    latest_event_per_user,
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
    "events": {"format": "csv", "path": "data/raw_events.csv"},
    "products": {"format": "csv", "path": "data/raw_products.csv"},
}


def _connect(backend: str, warehouse_dir: str | None = None):
    if backend == "duckdb":
        return ibis.duckdb.connect()
    if backend == "polars":
        return ibis.polars.connect()
    if backend == "pyspark":
        pytest.importorskip("pyspark")
        # skip only on the known environmental prerequisite (a JVM);
        # any other spark startup failure is a real defect, let it fail
        if shutil.which("java") is None:
            pytest.skip("pyspark installed but no java on PATH")
        from pyspark.sql import SparkSession

        # A fresh warehouse dir per run: spark's local catalog is
        # in-memory, so a fixed dir would leave orphaned table dirs
        # that break saveAsTable on the next run.
        session = (
            SparkSession.builder.master("local[2]")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .getOrCreate()
        )
        return ibis.pyspark.connect(session)
    raise ValueError(backend)


@pytest.fixture(params=["duckdb", "polars", "pyspark"])
def con(request, tmp_path_factory):
    return _connect(request.param, str(tmp_path_factory.mktemp("spark-warehouse")))


@pytest.fixture
def cleaned(con):
    # Seed through the same CSV the pipeline reads in prod: each backend's
    # own reader decodes the empty user_id as a real NULL. The memtable
    # path doesn't survive on pyspark (pandas 3's str dtype encodes
    # missing strings as NaN, which Spark stores as the literal 'NaN').
    raw = con.read_csv("data/raw_events.csv")
    return con.create_table(
        "cleaned_events", transforms.clean_events(raw), overwrite=True
    )


def test_clean_events_removes_duplicates_and_nulls(cleaned):
    assert cleaned.count().execute() == 12  # 14 raw - 1 dupe - 1 null user_id
    df = cleaned.execute()
    assert df["event_type"].str.islower().all()
    assert df["amount"].isna().sum() == 0


def test_daily_active_users(cleaned):
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
    df = transforms.category_revenue(cleaned, products).execute().set_index("category")
    assert df.loc["electronics", "n_orders"] == 3
    assert df.loc["electronics", "revenue"] == pytest.approx(198.80)
    assert df.loc["office", "revenue"] == pytest.approx(5.75)


def test_latest_event_per_user_windowed(con, cleaned):
    """Window functions run on SQL backends and fail loudly on polars:
    OperationNotDefinedError at translate time, before touching data."""
    expr = transforms.latest_event_per_user(cleaned)
    if con.name == "polars":
        with pytest.raises(ibis.common.exceptions.OperationNotDefinedError):
            expr.execute()
        return
    df = expr.execute()
    assert len(df) == 5
    assert set(df["event_id"]) == {7, 8, 10, 11, 12}


def test_latest_event_per_user_portable(cleaned):
    """The group_by+join rewrite produces the same result on every backend."""
    df = transforms.latest_event_per_user_portable(cleaned).execute()
    assert len(df) == 5
    assert set(df["event_id"]) == {7, 8, 10, 11, 12}


def test_latest_event_per_user_tied_timestamps(con):
    """Tied max timestamps per user: both implementations pick the same
    deterministic winner (largest event_id)."""
    tied = ibis.memtable(
        [
            {"event_id": 1, "user_id": "u1", "ts": "2024-01-02 10:00"},
            {"event_id": 3, "user_id": "u1", "ts": "2024-01-02 10:00"},
            {"event_id": 2, "user_id": "u1", "ts": "2024-01-02 10:00"},
            {"event_id": 4, "user_id": "u2", "ts": "2024-01-01 09:00"},
        ]
    ).mutate(ts=_.ts.cast("timestamp"))
    expected = {"u1": 3, "u2": 4}

    portable = transforms.latest_event_per_user_portable(tied)
    df = con.execute(portable)
    assert dict(zip(df["user_id"], df["event_id"])) == expected

    windowed = transforms.latest_event_per_user(tied)
    if con.name == "polars":
        with pytest.raises(ibis.common.exceptions.OperationNotDefinedError):
            con.execute(windowed)
        return
    df = con.execute(windowed)
    assert dict(zip(df["user_id"], df["event_id"])) == expected


def test_latest_event_per_user_implementations_equivalent(con, cleaned):
    """The windowed and portable implementations agree, not just with the
    expected IDs but with each other, on the full pipeline data."""
    if con.name == "polars":
        pytest.skip("windowed version unsupported on polars")
    import pandas as pd

    w = transforms.latest_event_per_user(cleaned).execute()
    p = transforms.latest_event_per_user_portable(cleaned).execute()
    by_id = lambda d: d.sort_values("event_id").reset_index(drop=True)
    pd.testing.assert_frame_equal(by_id(w), by_id(p), check_dtype=False)


def test_full_pipeline_on_duckdb(tmp_path):
    """Materialize the whole asset graph through the IO manager."""
    db = tmp_path / "warehouse.duckdb"
    result = dg.materialize(
        [*ALL_ASSETS, latest_event_per_user],
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
    assert check.table("latest_event_per_user").count().execute() == 5


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


def test_windowed_asset_fails_loudly_on_polars():
    """Materializing the windowed asset on polars fails with a named,
    translate-time error; the boundary is loud, not silent."""
    with pytest.raises(
        ibis.common.exceptions.OperationNotDefinedError, match="WindowFunction"
    ):
        dg.materialize(
            [*ALL_ASSETS, latest_event_per_user],
            resources={
                "io_manager": IbisIOManager(
                    ibis=IbisResource(backend="polars"), sources=CSV_SOURCES
                )
            },
        )


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


def test_unresolved_env_var_in_source_path_fails_clearly():
    """An unset ${VAR} in a source path fails at the config boundary with a
    clear error, not as a backend "file not found" on the literal path."""
    with pytest.raises(ValueError, match="Unresolved environment variable"):
        dg.materialize(
            [*SOURCE_SPECS, raw_events],
            resources={
                "io_manager": IbisIOManager(
                    ibis=IbisResource(backend="duckdb", duckdb_path=":memory:"),
                    sources={
                        "events": {
                            "format": "csv",
                            "path": "${IBIS_DEMO_UNSET_VAR}/e.csv",
                        }
                    },
                )
            },
        )


def test_pipeline_with_prod_style_parquet_sources(tmp_path):
    """Same asset code, different environment: parquet lake sources on duckdb
    (stands in for the prod deployment, which uses pyspark)."""
    lake = tmp_path / "lake"
    (lake / "landing").mkdir(parents=True)
    seed = ibis.duckdb.connect()
    seed.read_csv("data/raw_events.csv").to_parquet(str(lake / "landing/events"))
    seed.read_csv("data/raw_products.csv").to_parquet(str(lake / "landing/products"))

    db = tmp_path / "prod.duckdb"
    result = dg.materialize(
        ALL_ASSETS,
        resources={
            "io_manager": IbisIOManager(
                ibis=IbisResource(backend="duckdb", duckdb_path=str(db)),
                sources={
                    "events": {
                        "format": "parquet",
                        "path": str(lake / "landing/events"),
                    },
                    "products": {
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
