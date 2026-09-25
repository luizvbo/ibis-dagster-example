"""Offline dialect-compilation checks.

`ibis.<backend>.compile()` instantiates the backend's compiler without
connecting, so we can prove the same expressions compile for engines we
can't run here (bigquery, pyspark — no credentials or JVM required).
This is the portability claim made executable in CI: an op a target
dialect can't express fails here, before any deployment.
"""

import ibis
import pytest

from ibis_dagster_example import transforms

RAW_EVENTS = ibis.table(
    {
        "event_id": "int64",
        "user_id": "string",
        "event_type": "string",
        "product_id": "string",
        "amount": "float64",
        "ts": "timestamp",
    },
    name="raw_events",
)
PRODUCTS = ibis.table(
    {"product_id": "string", "product_name": "string", "category": "string"},
    name="raw_products",
)

CLEANED = transforms.clean_events(RAW_EVENTS)

TRANSFORMS = {
    "cleaned_events": CLEANED,
    "daily_active_users": transforms.daily_active_users(CLEANED),
    "category_revenue": transforms.category_revenue(CLEANED, PRODUCTS),
}

# dialect fingerprints that differ per backend — the demo's "aha"
DIALECT_FINGERPRINTS = {
    "duckdb": ["date_trunc", '"'],
    "pyspark": ["date_trunc", "`"],
    "bigquery": ["timestamp_trunc", "`"],
}


@pytest.mark.parametrize("backend", ["duckdb", "pyspark", "bigquery"])
def test_transforms_compile_to_dialect(backend):
    compile_fn = getattr(ibis, backend, None)
    if compile_fn is None or not hasattr(compile_fn, "compile"):
        pytest.skip(f"ibis backend {backend!r} not installed")
    try:
        sql_by_name = {
            name: getattr(ibis, backend).compile(expr)
            for name, expr in TRANSFORMS.items()
        }
    except ImportError:
        pytest.skip(f"ibis backend {backend!r} not installed")

    for name, sql in sql_by_name.items():
        assert isinstance(sql, str) and sql.strip(), (backend, name)
        assert "raw_events" in sql.lower()

    joined = " ".join(sql_by_name.values()).lower()
    for fingerprint in DIALECT_FINGERPRINTS[backend]:
        assert fingerprint in joined, f"{backend} SQL missing {fingerprint!r}"
