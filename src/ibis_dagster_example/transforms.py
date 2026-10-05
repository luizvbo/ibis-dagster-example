"""Backend-agnostic pipeline logic, written as pure Ibis expressions.

Each function takes Ibis table expressions and returns a new table
expression. Nothing here knows which engine will run it — DuckDB compiles
it to DuckDB SQL, PySpark to Spark SQL, Polars to a Polars plan.
Only portable, widely-supported operations are used.
"""

import ibis
from ibis import _
from ibis.expr import types as ir


def clean_events(raw_events: ir.Table) -> ir.Table:
    """Normalize raw clickstream events: fix types/casing, fill nulls, dedupe.

    The casts are portable and no-ops when the column is already typed —
    they matter because CSV readers type `ts`/`amount` differently per
    backend (polars reads ts as string, spark CSV reads all strings).
    """
    normalized = raw_events.mutate(
        ts=_.ts.cast("timestamp"),
        amount=_.amount.cast("float64"),
        event_type=_.event_type.lower().strip(),
    )
    return (
        normalized.mutate(amount=_.amount.fill_null(0.0), date=_.ts.truncate("D"))
        .filter(_.event_id.notnull(), _.user_id.notnull())
        .distinct()
    )


def daily_active_users(cleaned_events: ir.Table) -> ir.Table:
    """Per-day event volume, distinct active users, and purchase counts."""
    return (
        cleaned_events.group_by("date")
        .agg(
            n_events=_.count(),
            n_active_users=_.user_id.nunique(),
            n_purchases=ibis.ifelse(_.event_type == "purchase", 1, 0).sum(),  # ty: ignore[unresolved-attribute] — ifelse() is typed as generic Value
        )
        .order_by("date")
    )


def category_revenue(cleaned_events: ir.Table, products: ir.Table) -> ir.Table:
    """Revenue per product category, from purchases joined to products."""
    purchases = cleaned_events.filter(_.event_type == "purchase")
    return (
        purchases.left_join(products, "product_id")
        .group_by("category")
        .agg(
            n_orders=_.count(),
            revenue=_.amount.sum(),
            avg_order_value=_.amount.mean(),
        )
        .order_by(_.revenue.desc())
    )


def latest_event_per_user(events: ir.Table) -> ir.Table:
    """Most recent event per user, via a row_number window.

    NOT portable: the ibis polars backend has no WindowFunction translation
    (polars's native .over() isn't wired up). Compiles/runs on duckdb,
    pyspark, bigquery — raises OperationNotDefinedError on polars at
    compile time. See latest_event_per_user_portable for the fallback.
    """
    return (
        events.mutate(
            rn=ibis.row_number().over(
                ibis.window(group_by="user_id", order_by=_.ts.desc())
            )
        )
        .filter(_.rn == 0)  # ibis's row_number() is zero-based
        .drop("rn")
    )


def latest_event_per_user_portable(events: ir.Table) -> ir.Table:
    """Same result, no window functions — runs on every backend
    (group_by + join instead of row_number)."""
    latest = events.group_by("user_id").agg(latest_ts=_.ts.max())
    return (
        events.inner_join(latest, "user_id")
        .filter(_.ts == _.latest_ts)
        .drop("latest_ts")
    )
