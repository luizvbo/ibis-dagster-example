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
    """Normalize raw clickstream events: fix casing, fill nulls, dedupe."""
    return (
        raw_events.mutate(
            event_type=_.event_type.lower().strip(),
            amount=_.amount.fill_null(0.0),
            date=_.ts.truncate("D"),
        )
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
            n_purchases=ibis.ifelse(_.event_type == "purchase", 1, 0).sum(),
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
