"""Dagster assets as pure Ibis transforms.

Assets take and return `ir.Table` expressions: no engine, no storage
details. The `IbisIOManager` persists outputs and resolves inputs;
which backend runs the expressions and where sources point is entirely
deployment configuration.
"""

import dagster as dg
from ibis.expr import types as ir

from . import transforms

# External (non-materializable) upstream sources. The IO manager maps each
# name to a physical read via its `sources` config (per deployment).
SOURCE_SPECS = [
    # kind is storage-neutral: the same sources are csv locally, parquet in prod
    dg.AssetSpec("events_csv", group_name="sources", kinds={"external"}),
    dg.AssetSpec("products_csv", group_name="sources", kinds={"external"}),
]


@dg.asset(group_name="bronze", kinds={"ibis"})
def raw_events(events_csv: ir.Table) -> ir.Table:
    """Land the raw clickstream events source as a managed table."""
    return events_csv


@dg.asset(group_name="bronze", kinds={"ibis"})
def raw_products(products_csv: ir.Table) -> ir.Table:
    """Land the products dimension source as a managed table."""
    return products_csv


@dg.asset(group_name="silver", kinds={"ibis"})
def cleaned_events(raw_events: ir.Table) -> ir.Table:
    """Cleaned events: normalized types/casing, no nulls, no duplicates."""
    return transforms.clean_events(raw_events)


@dg.asset(group_name="gold", kinds={"ibis"})
def daily_active_users(cleaned_events: ir.Table) -> ir.Table:
    """Daily aggregates: events, active users, purchases."""
    return transforms.daily_active_users(cleaned_events)


@dg.asset(group_name="gold", kinds={"ibis"})
def category_revenue(cleaned_events: ir.Table, raw_products: ir.Table) -> ir.Table:
    """Revenue per product category (purchase events joined to products)."""
    return transforms.category_revenue(cleaned_events, raw_products)


@dg.asset(group_name="gold", kinds={"ibis"})
def latest_event_per_user(cleaned_events: ir.Table) -> ir.Table:
    """Most recent event per user, via window functions (SQL backends only).

    Intentionally non-portable: this materializes on duckdb/pyspark and
    fails loudly at compile time on polars (OperationNotDefinedError:
    windows aren't implemented in ibis's polars backend). For a portable
    equivalent (same result incl. tie-breaks) see
    transforms.latest_event_per_user_portable.
    """
    return transforms.latest_event_per_user(cleaned_events)
