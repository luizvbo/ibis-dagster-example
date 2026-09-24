"""Dagster assets. Thin orchestration layer: pull a table from the Ibis
backend, run a pure-Ibis transform, persist the result as a table on the
same backend, and attach observability metadata.
"""

import dagster as dg
from ibis.backends import BaseBackend
from ibis.expr import types as ir

from . import data, transforms
from .resources import IbisResource


def _persist(
    context: dg.AssetExecutionContext,
    con: BaseBackend,
    name: str,
    expr: ir.Table,
) -> ir.Table:
    """Materialize `expr` as table `name` on the backend + emit metadata."""
    created = con.create_table(name, expr, overwrite=True)

    metadata = {
        "ibis_backend": con.name,
        "row_count": int(created.count().execute()),
        "preview": dg.MetadataValue.md(
            created.head(10).execute().to_markdown(index=False)
        ),
    }
    # SQL backends can show the compiled SQL — a nice way to demo that Ibis
    # is compiling the same expression to different dialects/engines.
    try:
        metadata["compiled"] = dg.MetadataValue.md(
            f"```\n{con.compile(expr)}\n```"
        )
    except Exception:
        pass
    context.add_output_metadata(metadata)
    return created


@dg.asset(group_name="bronze", kinds={"ibis"})
def raw_events(context: dg.AssetExecutionContext, ibis: IbisResource) -> ir.Table:
    """Seed the raw clickstream events table (from an Ibis memtable)."""
    con = ibis.connect()
    return _persist(context, con, "raw_events", data.raw_events)


@dg.asset(group_name="bronze", kinds={"ibis"})
def raw_products(
    context: dg.AssetExecutionContext, ibis: IbisResource
) -> ir.Table:
    """Seed the products dimension table (from an Ibis memtable)."""
    con = ibis.connect()
    return _persist(context, con, "raw_products", data.raw_products)


@dg.asset(group_name="silver", kinds={"ibis"}, deps=[raw_events])
def cleaned_events(
    context: dg.AssetExecutionContext, ibis: IbisResource
) -> ir.Table:
    """Cleaned events: normalized event types, no nulls, no duplicates."""
    con = ibis.connect()
    return _persist(
        context, con, "cleaned_events", transforms.clean_events(con.table("raw_events"))
    )


@dg.asset(group_name="gold", kinds={"ibis"}, deps=[cleaned_events])
def daily_active_users(
    context: dg.AssetExecutionContext, ibis: IbisResource
) -> ir.Table:
    """Daily aggregates: events, active users, purchases."""
    con = ibis.connect()
    return _persist(
        context,
        con,
        "daily_active_users",
        transforms.daily_active_users(con.table("cleaned_events")),
    )


@dg.asset(group_name="gold", kinds={"ibis"}, deps=[cleaned_events, raw_products])
def category_revenue(
    context: dg.AssetExecutionContext, ibis: IbisResource
) -> ir.Table:
    """Revenue per product category (purchase events joined to products)."""
    con = ibis.connect()
    return _persist(
        context,
        con,
        "category_revenue",
        transforms.category_revenue(
            con.table("cleaned_events"), con.table("raw_products")
        ),
    )
