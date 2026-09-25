"""Data quality checks — the dbt `test` analog, as Dagster asset checks.

Each check is a portable Ibis expression executed on the run's backend, so
the quality gates are engine-agnostic too. `blocking=True` gates downstream
materializations on the check, like `dbt build`'s test-then-run semantics.
"""

import dagster as dg
from ibis import _

from .assets import (
    category_revenue,
    cleaned_events,
    daily_active_users,
    raw_events,
    raw_products,
)
from .resources import IbisResource


def _violations(ibis: IbisResource, table: str, predicate) -> int:
    """Count rows in `table` matching `predicate` on the run's backend."""
    con = ibis.connect()
    return int(con.table(table).filter(predicate).count().execute())


@dg.asset_check(asset=raw_events, description="raw_events has rows")
def raw_events_not_empty(ibis: IbisResource) -> dg.AssetCheckResult:
    n = int(ibis.connect().table("raw_events").count().execute())
    return dg.AssetCheckResult(passed=n > 0, metadata={"row_count": n})


@dg.asset_check(
    asset=raw_products, description="product_id is unique in raw_products"
)
def raw_products_unique_product_id(ibis: IbisResource) -> dg.AssetCheckResult:
    t = ibis.connect().table("raw_products")
    dupes = int(t.count().execute() - t.product_id.nunique().execute())
    return dg.AssetCheckResult(passed=dupes == 0, metadata={"duplicates": dupes})


@dg.asset_check(
    asset=cleaned_events,
    blocking=True,  # dbt build semantics: don't materialize downstream on failure
    description="user_id has no nulls",
)
def cleaned_events_no_null_user_ids(ibis: IbisResource) -> dg.AssetCheckResult:
    n = _violations(ibis, "cleaned_events", _.user_id.isnull())
    return dg.AssetCheckResult(passed=n == 0, metadata={"null_user_ids": n})


@dg.asset_check(asset=cleaned_events, description="event_id is unique")
def cleaned_events_unique_event_ids(ibis: IbisResource) -> dg.AssetCheckResult:
    t = ibis.connect().table("cleaned_events")
    dupes = int(t.count().execute() - t.event_id.nunique().execute())
    return dg.AssetCheckResult(passed=dupes == 0, metadata={"duplicates": dupes})


@dg.asset_check(
    asset=cleaned_events,
    description="event_type in {page_view, add_to_cart, purchase}",
)
def cleaned_events_accepted_event_types(
    ibis: IbisResource,
) -> dg.AssetCheckResult:
    n = _violations(
        ibis,
        "cleaned_events",
        ~_.event_type.isin(["page_view", "add_to_cart", "purchase"]),
    )
    return dg.AssetCheckResult(passed=n == 0, metadata={"invalid_types": n})


@dg.asset_check(
    asset=cleaned_events,
    description="every product_id exists in raw_products (dbt relationships)",
)
def cleaned_events_products_referential(
    ibis: IbisResource,
) -> dg.AssetCheckResult:
    con = ibis.connect()
    events = con.table("cleaned_events")
    products = con.table("raw_products")
    n = int(events.anti_join(products, "product_id").count().execute())
    return dg.AssetCheckResult(passed=n == 0, metadata={"orphan_events": n})


@dg.asset_check(
    asset=daily_active_users, description="n_purchases never exceeds n_events"
)
def daily_active_users_consistent_counts(
    ibis: IbisResource,
) -> dg.AssetCheckResult:
    n = _violations(ibis, "daily_active_users", _.n_purchases > _.n_events)
    return dg.AssetCheckResult(passed=n == 0, metadata={"bad_days": n})


@dg.asset_check(
    asset=category_revenue, description="revenue and n_orders are non-negative"
)
def category_revenue_non_negative(ibis: IbisResource) -> dg.AssetCheckResult:
    n = _violations(
        ibis, "category_revenue", (_.revenue < 0) | (_.n_orders <= 0)
    )
    return dg.AssetCheckResult(passed=n == 0, metadata={"violations": n})


ALL_CHECKS = [
    raw_events_not_empty,
    raw_products_unique_product_id,
    cleaned_events_no_null_user_ids,
    cleaned_events_unique_event_ids,
    cleaned_events_accepted_event_types,
    cleaned_events_products_referential,
    daily_active_users_consistent_counts,
    category_revenue_non_negative,
]
