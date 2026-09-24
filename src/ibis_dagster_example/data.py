"""Seed data for the example pipeline, expressed as Ibis memtables.

Memtables are backend-agnostic: any Ibis backend can ingest them, so the
same seeding logic works on DuckDB, Polars, PySpark, or anything else.
"""

from datetime import datetime

import ibis

raw_events = ibis.memtable(
    [
        # event_id, user_id, event_type, product_id, amount, ts
        (1, "u001", "PAGE_VIEW", "p-100", None, datetime(2026, 9, 1, 9, 1)),
        (2, "u001", "ADD_TO_CART", "p-200", None, datetime(2026, 9, 1, 9, 3)),
        (3, "u001", "PURCHASE", "p-200", 49.90, datetime(2026, 9, 1, 9, 4)),
        (4, "u002", "PAGE_VIEW", "p-300", None, datetime(2026, 9, 1, 10, 15)),
        (5, "u002", "PURCHASE", "p-300", 19.95, datetime(2026, 9, 1, 10, 22)),
        (6, "u003", "page_view", "p-100", None, datetime(2026, 9, 1, 11, 0)),
        (7, "u003", "purchase", "p-100", 99.00, datetime(2026, 9, 1, 11, 5)),
        (7, "u003", "purchase", "p-100", 99.00, datetime(2026, 9, 1, 11, 5)),
        (8, "u004", "PAGE_VIEW", "p-400", None, datetime(2026, 9, 2, 8, 30)),
        (9, "u002", "ADD_TO_CART", "p-200", None, datetime(2026, 9, 2, 9, 41)),
        (10, "u002", "PURCHASE", "p-200", 49.90, datetime(2026, 9, 2, 9, 55)),
        (11, "u005", "PURCHASE", "p-500", 5.75, datetime(2026, 9, 2, 12, 10)),
        (12, "u001", "PAGE_VIEW", "p-300", None, datetime(2026, 9, 2, 14, 0)),
        (13, None, "PAGE_VIEW", "p-100", None, datetime(2026, 9, 2, 15, 33)),
    ],
    columns=["event_id", "user_id", "event_type", "product_id", "amount", "ts"],
)

raw_products = ibis.memtable(
    [
        ("p-100", "Mechanical Keyboard", "electronics"),
        ("p-200", "Ergonomic Mouse", "electronics"),
        ("p-300", "Coffee Mug", "kitchen"),
        ("p-400", "Desk Lamp", "office"),
        ("p-500", "Notebook", "office"),
    ],
    columns=["product_id", "product_name", "category"],
)
