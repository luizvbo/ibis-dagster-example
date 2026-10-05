"""Seed a local parquet "lake" from the CSV sources.

The prod deployment reads ${DATA_LAKE}/landing/{events,products}/ as
parquet (LAKE_SOURCES in definitions.py). This script writes that layout
from data/*.csv so `DAGSTER_DEPLOYMENT_NAME=prod` runs locally:

    uv run python scripts/seed_lake.py             # -> data/lake/
    uv run python scripts/seed_lake.py /tmp/lake   # -> /tmp/lake/
"""

import sys
from pathlib import Path

import ibis

SOURCES = {
    "events": "data/raw_events.csv",
    "products": "data/raw_products.csv",
}


def main() -> None:
    lake = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/lake")
    con = ibis.duckdb.connect()
    for name, csv in SOURCES.items():
        out = lake / "landing" / name / "part-0.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        con.read_csv(csv).to_parquet(str(out))
        print(f"wrote {out}")


if __name__ == "__main__":
    main()
