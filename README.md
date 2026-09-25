# ibis-dagster-example

An example [Dagster](https://dagster.io) project where all data logic is
written in [Ibis](https://ibis-project.org) expressions and runs on
**DuckDB**, **Polars**, or **PySpark**. Environment differences — which
engine executes, where inputs come from, where outputs land — are pure
resource configuration, following the standard Dagster patterns:

- **`ConfigurableIOManager`** owns all data I/O (`IbisIOManager`): assets are
  pure `ir.Table -> ir.Table` transforms; the manager persists outputs via
  `con.create_table` and resolves inputs via `con.table` / `con.read_*`.
- **`resources_by_deployment[DAGSTER_DEPLOYMENT_NAME]`** binds a
  differently-configured I/O manager + backend resource per environment —
  the documented Dagster equivalent of a per-env data catalog.

```
asset graph (dagster)          assets: pure ibis exprs (ir.Table -> ir.Table)
        │                                   │
        ▼                                   ▼
   IbisIOManager  ──load_input/handle_output──►  con.read_*/con.table/create_table
        │  ▲ sources={...} per deployment
        └── IbisResource.connect()  (duckdb | polars | pyspark)
```

## Layout

```
src/ibis_dagster_example/
├── resources.py      # IbisResource: engine connection (cached per run)
├── io_manager.py     # IbisIOManager: asset<->table IO + source config
├── transforms.py     # the actual logic: pure, backend-agnostic ibis exprs
├── assets.py         # dagster assets: ir.Table in, ir.Table out
├── checks.py         # asset checks (dbt-test equivalents) as ibis exprs
├── data.py           # memtable fixtures (unit tests only)
└── definitions.py    # defs + resources_by_deployment + daily schedule
data/                 # local CSV sources
tests/                # transform tests per backend + offline dialect compiles
```

Asset graph: `events_csv` + `products_csv` (external sources) →
`raw_events` / `raw_products` (bronze) → `cleaned_events` (silver) →
`daily_active_users` + `category_revenue` (gold).

## Quickstart

```bash
uv run dagster dev   # defaults to DAGSTER_DEPLOYMENT_NAME=local (duckdb)
```

Pick the deployment — engine and storage switch together:

```bash
DAGSTER_DEPLOYMENT_NAME=local   uv run dagster dev                    # duckdb + csv
DAGSTER_DEPLOYMENT_NAME=polars  uv run dagster dev                    # polars + csv
DAGSTER_DEPLOYMENT_NAME=prod    uv run --extra pyspark dagster dev    # pyspark + lake parquet (needs JDK)
```

Headless:

```bash
DAGSTER_DEPLOYMENT_NAME=polars uv run dagster asset materialize \
    -m ibis_dagster_example.definitions --select "*"
```

## How environments work

`definitions.py` holds `resources_by_deployment`: one configured
`IbisIOManager` (with a nested `IbisResource`) per deployment name.

```python
resources_by_deployment = {
    "local":  {"io_manager": IbisIOManager(ibis=IbisResource(backend="duckdb", ...), sources=CSV_SOURCES)},
    "polars": {"io_manager": IbisIOManager(ibis=IbisResource(backend="polars"),      sources=CSV_SOURCES)},
    "prod":   {"io_manager": IbisIOManager(ibis=IbisResource(backend="pyspark", ...), sources=LAKE_SOURCES)},
}
```

`DAGSTER_DEPLOYMENT_NAME` is set automatically by Dagster+ (`prod`,
`staging`, branch deployments); for `dagster dev` export it yourself.

**Sources** — `IbisIOManager.sources` maps upstream asset names to reads:

```python
{"events_csv": {"format": "csv",     "path": "data/raw_events.csv"}}
{"events_csv": {"format": "parquet", "path": "${DATA_LAKE}/landing/events/"}}
{"events_csv": {"format": "table",   "name": "landing.events"}}  # existing table
```

`format` ∈ `csv | parquet | json | delta | table`; `path` supports
`${ENV_VAR}` expansion. Upstream assets *not* in `sources` resolve to
`con.table(<asset name>)` — pipeline tables.

**Outputs** — every materialized asset becomes `con.create_table(name,
overwrite=True)` on the backend (`name` = asset key, optional `database`
field namespaces them). Each materialization records `ibis_backend`,
`row_count`, a `preview`, and the compiled SQL/plan as metadata.

## Data quality (the `dbt test` analog)

`checks.py` defines `@dg.asset_check`s — the Dagster equivalent of dbt
tests. They run as steps in the same run, right after the asset they cover,
and are written as Ibis expressions evaluated on the backend — so the
quality gates are portable across engines too:

| check                                        | dbt equivalent      |
| -------------------------------------------- | ------------------- |
| `raw_events_not_empty`                       | implicit "has rows" |
| `raw_products_unique_product_id`             | `unique`            |
| `cleaned_events_no_null_user_ids` (blocking) | `not_null`          |
| `cleaned_events_unique_event_ids`            | `unique`            |
| `cleaned_events_accepted_event_types`        | `accepted_values`   |
| `cleaned_events_products_referential`        | `relationships`     |
| `daily_active_users_consistent_counts`       | custom/singular     |
| `category_revenue_non_negative`              | custom/singular     |

`blocking=True` on the not-null check reproduces `dbt build` semantics:
downstream assets don't materialize if it fails.

## Scheduling & dialect proof

- `daily_schedule` (`definitions.py`) runs `ibis_etl_job` daily at 06:00 —
  the `dbt run` cron analog (stopped by default; toggle in the UI).
- `tests/test_compilation.py` compiles every transform via
  `ibis.<backend>.compile()` — **offline, no engine needed** — for
  `duckdb`, `pyspark`, and `bigquery`, asserting dialect fingerprints
  (`DATE_TRUNC` vs `TIMESTAMP_TRUNC`, quoting styles). It's both the
  portability demo and a CI guardrail for backends you can't run locally.

Because both resources are `ConfigurableResource`s, every field can also be
overridden per-run in the Dagster Launchpad.

## Engine notes

- **duckdb** — persists to `IBIS_DUCKDB_PATH` (default `warehouse.duckdb`).
- **polars** — in-process/in-memory; tables live only for the run, so each
  run re-ingests from sources. Steps share one connection (in-process
  executor + cached connection in `IbisResource`).
- **pyspark** — lazily imported inside `connect()`; needs a JDK and the
  `pyspark` extra (`uv sync --extra pyspark`). `SPARK_MASTER` /
  `SPARK_WAREHOUSE_DIR` env vars configure it; point `SPARK_MASTER` at a
  cluster or Spark Connect URL.

## Tests

```bash
uv run pytest   # duckdb + polars; pyspark auto-skips if not installed
```

Covers the transforms on every backend plus full `dg.materialize` runs of
the asset graph through the I/O manager — including a "prod-style" variant
with parquet sources.
