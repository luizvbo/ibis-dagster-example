# ibis-dagster-example

An example [Dagster](https://dagster.io) project where all data logic is
written in [Ibis](https://ibis-project.org) expressions. The same pipeline
runs on **DuckDB**, **Polars**, or **PySpark** — the engine is pure
configuration, never code.

```
assets (dagster) ──► pure ibis expressions (transforms.py)
                          │
                IbisResource.connect()          ◄── config: backend
                          │
              ┌───────────┼────────────┐
          duckdb        polars      pyspark
         (SQL dialect) (LazyFrame)  (Spark SQL)
```

## Layout

```
src/ibis_dagster_example/
├── resources.py      # IbisResource: ConfigurableResource → ibis connection
├── data.py           # seed data as ibis.memtables (portable across backends)
├── transforms.py     # the actual logic: pure, backend-agnostic ibis exprs
├── assets.py         # dagster assets: seed → clean → aggregate (+ lineage)
└── definitions.py    # defs: assets, job, resource wired from env vars
tests/
└── test_transforms.py  # runs the same asserts on every backend
```

Asset graph: `raw_events` + `raw_products` (bronze, seeded) →
`cleaned_events` (silver) → `daily_active_users` + `category_revenue` (gold).

## Quickstart

```bash
uv run dagster dev          # default: duckdb backend → http://localhost:3000
# or, with pip: pip install -e ".[dev]" && dagster dev
```

Pick the engine with `IBIS_BACKEND`:

```bash
IBIS_BACKEND=duckdb   uv run dagster dev   # writes warehouse.duckdb
IBIS_BACKEND=polars   uv run dagster dev   # fully in-memory
IBIS_BACKEND=pyspark  uv run --extra pyspark dagster dev  # needs a JDK
```

Or materialize headless without the UI:

```bash
IBIS_BACKEND=polars uv run dagster asset materialize \
    -m ibis_dagster_example.definitions --select "*"
```

Each materialization reports `ibis_backend`, `row_count`, a `preview` table,
and the compiled SQL/plan as asset metadata — open a run in the UI to see the
same expression compiled to DuckDB SQL vs Spark SQL vs a Polars plan.

## How backend switching works

`IbisResource` (in `resources.py`) is a `ConfigurableResource` whose
`backend` field selects the engine. `definitions.py` populates it from env
vars:

| Env var               | Default             | Used by    |
| --------------------- | ------------------- | ---------- |
| `IBIS_BACKEND`        | `duckdb`            | all        |
| `IBIS_DUCKDB_PATH`    | `warehouse.duckdb`  | duckdb     |
| `SPARK_MASTER`        | `local[*]`          | pyspark    |
| `SPARK_WAREHOUSE_DIR` | `./spark-warehouse` | pyspark    |

Because it is a configurable resource you can also override `backend` (or any
field) per-run in the Dagster Launchpad, or from run config in a
`RunRequest`. For Dagster+ deployments, map `DAGSTER_DEPLOYMENT_NAME` to
different `IbisResource` instances in `definitions.py`.

## Backend notes

- **duckdb** — persists tables to `IBIS_DUCKDB_PATH`. Use `:memory:` for
  ephemeral runs.
- **polars** — in-process and in-memory: tables live only for the duration of
  the run, so every materialization re-seeds the bronze assets first. Nothing
  is written to disk.
- **pyspark** — lazily imports `pyspark` inside `IbisResource.connect()`, so
  the code location loads fine without it. Running it requires a JDK and
  `pip install -e ".[pyspark]"` (or `uv sync --extra pyspark`). Point
  `SPARK_MASTER` at a cluster or a Spark Connect URL.

## Tests

```bash
uv run pytest          # duckdb + polars; pyspark auto-skips if not installed
```

The test suite executes the same transform functions on every backend and
asserts identical results — that's the portability contract.
