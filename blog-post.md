# Portable data pipelines with Dagster + Ibis: making migrations less painfull

<!-- IMAGE: hero/cover image — something evoking "one codebase, many engines" (e.g., a fan-out diagram: Python code → duckdb / polars / spark / bigquery logos) -->

There are a lot of companies moving from in-house hosting to cloud providers.
And with that comes migrations...

For data pipelines, particularly, there's a question of whether the pipeline
should be also migrated to use the data warehouse solution provided by the
cloud provider. For instance, since our team is migrating to GCP, we had a
discussion on whether we should migrate our pipelines running on PySpark to
BigQuery or not. We considered multiple dimensions, like cost, effort, and
maintainability.

This decision involves rewriting a substantial codebase, like Spark DataFrame
calls, `spark.sql(...)` strings, `.toPandas()` escapes.

There is a lot of discussion in the European Union about moving away from US
Companies (see [this article from
Reuters](https://www.reuters.com/business/eu-targets-big-tech-dependence-with-made-in-europe-drive-2026-06-03/)
for instance). There is no guarantee that our migration to GCP will be the
last, and with that, no guarantee that we will have to rewrite our code again.

That forced a question: _is there a way to write data transformations so that
switching the execution engine is a configuration change, not a rewrite?_

This post is about a small proof-of-concept repo I built to try to answer this
question. While researching about alternatives, we found **[Ibis](https://ibis-project.org)**, a potential
solution to minimize the migration effort. It is a dataframe-style expression
API that compiles the same code to DuckDB SQL, Spark SQL, BigQuery SQL, Polars,
and many other backends, totaling 20+ backends.

Ibis, minimizes the migration effort, but it's just a "translation layer". We
needed structure and orchestration, which can be provided by a single tool,
instead of using Airflow with an internal framework for structure.
**[Dagster](https://dagster.io)** can solve that with many features provided
out-of-the-box. It is an orchestration with software-defined assets, giving us
the structure dbt users are used to: models, lineage, tests, schedules.

The thesis: **port your logic to Ibis once, and engine migrations become config
diffs.** Not "migrate to BigQuery for free this time" (the port to
Ibis is real work), but "this is the last engine migration that ever requires a
rewrite."

Repo: <!-- TODO: link to the repo -->

## The demo

A deliberately ordinary bronze → silver → gold pipeline:

```
events_csv + products_csv            (external sources: csv locally, parquet/tables in production)
       │
       ▼
raw_events, raw_products             (bronze: land sources into managed tables)
       │
       ▼
cleaned_events                       (silver: normalize types, trim/lower, dedupe, validate)
       │
       ├──► daily_active_users       (gold: group_by + agg)
       ├──► category_revenue         (gold: join + group_by + agg)
       └──► latest_event_per_user    (gold: row_number window, deliberately not portable*)
```

\* we'll come back to that asterisk. It's the most interesting part.

<!-- IMAGE: screenshot of the Dagster asset graph (lineage view) showing the external source nodes → bronze → silver → gold, with the three asset groups colored -->

The entire engine/env selection is two environment variables:

```bash
DAGSTER_DEPLOYMENT_NAME=local  dagster dev   # duckdb + local CSVs
DAGSTER_DEPLOYMENT_NAME=polars dagster dev   # polars + same CSVs
DAGSTER_DEPLOYMENT_NAME=prod   dagster dev   # pyspark + parquet "lake" sources
```

Same code. Same asset graph. Same checks. Different engine and different storage, selected by deployment config, the documented Dagster pattern (`resources_by_deployment`, keyed on `DAGSTER_DEPLOYMENT_NAME`, which Dagster+ sets automatically).

## The three pieces

### 1. Transforms are pure Ibis expressions

```python
# transforms.py: no imports from any engine, just ibis
def clean_events(raw_events: ir.Table) -> ir.Table:
    normalized = raw_events.mutate(
        ts=_.ts.cast("timestamp"),
        amount=_.amount.cast("float64"),
        event_type=_.event_type.lower().strip(),
    )
    return (
        normalized.mutate(
            amount=_.amount.fill_null(0.0), date=_.ts.truncate("D")
        )
        .filter(_.event_id.notnull(), _.user_id.notnull())
        .distinct()
    )
```

No `duckdb.`, no `spark.`, no `pl.` anywhere. These functions are pure `Table -> Table` expressions. They don't even know which backend they'll run on until Dagster binds a connection at runtime.

### 2. An IO manager owns all data I/O

This is the Dagster-native answer to dbt's "where does this model land" or Kedro's catalog. Assets declare inputs/outputs as `ir.Table`; `IbisIOManager` (a `ConfigurableIOManager`) handles persistence:

- `load_input` → `con.table(name)` for pipeline tables, or `con.read_csv/read_parquet/...` for external sources configured per deployment
- `handle_output` → `con.create_table(asset_name, expr, overwrite=True)` plus metadata (row count, preview, **the compiled SQL**)

So "the same pipeline, but sources are parquet in a lake in prod" is literally just:

```python
# resources_by_deployment: same logical source, different physical read
"local": IbisIOManager(
    sources={"events_csv": {"format": "csv",     "path": "data/raw_events.csv"}}, ...),
"prod":  IbisIOManager(
    sources={"events_csv": {"format": "parquet", "path": "${DATA_LAKE}/landing/events/"}}, ...),
```

### 3. Quality gates are portable too

The thing dbt users ask about first is `dbt test`. Dagster's `@asset_check` is the direct analog, and because _the checks themselves are Ibis expressions_, they're portable as well:

```python
@dg.asset_check(asset=cleaned_events, blocking=True)
def cleaned_events_no_null_user_ids(ibis: IbisResource) -> dg.AssetCheckResult:
    t = ibis.connect().table("cleaned_events")
    n_bad = t.filter(_.user_id.isnull()).count().execute()
    return dg.AssetCheckResult(passed=n_bad == 0, metadata={"null_user_ids": n_bad})
```

<!-- IMAGE: screenshot of the Checks tab / a run showing the 8 asset checks green -->

We implemented the usual dbt generic tests `not_null`, `unique`, `accepted_values`, `relationships` (an anti-join), plus a couple of custom checks. Marking the not-null check `blocking=True` reproduces `dbt build` semantics: if it fails, downstream assets don't materialize.

And since Dagster is an orchestrator we can set a `daily_schedule` running the whole job at 06:00

## The "aha": one expression, three dialects

This is the part that sells it. The same `daily_active_users(clean_events(raw_events))` expression is compiled offline. **No engine, no credentials, no JVM**:

```sql
-- duckdb
... DATE_TRUNC('DAY', "t0"."ts") AS "date" ... TRIM(LOWER("t0"."event_type"), ' ')
-- pyspark
... DATE_TRUNC('day', `t0`.`ts`) AS `date` ... TRIM(' ' FROM LOWER(`t0`.`event_type`))
-- bigquery
... TIMESTAMP_TRUNC(`t0`.`ts`, DAY) AS `date` ... TRIM(LOWER(`t0`.`event_type`), ' ')
```

`DATE_TRUNC` vs `TIMESTAMP_TRUNC`, double quotes vs backticks, `TRIM(x)` vs `TRIM(' ' FROM x)`: all the dialect trivia you never want to hand-maintain, generated from one expression.

<!-- IMAGE: optional — screenshot of a materialization's metadata tab showing the compiled SQL recorded on the run -->

This isn't just a demo trick. `ibis.<backend>.compile()` works without connecting, so a `pytest` file that compiles every transform against every target dialect is a **CI guardrail**. If someone adds an operation your production engine can't express, it fails before deployment, not after.

## The honest part: portability has edges

Here's where we keep the demo honest, because the claim above deserves an asterisk: **"backend-agnostic" means portable across the operations each backend can express, not that every expression runs everywhere.**

The repo deliberately includes `latest_event_per_user`, built on a window function:

```python
events.mutate(rn=ibis.row_number().over(
    ibis.window(group_by="user_id", order_by=_.ts.desc()))).filter(_.rn == 0)
```

Ibis's Polars backend has _no_ window-function translation (Polars natively has `.over()`, but Ibis doesn't map to it yet). When we materialize the graph on Polars we get:

```
latest_event_per_user — FAILED:
ibis.common.exceptions.OperationNotDefinedError:
    No translation rule for WindowFunction
```

<!-- IMAGE: screenshot of the Dagster run on the polars deployment: all steps green except latest_event_per_user red, with the OperationNotDefinedError visible in the logs pane -->

Two things make this acceptable, even good:

1. **It fails loudly and early.** The error fires at translate time, before any data is processed, names the exact operation, and is deterministic. No silent wrong answers at 3:00 AM.
2. **The mitigations are boring** (in a good way):
   - _Rewrite portably_: "latest per group" is `group_by` + `join`
     (`latest_event_per_user_portable` in the repo). Clunkier, but runs
     everywhere.
   - _Localize a backend branch_: `ibis.get_backend(t).name` inside a transform
     tells you which engine you're bound to. It's ugly, but contained to one
     function
   - _Engine-scope the asset_: accept that a given deployment can't materialize it

A more subtle gotcha we found the hard way: `ibis.row_number()` is **zero-based**. It compiles to `ROW_NUMBER() - 1`. `rn == 1` silently gives you the _second_-latest row. Abstraction means portable _syntax_; you still need to learn the portable _semantics_.

Before betting a codebase on a portability claim, check Ibis's [per-backend operations support matrix](https://ibis-project.org/backends/support/matrix).

## Okay, but why not just dbt?

Fair question. dbt solves a big chunk of this. The honest comparison:

|                                       | dbt (+ adapters)                        | Dagster + Ibis                   |
| ------------------------------------- | --------------------------------------- | -------------------------------- |
| Transform language                    | SQL                                     | Python expressions → SQL/plans   |
| Engine portability                    | per-adapter SQL macros                  | one expression → many dialects   |
| Non-SQL engines                       | no                                      | yes (Polars, DataFusion…)        |
| Python-native logic (ML, APIs, files) | bolted on                               | first-class (the same graph)     |
| Tests                                 | `dbt test`                              | `@asset_check`                   |
| Scheduling                            | needs dbt Cloud / external orchestrator | native (schedules, sensors)      |
| Incremental models                    | `dbt run` incremental                   | partitions + automation policies |
| Ecosystem maturity                    | bigger                                  | younger                          |

If your transforms are pure SQL and your targets are SQL warehouses, dbt is simpler and battle-tested and you should consider it. The Dagster + Ibis combo wins when: your logic outgrows SQL, you want one lineage graph spanning tables _and_ non-table work, or (our case) you're tired of paying a rewrite tax on every engine migration.

## Caveats worth stating

- **The port isn't free.** Existing `spark.sql`/DataFrame/UDF code must be rewritten as Ibis expressions. The payoff is that it's potentially the last port.
- **Semantics differ subtly per backend** (see the zero-based `row_number` story). Also type inference on file reads (we added portable casts to absorb that).
- **Performance isn't portable.** One expression compiles to all engines, but partitioning, clustering, broadcast hints, etc. are still per-engine work.
- **Some plumbing is real**: e.g., Polars tables live per-connection and DuckDB files allow one writer, so the demo uses the in-process executor and a cached connection.

## Takeaways

- **Ibis gives you a portability contract for transformation logic**: same expressions, engine as config. Parametrized tests that run every transform on every backend turn "portable" from a claim into a checked property.
- **Dagster supplies the dbt-shaped structure**: assets as models, `deps` as `ref()`, `@asset_check` as tests, schedules instead of cron.
- **The boundary is visible and fails safely**: unsupported ops raise `OperationNotDefinedError` at translate time, and the repo shows three ways to handle it.
- **The real pitch isn't "write once, run anywhere"**, it's "port once, never rewrite again," plus knowing exactly where "anywhere" ends.

<!-- IMAGE: closing diagram — same pipeline illustration as hero, annotated with "logic: write once" / "engine: config" / "storage: config" labels -->
