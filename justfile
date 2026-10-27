# `just` lists recipes. Deployments: local (duckdb + csv), polars
# (in-memory + csv), prod (pyspark + parquet lake; needs a JDK).

data_lake := env_var_or_default("DATA_LAKE", "data/lake")

# list recipes
default:
    @just --list

# dagster dev on a deployment
dev deployment="local":
    {{ if deployment == "prod" { "just seed-lake &&" } else { "" } }} \
        DAGSTER_DEPLOYMENT_NAME={{ deployment }} DATA_LAKE="{{ data_lake }}" \
        uv run {{ if deployment == "prod" { "--extra pyspark" } else { "" } }} \
        dagster dev

# headless: materialize all assets on a deployment
materialize deployment="local":
    {{ if deployment == "prod" { "just seed-lake &&" } else { "" } }} \
        DAGSTER_DEPLOYMENT_NAME={{ deployment }} DATA_LAKE="{{ data_lake }}" \
        uv run {{ if deployment == "prod" { "--extra pyspark" } else { "" } }} \
        dagster asset materialize -m ibis_dagster_example.definitions --select "*"

# write landing parquet under DATA_LAKE (default data/lake/) from data/*.csv
seed-lake lake=data_lake:
    uv run python scripts/seed_lake.py "{{ lake }}"

test:
    uv run pytest -v
