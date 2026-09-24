.PHONY: dev dev-polars dev-pyspark materialize materialize-polars materialize-pyspark test

dev: ## dagster dev on duckdb (default)
	IBIS_BACKEND=duckdb uv run dagster dev

dev-polars: ## dagster dev on polars
	IBIS_BACKEND=polars uv run dagster dev

dev-pyspark: ## dagster dev on pyspark (needs JDK + pyspark extra)
	IBIS_BACKEND=pyspark uv run --extra pyspark dagster dev

materialize: ## headless run on duckdb
	IBIS_BACKEND=duckdb uv run dagster asset materialize -m ibis_dagster_example.definitions --select "*"

materialize-polars:
	IBIS_BACKEND=polars uv run dagster asset materialize -m ibis_dagster_example.definitions --select "*"

materialize-pyspark:
	IBIS_BACKEND=pyspark uv run --extra pyspark dagster asset materialize -m ibis_dagster_example.definitions --select "*"

test:
	uv run pytest -v
