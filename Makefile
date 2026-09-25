.PHONY: dev dev-polars dev-prod materialize materialize-polars materialize-prod test

dev: ## dagster dev: local deployment (duckdb + csv)
	DAGSTER_DEPLOYMENT_NAME=local uv run dagster dev

dev-polars: ## dagster dev: polars deployment (in-memory + csv)
	DAGSTER_DEPLOYMENT_NAME=polars uv run dagster dev

dev-prod: ## dagster dev: prod deployment (pyspark + lake parquet; needs JDK)
	DAGSTER_DEPLOYMENT_NAME=prod uv run --extra pyspark dagster dev

materialize: ## headless run on the local deployment
	DAGSTER_DEPLOYMENT_NAME=local uv run dagster asset materialize -m ibis_dagster_example.definitions --select "*"

materialize-polars:
	DAGSTER_DEPLOYMENT_NAME=polars uv run dagster asset materialize -m ibis_dagster_example.definitions --select "*"

materialize-prod:
	DAGSTER_DEPLOYMENT_NAME=prod uv run --extra pyspark dagster asset materialize -m ibis_dagster_example.definitions --select "*"

test:
	uv run pytest -v
