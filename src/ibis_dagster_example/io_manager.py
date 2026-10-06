"""Dagster I/O manager that stores every asset as a table on the Ibis backend.

Assets are pure functions `ir.Table -> ir.Table`; this manager owns all I/O:

- `handle_output` persists the returned expression via `con.create_table`
  (full refresh) and records row_count/preview metadata, two extra backend
  queries per output, demo observability rather than a production default
- `load_input`   hands downstream assets `con.table(<upstream asset>)`
- `sources`      maps upstream (external) asset names to physical reads:
                 `{"events": {"format": "csv", "path": "..."}}`

Different deployments bind differently-configured instances of this manager
(see definitions.py): the Dagster equivalent of a per-environment catalog.

Simplification for the demo: the asset key's last path component becomes the
table name, so nested keys like "marketing/customers" + "finance/customers"
would collide. A production version should map keys to catalog/schema/table.
"""

import os
from contextlib import suppress
from typing import Literal

import dagster as dg
from ibis.backends import BaseBackend
from ibis.expr import types as ir
from pydantic import Field, model_validator

from .resources import IbisResource


class SourceSpec(dg.Config):
    """One external source: a file read or an existing catalog table."""

    format: Literal["csv", "parquet", "json", "delta", "table"]
    path: str | None = None  # file formats; supports ${ENV_VAR} expansion
    name: str | None = None  # "table" format; defaults to the asset name
    database: str | None = None  # "table" format; source's catalog namespace

    @model_validator(mode="after")
    def _file_sources_require_path(self):
        if self.format != "table" and not self.path:
            raise ValueError(f"{self.format!r} sources require 'path'")
        return self


class IbisIOManager(dg.ConfigurableIOManager):
    """Persists asset outputs as tables on the configured Ibis backend."""

    ibis: dg.ResourceDependency[IbisResource]

    # external source specs, keyed by upstream asset name:
    #   {"events": {"format": "csv",   "path": "data/raw_events.csv"}}
    #   {"events": {"format": "table", "name": "events", "database": "landing"}}
    # "path" values support ${ENV_VAR} expansion.
    sources: dict[str, SourceSpec] = Field(default_factory=dict)

    def _con(self) -> BaseBackend:
        return self.ibis.connect()

    def _namespace(self) -> dict:
        # namespace lives on the resource: checks read the same tables
        return {"database": self.ibis.database} if self.ibis.database else {}

    def handle_output(self, context: dg.OutputContext, obj: ir.Table) -> None:
        con = self._con()
        name = context.asset_key.path[-1]
        created = con.create_table(name, obj, overwrite=True, **self._namespace())

        metadata = {
            "ibis_backend": con.name,
            "table": name,
            "row_count": int(created.count().execute()),  # ty: ignore[invalid-argument-type] (scalar .execute() is typed DataFrame|Series|Any
            "preview": dg.MetadataValue.md(
                created.head(10).execute().to_markdown(index=False)
            ),
        }
        # SQL backends can show the compiled SQL, a nice way to demo that
        # Ibis compiles the same expression to different dialects/engines.
        with suppress(Exception):
            sql = con.compile(obj)
            # non-SQL backends return a plan object, not a SQL string
            if isinstance(sql, str):
                metadata["compiled"] = dg.MetadataValue.md(f"```\n{sql}\n```")
        context.add_output_metadata(metadata)

    def load_input(self, context: dg.InputContext) -> ir.Table:
        name = context.asset_key.path[-1]  # the upstream asset's key
        con = self._con()

        src = self.sources.get(name)
        if src is None:
            # regular pipeline table produced by an upstream asset
            return self.ibis.table(name)

        if src.format == "table":  # an existing table in the backend's catalog
            return con.table(src.name or name, database=src.database)

        path = os.path.expandvars(src.path or "")
        if "$" in path:
            raise ValueError(
                f"Unresolved environment variable in source path "
                f"{src.path!r} for {name!r}"
            )
        return getattr(con, f"read_{src.format}")(path)
