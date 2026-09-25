"""Dagster I/O manager that stores every asset as a table on the Ibis backend.

Assets are pure functions `ir.Table -> ir.Table`; this manager owns all I/O:

- `handle_output` persists the returned expression via `con.create_table`
- `load_input`   hands downstream assets `con.table(<upstream asset>)`
- `sources`      maps upstream (external) asset names to physical reads —
                 `{"events_csv": {"format": "csv", "path": "..."}}`

Different deployments bind differently-configured instances of this manager
(see definitions.py) — the Dagster equivalent of a per-environment catalog.
"""

import os

import dagster as dg
from ibis.backends import BaseBackend
from ibis.expr import types as ir

from .resources import IbisResource

_READERS = ("csv", "parquet", "json", "delta")  # con.read_<fmt>(path)


class IbisIOManager(dg.ConfigurableIOManager):
    """Persists asset outputs as tables on the configured Ibis backend."""

    ibis: dg.ResourceDependency[IbisResource]

    # external source specs, keyed by upstream asset name:
    #   {"events_csv": {"format": "csv",  "path": "data/raw_events.csv"}}
    #   {"events":     {"format": "table", "name": "landing.events"}}
    # "path" values support ${ENV_VAR} expansion.
    sources: dict = {}

    # optional namespace for pipeline tables (duckdb "catalog.db" / spark db)
    database: str | None = None

    def _con(self) -> BaseBackend:
        return self.ibis.connect()

    def _namespace(self) -> dict:
        return {"database": self.database} if self.database else {}

    def handle_output(self, context: dg.OutputContext, obj: ir.Table) -> None:
        con = self._con()
        name = context.asset_key.path[-1]
        created = con.create_table(name, obj, overwrite=True, **self._namespace())

        metadata = {
            "ibis_backend": con.name,
            "table": name,
            "row_count": int(created.count().execute()),
            "preview": dg.MetadataValue.md(
                created.head(10).execute().to_markdown(index=False)
            ),
        }
        # SQL backends can show the compiled SQL — a nice way to demo that
        # Ibis compiles the same expression to different dialects/engines.
        try:
            metadata["compiled"] = dg.MetadataValue.md(
                f"```\n{con.compile(obj)}\n```"
            )
        except Exception:
            pass
        context.add_output_metadata(metadata)

    def load_input(self, context: dg.InputContext) -> ir.Table:
        name = context.asset_key.path[-1]  # the upstream asset's key
        con = self._con()

        src = self.sources.get(name)
        if src is None:
            # regular pipeline table produced by an upstream asset
            return con.table(name, **self._namespace())

        fmt = src["format"]
        if fmt == "table":  # an existing table in the backend's catalog
            return con.table(src.get("name", name))
        if fmt in _READERS:
            return getattr(con, f"read_{fmt}")(os.path.expandvars(src["path"]))
        raise ValueError(f"Unknown source format {fmt!r} for {name!r}")
