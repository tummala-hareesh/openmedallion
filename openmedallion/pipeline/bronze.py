"""pipeline/bronze.py — dlt ingestion engine: source → raw Parquet (bronze layer).

Supported sources (``source.type``)
------------------------------------
``sql_database``
    Oracle, Postgres, MySQL, MSSQL, SQLite via SQLAlchemy.

``rest_api``
    Any REST API via dlt's rest_api source (pagination, auth, cursors built-in).

``filesystem``
    Read Parquet or CSV files from a local path or cloud bucket.

Supported destinations (``destination.type``)
---------------------------------------------
``filesystem``  Local path or cloud storage — writes Parquet shards.
``duckdb``      Local DuckDB file.
``bigquery``    Google BigQuery.
``snowflake``   Snowflake.

Incremental modes (``source.tables[].incremental.mode``)
---------------------------------------------------------
``replace``  Full overwrite each run.
``append``   Adds only new rows using a cursor column.
``merge``    Upserts on a primary key.
"""
import dlt
import dlt.sources
from dlt.sources.rest_api import rest_api_source
import polars as pl
from pathlib import Path

from openmedallion import storage
from openmedallion.config.loader import expand_env_str


class BronzeLoader:
    """Ingest source data via dlt into the bronze Parquet layer.

    Args:
        cfg: Merged project config dict. Must contain ``source``,
            ``destination``, ``pipeline.name``, and ``paths.bronze``.
    """

    def __init__(self, cfg: dict):
        self.src           = cfg["source"]
        self.dst           = cfg["destination"]
        self.pipeline_name = cfg["pipeline"]["name"]
        self.bronze_path   = cfg["paths"]["bronze"]

    def load(self) -> dict[str, str]:
        """Run the dlt pipeline and return a path for every loaded table."""
        pipeline = self._build_pipeline()
        sources  = self._build_sources()
        info = pipeline.run(sources)
        print(f"📥  [bronze] dlt pipeline complete: {info}")
        return self._collect_parquets()

    def _build_pipeline(self) -> dlt.Pipeline:
        dst = self.dst
        match dst["type"]:
            case "filesystem":
                destination = dlt.destinations.filesystem(bucket_url=dst["bucket_url"])
            case "duckdb":
                destination = dlt.destinations.duckdb(dst.get("db_path", "bronze.duckdb"))
            case "bigquery":
                destination = dlt.destinations.bigquery()
            case "snowflake":
                destination = dlt.destinations.snowflake()
            case _:
                raise NotImplementedError(f"Destination '{dst['type']}' not wired.")

        return dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=destination,
            dataset_name="bronze",
        )

    def _build_sources(self):
        src_type = self.src["type"]

        if src_type == "sql_database":
            return self._sql_source()

        if src_type == "rest_api":
            return [self._rest_api_source()]

        if src_type == "filesystem":
            from dlt.sources.filesystem import filesystem, read_parquet, read_csv
            fs  = filesystem(bucket_url=self.src["bucket_url"], file_glob=self.src["file_glob"])
            fmt = self.src.get("format", "parquet")
            return fs | (read_parquet() if fmt == "parquet" else read_csv())

        raise NotImplementedError(f"Source type '{src_type}' not wired.")

    def _sql_source(self):
        from dlt.sources.sql_database import sql_table

        conn = expand_env_str(self.src["connection_string"])
        schema = self.src.get("schema")
        tables_cfg = self.src.get("tables", [])

        resources = []
        for tbl in tables_cfg:
            inc  = tbl.get("incremental", {})
            mode = inc.get("mode", "replace")

            kwargs = dict(
                credentials=conn,
                schema=schema,
                table=tbl["name"],
            )

            if mode == "append":
                kwargs["incremental"] = dlt.sources.incremental(
                    inc["cursor_column"],
                    initial_value=inc.get("initial_value"),
                )
            elif mode == "merge":
                kwargs["write_disposition"] = "merge"
                kwargs["primary_key"]       = inc["primary_key"]

            resources.append(sql_table(**kwargs))

        return resources

    def _rest_api_source(self):
        resource = self.src["resource"]
        resource_cfg: dict = {
            "name": resource,
            "endpoint": self.src.get("endpoint", resource),
        }
        inc = self.src.get("incremental")
        if inc:
            if inc["mode"] == "append":
                resource_cfg["incremental"] = dlt.sources.incremental(
                    inc["cursor_column"], initial_value=inc.get("initial_value")
                )
            elif inc["mode"] == "merge":
                resource_cfg["write_disposition"] = "merge"
                resource_cfg["primary_key"]       = inc["primary_key"]

        return rest_api_source(
            {"client": {"base_url": self.src["base_url"]}, "resources": [resource_cfg]}
        )

    def _collect_parquets(self) -> dict[str, str]:
        results = {}
        table_names = [t["name"] for t in self.src.get("tables", [])]

        if not table_names and self.src.get("resource"):
            table_names = [self.src["resource"]]

        for name in table_names:
            shard_dir = storage.join(self.bronze_path, "bronze", name)
            shards    = storage.ls_parquets(shard_dir)
            if not shards:
                print(f"⚠️   [bronze] no parquet shards found for '{name}' at {shard_dir}")
                continue

            out = storage.join(self.bronze_path, f"{name}.parquet")
            if len(shards) == 1:
                storage.copy(shards[0], out)
            else:
                storage.write_parquet(
                    pl.concat([storage.read_parquet(s) for s in shards]), out
                )
                print(f"📥  [bronze] merged {len(shards)} shards → {out}")

            results[name] = out

        return results
