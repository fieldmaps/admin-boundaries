"""Load ADM0 templates and the global source table into the build DuckDB."""

from logging import getLogger

import duckdb

from app.config import PREPARE_DB, WLD

from .config import ADM0_BASE

logger = getLogger(__name__)


def load_adm0_clip(conn: duckdb.DuckDBPyConnection) -> None:
    """Load ADM0 clip polygons, one row per iso3 (unioned)."""
    url = _adm0_url("clip")
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE adm0_clip AS
        SELECT iso3, ST_Multi(ST_Union(geom))::GEOMETRY AS geom
        FROM read_parquet('{url}')
        GROUP BY iso3
    """)


def load_adm0_wld(conn: duckdb.DuckDBPyConnection) -> None:
    """Load ADM0 world-view polygons into adm0_wld."""
    url = _adm0_url("polygons")
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE adm0_wld AS
        SELECT * FROM read_parquet('{url}')
    """)


def load_source(conn: duckdb.DuckDBPyConnection) -> None:
    """Load the global admin table from tmp/prepare.duckdb into src."""
    conn.execute(f"ATTACH '{PREPARE_DB}' AS prepare (READ_ONLY)")
    conn.execute("CREATE OR REPLACE TABLE src AS SELECT * FROM prepare.admin")
    conn.execute("DETACH prepare")


def _adm0_url(geom: str) -> str:
    return f"{ADM0_BASE}/{WLD}/adm0_{geom}.parquet"
