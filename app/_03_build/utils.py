"""Shared utilities for the build stage."""

import duckdb

import app.config
from app.config import BUILD_DB
from app.utils import ProfiledConnection


def describe(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    """Return the column names of a DuckDB table."""
    return [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]


def src_id_cols(level: int) -> list[str]:
    """Return ordered list of ID/name columns from level down to adm0."""
    result = []
    for lvl in range(level, -1, -1):
        result += [
            f"adm{lvl}_id",
            f"adm{lvl}_src",
            f"adm{lvl}_name",
            f"adm{lvl}_name1",
            f"adm{lvl}_name2",
        ]
    return result


def src_meta_cols() -> list[str]:
    """Return ordered list of source metadata column names."""
    return [
        "src_lvl",
        "src_lang",
        "src_lang1",
        "src_lang2",
        "src_date",
        "src_update",
        "src_name",
        "src_name1",
        "src_lic",
        "src_url",
        "src_grp",
    ]


def wld_cols() -> list[str]:
    """Return ordered list of world-view attribute column names."""
    return [
        "adm0_id",
        "adm0_src",
        "adm0_name",
        "adm0_name1",
        "adm0_name2",
        "iso_cd",
        "iso_2",
        "iso_3",
        "iso_3_grp",
        "region3_cd",
        "region3_nm",
        "region2_cd",
        "region2_nm",
        "region1_cd",
        "region1_nm",
        "status_cd",
        "status_nm",
        "wld_date",
        "wld_update",
        "wld_view",
        "wld_notes",
    ]


def all_output_cols(level: int) -> list[str]:
    """Return all output columns for a given admin level."""
    return src_id_cols(level) + src_meta_cols()


def open_build_conn(*, reset: bool = False) -> duckdb.DuckDBPyConnection | ProfiledConnection:
    """Open the shared build DuckDB at tmp/build.duckdb.

    Returns a ProfiledConnection when DEBUG is enabled.
    """
    BUILD_DB.parent.mkdir(parents=True, exist_ok=True)
    if reset:
        BUILD_DB.unlink(missing_ok=True)
    conn = duckdb.connect(str(BUILD_DB))
    conn.execute("LOAD spatial;")
    conn.execute("LOAD httpfs;")
    return ProfiledConnection(conn) if app.config.DEBUG else conn
