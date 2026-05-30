"""Shared utilities for the prepare (ingest) stage."""

from logging import getLogger
from pathlib import Path

import duckdb

from app.utils import get_conn

logger = getLogger(__name__)


def load_metadata(meta_csv: Path) -> dict:
    """Return {iso3: row_dict} with standardized field names from the metadata CSV."""
    conn = get_conn()
    rows = conn.execute(f"""--sql
        SELECT
            country_iso3 AS iso3,
            TRY_CAST(admin_level_full AS INTEGER) AS src_lvl,
            TRY_CAST(date_source AS DATE) AS src_date,
            TRY_CAST(date_updated AS DATE) AS src_update,
            COALESCE(source, '') AS src_name,
            COALESCE(contributor, '') AS src_name1,
            COALESCE(methodology_pcodes, '') AS src_lic
        FROM read_csv('{meta_csv}', nullstr=['', '#N/A'])
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY country_iso3
            ORDER BY TRY_CAST(date_updated AS DATE) DESC NULLS LAST
        ) = 1
    """).fetchall()
    conn.close()
    return {
        row[0]: {
            "src_lvl": row[1],
            "src_date": row[2],
            "src_update": row[3],
            "src_name": row[4],
            "src_name1": row[5],
            "src_lic": row[6],
        }
        for row in rows
        if row[0] is not None
    }


def admin_cols(conn: duckdb.DuckDBPyConnection) -> list[str]:
    return [row[0] for row in conn.execute("DESCRIBE admin").fetchall()]


def projection_sql(cols: list[str], stage_cols: set[str], iso3: str) -> str:
    """Project stage cols to admin's column order; substitute iso3, NULL for missing."""
    parts: list[str] = []
    for col in cols:
        if col == "iso3":
            parts.append(f"'{iso3}' AS iso3")
        elif col in stage_cols:
            parts.append(f'"{col}"')
        else:
            parts.append(f'NULL AS "{col}"')
    return ", ".join(parts)


def insert_remote(
    conn: duckdb.DuckDBPyConnection,
    iso3: str,
    url: str,
    src_label: str,
    cols: list[str],
) -> bool:
    """Stage a remote parquet, insert into admin + metadata. Return True on success."""
    try:
        conn.execute(
            "CREATE OR REPLACE TEMP TABLE stage AS "
            f"SELECT * FROM read_parquet('{url}')",
        )
    except duckdb.Error as e:
        logger.warning("%s: failed to read %s (%s)", iso3, url, e)
        return False

    stage_cols = {row[0] for row in conn.execute("DESCRIBE stage").fetchall()}
    if "geometry" in stage_cols and "geom" not in stage_cols:
        conn.execute("ALTER TABLE stage RENAME COLUMN geometry TO geom")
        stage_cols.remove("geometry")
        stage_cols.add("geom")
    if "src_lvl" not in stage_cols:
        logger.warning("%s: no src_lvl column at %s", iso3, url)
        conn.execute("DROP TABLE stage")
        return False

    lvl_val = conn.execute("SELECT MAX(src_lvl) FROM stage").fetchone()
    if lvl_val is None or lvl_val[0] is None or int(lvl_val[0]) < 1:
        logger.warning("%s: src_lvl=%s, skipping", iso3, lvl_val)
        conn.execute("DROP TABLE stage")
        return False
    lvl = int(lvl_val[0])

    src_name1_expr = "ANY_VALUE(src_name1)" if "src_name1" in stage_cols else "NULL"
    meta = conn.execute(f"""--sql
        SELECT
            MAX(src_date), MAX(src_update),
            ANY_VALUE(src_name), {src_name1_expr},
            ANY_VALUE(src_lic), ANY_VALUE(src_url)
        FROM stage
    """).fetchone()

    proj = projection_sql(cols, stage_cols, iso3)
    conn.execute(f"INSERT INTO admin SELECT {proj} FROM stage")
    conn.execute(
        """--sql
        INSERT INTO metadata (iso3, src, lvl, src_date, src_update,
                              src_name, src_name1, src_lic, src_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [iso3, src_label, lvl, *(meta or [None] * 6)],
    )
    conn.execute("DROP TABLE stage")
    return True
