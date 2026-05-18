"""Insert fallback (COD + GeoBoundaries) sources into prepare.duckdb."""

from logging import getLogger

import duckdb
import httpx

from app.config import HTTP_TIMEOUT

from .config import COD_BASE, GB_API, GB_BASE

logger = getLogger(__name__)


def load_cod(conn: duckdb.DuckDBPyConnection, cod_meta_iso3s: set[str]) -> set[str]:
    """Insert COD fallback countries (in HDX metadata CSV but not yet in admin)."""
    covered = {row[0] for row in conn.execute("SELECT iso3 FROM metadata").fetchall()}
    todo = sorted(cod_meta_iso3s - covered)
    admin_cols = _admin_cols(conn)
    inserted: set[str] = set()
    for iso3 in todo:
        url = f"{COD_BASE}/{iso3.lower()}.parquet"
        if _insert_remote(conn, iso3, url, "cod", admin_cols):
            inserted.add(iso3)
    logger.info("COD fallback: %s/%s countries inserted", len(inserted), len(todo))
    return inserted


def load_geoboundaries(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Insert GeoBoundaries countries not already covered by HDX or COD fallback."""
    covered = {row[0] for row in conn.execute("SELECT iso3 FROM metadata").fetchall()}
    with httpx.Client(follow_redirects=True, timeout=HTTP_TIMEOUT) as client:
        r = client.get(GB_API)
        r.raise_for_status()
    gb_iso3s = {row["boundaryISO"] for row in r.json() if row.get("boundaryISO")}
    todo = sorted(gb_iso3s - covered)
    admin_cols = _admin_cols(conn)
    inserted: set[str] = set()
    for iso3 in todo:
        url = f"{GB_BASE}/{iso3.lower()}.parquet"
        if _insert_remote(conn, iso3, url, "geoboundaries", admin_cols):
            inserted.add(iso3)
    logger.info("GeoBoundaries: %s/%s countries inserted", len(inserted), len(todo))
    return inserted


def _admin_cols(conn: duckdb.DuckDBPyConnection) -> list[str]:
    return [row[0] for row in conn.execute("DESCRIBE admin").fetchall()]


def _projection_sql(admin_cols: list[str], stage_cols: set[str], iso3: str) -> str:
    """Project stage cols to admin's column order; substitute iso3, NULL for missing."""
    parts: list[str] = []
    for col in admin_cols:
        if col == "iso3":
            parts.append(f"'{iso3}' AS iso3")
        elif col in stage_cols:
            parts.append(f'"{col}"')
        else:
            parts.append(f'NULL AS "{col}"')
    return ", ".join(parts)


def _insert_remote(
    conn: duckdb.DuckDBPyConnection,
    iso3: str,
    url: str,
    src_label: str,
    admin_cols: list[str],
) -> bool:
    """Stage a remote parquet, insert into admin + metadata. Return True on success."""
    try:
        conn.execute(
            f"CREATE OR REPLACE TEMP TABLE stage AS SELECT * FROM read_parquet('{url}')"
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

    lvl_val = conn.execute("SELECT MAX(src_lvl) FROM stage").fetchone()[0]
    if lvl_val is None or int(lvl_val) < 1:
        logger.warning("%s: src_lvl=%s, skipping", iso3, lvl_val)
        conn.execute("DROP TABLE stage")
        return False
    lvl = int(lvl_val)

    src_name1_expr = "ANY_VALUE(src_name1)" if "src_name1" in stage_cols else "NULL"
    meta = conn.execute(f"""--sql
        SELECT
            MAX(src_date), MAX(src_update),
            ANY_VALUE(src_name), {src_name1_expr},
            ANY_VALUE(src_lic), ANY_VALUE(src_url)
        FROM stage
    """).fetchone()

    proj = _projection_sql(admin_cols, stage_cols, iso3)
    conn.execute(f"INSERT INTO admin SELECT {proj} FROM stage")
    conn.execute(
        """--sql
        INSERT INTO metadata (iso3, src, lvl, src_date, src_update,
                              src_name, src_name1, src_lic, src_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [iso3, src_label, lvl, *meta],
    )
    conn.execute("DROP TABLE stage")
    return True
