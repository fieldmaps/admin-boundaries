"""Insert COD and GeoBoundaries fallback sources into prepare.duckdb."""

from logging import getLogger

import duckdb
import httpx

from app.config import HTTP_TIMEOUT

logger = getLogger(__name__)


def load_fallback(
    conn: duckdb.DuckDBPyConnection,
    list_url: str,
    base_url: str,
    src_label: str,
) -> set[str]:
    """Insert fallback countries from a curated list not yet in admin."""
    covered = {row[0] for row in conn.execute("SELECT iso3 FROM metadata").fetchall()}
    with httpx.Client(follow_redirects=True, timeout=HTTP_TIMEOUT) as client:
        r = client.get(list_url)
        r.raise_for_status()
    todo = [row for row in r.json() if row.get("iso_3") and row["iso_3"] not in covered]
    cols = _admin_cols(conn)
    inserted: set[str] = set()
    for row in todo:
        iso3, src_id = row["iso_3"], row["id"]
        url = f"{base_url}/{src_id}.parquet"
        if _insert_remote(conn, iso3, url, src_label, cols):
            inserted.add(iso3)
    logger.info("%s: %s/%s countries inserted", src_label, len(inserted), len(todo))
    return inserted


def _admin_cols(conn: duckdb.DuckDBPyConnection) -> list[str]:
    return [row[0] for row in conn.execute("DESCRIBE admin").fetchall()]


def _projection_sql(cols: list[str], stage_cols: set[str], iso3: str) -> str:
    parts: list[str] = []
    for col in cols:
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
    cols: list[str],
) -> bool:
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

    proj = _projection_sql(cols, stage_cols, iso3)
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
