"""Insert COD fallback sources into prepare.duckdb."""

from logging import getLogger

import duckdb
import httpx

from app.config import HTTP_TIMEOUT

from .config import COD_BASE, COD_LIST_URL
from .utils import admin_cols, insert_remote

logger = getLogger(__name__)


def load_cod(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Insert COD fallback countries (in curated list but not yet in admin)."""
    covered = {row[0] for row in conn.execute("SELECT iso3 FROM metadata").fetchall()}
    with httpx.Client(follow_redirects=True, timeout=HTTP_TIMEOUT) as client:
        r = client.get(COD_LIST_URL)
        r.raise_for_status()
    todo = [row for row in r.json() if row.get("iso_3") and row["iso_3"] not in covered]
    cols = admin_cols(conn)
    inserted: set[str] = set()
    for row in todo:
        iso3, src_id = row["iso_3"], row["id"]
        url = f"{COD_BASE}/{src_id}.parquet"
        if insert_remote(conn, iso3, url, "cod", cols):
            inserted.add(iso3)
    logger.info("COD fallback: %s/%s countries inserted", len(inserted), len(todo))
    return inserted
