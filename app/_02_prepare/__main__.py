"""Download and ingest all source data into tmp/prepare.duckdb."""

from logging import getLogger

import app.config
from app.config import PREPARE_DB
from app.utils import export_debug_tables, get_conn

from . import _01_hdx, _02_cod, _03_geoboundaries

logger = getLogger(__name__)


def main() -> None:
    """Build tmp/prepare.duckdb with admin + metadata tables from all sources."""
    conn = get_conn(PREPARE_DB, reset=True)
    _01_hdx.main(conn)
    _02_cod.load_cod(conn)
    _03_geoboundaries.load_geoboundaries(conn)
    if app.config.DEBUG:
        export_debug_tables(conn, "prepare")
    conn.close()
    logger.info("inputs done")


if __name__ == "__main__":
    main()
