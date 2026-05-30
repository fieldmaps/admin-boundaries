"""Download and ingest all source data into tmp/prepare.duckdb."""

from logging import getLogger

import app.config
from app.config import PREPARE_DB
from app.utils import export_debug_tables, get_conn

from . import _01_hdx, _02_fallbacks

logger = getLogger(__name__)


def main() -> None:
    """Build tmp/prepare.duckdb with admin + metadata tables from all sources."""
    cod_meta_iso3s = _01_hdx.main()
    conn = get_conn(PREPARE_DB)
    _02_fallbacks.load_cod(conn, cod_meta_iso3s)
    _02_fallbacks.load_geoboundaries(conn)
    if app.config.DEBUG:
        export_debug_tables(conn)
    conn.close()
    logger.info("inputs done")


if __name__ == "__main__":
    main()
