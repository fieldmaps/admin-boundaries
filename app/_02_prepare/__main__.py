"""Download and ingest all source data into tmp/prepare.duckdb."""

from logging import getLogger

from . import _01_hdx, _02_fallbacks
from .utils import get_conn

logger = getLogger(__name__)


def main() -> None:
    """Build tmp/prepare.duckdb with admin + metadata tables from all sources."""
    cod_meta_iso3s = _01_hdx.main()
    conn = get_conn("prepare", reset=False)
    _02_fallbacks.load_cod(conn, cod_meta_iso3s)
    _02_fallbacks.load_geoboundaries(conn)
    conn.close()
    logger.info("inputs done")


if __name__ == "__main__":
    main()
