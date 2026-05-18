"""Export stage: turn tmp/build.duckdb's tables into all on-disk file formats."""

from logging import getLogger

from app.config import WLD

from . import _01_outputs, _02_area, _03_formats, _04_dest

logger = getLogger(__name__)


def main() -> None:
    """Write GeoParquets, area stats, format conversions, and metadata indices."""
    logger.info("starting export (wld=%s)", WLD)
    _01_outputs.main()
    _02_area.main()
    _03_formats.main()
    _04_dest.main("edge-matched")
    logger.info("export done")


if __name__ == "__main__":
    main()
