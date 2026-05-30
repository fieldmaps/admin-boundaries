"""Build edge-matched DuckDB tables globally from prepare.duckdb's admin table.

Produces tmp/build.duckdb with 15 global tables: adm{0..4}_{polygons,lines,points}.
All operations are vectorized SQL over the full admin table — no per-country
iteration.
"""

from logging import getLogger

import app.config
from app.config import BUILD_DB, WLD
from app.utils import export_debug_tables, get_conn

from . import _01_inputs, _02_clip, _03_lines, _04_points, _05_attributes

logger = getLogger(__name__)


def main() -> None:
    """Run the global build stage end-to-end."""
    logger.info("starting build (wld=%s)", WLD)
    conn = get_conn(BUILD_DB, reset=True)

    _01_inputs.load_adm0_clip(conn)
    _01_inputs.load_adm0_wld(conn)
    _01_inputs.load_source(conn)

    _02_clip.clip_and_aggregate(conn)
    _03_lines.generate_lines(conn)
    _04_points.generate_points(conn)
    _05_attributes.main(conn)

    if app.config.DEBUG:
        export_debug_tables(conn, "build")
    conn.close()
    logger.info("build done")


if __name__ == "__main__":
    main()
