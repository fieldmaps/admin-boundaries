"""Generate centroid points for every admin level from the polygon tables."""

from logging import getLogger

import duckdb

from .config import CENTROID_PRECISION
from .utils import describe, src_id_cols, src_meta_cols

logger = getLogger(__name__)


def generate_points(conn: duckdb.DuckDBPyConnection) -> None:
    """Build adm{0..4}_points as centroids of the corresponding polygons."""
    for lvl in range(5):
        poly = f"adm{lvl}_polygons"
        attrs = [
            c for c in src_id_cols(lvl) + src_meta_cols() if c in describe(conn, poly)
        ]
        attr_select = ", ".join(f'"{c}"' for c in attrs)
        conn.execute(f"""--sql
            CREATE OR REPLACE TABLE adm{lvl}_points AS
            SELECT iso3,
                   {attr_select},
                   ST_ReducePrecision(
                       ST_Centroid(geom), {CENTROID_PRECISION}
                   )::GEOMETRY AS geom
            FROM {poly}
        """)

    logger.info("points adm0..adm4")
