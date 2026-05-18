"""Clip the global source to ADM0 templates and aggregate up admin levels."""

from logging import getLogger

import duckdb

from .utils import describe, src_id_cols, src_meta_cols

logger = getLogger(__name__)


def clip_and_aggregate(conn: duckdb.DuckDBPyConnection) -> None:
    """Clip src globally to ADM0 clip, then build adm{0..4}_polygons by aggregation.

    Each adm{L}_polygons table contains rows from countries with src_lvl >= L,
    grouped at level L by adm{L}_id (which already encodes iso3).
    """
    conn.execute("""--sql
        CREATE OR REPLACE TABLE src_clipped AS
        SELECT s.* EXCLUDE (geom),
               ST_Multi(ST_CollectionExtract(
                   ST_Intersection(s.geom, c.geom), 3
               ))::GEOMETRY AS geom
        FROM src s
        JOIN adm0_clip c ON s.iso3 = c.iso3
        WHERE ST_Intersects(s.geom, c.geom)
          AND ST_IsValid(s.geom)
    """)

    clipped_cols = describe(conn, "src_clipped")
    for lvl in range(5):
        attr_cols = [c for c in src_id_cols(lvl) + src_meta_cols() if c in clipped_cols]
        attr_select = ", ".join(f'"{c}"' for c in attr_cols)
        conn.execute(f"""--sql
            CREATE OR REPLACE TABLE adm{lvl}_polygons AS
            SELECT iso3,
                   {attr_select},
                   ST_Multi(ST_Union(geom))::GEOMETRY AS geom
            FROM src_clipped
            WHERE adm{lvl}_id IS NOT NULL
            GROUP BY ALL
        """)

    logger.info("clipped + aggregated adm0..adm4")
