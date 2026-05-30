"""Generate boundary lines for every admin level from the polygon tables."""

from logging import getLogger

import duckdb

from .utils import describe, src_id_cols, src_meta_cols

logger = getLogger(__name__)


def generate_lines(conn: duckdb.DuckDBPyConnection) -> None:
    """Build adm{0..4}_lines from adm{0..4}_polygons.

    adm0_lines is the boundary of each adm0 polygon. For deeper levels each row
    is the interior boundary of children within their parent (child boundary
    minus parent boundary, unioned per parent).
    """
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE adm0_lines AS
        SELECT iso3,
               {_attr_select(conn, "adm0_polygons", 0)},
               ST_Multi(ST_Boundary(geom))::GEOMETRY AS geom
        FROM adm0_polygons
    """)

    for lvl in range(1, 5):
        parent_id = f"adm{lvl - 1}_id"
        conn.execute(f"""--sql
            CREATE OR REPLACE TABLE adm{lvl}_lines AS
            SELECT a.iso3,
                   {_attr_select(conn, f"adm{lvl}_polygons", lvl - 1, prefix="a.")},
                   ST_Multi(ST_Union(
                       ST_Difference(ST_Boundary(a.geom), ST_Boundary(b.geom))
                   ))::GEOMETRY AS geom
            FROM adm{lvl}_polygons a
            JOIN adm{lvl - 1}_polygons b
              ON a.iso3 = b.iso3 AND a."{parent_id}" = b."{parent_id}"
            GROUP BY ALL
        """)

    logger.info("lines adm0..adm4")


def _attr_select(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    level: int,
    prefix: str = "",
) -> str:
    """Return a comma-joined SQL column list (src_id + meta cols) present in `table`."""
    cols = describe(conn, table)
    attrs = [c for c in src_id_cols(level) + src_meta_cols() if c in cols]
    return ", ".join(f'{prefix}"{c}"' for c in attrs)
