import logging

from psycopg.sql import SQL, Identifier

from .utils import get_cols

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids},
        ST_Multi(
            ST_Union(geom)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in}
    GROUP BY {ids};
"""


def main(conn, name, level, level_max, row):
    level = level_max or level
    for lvl in range(level - 1, -1, -1):
        ids = get_cols(lvl, row)
        conn.execute(
            SQL(query_1).format(
                table_in=Identifier(f"{name}_adm{lvl+1}"),
                ids=SQL(",").join(map(Identifier, ids)),
                table_out=Identifier(f"{name}_adm{lvl}"),
            )
        )
    logger.info(name)
