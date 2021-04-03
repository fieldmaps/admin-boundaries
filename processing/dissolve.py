from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import logging

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        id_0, id_1,
        ST_Multi(
            ST_Union(geom)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in}
    WHERE id_1 IS NOT NULL
    GROUP BY id_0, id_1;
    CREATE INDEX ON {table_out} USING GIST(geom);
"""

query_2 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        id_0, id_1, id_2,
        ST_Multi(
            ST_Union(geom)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in}
    WHERE id_2 IS NOT NULL
    GROUP BY id_0, id_1, id_2;
    CREATE INDEX ON {table_out} USING GIST(geom);
"""

query_3 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        id_0, id_1, id_2, id_3,
        ST_Multi(
            ST_Union(geom)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in}
    WHERE id_3 IS NOT NULL
    GROUP BY id_0, id_1, id_2, id_3;
    CREATE INDEX ON {table_out} USING GIST(geom);
"""

query_4 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        id_0, id_1, id_2, id_3, id_4,
        ST_Multi(
            ST_Union(geom)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in}
    WHERE id_4 IS NOT NULL
    GROUP BY id_0, id_1, id_2, id_3, id_4;
    CREATE INDEX ON {table_out} USING GIST(geom);
"""

query_5 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        id_0, id_1, id_2, id_3, id_4, id_5,
        ST_Multi(
            ST_Union(geom)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in}
    WHERE id_5 IS NOT NULL
    GROUP BY id_0, id_1, id_2, id_3, id_4, id_5;
    CREATE INDEX ON {table_out} USING GIST(geom);
"""

query = {
    1: query_1,
    2: query_2,
    3: query_3,
    4: query_4,
    5: query_5,
}


def main(level, *args):
    logger.info(f'Starting polygon level {level}')
    con = connect(database='edge_matcher')
    cur = con.cursor()
    cur.execute(SQL(query[level]).format(
        table_in=Identifier('admx_polygons_00'),
        table_out=Identifier(f'adm{level}_polygons_00'),
    ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'Finished polygon level {level}')
