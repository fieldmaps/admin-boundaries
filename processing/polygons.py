from psycopg2 import connect
from psycopg2.sql import SQL, Identifier, Literal
from .utils import DATABASE, logging, get_ids

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids},
        a.geom
    FROM {table_in1} AS a
    JOIN {table_in2} AS b
    ON ST_Within(a.geom, b.geom)
    WHERE b.id = {id}
    UNION ALL
    SELECT
        {ids},
        ST_Multi(
            ST_CollectionExtract(
                ST_Intersection(a.geom, b.geom)
            , 3)
        )::GEOMETRY(MultiPolygon, 4326) as geom
    FROM {table_in1} AS a
    JOIN {table_in2} AS b
    ON ST_Overlaps(a.geom, b.geom)
    WHERE b.id = {id};
"""
query_2 = """
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


def main(src, name, level):
    con = connect(database=DATABASE)
    cur = con.cursor()
    cur.execute(SQL(query_1).format(
        table_in1=Identifier(f'{src}_{name}_adm{level}_voronoi'),
        table_in2=Identifier('adm0_polygons'),
        ids=SQL(',').join(map(Identifier, get_ids(level))),
        id=Literal(name),
        table_out=Identifier(f'{src}_{name}_adm{level}_polygons'),
    ))
    for l in range(level-1, -1, -1):
        cur.execute(SQL(query_2).format(
            table_in=Identifier(f'{src}_{name}_adm{l+1}_polygons'),
            ids=SQL(',').join(map(Identifier, get_ids(l))),
            table_out=Identifier(f'{src}_{name}_adm{l}_polygons'),
        ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'{name}_{src}')
