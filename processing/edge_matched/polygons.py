from psycopg2.sql import SQL, Identifier, Literal
from .utils import logging, get_src_ids, get_wld_ids

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        a.geom
    FROM {table_in1} AS a
    JOIN {table_in2} AS b
    ON ST_Within(a.geom, b.geom)
    WHERE b.adm0_src = {id}
    UNION ALL
    SELECT
        {ids_src},
        {ids_wld},
        ST_Multi(
            ST_CollectionExtract(
                ST_Intersection(a.geom, b.geom)
            , 3)
        )::GEOMETRY(MultiPolygon, 4326) as geom
    FROM {table_in1} AS a
    JOIN {table_in2} AS b
    ON ST_Intersects(a.geom, b.geom)
    AND NOT ST_Within(a.geom, b.geom)
    WHERE b.adm0_src = {id};
"""
query_2 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        ST_Multi(
            ST_Union(geom)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in} AS a
    GROUP BY {ids_src}, {ids_wld};
"""


def main(cur, src, wld, row):
    name = row['id']
    adm0 = row['id_clip']
    lvl = row['lvl']
    cur.execute(SQL(query_1).format(
        table_in1=Identifier(f'{src}_{name}_adm{lvl}_voronoi_{wld}'),
        table_in2=Identifier(f'adm0_clip_{wld}'),
        id=Literal(adm0),
        ids_src=SQL(',').join(
            map(lambda x: Identifier('a', x), get_src_ids(4))),
        ids_wld=SQL(',').join(
            map(lambda x: Identifier('a', x), get_wld_ids(False))),
        table_out=Identifier(f'{src}_{name}_adm{lvl}_polygons_{wld}'),
    ))
    for l in range(lvl-1, -1, -1):
        cur.execute(SQL(query_2).format(
            table_in=Identifier(f'{src}_{name}_adm{l+1}_polygons_{wld}'),
            ids_src=SQL(',').join(
                map(lambda x: Identifier('a', x), get_src_ids(l))),
            ids_wld=SQL(',').join(
                map(lambda x: Identifier('a', x), get_wld_ids(False))),
            table_out=Identifier(f'{src}_{name}_adm{l}_polygons_{wld}'),
        ))
    logger.info(f'{src}_{wld}_{name}')
