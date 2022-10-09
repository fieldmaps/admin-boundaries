from psycopg.sql import SQL, Identifier
from processing.edge_matched.utils import logging, get_src_ids, get_wld_ids

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        ST_Multi(ST_Union(
            ST_Difference(ST_Boundary(a.geom), ST_Boundary(b.geom))
        ))::GEOMETRY(MultiLineString, 4326) AS geom
    FROM {table_in1} AS a
    JOIN {table_in2} AS b
    ON a.{id} = b.{id}
    GROUP BY {ids_src}, {ids_wld};
"""
query_2 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        ST_Multi(
            ST_Boundary(geom)
        )::GEOMETRY(MultiLineString, 4326) AS geom
    FROM {table_in};
"""


def main(conn, dest, wld, row):
    name = row['id']
    lvl = row['lvl']
    for l in range(lvl, 0, -1):
        conn.execute(SQL(query_1).format(
            table_in1=Identifier(f'{dest}_{name}_adm{l}_polygons_{wld}'),
            table_in2=Identifier(f'{dest}_{name}_adm{l-1}_polygons_{wld}'),
            ids_src=SQL(',').join(
                map(lambda x: Identifier('a', x), get_src_ids(l-1))),
            ids_wld=SQL(',').join(
                map(lambda x: Identifier('a', x), get_wld_ids(False))),
            id=Identifier(f'adm{l-1}_id'),
            table_out=Identifier(f'{dest}_{name}_adm{l}_lines_{wld}'),
        ))
    conn.execute(SQL(query_2).format(
        table_in=Identifier(f'{dest}_{name}_adm0_polygons_{wld}'),
        table_out=Identifier(f'{dest}_{name}_adm0_lines_{wld}'),
    ))
    logger.info(f'{dest}_{wld}_{name}')
