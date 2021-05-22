from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import DATABASE, logging, get_ids

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids},
        ST_Multi(ST_Union(
            ST_Difference(ST_Boundary(a.geom), ST_Boundary(b.geom))
        ))::GEOMETRY(MultiLineString, 4326) AS geom
    FROM {table_in1} AS a
    JOIN {table_in2} AS b
    ON a.{id} = b.{id}
    GROUP BY {ids};
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


def main(src, name, level):
    con = connect(database=DATABASE)
    cur = con.cursor()
    for l in range(level, 0, -1):
        cur.execute(SQL(query_1).format(
            table_in1=Identifier(f'{src}_{name}_adm{l}_polygons'),
            table_in2=Identifier(f'{src}_{name}_adm{l-1}_polygons'),
            ids=SQL(',').join(map(lambda x: Identifier('a', x), get_ids(l-1))),
            id=Identifier(f'adm{l-1}_id'),
            table_out=Identifier(f'{src}_{name}_adm{l}_lines'),
        ))
    cur.execute(SQL(query_2).format(
        table_in=Identifier(f'{src}_{name}_adm0_polygons'),
        table_out=Identifier(f'{src}_{name}_adm0_lines'),
    ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'{name}_{src}')
