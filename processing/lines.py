from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .attribute_queries import queries
from .utils import logging, table_exists

logger = logging.getLogger(__name__)


def main(level):
    logger.info(f'Starting adm{level}')
    con = connect(database='edge_matcher')
    cur = con.cursor()
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
    drop_tmp = """
        DROP TABLE IF EXISTS {table_tmp1};
    """
    ids = map(lambda x: f'adm{x}_id', range(level))
    if table_exists(cur, f'adm{level}_attributes'):
        cur.execute(SQL(query_1).format(
            table_in1=Identifier(f'adm{level}_polygons'),
            table_in2=Identifier(f'adm{level-1}_polygons'),
            ids=SQL(',').join(map(lambda x: Identifier('a', x), ids)),
            id=Identifier(f'adm{level-1}_id'),
            table_out=Identifier(f'adm{level}_lines_tmp1'),
        ))
        cur.execute(SQL(queries[level-1]).format(
            table_in=Identifier(f'adm{level}_lines_tmp1'),
            table_out=Identifier(f'adm{level}_lines'),
        ))
        cur.execute(SQL(drop_tmp).format(
            table_tmp1=Identifier(f'adm{level}_lines_tmp1'),
        ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'Finished adm{level}')
