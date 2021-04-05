from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import logging

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
            (
                ST_MaximumInscribedCircle(geom)
            ).center::GEOMETRY(Point, 4326) AS geom
        FROM {table_in};
    """
    ids = map(lambda x: f'adm{x}_id', range(level+1))
    cur.execute(SQL(query_1).format(
        table_in=Identifier(f'adm{level}_polygons'),
        ids=SQL(',').join(map(Identifier, ids)),
        table_out=Identifier(f'adm{level}_points'),
    ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'Finished adm{level}')
