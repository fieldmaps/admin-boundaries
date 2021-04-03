from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import logging

logger = logging.getLogger(__name__)


def main(layers):
    logger.info(f'Starting merge')
    con = connect(database='edge_matcher')
    cur = con.cursor()
    query_1 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} (
            id_0 varchar,
            id_1 varchar,
            id_2 varchar,
            id_3 varchar,
            id_4 varchar,
            id_5 varchar,
            geom geometry(MultiPolygon, 4326)
        );
    """
    query_2 = """
        INSERT INTO {table_out} (
            id_0, id_1, id_2, id_3, id_4, id_5,
            geom
        )
        SELECT *
        FROM {table_in};
    """
    query_3 = """
        CREATE INDEX ON {table_out} USING GIST(geom);
    """
    cur.execute(SQL(query_1).format(
        table_out=Identifier('admx_polygons_00'),
    ))
    for name in layers:
        cur.execute(SQL(query_2).format(
            table_in=Identifier(f'{name}_01'),
            table_out=Identifier('admx_polygons_00'),
        ))
    cur.execute(SQL(query_3).format(
        table_out=Identifier('admx_polygons_00'),
    ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'Finished merge')
