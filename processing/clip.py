from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import logging

logger = logging.getLogger(__name__)


def main(name, *args):
    logger.info(f'Starting {name}')
    con = connect(database='edge_matcher')
    cur = con.cursor()
    query_1 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT
            a.adm0_id,
            a.adm1_id,
            a.adm2_id,
            a.adm3_id,
            a.adm4_id,
            a.adm5_id,
            ST_Multi(
                ST_CollectionExtract(ST_Intersection(a.geom, b.geom), 3)
            )::GEOMETRY(MultiPolygon, 4326) as geom
        FROM {table_in1} AS a
        JOIN {table_in2} AS b
        ON a.adm0_id = b.adm0_id;
    """
    cur.execute(SQL(query_1).format(
        table_in1=Identifier(f'{name}_00'),
        table_in2=Identifier('adm0_polygons'),
        table_out=Identifier(f'{name}_01'),
    ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'Finished {name}')
