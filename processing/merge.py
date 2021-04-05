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
            adm0_fid varchar,
            adm0_id varchar,
            adm1_id varchar,
            adm2_id varchar,
            adm3_id varchar,
            adm4_id varchar,
            adm5_id varchar,
            geom geometry(MultiPolygon, 4326)
        );
    """
    query_2 = """
        INSERT INTO {table_out} (
            adm0_id,
            adm1_id,
            adm2_id,
            adm3_id,
            adm4_id,
            adm5_id,
            geom
        )
        SELECT *
        FROM {table_in};
    """
    query_3 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT *
        FROM {table_in1}
        UNION ALL
        SELECT
            a.adm0_fid,
            a.adm0_id,
            NULL AS adm1_id,
            NULL AS adm2_id,
            NULL AS adm3_id,
            NULL AS adm4_id,
            NULL AS adm5_id,
            a.geom
        FROM {table_in2} AS a
        LEFT JOIN {table_in1} AS b
        ON a.adm0_id = b.adm0_id
        WHERE b.adm0_id IS NULL
    """
    drop_tmp = """
        DROP TABLE IF EXISTS {table_tmp1};
    """
    cur.execute(SQL(query_1).format(
        table_out=Identifier('admx_polygons_tmp1'),
    ))
    for name in layers:
        cur.execute(SQL(query_2).format(
            table_in=Identifier(f'{name}_01'),
            table_out=Identifier('admx_polygons_tmp1'),
        ))
    cur.execute(SQL(query_3).format(
        table_in1=Identifier('admx_polygons_tmp1'),
        table_in2=Identifier('adm0_polygons'),
        table_out=Identifier('admx_polygons'),
    ))
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier('admx_polygons_tmp1'),
    ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'Finished merge')
