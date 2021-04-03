from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import logging

logger = logging.getLogger(__name__)


def main(name, overlay):
    logger.info(f'Starting intersection {name} with {overlay}')
    con = connect(database='edge_matcher')
    cur = con.cursor()
    query_1 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT a.*
        FROM {table_in1} AS a
        JOIN {table_in2} AS b
        ON ST_Intersects(a.geom, b.geom)
        AND NOT ST_Within(a.geom, b.geom);
        CREATE INDEX ON {table_out} USING GIST(geom);
    """
    query_2 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT
            b.id_0,
            a.geom
        FROM {table_in1} AS a
        JOIN {table_in3} AS b
        ON ST_Within(a.geom, b.geom)
        UNION ALL
        SELECT
            b.id_0,
            ST_Intersection(a.geom, b.geom)::GEOMETRY(Geometry, 4326) AS geom
        FROM {table_in2} AS a
        JOIN {table_in3} AS b
        ON ST_Intersects(a.geom, b.geom);
        CREATE INDEX ON {table_out} USING GIST(geom);
    """
    query_3 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT
            id_0,
            ST_Multi(
                ST_Collect(geom)
            )::GEOMETRY(MultiPolygon, 4326) as geom
        FROM {table_in}
        GROUP BY id_0;
        CREATE INDEX ON {table_out} USING GIST(geom);
    """
    drop_tmp = """
        DROP TABLE IF EXISTS {table_tmp1};
        DROP TABLE IF EXISTS {table_tmp2};
    """
    cur.execute(SQL(query_1).format(
        table_in1=Identifier(f'{overlay}_00'),
        table_in2=Identifier(f'{name}_00'),
        table_out=Identifier(f'{overlay}_tmp1'),
    ))
    logger.info(f'Finished polygons query 1')
    cur.execute(SQL(query_2).format(
        table_in1=Identifier(f'{overlay}_00'),
        table_in2=Identifier(f'{overlay}_tmp1'),
        table_in3=Identifier(f'{name}_00'),
        table_out=Identifier(f'{name}_tmp2'),
    ))
    logger.info(f'Finished polygons query 2')
    cur.execute(SQL(query_3).format(
        table_in=Identifier(f'{name}_tmp2'),
        table_out=Identifier(f'{name}_01'),
    ))
    cur.execute(SQL(drop_tmp).format(
        table_attr=Identifier(f'{name}_tmp1'),
    ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'Finished intersection {name} with {overlay}')


if __name__ == '__main__':
    main()
