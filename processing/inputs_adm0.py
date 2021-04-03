import subprocess
from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import adm0, logging

logger = logging.getLogger(__name__)


def main(name, file):
    logger.info(f'Starting {name}')
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-lco', 'LAUNDER=NO',
        '-nlt', 'PROMOTE_TO_MULTI',
        '-nln', f'{name}_tmp1',
        '-f', 'PostgreSQL', 'PG:dbname=edge_matcher',
        file,
    ])
    con = connect(database='edge_matcher')
    cur = con.cursor()
    query_1 = """
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {id_0} VARCHAR;
    """
    query_2 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT
            {id_0} AS id_0,
            ST_Transform(ST_Multi(
                ST_CollectionExtract(ST_MakeValid(
                    ST_Force2D(ST_SnapToGrid(geom, 0.000000001))
                ), 3)
            ), 4326)::GEOMETRY(MultiPolygon, 4326) as geom
        FROM {table_in};
        CREATE INDEX ON {table_out} USING GIST(geom);
    """
    drop_tmp = """
        DROP TABLE IF EXISTS {table_tmp1};
    """
    cur.execute(SQL(query_1).format(
        id_0=Identifier(adm0['adm0']),
        table_in=Identifier(f'{name}_tmp1'),
    ))
    cur.execute(SQL(query_2).format(
        id_0=Identifier(adm0['adm0']),
        table_in=Identifier(f'{name}_tmp1'),
        table_out=Identifier(f'{name}_00'),
    ))
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'{name}_tmp1'),
    ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'Finished {name}')
