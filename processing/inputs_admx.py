import subprocess
from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import admx, logging

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
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {id_1} VARCHAR;
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {id_2} VARCHAR;
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {id_3} VARCHAR;
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {id_4} VARCHAR;
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {id_5} VARCHAR;
    """
    query_2 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT
            {id_0} AS id_0,
            {id_1} AS id_1,
            {id_2} AS id_2,
            {id_3} AS id_3,
            {id_4} AS id_4,
            {id_5} AS id_5,
            ST_Transform(ST_Multi(
                ST_Force2D(ST_MakeValid(
                    ST_SnapToGrid(geom, 0.000000001)
                ))
            ), 4326)::GEOMETRY(MultiPolygon, 4326) as geom
        FROM {table_in};
        CREATE INDEX ON {table_out} USING GIST(geom);
    """
    drop_tmp = """
        DROP TABLE IF EXISTS {table_tmp1};
    """
    cur.execute(SQL(query_1).format(
        id_0=Identifier(admx['adm0']),
        id_1=Identifier(admx['adm1']),
        id_2=Identifier(admx['adm2']),
        id_3=Identifier(admx['adm3']),
        id_4=Identifier(admx['adm4']),
        id_5=Identifier(admx['adm5']),
        table_in=Identifier(f'{name}_tmp1'),
    ))
    cur.execute(SQL(query_2).format(
        id_0=Identifier(admx['adm0']),
        id_1=Identifier(admx['adm1']),
        id_2=Identifier(admx['adm2']),
        id_3=Identifier(admx['adm3']),
        id_4=Identifier(admx['adm4']),
        id_5=Identifier(admx['adm5']),
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
