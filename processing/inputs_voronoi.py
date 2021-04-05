import subprocess
from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import voronoi, logging

logger = logging.getLogger(__name__)


def main(name, file):
    logger.info(f'Starting {name}')
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-lco', 'LAUNDER=NO',
        '-lco', 'SPATIAL_INDEX=NONE',
        '-nlt', 'PROMOTE_TO_MULTI',
        '-nln', f'{name}_tmp1',
        '-f', 'PostgreSQL', 'PG:dbname=edge_matcher',
        file,
    ])
    con = connect(database='edge_matcher')
    cur = con.cursor()
    query_1 = """
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {adm0_id} VARCHAR;
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {adm1_id} VARCHAR;
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {adm2_id} VARCHAR;
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {adm3_id} VARCHAR;
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {adm4_id} VARCHAR;
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {adm5_id} VARCHAR;
    """
    query_2 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT
            {adm0_id} AS adm0_id,
            {adm1_id} AS adm1_id,
            {adm2_id} AS adm2_id,
            {adm3_id} AS adm3_id,
            {adm4_id} AS adm4_id,
            {adm5_id} AS adm5_id,
            ST_Transform(ST_Multi(
                ST_CollectionExtract(ST_MakeValid(
                    ST_Force2D(ST_SnapToGrid(geom, 0.000000001))
                ), 3)
            ), 4326)::GEOMETRY(MultiPolygon, 4326) AS geom
        FROM {table_in};
    """
    drop_tmp = """
        DROP TABLE IF EXISTS {table_tmp1};
    """
    cur.execute(SQL(query_1).format(
        adm0_id=Identifier(voronoi['adm0_id']),
        adm1_id=Identifier(voronoi['adm1_id']),
        adm2_id=Identifier(voronoi['adm2_id']),
        adm3_id=Identifier(voronoi['adm3_id']),
        adm4_id=Identifier(voronoi['adm4_id']),
        adm5_id=Identifier(voronoi['adm5_id']),
        table_in=Identifier(f'{name}_tmp1'),
    ))
    cur.execute(SQL(query_2).format(
        adm0_id=Identifier(voronoi['adm0_id']),
        adm1_id=Identifier(voronoi['adm1_id']),
        adm2_id=Identifier(voronoi['adm2_id']),
        adm3_id=Identifier(voronoi['adm3_id']),
        adm4_id=Identifier(voronoi['adm4_id']),
        adm5_id=Identifier(voronoi['adm5_id']),
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
