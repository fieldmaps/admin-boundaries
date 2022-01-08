import subprocess
from pathlib import Path
from psycopg2.sql import SQL, Identifier
from .utils import logging, DATABASE

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent
inputs = cwd / f'../../../inputs/geoboundaries'

query_1 = """
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "shapeName" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "shapeISO" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "ADM1_NAME" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "PROV_34_NA" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "DIST_34_NA" VARCHAR;
"""
query_2 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        fid,
        COALESCE("shapeName", "ADM1_NAME", "PROV_34_NA", "DIST_34_NA") AS "shapeName",
        "shapeISO",
        "shapeID",
        "shapeGroup",
        "shapeType",
        ST_Multi(
            ST_ReducePrecision(geom, 0.000000001)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in};
    CREATE INDEX ON {table_out} USING GIST(geom);
"""
drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def boundaries(cur, name, level):
    file = (inputs / f'{name}_adm{level}.gpkg')
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-makevalid',
        '-dim', 'XY',
        '-t_srs', 'EPSG:4326',
        '-nlt', 'PROMOTE_TO_MULTI',
        '-lco', 'OVERWRITE=YES',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-lco', 'LAUNDER=NO',
        '-lco', 'SPATIAL_INDEX=NONE',
        '-nln', f'{name}_adm{level}_tmp1',
        '-f', 'PostgreSQL', f'PG:dbname={DATABASE}',
        file,
    ])
    cur.execute(SQL(query_1).format(
        table_in=Identifier(f'{name}_adm{level}_tmp1'),
    ))
    cur.execute(SQL(query_2).format(
        table_in=Identifier(f'{name}_adm{level}_tmp1'),
        table_out=Identifier(f'{name}_adm{level}_00'),
    ))
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'{name}_adm{level}_tmp1'),
    ))


def main(cur, name, level):
    for l in range(level, -1, -1):
        boundaries(cur, name, l)
    logger.info(f'{name}_adm{level}')
