import subprocess
from pathlib import Path
from psycopg2.sql import SQL, Identifier
from .utils import logging, DATABASE

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids},
        ST_Multi(
            ST_ReducePrecision(geom, 0.000000001)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in};
"""
drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def main(cur, name, level, _):
    file = (cwd / f'../../../data/cod/originals/boundaries/{name}.gpkg')
    ids = map(lambda x: f'admin{x}Pcode', range(level, -1, -1))
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-makevalid',
        '-dim', 'XY',
        '-t_srs', 'EPSG:4326',
        '-nlt', 'PROMOTE_TO_MULTI',
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
        ids=SQL(',').join(map(Identifier, ids)),
        table_out=Identifier(f'{name}_adm{level}_00'),
    ))
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'{name}_adm{level}_tmp1'),
    ))
    logger.info(name)
