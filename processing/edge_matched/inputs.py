import subprocess
from pathlib import Path
from psycopg import connect
from psycopg.sql import SQL, Identifier
from processing.edge_matched.utils import DATABASE, logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent

query_1 = """
    ALTER TABLE {table_out}
    DROP COLUMN IF EXISTS fid;
"""


def adm0(wld, geom):
    file = cwd / f'../../../adm0-generator/data/osm/{wld}/adm0_{geom}.gpkg'
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-makevalid',
        '-dim', 'XY',
        '-t_srs', 'EPSG:4326',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-nln', f'adm0_{geom}_{wld}',
        '-f', 'PostgreSQL', f'PG:dbname={DATABASE}',
        file,
    ])
    conn = connect(f'dbname={DATABASE}', autocommit=True)
    conn.execute(SQL(query_1).format(
        table_out=Identifier(f'adm0_{geom}_{wld}'),
    ))
    conn.close()
    logger.info(f'{wld}_{geom}')


def admx(src, row):
    name = row['id']
    lvl = row['lvl']
    file = cwd / f'../../data/{src}/extended/{name}.gpkg'
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-makevalid',
        '-dim', 'XY',
        '-t_srs', 'EPSG:4326',
        '-nlt', 'PROMOTE_TO_MULTI',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-nln', f'{src}_{name}_adm{lvl}_voronoi',
        '-f', 'PostgreSQL', f'PG:dbname={DATABASE}',
        file,
    ])
    logger.info(f'{src}_{name}')
