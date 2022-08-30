import subprocess
from pathlib import Path
from processing.cod.standardize.utils import logging, DATABASE

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def main(_, name, level, *__):
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-makevalid',
        '-dim', 'XY',
        '-t_srs', 'EPSG:4326',
        '-nlt', 'PROMOTE_TO_MULTI',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-lco', 'SPATIAL_INDEX=NONE',
        '-nln', f'{name}_adm{level}_00',
        '-f', 'PostgreSQL', f'PG:dbname={DATABASE}',
        cwd / f'../../../data/cod/originals/{name}.gpkg',
        f'{name}_adm{level}',
    ])
