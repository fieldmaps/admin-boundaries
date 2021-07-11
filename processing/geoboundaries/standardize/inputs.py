import subprocess
from pathlib import Path
from .utils import logging, DATABASE

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent
inputs = (cwd / f'../../../data/geoboundaries/normalized').resolve()


def main(cur, name, level, _):
    file = (inputs / f'{name}.gpkg').resolve()
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-lco', 'SPATIAL_INDEX=NONE',
        '-nln', f'{name}_adm{level}_00',
        '-f', 'PostgreSQL', f'PG:dbname={DATABASE}',
        file, f'{name}_adm{level}',
    ])
    logger.info(f'{name}_adm{level}')
