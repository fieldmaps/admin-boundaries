import subprocess
from pathlib import Path
from .utils import logging, DATABASE

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent
inputs = cwd / f'../../../data/geoboundaries/originals'


def main(_, name, level, __):
    file = inputs / f'{name}.gpkg'
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
