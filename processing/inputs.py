import subprocess
from pathlib import Path
from .utils import DATABASE, logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def main(src, name, level):
    file = (cwd / f'../data/{src}/extended/{name}.gpkg').resolve()
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-makevalid',
        '-dim', 'XY',
        '-t_srs', 'EPSG:4326',
        '-nlt', 'PROMOTE_TO_MULTI',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-nln', f'{src}_{name}_adm{level}_voronoi',
        '-f', 'PostgreSQL', f'PG:dbname={DATABASE}',
        file,
    ])
    logger.info(f'{name}_{src}')
