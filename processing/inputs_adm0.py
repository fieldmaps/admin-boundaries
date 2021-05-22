import subprocess
from pathlib import Path
from .utils import DATABASE, logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def main():
    file = (cwd / f'../inputs/adm0/adm0_polygons.gpkg').resolve()
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-makevalid',
        '-dim', 'XY',
        '-t_srs', 'EPSG:4326',
        '-nlt', 'PROMOTE_TO_MULTI',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-f', 'PostgreSQL', f'PG:dbname={DATABASE}',
        file,
    ])
    logger.info('polygons')
