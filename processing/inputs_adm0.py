import subprocess
from pathlib import Path
from .utils import DATABASE, logging, geoms

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def import_adm0(geom):
    file = (cwd / f'../data/adm0/adm0_{geom}.gpkg').resolve()
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-makevalid',
        '-dim', 'XY',
        '-t_srs', 'EPSG:4326',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-f', 'PostgreSQL', f'PG:dbname={DATABASE}',
        file,
    ])
    logger.info(f'adm0_{geom}')


def main():
    for geom in geoms:
        import_adm0(geom)
