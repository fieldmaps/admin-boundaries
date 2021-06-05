import subprocess
from pathlib import Path
from .utils import DATABASE, logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def import_layer(file, src, name, level):
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
        file, f'{name}_adm{level}'
    ])


def main(cur, src, name, level, ids):
    file = (cwd / f'../data/{src}/extended/{name}.gpkg').resolve()
    if ids is not None:
        for num in range(1, ids+1):
            import_layer(file, src, f'{name}_{num}', level)
    else:
        import_layer(file, src, name, level)
    logger.info(f'{name}_{src}')
