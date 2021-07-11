import subprocess
from pathlib import Path
from .utils import logging, DATABASE

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
outputs = (cwd / '../../../data/geoboundaries/normalized').resolve()


def export_gpkg(name, level):
    file = (outputs / f'{name}.gpkg').resolve()
    file.unlink(missing_ok=True)
    for l in range(level+1):
        subprocess.run([
            'ogr2ogr',
            '-overwrite',
            '-nln', f'{name}_adm{l}',
            '-mapFieldType', 'DateTime=Date',
            file,
            f'PG:dbname={DATABASE}', f'{name}_adm{l}_01',
        ])


def export_multi(name, ext):
    gpkg = (outputs / f'{name}.gpkg').resolve()
    file = (outputs / f'{name}.{ext}').resolve()
    file.unlink(missing_ok=True)
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-lco', 'ENCODING=UTF-8',
        file,
        gpkg,
    ])


def main(cur, name, level):
    outputs.mkdir(exist_ok=True, parents=True)
    export_gpkg(name, level)
    export_multi(name, 'shp.zip')
    export_multi(name, 'xlsx')
    logger.info(f'{name}_adm{level}')
