import subprocess
from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
from .utils import logging, DATABASE

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
data = cwd / '../../../data/geoboundaries/originals'
outputs = cwd / '../../../outputs/geoboundaries/originals'


def export_data(name, level):
    file = data / f'{name}.gpkg'
    file.unlink(missing_ok=True)
    for l in range(level, -1, -1):
        subprocess.run([
            'ogr2ogr',
            '-overwrite',
            '-nln', f'{name}_adm{l}',
            file,
            f'PG:dbname={DATABASE}', f'{name}_adm{l}_01',
        ])


def export_gpkg(name):
    gpkg = data / f'{name}.gpkg'
    file = outputs / f'{name}.gpkg.zip'
    file.unlink(missing_ok=True)
    with ZipFile(file, 'w', ZIP_DEFLATED) as z:
        z.write(gpkg, gpkg.name)


def export_multi(name, ext):
    gpkg = data / f'{name}.gpkg'
    file = outputs / f'{name}.{ext}'
    file.unlink(missing_ok=True)
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-lco', 'ENCODING=UTF-8',
        file,
        gpkg,
    ])


def main(_, name, level):
    data.mkdir(exist_ok=True, parents=True)
    outputs.mkdir(exist_ok=True, parents=True)
    export_data(name, level)
    export_gpkg(name)
    export_multi(name, 'shp.zip')
    export_multi(name, 'xlsx')
    logger.info(f'{name}_adm{level}')
