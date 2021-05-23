import subprocess
from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
from .utils import logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def export_gpkg(outputs, name):
    file = (outputs / f'{name}.gpkg').resolve()
    file_zip = (outputs / f'{name}.gpkg.zip').resolve()
    file_zip.unlink(missing_ok=True)
    with ZipFile(file_zip, 'w', ZIP_DEFLATED) as z:
        z.write(file, file.name)


def export_shp(outputs, name):
    file = (outputs / f'{name}.shp.zip').resolve()
    file.unlink(missing_ok=True)
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-lco', 'ENCODING=UTF-8',
        file,
        (outputs / f'{name}.gpkg').resolve(),
    ])


def export_multi(outputs, name, ext):
    file = (outputs / f'{name}.{ext}').resolve()
    file.unlink(missing_ok=True)
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        file,
        (outputs / f'{name}.gpkg').resolve(),
    ])
    file_zip = (outputs / f'{name}.{ext}.zip').resolve()
    file_zip.unlink(missing_ok=True)
    with ZipFile(file_zip, 'w', ZIP_DEFLATED) as z:
        z.write(file, file.name)
    file.unlink(missing_ok=True)


def main(dest, geom):
    outputs = (cwd / f'../outputs/{dest}').resolve()
    name = f'adm_{geom}'
    export_gpkg(outputs, name)
    export_multi(outputs, name, 'xlsx')
    export_shp(outputs, name)
    logger.info(f'{dest}_{geom}')
