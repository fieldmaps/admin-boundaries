import subprocess
from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
from .utils import logging

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent


def export_gpkg(inputs, outputs, name):
    gpkg = inputs / f'{name}.gpkg'
    file = outputs / f'{name}.gpkg.zip'
    file.unlink(missing_ok=True)
    with ZipFile(file, 'w', ZIP_DEFLATED) as z:
        z.write(gpkg, gpkg.name)


def export_multi(inputs, outputs, name, ext):
    gpkg = inputs / f'{name}.gpkg'
    file = outputs / f'{name}.{ext}'
    file.unlink(missing_ok=True)
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-lco', 'ENCODING=UTF-8',
        file,
        gpkg,
    ])


def main(src, name):
    inputs = cwd / f'../../data/{src}/extended'
    outputs = cwd / f'../../outputs/{src}/extended'
    outputs.mkdir(exist_ok=True, parents=True)
    export_gpkg(inputs, outputs, name)
    export_multi(inputs, outputs, name, 'shp.zip')
    logger.info(name)
