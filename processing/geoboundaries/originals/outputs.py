import subprocess
import shutil
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
    for l in range(level+1):
        subprocess.run([
            'ogr2ogr',
            '-overwrite',
            '-nln', f'{name}_adm{l}',
            file,
            f'PG:dbname={DATABASE}', f'{name}_adm{l}_01',
        ])


def export_output(name, ext):
    gpkg = data / f'{name}.gpkg'
    tmp = outputs / f'{name}.{ext}'
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        tmp,
        gpkg,
    ])
    file = outputs / f'{name}.{ext}.zip'
    file.unlink(missing_ok=True)
    with ZipFile(file, 'w', ZIP_DEFLATED) as z:
        z.write(tmp, tmp.name)
    tmp.unlink(missing_ok=True)


def export_shp(name):
    gpkg = data / f'{name}.gpkg'
    file = outputs / f'{name}.shp.zip'
    file.unlink(missing_ok=True)
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-lco', 'ENCODING=UTF-8',
        file,
        gpkg,
    ])


def export_svg(name, level):
    tmp = outputs / f'{name}_tmp'
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(exist_ok=True, parents=True)
    for l in range(level+1):
        file = tmp / f'{name}_adm{l}.shp'
        file_svg = tmp / f'{name}_adm{l}.svg'
        subprocess.run([
            'ogr2ogr',
            '-overwrite',
            '-nln', f'{name}_adm{l}',
            '-lco', 'ENCODING=UTF-8',
            file,
            f'PG:dbname={DATABASE}', f'{name}_adm{l}_01',
        ])
        subprocess.run([
            'mapshaper',
            file,
            '-quiet',
            '-style', 'fill="#fff"', 'stroke="#000"',
            '-o', file_svg,
        ])
    file_zip = outputs / f'{name}.svg.zip'
    file_zip.unlink(missing_ok=True)
    with ZipFile(file_zip, 'w', ZIP_DEFLATED) as z:
        for l in range(level, -1, -1):
            file_svg = tmp / f'{name}_adm{l}.svg'
            z.write(file_svg, file_svg.name)
            file_svg.unlink(missing_ok=True)
    shutil.rmtree(tmp, ignore_errors=True)


def main(_, name, level):
    data.mkdir(exist_ok=True, parents=True)
    outputs.mkdir(exist_ok=True, parents=True)
    export_data(name, level)
    export_output(name, 'gpkg')
    export_output(name, 'xlsx')
    export_shp(name)
    export_svg(name, level)
    logger.info(f'{name}_adm{level}')
