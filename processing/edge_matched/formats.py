import subprocess
from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
from .utils import logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def export_gpkg(outputs, name):
    gpkg = outputs / f'{name}.gpkg'
    gpkg_zip = outputs / f'{name}.gpkg.zip'
    gpkg_zip.unlink(missing_ok=True)
    with ZipFile(gpkg_zip, 'w', ZIP_DEFLATED) as z:
        z.write(gpkg, gpkg.name)


def export_shp(outputs, lvl, geom):
    shp_zip = outputs / f'adm{lvl}_{geom}.shp.zip'
    shp_zip.unlink(missing_ok=True)
    with ZipFile(shp_zip, 'w', ZIP_DEFLATED) as z:
        start = lvl if geom == 'polygons' else 0
        for l in range(start, lvl+1):
            name = f'adm{l}_{geom}'
            for ext in ['cpg', 'dbf', 'prj', 'shp', 'shx']:
                shp_part = outputs / f'{name}.{ext}'
                z.write(shp_part, shp_part.name)


def export_xlsx(outputs, name):
    gpkg = outputs / f'{name}.gpkg'
    file = outputs / f'{name}.xlsx'
    file.unlink(missing_ok=True)
    subprocess.run(['ogr2ogr', '-overwrite', file, gpkg])


def cleanup(dest, wld, lvl, geom):
    outputs = cwd / f'../../outputs/edge-matched/{dest}/{wld}'
    gpkg = outputs / f'adm{lvl}_{geom}.gpkg'
    gpkg.unlink(missing_ok=True)
    for ext in ['cpg', 'dbf', 'prj', 'shp', 'shx']:
        shp_part = outputs / f'adm{lvl}_{geom}.{ext}'
        shp_part.unlink(missing_ok=True)


def main(dest, wld, lvl, geom):
    outputs = cwd / f'../../outputs/edge-matched/{dest}/{wld}'
    name = f'adm{lvl}_{geom}'
    export_gpkg(outputs, name)
    export_shp(outputs, lvl, geom)
    export_xlsx(outputs, name)
    gpkg = outputs / f'adm{lvl}_{geom}.gpkg'
    gpkg.unlink(missing_ok=True)
    if geom == 'polygons':
        cleanup(dest, wld, lvl, geom)
    logger.info(f'{dest}_{wld}_adm{lvl}_{geom}')
