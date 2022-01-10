import subprocess
from pathlib import Path
from .utils import DATABASE, logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def output_geom(gpkg, shp, dest, wld, lvl, geom):
    for l in range(lvl, -1, -1):
        id = f'adm{l}_id' if geom == 'points' else f'adm{l-1}_id'
        id = 'fid_1' if geom == 'lines' and l == 0 else id
        subprocess.run([
            'ogr2ogr',
            '-append',
            '-makevalid',
            '-mapFieldType', 'DateTime=Date',
            '-sql',
            f'SELECT * FROM {dest}_adm{l}_{geom}_{wld} ORDER BY {id}',
            '-nln', f'adm{l}_{geom}',
            gpkg,
            f'PG:dbname={DATABASE}',
        ])
    id = f'adm{lvl}_id' if geom == 'points' else f'adm{lvl-1}_id'
    id = 'fid_1' if geom == 'lines' and lvl == 0 else id
    subprocess.run([
        'pgsql2shp', '-k', '-q',
        '-f', shp,
        DATABASE,
        f'SELECT * FROM {dest}_adm{lvl}_{geom}_{wld} ORDER BY {id}',
    ])


def output_polygons(gpkg, shp, dest, lvl, wld):
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-makevalid',
        '-mapFieldType', 'DateTime=Date',
        '-sql',
        f'SELECT * FROM {dest}_adm{lvl}_polygons_{wld} ORDER BY adm{lvl}_id',
        '-nln', f'adm{lvl}_polygons',
        gpkg,
        f'PG:dbname={DATABASE}',
    ])
    subprocess.run([
        'pgsql2shp', '-k', '-q',
        '-f', shp,
        DATABASE,
        f'SELECT * FROM {dest}_adm{lvl}_polygons_{wld} ORDER BY adm{lvl}_id',
    ])


def main(dest, wld, lvl, geom):
    outputs = cwd / f'../../outputs/edge-matched/{dest}/{wld}'
    outputs.mkdir(exist_ok=True, parents=True)
    gpkg = outputs / f'adm{lvl}_{geom}.gpkg'
    shp = outputs / f'adm{lvl}_{geom}.shp'
    gpkg.unlink(missing_ok=True)
    for ext in ['cpg', 'dbf', 'prj', 'shp', 'shx']:
        shp_part = outputs / f'adm{lvl}_{geom}.{ext}'
        shp_part.unlink(missing_ok=True)
    if geom == 'polygons':
        output_polygons(gpkg, shp, dest, lvl, wld)
    else:
        output_geom(gpkg, shp, dest, wld, lvl, geom)
    logger.info(f'{dest}_{wld}_adm{lvl}_{geom}')
