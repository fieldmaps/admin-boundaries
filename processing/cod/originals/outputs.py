import subprocess
import shutil
from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
from psycopg2.sql import SQL, Identifier
from .utils import logging, DATABASE

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
data = cwd / '../../../data/cod/originals'
outputs = cwd / '../../../outputs/cod/originals'

query_1 = """
    DROP VIEW IF EXISTS {view_out};
    CREATE VIEW {view_out} AS
    SELECT DISTINCT ON (b.{id2})
        b.*,
        a.geom
    FROM {table_in1} as a
    LEFT JOIN {table_in2} as b
    ON a.{id1} = b.{id2};
"""


def export_data(name, level):
    file = data / f'{name}.gpkg'
    file.unlink(missing_ok=True)
    for l in range(level, -1, -1):
        subprocess.run([
            'ogr2ogr',
            '-overwrite',
            '-nln', f'{name}_adm{l}',
            file,
            f'PG:dbname={DATABASE}', f'{name}_adm{l}',
        ])


def export_gpkg(name):
    gpkg = data / f'{name}.gpkg'
    file = outputs / f'{name}.gpkg.zip'
    file.unlink(missing_ok=True)
    with ZipFile(file, 'w', ZIP_DEFLATED) as z:
        z.write(gpkg, gpkg.name)


def export_xlsx(name):
    gpkg = data / f'{name}.gpkg'
    file = outputs / f'{name}.xlsx'
    subprocess.run(['ogr2ogr', '-overwrite', file, gpkg])


def export_shp(name, level):
    tmp = outputs / f'{name}_tmp'
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(exist_ok=True, parents=True)
    for l in range(level+1):
        file = tmp / f'{name}_adm{l}.shp'
        subprocess.run([
            'pgsql2shp', '-k', '-q',
            '-f', file,
            DATABASE,
            f'{name}_adm{l}_shp',
        ])
    file_zip = outputs / f'{name}.shp.zip'
    file_zip.unlink(missing_ok=True)
    with ZipFile(file_zip, 'w', ZIP_DEFLATED) as z:
        for l in range(level, -1, -1):
            for ext in ['cpg', 'dbf', 'prj', 'shp', 'shx']:
                file = tmp / f'{name}_adm{l}.{ext}'
                z.write(file, file.name)
    shutil.rmtree(tmp, ignore_errors=True)


def main(cur, name, level, _):
    data.mkdir(exist_ok=True, parents=True)
    outputs.mkdir(exist_ok=True, parents=True)
    for l in range(level, -1, -1):
        cur.execute(SQL(query_1).format(
            table_in1=Identifier(f'{name}_adm{l}_00'),
            table_in2=Identifier(f'{name}_adm{l}_attr'),
            id1=Identifier(f'admin{l}Pcode'),
            id2=Identifier(f'admin{l}Pcode'),
            view_out=Identifier(f'{name}_adm{l}'),
        ))
        cur.execute(SQL(query_1).format(
            table_in1=Identifier(f'{name}_adm{l}_00'),
            table_in2=Identifier(f'{name}_adm{l}_attr_shp'),
            id1=Identifier(f'admin{l}Pcode'),
            id2=Identifier(f'ADM{l}_PCODE'),
            view_out=Identifier(f'{name}_adm{l}_shp'),
        ))
    export_data(name, level)
    export_gpkg(name)
    export_shp(name, level)
    export_xlsx(name)
    logger.info(name)
