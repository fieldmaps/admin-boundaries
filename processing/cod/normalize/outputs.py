import subprocess
import shutil
from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
from psycopg2.sql import SQL, Identifier
from .utils import logging, DATABASE

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
outputs = (cwd / '../../../data/cod/normalized').resolve()

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


def export_multi(name, level, ext):
    file = (outputs / f'{name}.{ext}').resolve()
    file.unlink(missing_ok=True)
    for l in range(level+1):
        subprocess.run([
            'ogr2ogr',
            '-overwrite',
            '-nln', f'{name}_adm{l}',
            '-mapFieldType', 'DateTime=Date',
            file,
            f'PG:dbname={DATABASE}', f'{name}_adm{l}',
        ])


def export_shp(name, level):
    tmp = (outputs / f'{name}_shp').resolve()
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(exist_ok=True, parents=True)
    for l in range(level+1):
        file = (tmp / f'{name}_adm{l}.shp').resolve()
        subprocess.run([
            'ogr2ogr',
            '-overwrite',
            '-lco', 'ENCODING=UTF-8',
            '-mapFieldType', 'DateTime=Date',
            file,
            f'PG:dbname={DATABASE}', f'{name}_adm{l}_shp',
        ])
    file_zip = (outputs / f'{name}.shp.zip').resolve()
    file_zip.unlink(missing_ok=True)
    with ZipFile(file_zip, 'w', ZIP_DEFLATED) as z:
        for l in range(level, -1, -1):
            for ext in ['cpg', 'dbf', 'prj', 'shp', 'shx']:
                file = (tmp / f'{name}_adm{l}.{ext}').resolve()
                z.write(file, file.name)
                file.unlink(missing_ok=True)
    shutil.rmtree(tmp, ignore_errors=True)


def main(cur, name, level, _):
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
    export_multi(name, level, 'gpkg')
    export_multi(name, level, 'xlsx')
    export_shp(name, level)
    logger.info(name)
