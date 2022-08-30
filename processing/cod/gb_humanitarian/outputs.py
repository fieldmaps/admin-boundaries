import shutil
import subprocess
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from psycopg2.sql import SQL, Identifier, Literal
from processing.cod.gb_humanitarian.utils import logging, meta, DATABASE

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
outputs = cwd / '../../../data/cod/gb_humanitarian'

query_1 = """
    DROP VIEW IF EXISTS {view_out};
    CREATE VIEW {view_out} AS
    SELECT
        {name} AS Name,
        {pcode} AS PCode,
        coalesce({level}) AS Level,
        geom
    FROM {table_in};
"""


def save_meta(name, level, output):
    r = next(x for x in meta if x['id'] == name)
    text = f"""Boundary Representative of Year: {r['src_date'][:4]}
ISO-3166-1 (Alpha-3): {r['iso3']}
Boundary Type: ADM{level}
Canonical Boundary Type Name:
Source 1: {r['src_name']}
Source 2: HDX
Release Type: gbHumanitarian
License: {r['src_lic']}
License Notes:
License Source: {r['src_url']}
Link to Source Data: {r['src_url']}
Other Notes: """
    with open(output / 'meta.txt', 'w') as f:
        f.write(text)


def compress_output(name, level, output):
    zip_file = outputs / f'{name.upper()}_ADM{level}.zip'
    with ZipFile(zip_file, 'w', ZIP_DEFLATED) as z:
        for ext in ['cpg', 'dbf', 'prj', 'shp', 'shx']:
            file = output / f'{name.upper()}_ADM{level}.{ext}'
            z.write(file, file.name)
        z.write(output / 'meta.txt', 'meta.txt')


def main(cur, name, level, langs, *_):
    output = outputs / f'{name.upper()}_ADM{level}'
    shutil.rmtree(output, ignore_errors=True)
    output.mkdir(exist_ok=True, parents=True)
    file = output / f'{name.upper()}_ADM{level}.shp'
    cur.execute(SQL(query_1).format(
        table_in=Identifier(f'{name}_adm{level}_00'),
        name=Identifier(f'admin{level}name_{langs[0]}'),
        pcode=Identifier(f'admin{level}pcode'),
        level=Literal(f'ADM{level}'),
        view_out=Identifier(f'{name}_adm{level}_01'),
    ))
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-lco', 'ENCODING=UTF-8',
        file,
        f'PG:dbname={DATABASE}', f'{name}_adm{level}_01',
    ])
    save_meta(name, level, output)
    compress_output(name, level, output)
    shutil.rmtree(output, ignore_errors=True)
    logger.info(f'{name}_adm{level}')
