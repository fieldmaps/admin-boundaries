import subprocess
from pathlib import Path
from psycopg2.sql import SQL, Identifier
from .utils import logging, filter_config, DATABASE

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
outputs = (cwd / '../../../data/cod/standardized')

query_1 = """
    DROP VIEW IF EXISTS {view_out};
    CREATE VIEW {view_out} AS
    SELECT
        b.*,
        a.geom
    FROM {table_in1} as a
    RIGHT JOIN {table_in2} as b
    ON a.{id1} = b.{id2}
    ORDER BY a.{id1};
"""


def create_export(file, cur, name1, name2, level):
    cur.execute(SQL(query_1).format(
        table_in1=Identifier(f'{name1}_adm{level}_00'),
        table_in2=Identifier(f'{name2}_adm{level}_01'),
        id1=Identifier(f'admin{level}pcode'),
        id2=Identifier(f'adm{level}_src'),
        view_out=Identifier(f'{name2}_adm{level}_02'),
    ))
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-mapFieldType', 'DateTime=Date',
        '-nln', f'{name2}_adm{level}',
        file,
        f'PG:dbname={DATABASE}', f'{name2}_adm{level}_02',
    ])


def main(cur, name, level, *_):
    outputs.mkdir(exist_ok=True, parents=True)
    file = (outputs / f'{name}.gpkg')
    file.unlink(missing_ok=True)
    if name in filter_config.keys():
        for name2 in filter_config[name]['layers']:
            create_export(file, cur, name, name2, level)
    else:
        create_export(file, cur, name, name, level)
    logger.info(f'{name}_adm{level}')
