from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import DATABASE, geoms, logging

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1} CASCADE;
"""


def main(cur, src, wld, row):
    name = row['id']
    lvl = row['lvl']
    for l in range(lvl+1):
        for geom in [*geoms, 'voronoi']:
            cur.execute(SQL(drop_tmp).format(
                table_tmp1=Identifier(f'{src}_{name}_adm{l}_{geom}_{wld}'),
            ))
    logger.info(f'{src}_{wld}_{name}')


def adm0(wld, geom):
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'adm0_{geom}_{wld}'),
    ))
    cur.close()
    con.close()
    logger.info(f'{wld}_{geom}')


def admx(src, row):
    name = row['id']
    lvl = row['lvl']
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'{src}_{name}_adm{lvl}_voronoi'),
    ))
    cur.close()
    con.close()
    logger.info(f'{src}_{name}')


def dest_admx(dest, wld, lvl, geom):
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'{dest}_adm{lvl}_{geom}_{wld}'),
    ))
    cur.close()
    con.close()
    logger.info(f'{dest}_{wld}_adm{lvl}_{geom}')
