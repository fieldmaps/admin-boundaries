from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import DATABASE, logging

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
    DROP TABLE IF EXISTS {table_tmp2};
    DROP TABLE IF EXISTS {table_tmp3};
"""


def main():
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'adm0_points'),
        table_tmp2=Identifier(f'adm0_lines'),
        table_tmp3=Identifier(f'adm0_polygons'),
    ))
    cur.close()
    con.close()
    logger.info('adm0')
