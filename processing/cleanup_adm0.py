from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import DATABASE, logging

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def main():
    con = connect(database=DATABASE)
    cur = con.cursor()
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'adm0_polygons'),
    ))
    con.commit()
    cur.close()
    con.close()
    logger.info('polygons')
