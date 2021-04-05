from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import logging

logger = logging.getLogger(__name__)


def main(levels, layers):
    logger.info(f'Starting cleanup')
    con = connect(database='edge_matcher')
    cur = con.cursor()
    drop_tmp = """
        DROP TABLE IF EXISTS {table_tmp1};
    """
    for layer in layers:
        for level in levels:
            cur.execute(SQL(drop_tmp).format(
                table_tmp1=Identifier(f'adm{level}_{layer}'),
            ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'Finished cleanup')
