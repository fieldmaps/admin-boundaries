from psycopg2.sql import SQL, Identifier
from .utils import logging

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP VIEW IF EXISTS {table_tmp1};
    DROP TABLE IF EXISTS {table_tmp2} CASCADE;
"""


def main(cur, name, level, *_):
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'{name}_adm{level}_01'),
        table_tmp2=Identifier(f'{name}_adm{level}_00'),
    ))
    logger.info(f'{name}_adm{level}')
