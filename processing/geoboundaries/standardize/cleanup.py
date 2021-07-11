from psycopg2.sql import SQL, Identifier
from .utils import logging, filter_config

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP VIEW IF EXISTS {table_tmp1};
    DROP TABLE IF EXISTS {table_tmp2};
    DROP TABLE IF EXISTS {table_tmp3} CASCADE;
"""


def main(cur, name, level, _):
    if name in filter_config.keys():
        for name2 in filter_config[name]['layers']:
            cur.execute(SQL(drop_tmp).format(
                table_tmp1=Identifier(f'{name2}_adm{level}_02'),
                table_tmp2=Identifier(f'{name2}_adm{level}_01'),
                table_tmp3=Identifier(f'{name}_adm{level}_00'),
            ))
    else:
        cur.execute(SQL(drop_tmp).format(
            table_tmp1=Identifier(f'{name}_adm{level}_02'),
            table_tmp2=Identifier(f'{name}_adm{level}_01'),
            table_tmp3=Identifier(f'{name}_adm{level}_00'),
        ))
    logger.info(f'{name}_adm{level}')
