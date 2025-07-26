from psycopg.sql import SQL, Identifier

from .utils import logging

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
    DROP TABLE IF EXISTS {table_tmp2};
    DROP TABLE IF EXISTS {table_tmp3};
"""


def main(conn, layer, level):
    for l in range(level, -1, -1):
        conn.execute(
            SQL(drop_tmp).format(
                table_tmp1=Identifier(f"{layer}_adm{l}_00"),
                table_tmp2=Identifier(f"{layer}_adm{l}_01"),
                table_tmp3=Identifier(f"{layer}_adm{l}_02"),
            ),
        )
    logger.info(f"{layer}_adm{level}")
