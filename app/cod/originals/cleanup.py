from psycopg.sql import SQL, Identifier

from .utils import logging

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP TABLE IF EXISTS {view};
    DROP TABLE IF EXISTS {table_00};
    DROP TABLE IF EXISTS {table_tmp1};
"""


def main(conn, name, level, level_max, _):
    level = level_max or level
    for lvl in range(level, -1, -1):
        conn.execute(
            SQL(drop_tmp).format(
                view=Identifier(f"{name}_adm{lvl}"),
                table_00=Identifier(f"{name}_adm{lvl}_00"),
                table_tmp1=Identifier(f"{name}_adm{lvl}_tmp1"),
            ),
        )
    logger.info(name)
