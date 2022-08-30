from psycopg2.sql import SQL, Identifier
from processing.cod.originals.utils import logging

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP VIEW IF EXISTS {view};
    DROP VIEW IF EXISTS {view_shp};
    DROP TABLE IF EXISTS {table_attr};
    DROP TABLE IF EXISTS {table_attr_shp};
    DROP TABLE IF EXISTS {table_00};
"""


def main(cur, name, level, _):
    for l in range(level, -1, -1):
        cur.execute(SQL(drop_tmp).format(
            view=Identifier(f'{name}_adm{l}'),
            view_shp=Identifier(f'{name}_adm{l}_shp'),
            table_attr=Identifier(f'{name}_adm{l}_attr'),
            table_attr_shp=Identifier(f'{name}_adm{l}_attr_shp'),
            table_00=Identifier(f'{name}_adm{l}_00'),
        ))
    logger.info(name)
