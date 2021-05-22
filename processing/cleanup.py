from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import DATABASE, logging, geoms

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def main(src, name, level):
    con = connect(database=DATABASE)
    cur = con.cursor()
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'{src}_{name}_adm{level}_voronoi'),
    ))
    for l in range(level+1):
        for geom in geoms:
            cur.execute(SQL(drop_tmp).format(
                table_tmp1=Identifier(f'{src}_{name}_adm{l}_{geom}'),
            ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'{name}_{src}')
