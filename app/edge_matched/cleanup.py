from psycopg import connect
from psycopg.sql import SQL, Identifier

from .utils import DATABASE, geoms, logging

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1} CASCADE;
"""


def main(conn, dest, wld, row):
    name = row["id"]
    lvl = row["lvl"]
    for l in range(lvl + 1):
        for geom in [*geoms, "voronoi"]:
            conn.execute(
                SQL(drop_tmp).format(
                    table_tmp1=Identifier(f"{dest}_{name}_adm{l}_{geom}_{wld}"),
                ),
            )
    logger.info(f"{dest}_{wld}_{name}")


def adm0(dest, wld, geom):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    conn.execute(
        SQL(drop_tmp).format(
            table_tmp1=Identifier(f"{dest}_adm0_{geom}_{wld}"),
        ),
    )
    conn.execute(
        SQL(drop_tmp).format(
            table_tmp1=Identifier(f"{dest}_adm4_polygons_{wld}_area"),
        ),
    )
    conn.close()
    logger.info(f"{wld}_{geom}")


def admx(src, row):
    name = row["id"]
    lvl = row["lvl"]
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    conn.execute(
        SQL(drop_tmp).format(
            table_tmp1=Identifier(f"{src}_{name}_adm{lvl}_voronoi"),
        ),
    )
    conn.close()
    logger.info(f"{src}_{name}")


def dest_admx(dest, wld, lvl, geom):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    conn.execute(
        SQL(drop_tmp).format(
            table_tmp1=Identifier(f"{dest}_adm{lvl}_{geom}_{wld}"),
        ),
    )
    conn.close()
    logger.info(f"{dest}_{wld}_adm{lvl}_{geom}")
