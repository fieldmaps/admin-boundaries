from psycopg2.sql import SQL, Identifier
from .utils import geoms

drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def apply_queries(cur, src, name, level):
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'{src}_{name}_adm{level}_voronoi'),
    ))
    for l in range(level+1):
        for geom in geoms:
            cur.execute(SQL(drop_tmp).format(
                table_tmp1=Identifier(f'{src}_{name}_adm{l}_{geom}'),
            ))


def main(cur, src, name, level, ids):
    if ids is not None:
        for num in range(1, ids+1):
            apply_queries(cur, src, f'{name}_{num}', level)
    else:
        apply_queries(cur, src, name, level)
