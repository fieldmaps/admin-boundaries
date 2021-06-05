from psycopg2.sql import SQL, Identifier
from .utils import logging, get_ids

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids},
        (
            ST_MaximumInscribedCircle(geom)
        ).center::GEOMETRY(Point, 4326) AS geom
    FROM {table_in};
"""


def apply_queries(cur, src, name, level):
    for l in range(level, -1, -1):
        cur.execute(SQL(query_1).format(
            table_in=Identifier(f'{src}_{name}_adm{l}_polygons'),
            ids=SQL(',').join(map(Identifier, get_ids(l))),
            table_out=Identifier(f'{src}_{name}_adm{l}_points'),
        ))


def main(cur, src, name, level, ids):
    if ids is not None:
        for num in range(1, ids+1):
            apply_queries(cur, src, f'{name}_{num}', level)
    else:
        apply_queries(cur, src, name, level)
    logger.info(f'{name}_{src}')
