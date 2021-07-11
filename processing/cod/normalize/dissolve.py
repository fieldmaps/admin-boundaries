from psycopg2.sql import SQL, Identifier
from .utils import logging

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids},
        ST_Multi(
            ST_Union(geom)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in}
    GROUP BY {ids};
"""


def main(cur, name, level, _):
    for l in range(level-1, -1, -1):
        ids = map(lambda x: f'admin{x}Pcode', range(l, -1, -1))
        cur.execute(SQL(query_1).format(
            table_in=Identifier(f'{name}_adm{l+1}_00'),
            ids=SQL(',').join(map(Identifier, ids)),
            table_out=Identifier(f'{name}_adm{l}_00'),
        ))
    logger.info(name)
