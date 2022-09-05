from psycopg.sql import SQL, Identifier
from processing.edge_matched.utils import logging, get_src_ids, get_wld_ids

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        ST_ReducePrecision(
            (ST_MaximumInscribedCircle(geom)).center
        , 0.000000001)::GEOMETRY(Point, 4326) AS geom
    FROM {table_in};
"""


def main(conn, src, wld, row):
    name = row['id']
    lvl = row['lvl']
    for l in range(lvl, -1, -1):
        conn.execute(SQL(query_1).format(
            table_in=Identifier(f'{src}_{name}_adm{l}_polygons_{wld}'),
            ids_src=SQL(',').join(map(Identifier, get_src_ids(l))),
            ids_wld=SQL(',').join(map(Identifier, get_wld_ids(False))),
            table_out=Identifier(f'{src}_{name}_adm{l}_points_{wld}'),
        ))
    logger.info(f'{src}_{wld}_{name}')
