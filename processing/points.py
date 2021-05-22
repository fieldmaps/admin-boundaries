from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import DATABASE, logging, get_ids

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


def main(src, name, level):
    con = connect(database=DATABASE)
    cur = con.cursor()
    for l in range(level, -1, -1):
        cur.execute(SQL(query_1).format(
            table_in=Identifier(f'{src}_{name}_adm{l}_polygons'),
            ids=SQL(',').join(map(Identifier, get_ids(l))),
            table_out=Identifier(f'{src}_{name}_adm{l}_points'),
        ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'{name}_{src}')
