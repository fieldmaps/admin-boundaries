from psycopg.sql import SQL, Identifier

from .utils import logging

logger = logging.getLogger(__name__)


def get_ids(level):
    result = []
    for l in range(level, -1, -1):
        result += [f"adm{l}_id", f"adm{l}_name"]
    return result


def main(conn, name, level):
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
    for l in range(level - 1, -1, -1):
        conn.execute(
            SQL(query_1).format(
                table_in=Identifier(f"{name}_adm{level}_01"),
                ids=SQL(",").join(map(Identifier, get_ids(l))),
                table_out=Identifier(f"{name}_adm{l}_01"),
            ),
        )
    logger.info(f"{name}_adm{level}")
