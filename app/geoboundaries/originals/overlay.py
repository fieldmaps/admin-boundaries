from psycopg.sql import SQL, Identifier

from .utils import logging

logger = logging.getLogger(__name__)


def base(conn, name, level):
    query_1 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT
            geom,
            "shapeID" AS {adm_id},
            "shapeName" AS {adm_name}
        FROM {table_in};
        CREATE INDEX ON {table_out} USING GIST(geom);
    """
    conn.execute(
        SQL(query_1).format(
            table_in=Identifier(f"{name}_adm{level}_00"),
            adm_id=Identifier(f"adm{level}_id"),
            adm_name=Identifier(f"adm{level}_name"),
            table_out=Identifier(f"{name}_adm{level}_tmp{level}"),
        ),
    )


def overlay(conn, name, level):
    query_1 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT DISTINCT ON (a.{adm_id_level})
            a.*,
            b."shapeID" AS {adm_id},
            b."shapeName" AS {adm_name}
        FROM {table_in1} AS a
        JOIN {table_in2} AS b
        ON ST_Intersects(a.geom, b.geom)
        ORDER BY
            a.{adm_id_level},
            ST_Area(ST_Intersection(a.geom, b.geom)) DESC;
        CREATE INDEX ON {table_out} USING GIST(geom);
    """
    for l in range(level - 1, 0, -1):
        conn.execute(
            SQL(query_1).format(
                table_in1=Identifier(f"{name}_adm{level}_tmp{l + 1}"),
                table_in2=Identifier(f"{name}_adm{l}_00"),
                adm_id_level=Identifier(f"adm{level}_id"),
                adm_id=Identifier(f"adm{l}_id"),
                adm_name=Identifier(f"adm{l}_name"),
                table_out=Identifier(f"{name}_adm{level}_tmp{l}"),
            ),
        )


def adm0(conn, name, level):
    query_1 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT
            a.*,
            b."shapeID" AS adm0_id,
            b."shapeName" AS adm0_name
        FROM
            {table_in1} AS a,
            {table_in2} AS b;
    """
    conn.execute(
        SQL(query_1).format(
            table_in1=Identifier(f"{name}_adm{level}_tmp1"),
            table_in2=Identifier(f"{name}_adm0_00"),
            table_out=Identifier(f"{name}_adm{level}_01"),
        ),
    )


def cleanup(conn, name, level):
    drop_tmp = """
        DROP TABLE IF EXISTS {table_tmp1};
    """
    for l in range(level, 0, -1):
        conn.execute(
            SQL(drop_tmp).format(
                table_tmp1=Identifier(f"{name}_adm{level}_tmp{l}"),
            ),
        )


def main(conn, name, level):
    base(conn, name, level)
    overlay(conn, name, level)
    adm0(conn, name, level)
    cleanup(conn, name, level)
    logger.info(f"{name}_adm{level}")
