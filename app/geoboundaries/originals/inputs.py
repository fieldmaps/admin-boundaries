import subprocess
from pathlib import Path

from psycopg.sql import SQL, Identifier

from .utils import DATABASE, logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent
inputs = cwd / f"../../../inputs/geoboundaries"

query_1 = """
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "shapeName" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "shapeISO" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "ADM1_NAME" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "ADM2_NAME" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "ADM3_NAME" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "ADM4_NAME" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "admin1Name" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "admin2Name" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "admin3Name" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "admin4Name" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "DISTRICT" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "DIST_34_NA" VARCHAR;
    ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS "PROV_34_NA" VARCHAR;
"""
query_2 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        fid,
        COALESCE("shapeName",
            "ADM1_NAME", "ADM2_NAME", "ADM3_NAME", "ADM4_NAME",
            "admin1Name", "admin2Name", "admin3Name", "admin4Name",
            "DISTRICT", "DIST_34_NA", "PROV_34_NA") AS "shapeName",
        "shapeISO",
        "shapeID",
        "shapeGroup",
        "shapeType",
        ST_Multi(
            ST_ReducePrecision(geom, 0.000000001)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in};
    CREATE INDEX ON {table_out} USING GIST(geom);
"""
query_3 = """
    SELECT max(count) FROM (
        SELECT count(*) FROM {table_in}
        GROUP BY {id}
    ) AS a
"""
drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def boundaries(conn, name, level):
    file = inputs / f"{name}_adm{level}.gpkg"
    subprocess.run(
        [
            "ogr2ogr",
            "-overwrite",
            "-makevalid",
            "-dim",
            "XY",
            "-t_srs",
            "EPSG:4326",
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-lco",
            "OVERWRITE=YES",
            "-lco",
            "FID=fid",
            "-lco",
            "GEOMETRY_NAME=geom",
            "-lco",
            "LAUNDER=NO",
            "-lco",
            "SPATIAL_INDEX=NONE",
            "-nln",
            f"{name}_adm{level}_tmp1",
            "-f",
            "PostgreSQL",
            f"PG:dbname={DATABASE}",
            file,
        ]
    )
    conn.execute(
        SQL(query_1).format(
            table_in=Identifier(f"{name}_adm{level}_tmp1"),
        )
    )
    conn.execute(
        SQL(query_2).format(
            table_in=Identifier(f"{name}_adm{level}_tmp1"),
            table_out=Identifier(f"{name}_adm{level}_00"),
        )
    )
    conn.execute(
        SQL(drop_tmp).format(
            table_tmp1=Identifier(f"{name}_adm{level}_tmp1"),
        )
    )
    has_duplicates = (
        conn.execute(
            SQL(query_3).format(
                table_in=Identifier(f"{name}_adm{level}_00"),
                id=Identifier("shapeID"),
            )
        ).fetchone()[0]
        > 1
    )
    if has_duplicates:
        logger.info(f"DUPLICATE shapeID: {name}_adm{level}")


def main(conn, name, level):
    for l in range(level, -1, -1):
        boundaries(conn, name, l)
    logger.info(f"{name}_adm{level}")
