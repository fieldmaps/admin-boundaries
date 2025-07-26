import logging
import subprocess
from pathlib import Path

from psycopg.sql import SQL, Identifier, Literal

from .utils import DATABASE, get_cols

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent

query_0 = """
    ALTER TABLE {table_out} ADD COLUMN IF NOT EXISTS date DATE DEFAULT {date};
    ALTER TABLE {table_out} ADD COLUMN IF NOT EXISTS validon DATE DEFAULT {update};
    ALTER TABLE {table_out} ADD COLUMN IF NOT EXISTS validto DATE DEFAULT NULL;
"""

query_0a = """
    ALTER TABLE {table_out} RENAME COLUMN {column_old} TO {column_new};
"""

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids},
        ST_Multi(
            ST_ReducePrecision(geom, 0.000000001)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in};
    CREATE INDEX ON {table_out} USING GIST(geom);
"""

query_2 = """
    SELECT max(count) FROM (
        SELECT count(*) FROM {table_in}
        GROUP BY {id}
    ) AS a;
"""

drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def run(conn, name, level, row):
    file = cwd / f"../../../inputs/cod/{name}_adm{level}.gpkg"
    file_custom = cwd / f"../../../inputs/cod_custom/{name}_adm{level}.gpkg"
    if file_custom.is_file():
        file = file_custom
    ids = get_cols(level, row)
    subprocess.run(
        [
            "ogr2ogr",
            "-overwrite",
            "-makevalid",
            *["-dim", "XY"],
            *["-t_srs", "EPSG:4326"],
            *["-nlt", "PROMOTE_TO_MULTI"],
            *["-lco", "FID=fid"],
            *["-lco", "GEOMETRY_NAME=geom"],
            *["-lco", "SPATIAL_INDEX=NONE"],
            *["-lco", "PRECISION=NO"],
            *["-nln", f"{name}_adm{level}_tmp1"],
            *["-f", "PostgreSQL", f"PG:dbname={DATABASE}"],
            file,
        ],
        check=False,
    )
    conn.execute(
        SQL(query_0).format(
            date=Literal(row["src_date"]),
            update=Literal(row["src_update"]),
            table_out=Identifier(f"{name}_adm{level}_tmp1"),
        ),
    )
    for col in ids:
        conn.execute(
            SQL(query_0a).format(
                column_old=Identifier(col.lower()),
                column_new=Identifier(col),
                table_out=Identifier(f"{name}_adm{level}_tmp1"),
            ),
        )
    conn.execute(
        SQL(query_1).format(
            table_in=Identifier(f"{name}_adm{level}_tmp1"),
            ids=SQL(",").join(map(Identifier, ids)),
            table_out=Identifier(f"{name}_adm{level}"),
        ),
    )
    conn.execute(
        SQL(drop_tmp).format(
            table_tmp1=Identifier(f"{name}_adm{level}_tmp1"),
        ),
    )
    has_duplicates = (
        conn.execute(
            SQL(query_2).format(
                table_in=Identifier(f"{name}_adm{level}"),
                id=Identifier(f"ADM{level}_PCODE"),
            ),
        ).fetchone()[0]
        > 1
    )
    if has_duplicates:
        logger.info(f"DUPLICATE ADM{level}_PCODE: {name}")
        raise RuntimeError(f"DUPLICATE ADM{level}_PCODE: {name}")
    logger.info(name)


def main(conn, name, level, level_max, row):
    run(conn, name, level, row)
    if level_max is not None:
        run(conn, name, level_max, row)
