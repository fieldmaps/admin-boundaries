import subprocess

from psycopg import connect
from psycopg.sql import SQL, Identifier

from .utils import DATABASE, dest_list, logging

logger = logging.getLogger(__name__)

query_1 = """
    ALTER TABLE {table_out}
    DROP COLUMN IF EXISTS ogc_fid;
"""


def merge(dest, wld, lvl, geom):
    for row in dest_list[f"{dest}_{wld}"]:
        name = row["id"]
        if lvl <= row["lvl"]:
            subprocess.run(
                [
                    "ogr2ogr",
                    *["--config", "PG_USE_COPY", "YES"],
                    "-append",
                    *["-f", "PostgreSQL"],
                    *["-nln", f"{dest}_adm{lvl}_{geom}_{wld}"],
                    f"PG:dbname={DATABASE}",
                    *[f"PG:dbname={DATABASE}", f"{dest}_{name}_adm{lvl}_{geom}_{wld}"],
                ]
            )
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    conn.execute(
        SQL(query_1).format(
            table_out=Identifier(f"{dest}_adm{lvl}_{geom}_{wld}"),
        )
    )
    conn.close()


def merge_polygons(dest, lvl, wld):
    for row in dest_list[f"{dest}_{wld}"]:
        name = row["id"]
        l = min(lvl, row["lvl"])
        subprocess.run(
            [
                "ogr2ogr",
                *["--config", "PG_USE_COPY", "YES"],
                "-append",
                *["-f", "PostgreSQL"],
                *["-nln", f"{dest}_adm{lvl}_polygons_{wld}"],
                f"PG:dbname={DATABASE}",
                *[f"PG:dbname={DATABASE}", f"{dest}_{name}_adm{l}_polygons_{wld}"],
            ]
        )


def main(dest, wld, lvl, geom):
    if geom == "polygons":
        merge_polygons(dest, lvl, wld)
    else:
        merge(dest, wld, lvl, geom)
    logger.info(f"{dest}_{wld}_adm{lvl}_{geom}")
