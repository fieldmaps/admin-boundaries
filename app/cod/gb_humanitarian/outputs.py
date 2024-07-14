import shutil
import subprocess
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from psycopg.sql import SQL, Identifier, Literal

from .utils import DATABASE, adm0_list, logging

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
outputs = cwd / "../../../outputs/gb-humanitarian/originals"

query_1 = """
    DROP VIEW IF EXISTS {view_out};
    CREATE VIEW {view_out} AS
    SELECT
        {name} AS Name,
        {pcode} AS PCode,
        coalesce({level}) AS Level,
        geom
    FROM {table_in};
"""


def save_meta(name, level, output):
    r = next(x for x in adm0_list if x["id"] == name)
    text = f"""Boundary Representative of Year: {r['src_date'][:4]}
ISO-3166-1 (Alpha-3): {r['iso_3']}
Boundary Type: ADM{level}
Canonical Boundary Type Name:
Source 1: {r['src_name']}
Source 2: HDX
Release Type: gbHumanitarian
License: Creative Commons Attribution 3.0 Intergovernmental Organisations (CC BY 3.0 IGO)
License Notes:
License Source: {r['src_url']}
Link to Source Data: {r['src_url']}
Other Notes: """
    with open(output / "meta.txt", "w") as f:
        f.write(text)


def compress_output(name, level, output):
    zip_file = outputs / f"{name.upper()}_ADM{level}.zip"
    with ZipFile(zip_file, "w", ZIP_DEFLATED) as z:
        for ext in ["cpg", "dbf", "prj", "shp", "shx"]:
            file = output / f"{name.upper()}_ADM{level}.{ext}"
            z.write(file, file.name)
        z.write(output / "meta.txt", "meta.txt")


def main(conn, name, level, langs, *_):
    output = outputs / f"{name.upper()}_ADM{level}"
    shutil.rmtree(output, ignore_errors=True)
    output.mkdir(exist_ok=True, parents=True)
    file = output / f"{name.upper()}_ADM{level}.shp"
    conn.execute(
        SQL(query_1).format(
            table_in=Identifier(f"{name}_adm{level}_00"),
            name=Identifier(f"adm{level}_{langs[0]}"),
            pcode=Identifier(f"adm{level}_pcode"),
            level=Literal(f"ADM{level}"),
            view_out=Identifier(f"{name}_adm{level}_01"),
        )
    )
    subprocess.run(
        [
            "ogr2ogr",
            "-overwrite",
            *["-lco", "ENCODING=UTF-8"],
            file,
            f"PG:dbname={DATABASE}",
            f"{name}_adm{level}_01",
        ]
    )
    save_meta(name, level, output)
    compress_output(name, level, output)
    shutil.rmtree(output, ignore_errors=True)
    logger.info(f"{name}_adm{level}")
