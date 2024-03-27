import shutil
import subprocess
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from .utils import DATABASE, logging

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
data = cwd / "../../../data/geoboundaries/originals"
outputs = cwd / "../../../outputs/geoboundaries/originals"


def export_data(name, level):
    file = data / f"{name}.gpkg"
    file.unlink(missing_ok=True)
    for l in range(level, -1, -1):
        subprocess.run(
            [
                "ogr2ogr",
                "-overwrite",
                *["-nln", f"{name}_adm{l}"],
                file,
                f"PG:dbname={DATABASE}",
                f"{name}_adm{l}_01",
            ]
        )


def export_gpkg(name):
    gpkg = data / f"{name}.gpkg"
    file = outputs / f"{name}.gpkg.zip"
    file.unlink(missing_ok=True)
    with ZipFile(file, "w", ZIP_DEFLATED) as z:
        z.write(gpkg, gpkg.name)


def export_shp(name, level):
    tmp = outputs / f"{name}_tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(exist_ok=True, parents=True)
    for l in range(level + 1):
        file = tmp / f"{name}_adm{l}.shp"
        subprocess.run(
            [
                "pgsql2shp",
                *["-k", "-q", "-f"],
                file,
                DATABASE,
                f"{name}_adm{l}_01",
            ]
        )
    file_zip = outputs / f"{name}.shp.zip"
    file_zip.unlink(missing_ok=True)
    with ZipFile(file_zip, "w", ZIP_DEFLATED) as z:
        for l in range(level, -1, -1):
            for ext in ["cpg", "dbf", "prj", "shp", "shx"]:
                file = tmp / f"{name}_adm{l}.{ext}"
                z.write(file, file.name)
    shutil.rmtree(tmp, ignore_errors=True)


def export_xlsx(name):
    gpkg = data / f"{name}.gpkg"
    file = outputs / f"{name}.xlsx"
    subprocess.run(["ogr2ogr", "-overwrite", file, gpkg])


def main(_, name, level):
    data.mkdir(exist_ok=True, parents=True)
    outputs.mkdir(exist_ok=True, parents=True)
    export_data(name, level)
    export_gpkg(name)
    export_shp(name, level)
    export_xlsx(name)
    logger.info(f"{name}_adm{level}")
