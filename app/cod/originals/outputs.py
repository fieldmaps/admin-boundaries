import shutil
import subprocess
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from .utils import DATABASE, logging

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
data = cwd / "../../../data/cod/originals"
outputs = cwd / "../../../outputs/cod/originals"


def export_data(name, level):
    gpkg = data / f"{name}.gpkg"
    gdb = data / f"{name}.gdb"
    gpkg.unlink(missing_ok=True)
    for lvl in range(level, -1, -1):
        subprocess.run(
            [
                "ogr2ogr",
                "-overwrite",
                *["-nln", f"{name}_adm{lvl}"],
                gpkg,
                *[f"PG:dbname={DATABASE}", f"{name}_adm{lvl}"],
            ]
        )
        subprocess.run(
            [
                "ogr2ogr",
                "-overwrite",
                "-unsetFid",
                *["--config", "OGR_ORGANIZE_POLYGONS", "ONLY_CCW"],
                *["-f", "OpenFileGDB"],
                *["-mapFieldType", "Integer64=Real,Date=DateTime"],
                *["-nln", f"{name}_adm{lvl}"],
                gdb,
                *[f"PG:dbname={DATABASE}", f"{name}_adm{lvl}"],
            ]
        )


def export_geojson(name, level):
    geojson_data = data / f"{name}.geojson"
    shutil.rmtree(geojson_data, ignore_errors=True)
    geojson_data.mkdir(exist_ok=True, parents=True)
    for lvl in range(level, -1, -1):
        subprocess.run(
            [
                "ogr2ogr",
                "-overwrite",
                *["-nln", f"{name}_adm{lvl}"],
                geojson_data / f"{name}_adm{lvl}.geojson",
                *[f"PG:dbname={DATABASE}", f"{name}_adm{lvl}"],
            ]
        )
    zip_file(name, "geojson")


def export_ogr(name, file):
    gpkg = data / f"{name}.gpkg"
    subprocess.run(["ogr2ogr", "-overwrite", "-lco", "ENCODING=UTF-8", file, gpkg])


def zip_file(name, ext, delete=True):
    file = data / f"{name}.{ext}"
    file_zip = outputs / f"{name}.{ext}.zip"
    file_zip.unlink(missing_ok=True)
    if file.is_file():
        with ZipFile(file_zip, "w", ZIP_DEFLATED) as z:
            z.write(file, file.name)
        if delete:
            file.unlink(missing_ok=True)
    if file.is_dir():
        shutil.make_archive(str(file_zip.with_suffix("")), "zip", file)
        if delete:
            shutil.rmtree(file, ignore_errors=True)


def main(conn, name, level, level_max, _):
    level = level_max or level
    data.mkdir(exist_ok=True, parents=True)
    outputs.mkdir(exist_ok=True, parents=True)
    export_data(name, level)
    export_geojson(name, level)
    export_ogr(name, outputs / f"{name}.xlsx")
    export_ogr(name, outputs / f"{name}.shp.zip")
    zip_file(name, "gpkg", False)
    zip_file(name, "gdb")
    logger.info(name)
