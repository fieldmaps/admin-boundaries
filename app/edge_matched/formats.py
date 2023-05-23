import shutil
import subprocess
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from .utils import logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def zip_ogr(outputs, name):
    file = outputs / name
    file_zip = outputs / f"{name}.zip"
    file_zip.parent.mkdir(parents=True, exist_ok=True)
    if file.is_file():
        with ZipFile(file_zip, "w", ZIP_DEFLATED) as z:
            z.write(file, file.name)
    if file.is_dir():
        shutil.make_archive(str(file_zip.with_suffix("")), "zip", file)


def export_xlsx(outputs, name):
    gpkg = outputs / f"{name}.gpkg"
    file = outputs / f"{name}.xlsx"
    file.unlink(missing_ok=True)
    subprocess.run(["ogr2ogr", "-overwrite", file, gpkg])


def cleanup(dest, wld, lvl, geom):
    outputs = cwd / f"../../outputs/edge-matched/{dest}/{wld}"
    gpkg = outputs / f"adm{lvl}_{geom}.gpkg"
    gpkg.unlink(missing_ok=True)
    gdb = outputs / f"adm{lvl}_{geom}.gdb"
    shutil.rmtree(gdb, ignore_errors=True)


def main(dest, wld, lvl, geom):
    outputs = cwd / f"../../outputs/edge-matched/{dest}/{wld}"
    name = f"adm{lvl}_{geom}"
    zip_ogr(outputs, f"{name}.gpkg")
    zip_ogr(outputs, f"{name}.gdb")
    export_xlsx(outputs, name)
    logger.info(f"{dest}_{wld}_adm{lvl}_{geom}")
