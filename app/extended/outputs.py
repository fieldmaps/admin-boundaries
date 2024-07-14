import subprocess
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from .utils import logging

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent


def export_single(inputs, outputs, name, ext):
    gpkg = inputs / f"{name}.gpkg"
    file_zip = outputs / f"{name}.{ext}.zip"
    file_zip.unlink(missing_ok=True)
    with ZipFile(file_zip, "w", ZIP_DEFLATED) as z:
        z.write(gpkg, gpkg.name)


def export_multi(inputs, outputs, name, ext):
    gpkg = inputs / f"{name}.gpkg"
    file = outputs / f"{name}.{ext}"
    file.unlink(missing_ok=True)
    subprocess.run(["ogr2ogr", "-overwrite", "-lco", "ENCODING=UTF-8", file, gpkg])


def main(src, name):
    inputs = cwd / f"../../data/{src}/extended"
    outputs = cwd / f"../../outputs/{src}/extended"
    outputs.mkdir(exist_ok=True, parents=True)
    export_single(inputs, outputs, name, "gpkg")
    export_single(inputs, outputs, name, "geojson")
    export_single(inputs, outputs, name, "gdb")
    export_multi(inputs, outputs, name, "shp.zip")
    export_multi(inputs, outputs, name, "xlsx")
    logger.info(f"{src}_{name}")
