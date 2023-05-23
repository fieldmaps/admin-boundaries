import shutil
import subprocess
from pathlib import Path

from .utils import DATABASE, logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent

layer_types = {
    "points": "Point",
    "lines": "MultiLineString",
    "polygons": "MultiPolygon",
}


def output_ogr(file, dest, wld, l, geom, id, mode):
    opts = (
        [
            *["--config", "OGR_ORGANIZE_POLYGONS", "ONLY_CCW"],
            *["-f", "OpenFileGDB"],
            *["-mapFieldType", "Integer64=Real"],
            "-unsetFid",
        ]
        if file.suffix == ".gdb"
        else ["-mapFieldType", "DateTime=Date"]
    )
    subprocess.run(
        [
            "ogr2ogr",
            "-makevalid",
            mode,
            *opts,
            *["-sql", f"SELECT * FROM {dest}_adm{l}_{geom}_{wld} ORDER BY {id};"],
            *["-nln", file.stem],
            *["-nlt", layer_types[geom]],
            file,
            f"PG:dbname={DATABASE}",
        ]
    )


def output_geom(file, dest, wld, lvl, geom):
    for l in range(lvl, -1, -1):
        id = f"adm{l}_id" if geom == "points" else f"adm{l-1}_id"
        id = "adm_id" if geom == "lines" and l == 0 else id
        output_ogr(file, dest, wld, l, geom, id, "-append")


def output_polygons(file, dest, wld, lvl, geom):
    id = f"adm{lvl}_id"
    output_ogr(file, dest, wld, lvl, geom, id, "-overwrite")


def main(dest, wld, lvl, geom):
    outputs = cwd / f"../../outputs/edge-matched/{dest}/{wld}"
    data = cwd / f"../../data/edge-matched/{dest}/{wld}"
    outputs.mkdir(exist_ok=True, parents=True)
    data.mkdir(exist_ok=True, parents=True)
    gpkg = outputs / f"adm{lvl}_{geom}.gpkg"
    gdb = outputs / f"adm{lvl}_{geom}.gdb"
    gpkg_data = data / f"adm{lvl}_{geom}.gpkg"
    gpkg.unlink(missing_ok=True)
    shutil.rmtree(gdb, ignore_errors=True)
    gpkg_data.unlink(missing_ok=True)
    if geom == "polygons":
        output_polygons(gpkg, dest, wld, lvl, geom)
        output_polygons(gdb, dest, wld, lvl, geom)
    else:
        output_geom(gpkg, dest, wld, lvl, geom)
        output_geom(gdb, dest, wld, lvl, geom)
    if dest == "humanitarian" and lvl == 4:
        shutil.copyfile(gpkg, gpkg_data)
    logger.info(f"{dest}_{wld}_adm{lvl}_{geom}")
