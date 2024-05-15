import logging
import subprocess
from pathlib import Path

from .utils import is_polygon

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
outputs = cwd / "../../../inputs/cod"
outputs.mkdir(exist_ok=True, parents=True)


def run(iso_3: str, lvl: int, idx: int | None, filename: str, url: str):
    idx = idx + lvl if idx is not None else lvl
    return subprocess.run(
        [
            "ogr2ogr",
            "-overwrite",
            "-makevalid",
            *["--config", "OGR_GEOJSON_MAX_OBJ_SIZE", "2048MB"],
            *["-dim", "XY"],
            *["-mapFieldType", "DateTime=Date"],
            *["-nln", filename],
            *["-nlt", "PROMOTE_TO_MULTI"],
            *["-oo", "FEATURE_SERVER_PAGING=YES"],
            *["-t_srs", "EPSG:4326"],
            outputs / f"{filename}.gpkg",
            f"https://codgis.itos.uga.edu/arcgis/rest/services/{url}/{iso_3}_pcode/FeatureServer/{idx}/query?where=1=1&outFields=*&f=json",
        ],
        stderr=subprocess.DEVNULL,
    )


def download(iso_3: str, lvl: int, idx: int | None):
    filename = f"{iso_3}_adm{lvl}".lower()
    success = False
    for i in range(3):
        result = run(iso_3, lvl, idx, filename, "COD_External")
        if result.returncode == 0:
            success = True
            break
        else:
            result = run(iso_3, lvl, idx, filename, "COD_NO_GEOM_CHECK")
            if result.returncode == 0:
                success = True
                break
    if success:
        logger.info(filename)
        if not is_polygon(outputs / f"{filename}.gpkg"):
            (outputs / f"{filename}.gpkg").unlink()
            logger.info(f"NOT POLYGON: {filename}")
    else:
        logger.info(f"NOT DOWNLOADED: {filename}")


def main(row):
    download(row["iso_3"], row["src_lvl"], row["src_api_idx"])
    if row["src_lvl_max"] is not None:
        download(row["iso_3"], row["src_lvl_max"], row["src_api_idx"])
