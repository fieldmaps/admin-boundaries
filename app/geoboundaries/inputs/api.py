import subprocess
from pathlib import Path

from .utils import logging

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
outputs = cwd / "../../../inputs/geoboundaries"
outputs.mkdir(exist_ok=True, parents=True)


def main(row):
    filename = f"{row['iso_3']}_adm{row['src_lvl']}".lower()
    subprocess.run(
        [
            "ogr2ogr",
            "-overwrite",
            "-makevalid",
            "--config",
            "OGR_GEOJSON_MAX_OBJ_SIZE",
            "2048MB",
            "-dim",
            "XY",
            "-t_srs",
            "EPSG:4326",
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-nln",
            filename,
            outputs / f"{filename}.gpkg",
            row["src_url1"],
        ]
    )
    logger.info(filename)
