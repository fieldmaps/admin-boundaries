"""Configuration constants for the prepare (ingest) stage."""

from app._01_download.config import GDB_NAME
from app.config import HDX_DIR

HDX_GDB_DIR = HDX_DIR / f"{GDB_NAME}.gdb"

HDX_SRC_URL = "https://data.humdata.org/dataset/cod-ab-global"

COD_BASE = "https://data.fieldmaps.io/cod/extended"
GB_API = "https://www.geoboundaries.org/api/current/gbOpen/ALL/ALL/"
GB_BASE = "https://data.fieldmaps.io/geoboundaries/extended"
