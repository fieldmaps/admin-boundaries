"""Shared paths and constants used across pipeline stages."""

import os
from pathlib import Path

PARQUET_OPTS = "(COMPRESSION ZSTD, COMPRESSION_LEVEL 15, GEOPARQUET_VERSION V2)"
os.environ["OGR_ORGANIZE_POLYGONS"] = "ONLY_CCW"

PKG_DIR = Path(__file__).parent
TMP_DIR = PKG_DIR.parent / "tmp"
OUTPUTS_DIR = PKG_DIR.parent / "outputs"

HDX_DIR = TMP_DIR / "hdx"
PREPARE_DB = TMP_DIR / "prepare.duckdb"
BUILD_DB = TMP_DIR / "build.duckdb"
EDGE_MATCHED_DIR = OUTPUTS_DIR / "edge-matched"

GEOMS = ("polygons", "lines", "points")
HTTP_TIMEOUT = 60
WLD = os.getenv("WLD", "intl")
