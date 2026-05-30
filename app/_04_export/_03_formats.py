"""Convert global GeoParquets to GPKG, GDB, and XLSX via DuckDB's built-in GDAL."""

from logging import getLogger
from pathlib import Path
from shutil import make_archive, rmtree
from zipfile import ZIP_DEFLATED, ZipFile

import duckdb

from app.config import EDGE_MATCHED_DIR, GEOMS, WLD
from app.utils import get_conn

logger = getLogger(__name__)

_GEOM_TYPE = {
    "polygons": "MULTIPOLYGON",
    "lines": "MULTILINESTRING",
    "points": "POINT",
}


def main() -> None:
    """Generate GPKG, GDB, and XLSX for every (lvl, geom) combination."""
    conn = get_conn()
    out_dir = EDGE_MATCHED_DIR / WLD
    for lvl in range(5):
        for geom in GEOMS:
            parquet = out_dir / f"adm{lvl}_{geom}.parquet"
            if not parquet.exists():
                continue
            _export_one(conn, parquet, geom)
            logger.info("%s_adm%s_%s", WLD, lvl, geom)
    conn.close()


def _zip(path: Path) -> None:
    zip_path = path.with_suffix(path.suffix + ".zip")
    zip_path.unlink(missing_ok=True)
    if path.is_file():
        with ZipFile(zip_path, "w", ZIP_DEFLATED) as z:
            z.write(path, path.name)
    elif path.is_dir():
        make_archive(str(zip_path.with_suffix("")), "zip", path)


def _export_one(conn: duckdb.DuckDBPyConnection, parquet: Path, geom: str) -> None:
    src = f"read_parquet('{parquet}')"

    gpkg = parquet.with_suffix(".gpkg")
    gpkg.unlink(missing_ok=True)
    conn.execute(f"COPY (SELECT * FROM {src}) TO '{gpkg}' (FORMAT GDAL, DRIVER 'GPKG')")
    _zip(gpkg)

    gdb = parquet.with_suffix(".gdb")
    rmtree(gdb, ignore_errors=True)
    geom_type = _GEOM_TYPE[geom]
    conn.execute(f"""--sql
        COPY (SELECT * EXCLUDE geom, ST_MakeValid(geom) AS geom FROM {src})
        TO '{gdb}' (FORMAT GDAL, DRIVER 'OpenFileGDB', GEOMETRY_TYPE '{geom_type}')
    """)
    _zip(gdb)
    rmtree(gdb, ignore_errors=True)

    xlsx = parquet.with_suffix(".xlsx")
    xlsx.unlink(missing_ok=True)
    conn.execute(f"""--sql
        COPY (SELECT * EXCLUDE geom FROM {src})
        TO '{xlsx}' (FORMAT GDAL, DRIVER 'XLSX')
    """)
