"""Prepare cloud-native GeoParquet files from COD and GeoBoundaries GPKG.zip sources.

Usage:
    uv run prep_sources.py cod [ID ...]        # one or all COD entries
    uv run prep_sources.py geoboundaries [ID ...]  # one or all GB entries

After conversion, inspect locally before uploading:
    gdal vector info data/cod/afg.parquet

Upload (single):
    rclone copyto data/cod/afg.parquet \
        r2://fieldmaps-data/cod/extended/afg.parquet --s3-no-check-bucket

Upload (bulk, after review):
    rclone copy data/cod/ r2://fieldmaps-data/cod/extended/ \
        --s3-no-check-bucket --include "*.parquet"
    rclone copy data/geoboundaries/ \
        r2://fieldmaps-data/geoboundaries/extended/ \
        --s3-no-check-bucket --include "*.parquet"
"""

import logging
import sys
import zipfile
from pathlib import Path

import duckdb
import httpx

logger = logging.getLogger(__name__)

PARQUET_OPTS = "(COMPRESSION ZSTD, COMPRESSION_LEVEL 15, GEOPARQUET_VERSION V2)"

cwd = Path(__file__).parent

MIN_ARGS = 2

SOURCES = {
    "cod": {
        "url": "https://data.fieldmaps.io/cod/extended/{id}.gpkg.zip",
        "out_dir": cwd / "data/cod",
    },
    "geoboundaries": {
        "url": "https://data.fieldmaps.io/geoboundaries/extended/{id}.gpkg.zip",
        "out_dir": cwd / "data/geoboundaries",
    },
}


def download_zip(url: str, dest: Path) -> bool:
    """Stream-download url to dest, returning False on 404."""
    with (
        httpx.Client(follow_redirects=True, timeout=120) as client,
        client.stream("GET", url) as r,
    ):
        if r.status_code == httpx.codes.NOT_FOUND:
            return False
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_bytes():
                f.write(chunk)
    return True


def convert_country(
    country_id: str, src_key: str, conn: duckdb.DuckDBPyConnection
) -> bool:
    """Download and convert a single country source file to GeoParquet."""
    cfg = SOURCES[src_key]
    out_dir = cfg["out_dir"]
    out_dir.mkdir(parents=True, exist_ok=True)

    out_parquet = out_dir / f"{country_id}.parquet"
    if out_parquet.exists():
        logger.info("skip (exists): %s", country_id)
        return True

    url = cfg["url"].format(id=country_id)
    zip_path = out_dir / f"{country_id}.gpkg.zip"
    gpkg_path = out_dir / f"{country_id}.gpkg"

    ok = download_zip(url, zip_path)
    if not ok:
        logger.warning("not found: %s", url)
        return False

    with zipfile.ZipFile(zip_path) as z:
        z.extractall(out_dir)
    zip_path.unlink(missing_ok=True)

    if not gpkg_path.exists():
        logger.warning("gpkg missing after extract: %s", gpkg_path)
        return False

    try:
        conn.execute(f"""
            COPY (SELECT * RENAME (geom AS geometry) FROM ST_Read('{gpkg_path}'))
            TO '{out_parquet}'
            {PARQUET_OPTS}
        """)
    except Exception:
        logger.exception("duckdb failed for %s", country_id)
        return False
    finally:
        gpkg_path.unlink(missing_ok=True)

    logger.info("converted: %s", out_parquet)
    return True


def get_ids(src_key: str) -> list[str]:
    """Fetch the list of country IDs from fieldmaps.io for a given source."""
    urls = {
        "cod": "https://data.fieldmaps.io/cod.json",
        "geoboundaries": "https://data.fieldmaps.io/geoboundaries.json",
    }
    with httpx.Client(follow_redirects=True, timeout=60) as client:
        r = client.get(urls[src_key])
        r.raise_for_status()
    return sorted(entry["id"] for entry in r.json())


def main() -> None:
    """Entry point: parse args, fetch IDs if needed, and convert each country."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if len(sys.argv) < MIN_ARGS or sys.argv[1] not in SOURCES:
        logger.error("Usage: prep_sources.py <%s> [ID ...]", "|".join(SOURCES))
        sys.exit(1)

    src_key = sys.argv[1]
    id_args = sys.argv[2:]

    if id_args:
        ids = id_args
    else:
        logger.info("fetching ID list from fieldmaps.io")
        ids = get_ids(src_key)

    conn = duckdb.connect()
    conn.execute("LOAD spatial;")

    logger.info("processing %s entries [%s]", len(ids), src_key)
    for country_id in ids:
        convert_country(country_id, src_key, conn)


if __name__ == "__main__":
    main()
