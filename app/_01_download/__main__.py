"""Download HDX GDB + metadata CSV to tmp/hdx/."""

from zipfile import ZipFile

from httpx import Client

from app.config import HDX_DIR, HTTP_TIMEOUT

from .config import GDB_NAME, HDX_PACKAGE, META_NAME
from .utils import download_file


def main() -> None:
    """Download GDB zip and metadata CSV from HDX, then extract GDB."""
    with Client(http2=True, follow_redirects=True, timeout=HTTP_TIMEOUT) as client:
        resp = client.get(HDX_PACKAGE)
        resp.raise_for_status()
        resources = resp.json()["result"]["resources"]
    gdb_url = next(
        r["url"]
        for r in resources
        if r["name"].endswith(".gdb.zip") and "extended_latest" in r["name"]
    )
    meta_url = next(
        r["url"]
        for r in resources
        if r["name"].endswith(".csv") and "metadata_latest" in r["name"]
    )
    gdb_zip = HDX_DIR / f"{GDB_NAME}.gdb.zip"
    meta_csv = HDX_DIR / f"{META_NAME}.csv"
    download_file(gdb_url, gdb_zip)
    download_file(meta_url, meta_csv)
    with ZipFile(gdb_zip) as z:
        z.extractall(HDX_DIR)
    gdb_zip.unlink()


if __name__ == "__main__":
    main()
