"""Shared utilities for the download stage."""

from pathlib import Path

from httpx import Client, Timeout

from app.config import HTTP_TIMEOUT

from .config import DOWNLOAD_CHUNK_SIZE, HTTP_CONNECT_TIMEOUT


def download_file(url: str, dest: Path) -> None:
    """Stream-download url to dest."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with (
        Client(
            http2=True,
            follow_redirects=True,
            timeout=Timeout(HTTP_TIMEOUT, connect=HTTP_CONNECT_TIMEOUT),
        ) as client,
        client.stream("GET", url) as r,
    ):
        r.raise_for_status()
        with dest.open("wb") as f:
            f.writelines(r.iter_bytes(chunk_size=DOWNLOAD_CHUNK_SIZE))
