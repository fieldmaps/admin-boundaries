"""Shared utilities for the export stage."""

from app.config import BUILD_DB
from app.utils import get_conn


def get_land_date() -> str:
    """Return the latest `wld_date` from the build DB's adm0_wld table."""
    conn = get_conn(BUILD_DB, read_only=True)
    try:
        result = conn.execute("SELECT MAX(wld_date) FROM adm0_wld").fetchone()
    finally:
        conn.close()
    return str(result[0])[:10] if result and result[0] else ""
