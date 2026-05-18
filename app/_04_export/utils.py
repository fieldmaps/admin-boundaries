"""Shared utilities for the export stage."""

import duckdb

from app.config import BUILD_DB


def get_land_date() -> str:
    """Return the latest `wld_date` from the build DB's adm0_wld table."""
    conn = duckdb.connect(str(BUILD_DB), read_only=True)
    try:
        result = conn.execute("SELECT MAX(wld_date) FROM adm0_wld").fetchone()
    finally:
        conn.close()
    return str(result[0])[:10] if result and result[0] else ""
