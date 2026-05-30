"""Shared utilities for the prepare (ingest) stage."""

from pathlib import Path

from app.utils import get_conn


def load_metadata(meta_csv: Path) -> dict:
    """Return {iso3: row_dict} with standardized field names from the metadata CSV."""
    conn = get_conn()
    rows = conn.execute(f"""--sql
        SELECT
            country_iso3 AS iso3,
            TRY_CAST(admin_level_full AS INTEGER) AS src_lvl,
            TRY_CAST(date_source AS DATE) AS src_date,
            TRY_CAST(date_updated AS DATE) AS src_update,
            COALESCE(source, '') AS src_name,
            COALESCE(contributor, '') AS src_name1,
            COALESCE(methodology_pcodes, '') AS src_lic
        FROM read_csv('{meta_csv}', nullstr=['', '#N/A'])
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY country_iso3
            ORDER BY TRY_CAST(date_updated AS DATE) DESC NULLS LAST
        ) = 1
    """).fetchall()
    conn.close()
    return {
        row[0]: {
            "src_lvl": row[1],
            "src_date": row[2],
            "src_update": row[3],
            "src_name": row[4],
            "src_name1": row[5],
            "src_lic": row[6],
        }
        for row in rows
        if row[0] is not None
    }
