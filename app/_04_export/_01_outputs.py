"""Copy build.duckdb's global tables out as GeoParquet files."""

from logging import getLogger

import duckdb

from app.config import BUILD_DB, EDGE_MATCHED_DIR, GEOMS, PARQUET_OPTS, WLD
from app.utils import get_conn

logger = getLogger(__name__)


def main() -> None:
    """Write outputs/edge-matched/{WLD}/adm{N}_{geom}.parquet for every table."""
    conn = get_conn(BUILD_DB, read_only=True)
    try:
        out_dir = EDGE_MATCHED_DIR / WLD
        out_dir.mkdir(parents=True, exist_ok=True)
        for lvl in range(5):
            for geom in GEOMS:
                table = f"adm{lvl}_{geom}"
                dest = out_dir / f"{table}.parquet"
                try:
                    conn.execute(f"""--sql
                        COPY (
                            SELECT * FROM {table}
                            ORDER BY adm0_id, adm1_id, adm2_id, adm3_id, adm4_id
                        ) TO '{dest}' {PARQUET_OPTS}
                    """)
                    logger.info("wrote %s", dest.name)
                except duckdb.Error as e:
                    logger.warning("skip %s: %s", table, e)
    finally:
        conn.close()
