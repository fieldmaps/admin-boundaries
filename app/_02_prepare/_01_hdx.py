"""Build prepare.duckdb's `admin` and `metadata` tables from the HDX Global GDB."""

from logging import getLogger

import duckdb

from app._01_download.config import META_NAME
from app.config import HDX_DIR

from .config import HDX_GDB_DIR, HDX_SRC_URL
from .utils import get_conn, load_metadata

logger = getLogger(__name__)


ADMIN_SCHEMA_SQL = """--sql
CREATE OR REPLACE TABLE admin (
    iso3 VARCHAR,
    adm0_id VARCHAR, adm0_src VARCHAR,
    adm0_name VARCHAR, adm0_name1 VARCHAR, adm0_name2 VARCHAR,
    adm1_id VARCHAR, adm1_src VARCHAR,
    adm1_name VARCHAR, adm1_name1 VARCHAR, adm1_name2 VARCHAR,
    adm2_id VARCHAR, adm2_src VARCHAR,
    adm2_name VARCHAR, adm2_name1 VARCHAR, adm2_name2 VARCHAR,
    adm3_id VARCHAR, adm3_src VARCHAR,
    adm3_name VARCHAR, adm3_name1 VARCHAR, adm3_name2 VARCHAR,
    adm4_id VARCHAR, adm4_src VARCHAR,
    adm4_name VARCHAR, adm4_name1 VARCHAR, adm4_name2 VARCHAR,
    src_lvl INTEGER,
    src_lang VARCHAR, src_lang1 VARCHAR, src_lang2 VARCHAR,
    src_date DATE, src_update DATE,
    src_name VARCHAR, src_name1 VARCHAR, src_lic VARCHAR, src_url VARCHAR,
    src_grp VARCHAR,
    geom GEOMETRY
)
"""

METADATA_SCHEMA_SQL = """--sql
CREATE OR REPLACE TABLE metadata (
    iso3 VARCHAR,
    src VARCHAR,
    lvl INTEGER,
    src_date DATE,
    src_update DATE,
    src_name VARCHAR,
    src_name1 VARCHAR,
    src_lic VARCHAR,
    src_url VARCHAR
)
"""


def main() -> set[str]:
    """Build admin + metadata tables in prepare.duckdb from the extracted HDX GDB.

    Assumes the GDB has already been downloaded and extracted by _01_download.
    Returns the full set of COD-AB iso3 codes from the HDX metadata CSV
    (used downstream to compute the COD fallback set).
    """
    meta_csv = HDX_DIR / f"{META_NAME}.csv"

    conn = get_conn("prepare", reset=True)
    conn.execute(ADMIN_SCHEMA_SQL)
    conn.execute(METADATA_SCHEMA_SQL)

    logger.info("loading HDX metadata")
    metadata = load_metadata(meta_csv)
    _load_metadata_table(conn, metadata)

    logger.info("loading HDX admin4 → admin (via ST_Read on GDB)")
    _load_admin_table(conn)

    logger.info("generating hierarchical IDs")
    conn.execute(_id_gen_ctas_sql())

    conn.close()
    logger.info("HDX → prepare.duckdb done")
    return set(metadata.keys())


def _id_gen_ctas_sql() -> str:
    """Build a CTAS that rewrites adm{0..4}_id with hierarchical IDs.

    adm0_id := ISO3 || '-' || YYYYMMDD (or just ISO3 when src_date is NULL).
    adm{N}_id (N>=1) := parent_id || '-' || zero-padded sequence within parent.
    Rows where adm{N}_src is NULL keep adm{N}_id NULL.
    """
    ctes: list[str] = [
        """--sql
        adm0_done AS (
            SELECT * REPLACE (
                CASE WHEN src_date IS NULL THEN iso3
                     ELSE iso3 || '-' || strftime(src_date, '%Y%m%d')
                END AS adm0_id
            )
            FROM admin
        )
        """,
    ]
    for lvl in range(1, 5):
        parents = ", ".join(f"adm{n}_id" for n in range(lvl))
        ctes.append(f"""--sql
        adm{lvl}_ranked AS (
            SELECT *,
                DENSE_RANK() OVER (
                    PARTITION BY iso3, {parents}
                    ORDER BY adm{lvl}_src
                ) AS seq{lvl}
            FROM adm{lvl - 1}_done
        ),
        adm{lvl}_widths AS (
            SELECT iso3,
                CAST(LENGTH(CAST(MAX(seq{lvl}) AS VARCHAR)) AS INTEGER) AS pad{lvl}
            FROM adm{lvl}_ranked
            WHERE adm{lvl}_src IS NOT NULL
            GROUP BY iso3
        ),
        adm{lvl}_done AS (
            SELECT * EXCLUDE (seq{lvl}, pad{lvl})
                   REPLACE (
                       CASE WHEN adm{lvl}_src IS NULL THEN NULL
                            ELSE adm{lvl - 1}_id || '-'
                                 || LPAD(CAST(seq{lvl} AS VARCHAR), pad{lvl}, '0')
                       END AS adm{lvl}_id
                   )
            FROM adm{lvl}_ranked
            LEFT JOIN adm{lvl}_widths USING (iso3)
        )
        """)
    return (
        "CREATE OR REPLACE TABLE admin AS WITH "
        + ",".join(ctes)
        + " SELECT * FROM adm4_done"
    )


def _load_metadata_table(conn: duckdb.DuckDBPyConnection, metadata: dict) -> None:
    """Insert one row per HDX country into the metadata table."""
    rows = [
        (
            iso3,
            "hdx",
            row["src_lvl"],
            row.get("src_date"),
            row.get("src_update"),
            row.get("src_name", ""),
            row.get("src_name1", ""),
            row.get("src_lic", ""),
            HDX_SRC_URL,
        )
        for iso3, row in metadata.items()
        if row.get("src_lvl") is not None and row["src_lvl"] >= 1
    ]
    if not rows:
        logger.warning("no HDX countries with valid src_lvl in metadata")
        return
    conn.executemany("INSERT INTO metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    logger.info("metadata: %s HDX countries", len(rows))


def _load_admin_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Insert HDX admin4 rows into the admin table with canonical column rename.

    Reads the admin4 layer directly from the extracted GDB via ST_Read; no
    intermediate Parquet is produced. Each `adm{N}_pcode` is duplicated into
    both `adm{N}_id` (later rewritten by _id_gen_ctas_sql) and `adm{N}_src`.
    """
    pcode_select = ", ".join(
        f"a.adm{n}_pcode AS adm{n}_id, a.adm{n}_pcode AS adm{n}_src, "
        f"a.adm{n}_name, a.adm{n}_name1, a.adm{n}_name2"
        for n in range(5)
    )
    conn.execute(f"""--sql
        INSERT INTO admin BY NAME
        SELECT a.iso3,
               {pcode_select},
               m.lvl AS src_lvl,
               a.lang AS src_lang, a.lang1 AS src_lang1, a.lang2 AS src_lang2,
               CAST(a.valid_on AS DATE) AS src_date,
               CAST(a.valid_to AS DATE) AS src_update,
               m.src_name, m.src_name1, m.src_lic,
               '{HDX_SRC_URL}' AS src_url,
               'COD' AS src_grp,
               a.geom
        FROM ST_Read('{HDX_GDB_DIR}', layer='admin4') a
        JOIN metadata m ON m.iso3 = a.iso3
        WHERE m.src = 'hdx'
    """)
    n = conn.execute("SELECT COUNT(*) FROM admin").fetchall()[0][0]
    logger.info("admin: %s HDX rows inserted", n)
