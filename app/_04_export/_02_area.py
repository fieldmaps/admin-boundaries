"""Calculate area statistics from the global adm4 polygons and write Excel."""

from logging import getLogger

import duckdb

from app.config import EDGE_MATCHED_DIR, WLD

from .config import AREA_CRS

logger = getLogger(__name__)


def main() -> None:
    """Write area stats."""
    parquet = EDGE_MATCHED_DIR / WLD / "adm4_polygons.parquet"
    if not parquet.exists():
        return

    out_dir = EDGE_MATCHED_DIR / WLD
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()
    conn.execute("LOAD spatial;")
    df = conn.execute(f"""--sql
        SELECT
            adm4_id, adm3_id, adm2_id, adm1_id, adm0_id, iso_3,
            ST_Area(ST_Transform(geom, 'EPSG:4326', '{AREA_CRS}')) / 1000000 AS area_km
        FROM read_parquet('{parquet}')
    """).df()
    conn.close()

    if df.empty:
        return

    df1 = (
        df.groupby("adm0_id", dropna=False)
        .sum(numeric_only=True, min_count=1)
        .reset_index()
    )
    df1 = df1.rename(columns={"area_km": "area_0_km"})
    df1["area_0_km"] = df1["area_0_km"].astype(int)
    for lvl in range(1, 5):
        df2 = df[["adm0_id", f"adm{lvl}_id"]].drop_duplicates(subset=[f"adm{lvl}_id"])
        df2 = df2.groupby("adm0_id", dropna=False).count().reset_index()
        df1 = df1.merge(df2, on="adm0_id")
        df1[f"area_{lvl}_km"] = df1["area_0_km"] / df1[f"adm{lvl}_id"]
        df1[f"area_{lvl}_km"] = df1[f"area_{lvl}_km"].astype(int)
        df1 = df1.drop(columns=[f"adm{lvl}_id"])
    df1.to_excel(out_dir / "area.xlsx", sheet_name="area", index=False)

    for lvl in range(4, -1, -1):
        dfx = (
            df.groupby(f"adm{lvl}_id", dropna=False)
            .sum(numeric_only=True, min_count=1)
            .reset_index()
        )
        dfx["area_km"] = dfx["area_km"].astype(int)
        dfx.to_excel(
            out_dir / f"area_{lvl}.xlsx", sheet_name=f"area_{lvl}", index=False,
        )

    logger.info("area stats written for %s", WLD)
