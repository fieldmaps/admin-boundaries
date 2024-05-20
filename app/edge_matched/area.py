import pandas as pd
from psycopg import connect
from psycopg.sql import SQL, Identifier

from .utils import DATABASE, cwd, logging

logger = logging.getLogger(__name__)

con = f"postgresql:///{DATABASE}"

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        adm4_id, adm3_id, adm2_id, adm1_id, adm0_id, iso_3,
        ST_Area(ST_Transform(geom, 6933)) / 1000000 as area_km
    FROM {table_in};
"""


def get_ids(lvl):
    return [f"adm{x}_id" for x in range(lvl, -1, -1)]


def export_attrs(df, data):
    df1 = (
        df.groupby("adm0_id", dropna=False)
        .sum(numeric_only=True, min_count=1)
        .reset_index()
    )
    df1 = df1.rename(columns={"area_km": "area_0_km"})
    df1["area_0_km"] = df1["area_0_km"].astype(int)
    for lvl in range(1, 5):
        df2 = df[["adm0_id", f"adm{lvl}_id"]]
        df2 = df2.drop_duplicates(subset=[f"adm{lvl}_id"])
        df2 = df2.groupby("adm0_id", dropna=False).count().reset_index()
        df1 = df1.merge(df2, on="adm0_id")
        df1[f"area_{lvl}_km"] = df1["area_0_km"] / df1[f"adm{lvl}_id"]
        df1[f"area_{lvl}_km"] = df1[f"area_{lvl}_km"].astype(int)
        df1 = df1.drop(columns=[f"adm{lvl}_id"])
    df1.to_excel(data / "area.xlsx", sheet_name="area", index=False)


def export_areas(df, data):
    for lvl in range(4, -1, -1):
        df1 = (
            df.groupby(f"adm{lvl}_id", dropna=False)
            .sum(numeric_only=True, min_count=1)
            .reset_index()
        )
        df1["area_km"] = df1["area_km"].astype(int)
        df1.to_excel(data / f"area_{lvl}.xlsx", sheet_name=f"area_{lvl}", index=False)


def main(dest, wld):
    data = cwd / "../../data/edge-matched" / dest / wld
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    conn.execute(
        SQL(query_1).format(
            table_in=Identifier(f"{dest}_adm4_polygons_{wld}"),
            table_out=Identifier(f"{dest}_adm4_polygons_{wld}_area"),
        )
    )
    conn.close()
    logger.info("finished")
    data.mkdir(parents=True, exist_ok=True)
    df = pd.read_sql_table(f"{dest}_adm4_polygons_{wld}_area", con)
    export_attrs(df, data)
    export_areas(df, data)
    logger.info("finished")
