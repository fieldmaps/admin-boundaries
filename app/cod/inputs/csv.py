import logging
import sqlite3
from pathlib import Path

import httpx
import pandas as pd

from .utils import get_all_meta, join_hdx_meta

cwd = Path(__file__).parent

logger = logging.getLogger(__name__)

columns = [
    "iso_3",
    "name",
    "src_date",
    "src_update",
    "src_api_date",
    "src_api_update",
    "src_name",
    "src_name1",
    "src_lic",
    "src_url",
]


def get_file_meta(iso_3: str, lvl: int):
    file = cwd / f"../../../inputs/cod/{iso_3}_adm{lvl}.gpkg"
    if file.is_file():
        query = f"SELECT * from {iso_3}_adm{lvl};"
        con = sqlite3.connect(file)
        df = pd.read_sql_query(query, con)
        df.columns = df.columns.str.lower()
        if "date" in df.columns and "validon" in df.columns:
            return df["date"].iloc[0], df["validon"].iloc[0]
    return None, None


def main():
    cod_meta = get_all_meta(False)
    for row in cod_meta:
        id = f"cod-ab-{row['iso_3'].lower()}"
        url = f"https://data.humdata.org/api/3/action/package_show?id={id}"
        hdx_meta = httpx.get(url).json().get("result")
        if hdx_meta is not None:
            row = join_hdx_meta(row, hdx_meta)
        src_date, src_update = get_file_meta(row["iso_3"], row["src_lvl"])
        if src_date is not None and src_update is not None:
            row["src_api_date"] = src_date
            row["src_api_update"] = src_update
        logger.info(row["iso_3"].lower())
    df = pd.DataFrame(cod_meta)
    df = df.drop(columns=["id", "src_lvl", "src_lvl_max", "src_api_idx"])
    df = df[df["src_date"].notna()]
    df["src_date"] = pd.to_datetime(df["src_date"])
    df["src_api_date"] = pd.to_datetime(df["src_api_date"])
    df["src_update"] = pd.to_datetime(df["src_update"])
    df["src_api_update"] = pd.to_datetime(df["src_api_update"])
    df = df.sort_values(by="iso_3")
    df = df[columns]
    df.to_csv(cwd / "../../../inputs/cod.csv", index=False, encoding="utf-8-sig")
