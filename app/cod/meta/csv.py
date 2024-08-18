import logging
from datetime import date
from pathlib import Path

import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt

from .utils import get_all_meta, join_hdx_meta

cwd = Path(__file__).parent

logger = logging.getLogger(__name__)

NO_DATA = None, None, None
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
    "src_api_url",
]


@retry(stop=stop_after_attempt(5))
def get_hdx_meta(url):
    with httpx.Client(http2=True, timeout=60) as client:
        return client.get(url).json().get("result")


@retry(stop=stop_after_attempt(5))
def get_file_meta(iso_3: str, lvl: int, idx: int | None):
    if lvl is None:
        return NO_DATA
    idx = idx + lvl if idx is not None else lvl
    path = "COD_External"
    query = "where=1=1&outFields=date,validon&f=json&resultRecordCount=1&returnGeometry=false"
    url = f"https://codgis.itos.uga.edu/arcgis/rest/services/{path}/{iso_3}_pcode/FeatureServer/{idx}/query?{query}"
    with httpx.Client(http2=True, timeout=60) as client:
        result = client.get(url).json()
        if "error" in result:
            path = "COD_NO_GEOM_CHECK"
            url = f"https://codgis.itos.uga.edu/arcgis/rest/services/{path}/{iso_3}_pcode/FeatureServer/{idx}/query?{query}"
            result = client.get(url).json()
            if "error" in result:
                return NO_DATA
    attrbutes = result["features"][0]["attributes"]
    if result["geometryType"] != "esriGeometryPolygon":
        return NO_DATA
    if attrbutes["date"] is None or attrbutes["validOn"] is None:
        return NO_DATA
    src_date = date.fromtimestamp(attrbutes["date"] / 1000)
    src_update = date.fromtimestamp(attrbutes["validOn"] / 1000)
    src_url = url.split("FeatureServer")[0] + "FeatureServer"
    return src_date, src_update, src_url


@retry(stop=stop_after_attempt(5))
def main():
    cod_meta = get_all_meta(False)
    for row in cod_meta:
        id = f"cod-ab-{row['iso_3'].lower()}"
        url = f"https://data.humdata.org/api/3/action/package_show?id={id}"
        hdx_meta = get_hdx_meta(url)
        if hdx_meta is not None:
            row = join_hdx_meta(row, hdx_meta)
        src_date, src_update, src_url = get_file_meta(
            row["iso_3"], row["src_lvl"], row["src_api_idx"]
        )
        if src_date is not None and src_update is not None:
            row["src_api_date"] = src_date
            row["src_api_update"] = src_update
        if src_url is not None:
            row["src_api_url"] = src_url
        logger.info(row["iso_3"].lower())
    df = pd.DataFrame(cod_meta)
    df = df.drop(columns=["id", "src_lvl", "src_lvl_max", "src_api_idx"])
    df = df[df["src_date"].notna()]
    df["src_date"] = pd.to_datetime(df["src_date"])
    df["src_api_date"] = pd.to_datetime(df["src_api_date"])
    df["src_update"] = pd.to_datetime(df["src_update"])
    df["src_api_update"] = pd.to_datetime(df["src_api_update"])
    df = df[columns]
    df = df.drop_duplicates(subset=["iso_3"])
    df.to_csv(cwd / "../../../inputs/cod.csv", index=False, encoding="utf-8-sig")
