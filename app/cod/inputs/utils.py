import logging
import re
import subprocess
from os import getenv
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)

cwd = Path(__file__).parent


def is_polygon(file):
    regex = re.compile(r"\((Multi Polygon|Polygon)\)")
    result = subprocess.run(["ogrinfo", file], capture_output=True)
    return regex.search(str(result.stdout))


def get_ids():
    ids = getenv("COD", "").split(",")
    return list(filter(lambda x: x != "", ids))


def join_hdx_meta(row, hdx):
    row["src_date"] = hdx["dataset_date"][1:11]
    row["src_update"] = hdx["last_modified"][:10]
    row["src_name"] = hdx["dataset_source"]
    row["src_name1"] = hdx["organization"]["title"]
    row["src_lic"] = hdx["license_title"]
    row["src_url"] = f"https://data.humdata.org/dataset/cod-ab-{row['iso_3'].lower()}"
    return row


def get_all_meta(filtered=True):
    dtypes = {"cod_lvl": "Int8", "cod_lvl_max": "Int8", "cod_api_idx": "Int8"}
    df = pd.read_csv(
        cwd / "../../../inputs/meta.csv",
        dtype=dtypes,
        keep_default_na=False,
        na_values=["", "#N/A"],
    )
    df = df.rename(
        columns={
            "adm0_name1": "name",
            "cod_lvl": "src_lvl",
            "cod_lvl_max": "src_lvl_max",
            "cod_api_idx": "src_api_idx",
            "cod_lang": "src_lang",
            "cod_lang1": "src_lang1",
            "cod_lang2": "src_lang2",
        }
    )
    df["id"] = df["id"].str[:3]
    df["id"] = df["id"].str.lower()
    df["iso_3"] = df["id"].str.upper()
    df = df[["id", "iso_3", "name", "src_lvl", "src_lvl_max", "src_api_idx"]]
    if filtered:
        df = df[df["src_lvl"] >= 0]
    return df.drop_duplicates().to_dict("records")


ids = get_ids()
adm0_list = get_all_meta()
if len(ids) > 0:
    adm0_list = filter(lambda x: x["id"] in ids, adm0_list)
