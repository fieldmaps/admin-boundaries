import json
import logging
from os import getenv
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from psycopg import connect

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

DATABASE = "app"
cwd = Path(__file__).parent
id_filter = list(
    filter(None, map(lambda x: x.lower(), getenv("GEOBOUNDARIES", "").split(",")))
)


def apply_funcs(name, level, row, *args):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    for func in args:
        func(conn, name, level, row)
    conn.close()


def get_all_meta():
    dtypes = {"geoboundaries_lvl": "Int8"}
    df = pd.read_csv(
        cwd / "../../../inputs/meta.csv",
        dtype=dtypes,
        keep_default_na=False,
        na_values=["", "#N/A"],
    )
    df = df.rename(columns={"geoboundaries_lvl": "src_lvl"})
    df["id"] = df["id"].str[:3]
    df["id"] = df["id"].str.lower()
    df["iso_3"] = df["id"].str.upper()
    df = df[["id", "iso_3", "src_lvl"]]
    df = df[df["src_lvl"] > 0]
    return df.drop_duplicates()


def get_src_meta():
    df = pd.read_csv(
        cwd / "../../../inputs/geoboundaries.csv",
        keep_default_na=False,
        na_values=["", "#N/A"],
    )
    df["src_date"] = pd.to_datetime(df["src_date"])
    df["src_update"] = pd.to_datetime(df["src_update"])
    return df


def join_meta(df1, df2):
    df = df1.merge(df2, on=["iso_3", "src_lvl"])
    df = df.where(df.notna(), None)
    return df.to_dict("records")


def get_filter_config():
    with open(cwd / "../../../inputs/geoboundaries.json") as f:
        return json.load(f)


meta_local = get_all_meta()
meta_src = get_src_meta()
adm0_list = join_meta(meta_local, meta_src)
filter_config = get_filter_config()
if len(id_filter) > 0:
    adm0_list = filter(lambda x: x["id"] in id_filter, adm0_list)
