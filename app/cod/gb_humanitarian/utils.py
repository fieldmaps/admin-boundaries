import logging
from os import getenv
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from psycopg import connect

load_dotenv()

DATABASE = "app"
DATA_URL = "https://data.fieldmaps.io"

cwd = Path(__file__).parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def apply_funcs(name, level, langs, row, *args):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    for func in args:
        func(conn, name, level, langs, row)
    conn.close()


def get_ids():
    ids = getenv("COD", "").split(",")
    return list(filter(lambda x: x != "", ids))


def get_all_meta():
    dtypes = {"cod_lvl": "Int8"}
    df = pd.read_csv(
        cwd / "../../../inputs/meta.csv",
        dtype=dtypes,
        keep_default_na=False,
        na_values=["", "#N/A"],
    )
    df = df.rename(
        columns={
            "cod_lvl": "src_lvl",
            "cod_lang": "src_lang",
            "cod_lang1": "src_lang1",
            "cod_lang2": "src_lang2",
        }
    )
    df["id"] = df["id"].str[:3]
    df["id"] = df["id"].str.lower()
    df["iso_3"] = df["id"].str.upper()
    df = df[["id", "iso_3", "src_lvl", "src_lang", "src_lang1", "src_lang2"]]
    df = df[df["src_lvl"] > 0]
    return df.drop_duplicates()


def get_src_meta():
    df = pd.read_csv(
        cwd / "../../../inputs/cod.csv", keep_default_na=False, na_values=["", "#N/A"]
    )
    return df


def join_meta(df1, df2):
    df = df1.merge(df2, on="iso_3")
    return df.to_dict("records")


def get_land_date():
    with open(cwd / "../../../../adm0-generator/data/date.txt") as f:
        return f.readline()


land_date = get_land_date()
ids = get_ids()
meta_local = get_all_meta()
meta_src = get_src_meta()
adm0_list_original = join_meta(meta_local, meta_src)
adm0_list = join_meta(meta_local, meta_src)
if len(ids) > 0:
    adm0_list = filter(lambda x: x["id"] in ids, adm0_list)
