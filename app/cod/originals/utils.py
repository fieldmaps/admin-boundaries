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
id_filter = list(filter(None, map(lambda x: x.lower(), getenv("COD", "").split(","))))


def apply_funcs(name, level, level_max, row, *args):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    for func in args:
        func(conn, name, level, level_max, row)
    conn.close()


def get_cols(level: int, row):
    langs = list(
        filter(
            lambda x: isinstance(x, str),
            [row["src_lang"], row["src_lang1"], row["src_lang2"]],
        ),
    )
    result = []
    for lvl in range(level, -1, -1):
        for lang in langs:
            result.append(f"ADM{lvl}_{lang.upper()}")
        result.append(f"ADM{lvl}_PCODE")
    result.extend(["DATE", "VALIDON", "VALIDTO"])
    return result


def get_all_meta():
    dtypes = {"cod_lvl": "Int8", "cod_lvl_max": "Int8"}
    df = pd.read_csv(
        cwd / "../../../inputs/meta.csv",
        dtype=dtypes,
        keep_default_na=False,
        na_values=["", "#N/A"],
    )
    df = df.rename(
        columns={
            "cod_lvl": "src_lvl",
            "cod_lvl_max": "src_lvl_max",
            "cod_lang": "src_lang",
            "cod_lang1": "src_lang1",
            "cod_lang2": "src_lang2",
        },
    )
    df["id"] = df["id"].str[:3]
    df["id"] = df["id"].str.lower()
    df["iso_3"] = df["id"].str.upper()
    df = df[
        ["id", "iso_3", "src_lvl", "src_lvl_max", "src_lang", "src_lang1", "src_lang2"]
    ]
    df = df[df["src_lvl"] >= 0]
    return df.drop_duplicates()


def get_src_meta():
    df = pd.read_csv(
        cwd / "../../../inputs/cod.csv",
        keep_default_na=False,
        na_values=["", "#N/A"],
    )
    df = df.drop(columns=["src_api_date", "src_api_update"])
    return df


def join_meta(df1, df2):
    df = df1.merge(df2, on="iso_3")
    return df.to_dict("records")


meta_local = get_all_meta()
meta_src = get_src_meta()
adm0_list = join_meta(meta_local, meta_src)
if len(id_filter) > 0:
    adm0_list = filter(lambda x: x["id"] in id_filter, adm0_list)
