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


def apply_funcs(file, level, *args):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    for func in args:
        func(conn, file, level)
    conn.close()


def get_meta():
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
    df = df[df["geoboundaries_grp"].isna()]
    df = df[["id", "src_lvl"]]
    df = df[df["src_lvl"] > 0]
    return df.drop_duplicates().to_dict("records")


adm0_list = get_meta()
if len(id_filter) > 0:
    adm0_list = filter(lambda x: x["id"] in id_filter, adm0_list)
