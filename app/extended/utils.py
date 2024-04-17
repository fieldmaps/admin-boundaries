import logging
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

srcs = ["geoboundaries", "cod"]
cwd = Path(__file__).parent
id_cod = list(filter(None, map(lambda x: x.lower(), getenv("COD", "").split(","))))
id_geoboundaries = list(
    filter(None, map(lambda x: x.lower(), getenv("GEOBOUNDARIES", "").split(",")))
)


def apply_funcs(file, level, *args):
    for func in args:
        func(file, level)


def get_meta():
    cwd = Path(__file__).parent
    df = pd.read_csv(
        cwd / "../../inputs/meta.csv", keep_default_na=False, na_values=["", "#N/A"]
    )
    return df


def get_input_list(df0):
    result = {}
    for src in srcs:
        df = df0.copy()
        df = df.rename(columns={f"{src}_lvl": "lvl"})
        df["id"] = df["id"].str.lower()
        df = df[["id", "lvl"]]
        df["lvl"] = df["lvl"].astype("Int64")
        df = df[df["lvl"] > 0]
        result[src] = df.to_dict("records")
    return result


meta = get_meta()
input_list = get_input_list(meta)
if len(id_cod) > 0:
    input_list["cod"] = filter(lambda x: x["id"] in id_cod, input_list["cod"])
if len(id_geoboundaries) > 0:
    input_list["geoboundaries"] = filter(
        lambda x: x["id"] in id_geoboundaries, input_list["geoboundaries"]
    )
