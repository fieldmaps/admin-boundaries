import logging
from pathlib import Path

import pandas as pd
from psycopg import connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

DATABASE = "app"

world_views = ["intl"]
geoms = ["polygons", "lines", "points"]
geoms_clip = ["clip", "polygons", "lines", "points"]
srcs = ["cod", "geoboundaries"]
dests = ["humanitarian", "open"]


def apply_funcs(src, wld, row, *args):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    for func in args:
        func(conn, src, wld, row)
    conn.close()


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


def get_src_list(df0):
    result = {}
    for src in srcs:
        for wld in world_views:
            df = df0.copy()
            df = df[df[f"clip_{wld}"] != 0]
            df["id_clip"] = df[f"clip_{wld}"].combine_first(df["id"])
            df["id_attr"] = df[f"attr_{wld}"].combine_first(df["id_clip"])
            df["id"] = df["id"].str.lower()
            df = df.rename(columns={f"{src}_lvl": "lvl"})
            df = df[["id", "id_clip", "id_attr", "lvl"]]
            df["lvl"] = df["lvl"].astype("Int64")
            df = df[df["lvl"] > 0]
            result[f"{src}_{wld}"] = df.to_dict("records")
    return result


def get_adm0_list(df0):
    result = {}
    for wld in world_views:
        df = df0.copy()
        df = df[df[f"clip_{wld}"] != 0]
        df["id_clip"] = df[f"clip_{wld}"].combine_first(df["id"])
        df["open_lvl"] = df["geoboundaries_lvl"]
        df["humanitarian_lvl"] = df["cod_lvl"].combine_first(df["open_lvl"])
        result[f"open_{wld}"] = df[df["open_lvl"].isna()]["id_clip"].tolist()
        result[f"humanitarian_{wld}"] = df[df["humanitarian_lvl"].isna()][
            "id_clip"
        ].tolist()
    return result


def get_dest_list(src_list):
    result = {}
    for wld in world_views:
        result[f"open_{wld}"] = {}
        for row in src_list[f"geoboundaries_{wld}"]:
            row["src"] = "geoboundaries"
            result[f"open_{wld}"][row["id"]] = row
        result[f"humanitarian_{wld}"] = {**result[f"open_{wld}"]}
        for row in src_list[f"cod_{wld}"]:
            row["src"] = "cod"
            result[f"humanitarian_{wld}"][row["id"]] = row
        result[f"open_{wld}"] = result[f"open_{wld}"].values()
        result[f"humanitarian_{wld}"] = result[f"humanitarian_{wld}"].values()
    return result


def get_src_ids(level, end=0, attr=True):
    ids = []
    for l in range(level, end - 1, -1):
        ids.extend(
            [
                f"adm{l}_id",
                f"adm{l}_src",
                f"adm{l}_name",
                f"adm{l}_name1",
                f"adm{l}_name2",
            ]
        )
    if attr is True:
        ids.extend(
            [
                "src_lvl",
                "src_lang",
                "src_lang1",
                "src_lang2",
                "src_date",
                "src_update",
                "src_name",
                "src_name1",
                "src_lic",
                "src_url",
                "src_grp",
            ]
        )
    return ids


def get_wld_ids(adm0=True):
    ids = []
    if adm0 is True:
        ids.extend(
            [
                "adm0_id",
                "adm0_src",
                "adm0_name",
                "adm0_name1",
                "adm0_name2",
            ]
        )
    ids.extend(
        [
            "iso_cd",
            "iso_2",
            "iso_3",
            "iso_3_grp",
            "region3_cd",
            "region3_nm",
            "region2_cd",
            "region2_nm",
            "region1_cd",
            "region1_nm",
            "status_cd",
            "status_nm",
            "wld_date",
            "wld_update",
            "wld_view",
            "wld_notes",
        ]
    )
    return ids


def add_col_query(col):
    query_1 = """
        ALTER TABLE {table_out}
        ADD COLUMN IF NOT EXISTS {name} INT8;
    """
    query_2 = """
        ALTER TABLE {table_out}
        ADD COLUMN IF NOT EXISTS {name} DATE;
    """
    query_3 = """
        ALTER TABLE {table_out}
        ADD COLUMN IF NOT EXISTS {name} VARCHAR;
    """
    if col.endswith("_lvl") or col.endswith("_cd"):
        return query_1
    elif col.endswith("date"):
        return query_2
    else:
        return query_3


meta = get_meta()
input_list = get_input_list(meta)
src_list = get_src_list(meta)
adm0_list = get_adm0_list(meta)
dest_list = get_dest_list(src_list)
