import sqlite3

import pandas as pd

from .utils import cwd

edge_matched = cwd / "../../data/edge-matched/humanitarian/intl/adm4_polygons.gpkg"
query = "SELECT * from adm4_polygons;"
cols = ["Location", "Admin Level", "P-Code", "Name", "Parent P-Code", "Valid from date"]
hlx = [
    "#country+code",
    "#geo+admin_level",
    "#adm+code",
    "#adm+name",
    "#adm+code+parent",
    "#date+start",
]


def main():
    con = sqlite3.connect(edge_matched)
    df4 = pd.read_sql_query(query, con)
    df4 = df4.drop(columns=["geom"])
    df4 = df4[df4["src_grp"] == "COD"]
    df = df4[["iso_3", "adm1_src", "adm1_name", "src_update"]].copy()
    df = df[~df["adm1_src"].isna()]
    df["Admin Level"] = 1
    df["Parent P-Code"] = df["iso_3"]
    df = df.drop_duplicates()
    df = df.rename(
        columns={
            "iso_3": "Location",
            "adm1_src": "P-Code",
            "adm1_name": "Name",
            "src_update": "Valid from date",
        },
    )
    df = df[cols]
    for lvl in [2, 3, 4]:
        cols_lvl = [
            "iso_3",
            f"adm{lvl}_src",
            f"adm{lvl - 1}_src",
            f"adm{lvl}_name",
            "src_update",
        ]
        dfx = df4[cols_lvl].copy()
        dfx[f"adm{lvl}_name"] = dfx[f"adm{lvl}_name"].str.replace("\r", "")
        dfx = dfx[~dfx[f"adm{lvl}_src"].isna()]
        dfx["Admin Level"] = lvl
        dfx["Parent P-Code"] = dfx[f"adm{lvl - 1}_src"]
        dfx = dfx.drop_duplicates()
        dfx = dfx.rename(
            columns={
                "iso_3": "Location",
                f"adm{lvl}_src": "P-Code",
                f"adm{lvl}_name": "Name",
                "src_update": "Valid from date",
            },
        )
        dfx = dfx[cols]
        df = pd.concat([df, dfx])
    df = df.sort_values(by=cols)
    df = df.reset_index(drop=True)
    df["Name"] = df["Name"].str.replace("\r", "")
    df.to_json(cwd / "../../outputs/global-pcodes.json", index=False, orient="records")
    df["Valid from date"] = pd.to_datetime(df["Valid from date"])
    df["Valid from date"] = df["Valid from date"].dt.date
    df.loc[-1] = hlx
    df = df.sort_index()
    df.to_csv(
        cwd / "../../outputs/global-pcodes.csv",
        index=False,
        encoding="utf-8-sig",
    )
    df.to_excel(cwd / "../../outputs/global-pcodes.xlsx", index=False)
