from pathlib import Path

import pandas as pd

from .utils import DATA_URL, meta

cwd = Path(__file__).parent
outputs = cwd / "../../outputs"


def add_meta(name, df):
    df["e_gpkg"] = f"{DATA_URL}/{name}/extended/" + df["id"] + ".gpkg.zip"
    df["e_shp"] = f"{DATA_URL}/{name}/extended/" + df["id"] + ".shp.zip"
    df["e_geojson"] = f"{DATA_URL}/{name}/extended/" + df["id"] + ".geojson.zip"
    df["e_gdb"] = f"{DATA_URL}/{name}/extended/" + df["id"] + ".gdb.zip"
    df["e_xlsx"] = f"{DATA_URL}/{name}/extended/" + df["id"] + ".xlsx"
    df["o_gpkg"] = (
        f"{DATA_URL}/{name}/originals/" + df["iso_3"].str.lower() + ".gpkg.zip"
    )
    df["o_shp"] = f"{DATA_URL}/{name}/originals/" + df["iso_3"].str.lower() + ".shp.zip"
    df["o_geojson"] = (
        f"{DATA_URL}/{name}/originals/" + df["iso_3"].str.lower() + ".geojson.zip"
    )
    df["o_gdb"] = f"{DATA_URL}/{name}/originals/" + df["iso_3"].str.lower() + ".gdb.zip"
    df["o_xlsx"] = f"{DATA_URL}/{name}/originals/" + df["iso_3"].str.lower() + ".xlsx"
    return df


def export_formats(name, df):
    df.to_csv(outputs / f"{name}.csv", index=False, encoding="utf-8-sig")
    df.to_excel(outputs / f"{name}.xlsx", index=False)
    df["src_date"] = pd.to_datetime(df["src_date"]).dt.strftime("%Y-%m-%d")
    df["src_update"] = pd.to_datetime(df["src_update"]).dt.strftime("%Y-%m-%d")
    data = df.to_json(orient="records")
    with open((outputs / f"{name}.json"), "w") as f:
        f.write(data)


def main(name):
    outputs.mkdir(exist_ok=True, parents=True)
    df = meta[name]
    df = add_meta(name, df)
    export_formats(name, df)
