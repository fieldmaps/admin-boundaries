from pathlib import Path

import httpx
import pandas as pd

from .utils import COD_URL, join_hdx_meta

cwd = Path(__file__).parent

if __name__ == "__main__":
    cod_meta = pd.read_csv(COD_URL).to_dict("records")
    for row in cod_meta:
        id = row["src_url"][33:]
        url = f"https://data.humdata.org/api/3/action/package_show?id={id}"
        hdx_meta = httpx.get(url).json().get("result")
        if hdx_meta is not None:
            row = join_hdx_meta(row, hdx_meta)
    df = pd.DataFrame(cod_meta)
    df["src_date"] = pd.to_datetime(df["src_date"])
    df["src_update"] = pd.to_datetime(df["src_update"])
    df = df.sort_values(by="iso_3")
    df.to_csv(cwd / "../../../inputs/cod.csv", index=False)
