"""Generate edge-matched metadata index files."""

import csv
from json import dump

import duckdb

from app.config import OUTPUTS_DIR, WLD

from .config import DATA_URL
from .utils import get_land_date


def main(name: str) -> None:
    """Write JSON, CSV, and XLSX metadata index for the given dataset name."""
    OUTPUTS_DIR.mkdir(exist_ok=True, parents=True)
    land_date = get_land_date()
    data = []
    for lvl in range(4, 0, -1):
        row = {
            "id": f"{WLD}_adm{lvl}",
            "wld": WLD,
            "adm": lvl,
            "date": land_date,
            "a_parquet": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_polygons.parquet",
            "a_gpkg": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_polygons.gpkg.zip",
            "a_gdb": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_polygons.gdb.zip",
            "a_xlsx": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_polygons.xlsx",
            "l_parquet": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_lines.parquet",
            "l_gpkg": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_lines.gpkg.zip",
            "l_gdb": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_lines.gdb.zip",
            "l_xlsx": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_lines.xlsx",
            "p_parquet": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_points.parquet",
            "p_gpkg": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_points.gpkg.zip",
            "p_gdb": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_points.gdb.zip",
            "p_xlsx": f"{DATA_URL}/{name}/{WLD}/adm{lvl}_points.xlsx",
        }
        data.append(row)
    with (OUTPUTS_DIR / f"{name}.json").open("w") as f:
        dump(data, f, separators=(",", ":"))
    csv_path = OUTPUTS_DIR / f"{name}.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    xlsx = OUTPUTS_DIR / f"{name}.xlsx"
    xlsx.unlink(missing_ok=True)
    conn = duckdb.connect()
    conn.execute("LOAD spatial;")
    conn.execute(f"""--sql
        COPY (SELECT * REPLACE (CAST(date AS DATE) AS date) FROM read_csv('{csv_path}'))
        TO '{xlsx}' (FORMAT GDAL, DRIVER 'XLSX')
    """)
    conn.close()
