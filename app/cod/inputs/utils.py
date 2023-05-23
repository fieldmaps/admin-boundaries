from pathlib import Path

cwd = Path(__file__).parent
COD_URL = cwd / "../../../inputs/cod_meta.csv"


def join_hdx_meta(row, hdx):
    row["src_date"] = hdx["dataset_date"][1:11]
    row["src_update"] = hdx["last_modified"][:10]
    row["src_name"] = hdx["dataset_source"]
    row["src_name1"] = hdx["organization"]["title"]
    row["src_lic"] = hdx["license_title"]
    return row
