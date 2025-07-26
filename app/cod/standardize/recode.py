import pandas as pd

from .utils import DATABASE, filter_config, logging

logger = logging.getLogger(__name__)
con = f"postgresql:///{DATABASE}"


def get_ids(level, langs):
    result = []
    for lvl in range(level, -1, -1):
        result.append(f"adm{lvl}_pcode")
        for lang in langs:
            result.append(f"adm{lvl}_{lang}")
    return ",".join(result)


def rename_id(df, level, langs):
    for lvl in range(level, -1, -1):
        df = df.rename(columns={f"adm{lvl}_pcode": f"adm{lvl}_id"})
        df[f"adm{lvl}_src"] = df[f"adm{lvl}_id"]
        df = df.rename(columns={f"adm{lvl}_{langs[0]}": f"adm{lvl}_name"})
        for i in range(1, 3):
            if len(langs) > i:
                df = df.rename(columns={f"adm{lvl}_{langs[i]}": f"adm{lvl}_name{i}"})
            else:
                df[f"adm{lvl}_name{i}"] = None
    return df


def get_max_pad(df, level):
    col = f"adm{level}_src"
    col_higher = f"adm{level - 1}_src"
    prev_id = None
    higher_id = None
    id_num = None
    id_max = 0
    for _, row in df.iterrows():
        if row[col_higher] != higher_id:
            id_num = 1
        elif row[col] != prev_id:
            id_num = id_num + 1
        higher_id = row[col_higher]
        prev_id = row[col]
        id_max = max(id_max, id_num)
    return len(str(id_max))


def create_ids(df, name, level, date):
    if date is pd.NaT:
        df["adm0_id"] = name.upper()
    else:
        df["adm0_id"] = f"{name.upper()}-{date.strftime('%Y%m%d')}"
    for lvl in range(1, level + 1):
        col = f"adm{lvl}_id"
        col_higher = f"adm{lvl - 1}_id"
        prev_id = None
        higher_id = None
        id_num = None
        id_max = get_max_pad(df, lvl)
        for i, row in df.iterrows():
            if row[col_higher] != higher_id:
                id_num = 1
            elif row[col] != prev_id:
                id_num = id_num + 1
            higher_id = row[col_higher]
            prev_id = row[col]
            new_val = f"{higher_id}-{str(id_num).zfill(id_max)}"
            df.at[i, f"adm{lvl}_id"] = new_val
    return df


def order_ids(level):
    result = []
    for lvl in range(level, -1, -1):
        result += [
            f"adm{lvl}_id",
            f"adm{lvl}_src",
            f"adm{lvl}_name",
            f"adm{lvl}_name1",
            f"adm{lvl}_name2",
        ]
    return result


def add_meta(df, row):
    meta = [
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
    ]
    for m in meta:
        df[m] = row[m]
    df["src_date"] = pd.to_datetime(df["src_date"])
    df["src_update"] = pd.to_datetime(df["src_update"])
    df["src_grp"] = "COD"
    return df


def handle_filter(df, level, config):
    col = f"adm{config['adm']}_src"
    for name, (switch, *args) in config["layers"].items():
        if switch == "==":
            df1 = df[df[col].isin(args)]
        elif switch == "!=":
            df1 = df[~df[col].isin(args)]
        df1.to_sql(
            f"{name}_adm{level}_01",
            con,
            if_exists="replace",
            index=False,
            method="multi",
        )


def main(_, name, level, langs, row):
    query = f"SELECT {get_ids(level, langs)} FROM {name}_adm{level}_00;"
    df = pd.read_sql_query(query, con)
    cols = list(map(lambda x: f"adm{x}_pcode", range(level + 1)))
    df = df.sort_values(by=cols)
    df = rename_id(df, level, langs)
    df = create_ids(df, name, level, row["src_update"])
    df = df[order_ids(level)]
    df = add_meta(df, row)
    if name in filter_config.keys():
        handle_filter(df, level, filter_config[name])
    else:
        df.to_sql(
            f"{name}_adm{level}_01",
            con,
            if_exists="replace",
            index=False,
            method="multi",
        )
