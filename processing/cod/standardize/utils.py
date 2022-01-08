import json
import logging
import pandas as pd
from pathlib import Path
from psycopg2 import connect

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

DATABASE = 'admin_boundaries'
cwd = Path(__file__).parent


def apply_funcs(name, level, langs, row, *args):
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    for func in args:
        func(cur, name, level, langs, row)
    cur.close()
    con.close()


def get_all_meta():
    config_path = cwd / '../../../inputs/meta.xlsx'
    dtypes = {'cod_lvl': 'Int8'}
    df = pd.read_excel(config_path, engine='openpyxl', dtype=dtypes,
                       keep_default_na=False, na_values=['', '#N/A'])
    df = df.rename(columns={'cod_lvl': 'src_lvl', 'cod_lang': 'src_lang',
                   'cod_lang1': 'src_lang1', 'cod_lang2': 'src_lang2'})
    df['id'] = df['id'].str[:3]
    df['id'] = df['id'].str.lower()
    df['iso_3'] = df['id'].str.upper()
    df = df[['id', 'iso_3', 'src_lvl', 'src_lang', 'src_lang1', 'src_lang2']]
    df = df[df['src_lvl'] > 0]
    return df.drop_duplicates()


def get_src_meta():
    config_path = cwd / '../../../inputs/cod.xlsx'
    df = pd.read_excel(config_path, engine='openpyxl',
                       keep_default_na=False, na_values=['', '#N/A'])
    return df


def join_meta(df1, df2):
    df = df1.merge(df2, on='iso_3')
    df = df.where(df.notna(), None)
    return df.to_dict('records')


def get_filter_config():
    with open(cwd / f'../../../inputs/cod.json') as f:
        return json.load(f)


meta_local = get_all_meta()
meta_src = get_src_meta()
adm0_list = join_meta(meta_local, meta_src)
filter_config = get_filter_config()
