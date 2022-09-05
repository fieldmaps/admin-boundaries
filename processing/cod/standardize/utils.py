import json
import logging
import pandas as pd
from configparser import ConfigParser
from pathlib import Path
from psycopg import connect

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

DATABASE = 'admin_boundaries'
cwd = Path(__file__).parent
cfg = ConfigParser()
cfg.read(cwd / '../../../config.ini')


def apply_funcs(name, level, langs, row, *args):
    conn = connect(f'dbname={DATABASE}', autocommit=True)
    for func in args:
        func(conn, name, level, langs, row)
    conn.close()


def get_all_meta():
    dtypes = {'cod_lvl': 'Int8'}
    df = pd.read_csv(cwd / '../../../inputs/meta.csv', dtype=dtypes,
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
    df = pd.read_csv(cwd / '../../../inputs/cod.csv',
                     keep_default_na=False, na_values=['', '#N/A'])
    df['src_date'] = pd.to_datetime(df['src_date'])
    df['src_update'] = pd.to_datetime(df['src_update'])
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
