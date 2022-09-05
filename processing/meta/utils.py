import logging
import pandas as pd
from pathlib import Path

DATA_URL = 'https://data.fieldmaps.io'
cwd = Path(__file__).parent
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

srcs = ['cod', 'geoboundaries']
join = {'cod': ['iso_3'], 'geoboundaries': ['iso_3', 'src_lvl']}
dests = ['humanitarian', 'open']
# world_views = ['intl', 'all', 'usa', 'chn', 'ind']
world_views = ['intl']
geoms = ['polygons', 'lines', 'points']
cols = ['id', 'iso_3', 'adm0_name', 'adm0_name1', 'src_lvl',
        'src_lang', 'src_lang1', 'src_lang2', 'src_date', 'src_update',
        'src_name', 'src_name1', 'src_lic', 'src_url']


def get_all_meta():
    df = pd.read_csv(cwd / '../../inputs/meta.csv',
                     keep_default_na=False, na_values=['', '#N/A'])
    df['iso_3'] = df['id'].str[:3]
    df['id'] = df['id'].str.lower()
    return df


def get_src_meta(src):
    df = pd.read_csv(cwd / f'../../inputs/{src}.csv',
                     keep_default_na=False, na_values=['', '#N/A'])
    df['src_date'] = pd.to_datetime(df['src_date'])
    df['src_update'] = pd.to_datetime(df['src_update'])
    return df


def join_meta(df1, df2, src):
    df1 = df1.rename(columns={f'{src}_lvl': 'src_lvl', f'{src}_lang': 'src_lang',
                     f'{src}_lang1': 'src_lang1', f'{src}_lang2': 'src_lang2'})
    df = df1.merge(df2, on=join[src])
    df = df[[x for x in cols if x in df.columns]]
    df['src_lvl'] = df['src_lvl'].astype('Int64')
    df['src_date'] = df['src_date'].dt.date
    df['src_update'] = df['src_update'].dt.date
    df = df[df['src_lvl'] > 0]
    return df


def get_land_date():
    with open(cwd / '../../../adm0-generator/data/land/README.txt') as f:
        return f.readlines()[21][25:35]


land_date = get_land_date()
meta_all = get_all_meta()
meta = {}
for src in srcs:
    meta_src = get_src_meta(src)
    meta[src] = join_meta(meta_all, meta_src, src)
