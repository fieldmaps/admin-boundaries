import logging
import pandas as pd
from pathlib import Path

DATA_URL = 'https://data.fieldmaps.io'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

srcs = ['cod', 'geoboundaries']
dests = ['humanitarian', 'open']
# world_views = ['intl', 'all', 'usa', 'chn', 'ind']
world_views = ['intl']
geoms = ['lines', 'points', 'polygons']


def get_meta():
    cwd = Path(__file__).parent
    df = pd.read_excel(cwd / '../../inputs/meta.xlsx', engine='openpyxl',
                       keep_default_na=False, na_values=['', '#N/A'])
    return df


def get_input_list(df0, originals=False):
    result = {}
    for src in srcs:
        df = df0.copy()
        df = df.rename(columns={f'{src}_lvl': 'lvl'})
        if originals:
            df['id'] = df['id'].str[:3]
        df['id'] = df['id'].str.lower()
        df = df[['id', 'lvl']]
        df['lvl'] = df['lvl'].astype('Int64')
        df = df[df['lvl'] > 0]
        df = df.drop_duplicates()
        result[src] = df.to_dict('records')
    return result


meta = get_meta()
originals_list = get_input_list(meta, True)
extended_list = get_input_list(meta)
