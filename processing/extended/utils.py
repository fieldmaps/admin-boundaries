import logging
import pandas as pd
from pathlib import Path

srcs = ['geoboundaries', 'cod']
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def apply_funcs(file, level, *args):
    for func in args:
        func(file, level)


def get_meta():
    cwd = Path(__file__).parent
    df = pd.read_csv(cwd / '../../inputs/meta.csv',
                     keep_default_na=False, na_values=['', '#N/A'])
    return df


def get_input_list(df0):
    result = {}
    for src in srcs:
        df = df0.copy()
        df = df.rename(columns={f'{src}_lvl': 'lvl'})
        df['id'] = df['id'].str.lower()
        df = df[['id', 'lvl']]
        df['lvl'] = df['lvl'].astype('Int64')
        df = df[df['lvl'] > 0]
        result[src] = df.to_dict('records')
    return result


meta = get_meta()
input_list = get_input_list(meta)
