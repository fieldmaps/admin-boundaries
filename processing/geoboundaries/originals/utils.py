import logging
import pandas as pd
from pathlib import Path
from psycopg2 import connect

DATABASE = 'admin_boundaries'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def apply_funcs(file, level, *args):
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    for func in args:
        func(cur, file, level)
    cur.close()
    con.close()


def get_meta():
    cwd = Path(__file__).parent
    config_path = cwd / '../../../inputs/meta.xlsx'
    dtypes = {'geoboundaries_lvl': 'Int8'}
    df = pd.read_excel(config_path, engine='openpyxl', dtype=dtypes,
                       keep_default_na=False, na_values=['', '#N/A'])
    df = df.rename(columns={'geoboundaries_lvl': 'src_lvl'})
    df['id'] = df['geoboundaries_id'].combine_first(df['id'])
    df['id'] = df['id'].str[:3]
    df['id'] = df['id'].str.lower()
    df = df[['id', 'src_lvl']]
    df = df[df['src_lvl'] > 0]
    return df.drop_duplicates().to_dict('records')


adm0_list = get_meta()
