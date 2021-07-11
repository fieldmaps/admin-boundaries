import json
import logging
import pandas as pd
from configparser import ConfigParser
from pathlib import Path
from psycopg2 import connect

DATABASE = 'admin_boundaries'
DATA_URL = 'https://data.fieldmaps.io'

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


def get_ids():
    cwd = Path(__file__).parent
    cfg = ConfigParser()
    cfg.read((cwd / '../../../config.ini'))
    config = cfg['default']
    ids = config['geoboundaries'].split(',')
    return list(filter(lambda x: x != '', ids))


def get_meta():
    cwd = Path(__file__).parent
    dtypes = {'ids': 'Int8', 'lvl_full': 'Int8', 'lvl_miss': 'Int8',
              'lvl_part': 'Int8', 'lvl_err': 'Int8'}
    df_path = (cwd / '../../../data/geoboundaries/originals/meta.xlsx').resolve()
    df = pd.read_excel(df_path, engine='openpyxl', dtype=dtypes)
    df['src_date'] = df['src_date'].astype(str).replace('NaT', None)
    df['src_update'] = df['src_update'].astype(str).replace('NaT', None)
    return json.loads(df.to_json(orient='records'))


ids = get_ids()
meta = get_meta()
adm0_list = list(
    filter(lambda x: x['lvl_full'] is not None and x['lvl_full'] >= 1, meta)
)
if len(ids) > 0:
    adm0_list = list(filter(lambda x: x['id'] in ids, adm0_list))
