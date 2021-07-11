import json
import logging
import pandas as pd
from configparser import ConfigParser
from pathlib import Path

API_URL = 'https://www.geoboundaries.org/gbRequest.html'
GIT_URL = 'https://raw.githubusercontent.com/wmgeolab/geoBoundaries/main/releaseData/gbOpen'
GIT_LFS_URL = 'https://media.githubusercontent.com/media/wmgeolab/geoBoundaries/main/releaseData/gbOpen'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def get_ids():
    cwd = Path(__file__).parent
    cfg = ConfigParser()
    cfg.read((cwd / '../../../config.ini'))
    config = cfg['default']
    ids = config['geoboundaries'].split(',')
    return list(filter(lambda x: x != '', ids))


def get_meta():
    cwd = Path(__file__).parent
    dtypes = {'lvl_full': 'Int8', 'lvl_miss': 'Int8',
              'lvl_part': 'Int8', 'lvl_err': 'Int8'}
    df_path = (cwd / '../../../data/geoboundaries/originals/meta.xlsx').resolve()
    df = pd.read_excel(df_path, engine='openpyxl', dtype=dtypes)
    df['src_date'] = df['src_date'].astype(str).replace('NaT', None)
    df['src_update'] = df['src_update'].astype(str).replace('NaT', None)
    return json.loads(df.to_json(orient='records'))


ids = get_ids()
meta = get_meta()
if len(ids) > 0:
    meta = list(filter(lambda x: x['id'] in ids, meta))
