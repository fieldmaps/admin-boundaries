import json
import logging
from pathlib import Path
import pandas as pd
from configparser import ConfigParser
from psycopg2 import connect


def apply_funcs(name, level, row, *args):
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    for func in args:
        func(cur, name, level, row)
    cur.close()
    con.close()


langs = [
    'aa', 'ab', 'ae', 'af', 'ak', 'am', 'an', 'ar', 'as', 'av', 'ay', 'az',
    'ba', 'be', 'bg', 'bh', 'bi', 'bm', 'bn', 'bo', 'br', 'bs', 'ca', 'ce',
    'ch', 'co', 'cr', 'cs', 'cu', 'cv', 'cy', 'da', 'de', 'dv', 'dz', 'ee',
    'el', 'en', 'eo', 'es', 'et', 'eu', 'fa', 'ff', 'fi', 'fj', 'fo', 'fr',
    'fy', 'ga', 'gd', 'gl', 'gn', 'gu', 'gv', 'ha', 'he', 'hi', 'ho', 'hr',
    'ht', 'hu', 'hy', 'hz', 'ia', 'id', 'ie', 'ig', 'ii', 'ik', 'io', 'is',
    'it', 'iu', 'ja', 'jv', 'ka', 'kg', 'ki', 'kj', 'kk', 'kl', 'km', 'kn',
    'ko', 'kr', 'ks', 'ku', 'kv', 'kw', 'ky', 'la', 'lb', 'lg', 'li', 'ln',
    'lo', 'lt', 'lu', 'lv', 'mg', 'mh', 'mi', 'mk', 'ml', 'mn', 'mr', 'ms',
    'mt', 'my', 'na', 'nb', 'nd', 'ne', 'ng', 'nl', 'nn', 'no', 'nr', 'nv',
    'ny', 'oc', 'oj', 'om', 'or', 'os', 'pa', 'pi', 'pl', 'ps', 'pt', 'qu',
    'rm', 'rn', 'ro', 'ru', 'rw', 'sa', 'sc', 'sd', 'se', 'sg', 'si', 'sk',
    'sl', 'sm', 'sn', 'so', 'sq', 'sr', 'ss', 'st', 'su', 'sv', 'sw', 'ta',
    'te', 'tg', 'th', 'ti', 'tk', 'tl', 'tn', 'to', 'tr', 'ts', 'tt', 'tw',
    'ty', 'ug', 'uk', 'ur', 'uz', 've', 'vi', 'vo', 'wa', 'wo', 'xh', 'yi',
    'yo', 'za', 'zh', 'zu',
]


def get_cols():
    result = {}
    for l in range(5, -1, -1):
        result[f'admin{l}Pcode'] = f'ADM{l}_PCODE'
        result[f'admin{l}RefName'] = f'ADM{l}_REF'
        for lang in langs:
            result[f'admin{l}Name_{lang}'] = f'ADM{l}_{lang.upper()}'
            result[f'admin{l}AltName1_{lang}'] = f'ADM{l}ALT1{lang.upper()}'
            result[f'admin{l}AltName2_{lang}'] = f'ADM{l}ALT2{lang.upper()}'
    result['date'] = 'DATE'
    result['validOn'] = 'VALIDON'
    result['validTo'] = 'VALIDTO'
    return result


def get_meta():
    cwd = Path(__file__).parent
    config_path = (cwd / '../../../data/cod/originals/meta.xlsx').resolve()
    dtypes = {'lvl_full': 'Int8', 'lvl_part': 'Int8',
              'ids': 'Int8', 'operational': 'bool',
              'api_cod': 'bool', 'api_fieldmaps': 'bool'}
    df = pd.read_excel(config_path, engine='openpyxl', dtype=dtypes)
    df['src_date'] = df['src_date'].astype(str).replace('NaT', None)
    df['src_update'] = df['src_update'].astype(str).replace('NaT', None)
    return json.loads(df.to_json(orient='records'))


def get_ids():
    cwd = Path(__file__).parent
    cfg = ConfigParser()
    cfg.read((cwd / '../../../config.ini'))
    config = cfg['default']
    ids = config['cod'].split(',')
    return list(filter(lambda x: x != '', ids))


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

DATABASE = 'admin_boundaries'
DATA_URL = 'https://data.fieldmaps.io'

ids = get_ids()
meta = get_meta()
adm0_list = list(
    filter(lambda x: x['lvl_full'] is not None and x['lvl_full'] >= 1, meta)
)
if len(ids) > 0:
    adm0_list = list(
        filter(lambda x: x['id'] in ids, adm0_list)
    )
