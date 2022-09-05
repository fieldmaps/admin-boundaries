import logging
import pandas as pd
from configparser import ConfigParser
from pathlib import Path
from psycopg import connect

DATABASE = 'admin_boundaries'

cwd = Path(__file__).parent
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

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


def apply_funcs(name, level, row, *args):
    conn = connect(f'dbname={DATABASE}', autocommit=True)
    for func in args:
        func(conn, name, level, row)
    conn.close()


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


def get_ids():
    cwd = Path(__file__).parent
    cfg = ConfigParser()
    cfg.read((cwd / '../../../config.ini'))
    config = cfg['default']
    ids = config['cod'].split(',')
    return list(filter(lambda x: x != '', ids))


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
    return df


def join_meta(df1, df2):
    df = df1.merge(df2, on='iso_3')
    return df.to_dict('records')


ids = get_ids()
meta_local = get_all_meta()
meta_src = get_src_meta()
adm0_list = join_meta(meta_local, meta_src)
if len(ids) > 0:
    adm0_list = filter(lambda x: x['id'] in ids, adm0_list)
