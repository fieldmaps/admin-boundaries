import json
import logging
from configparser import ConfigParser
from pathlib import Path
from psycopg2 import connect

DATABASE = 'admin_boundaries'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def apply_funcs(name, level, langs, row, *args):
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    for func in args:
        func(cur, name, level, langs, row)
    cur.close()
    con.close()


def get_ids():
    cwd = Path(__file__).parent
    cfg = ConfigParser()
    cfg.read((cwd / '../../../config.ini'))
    config = cfg['default']
    ids = config['cod'].split(',')
    return list(filter(lambda x: x != '', ids))


def get_meta():
    cwd = Path(__file__).parent
    with open(cwd / f'../../../data/cod.json') as f:
        return json.load(f)


def get_filter_config():
    cwd = Path(__file__).parent
    with open(cwd / f'../../../config_cod.json') as f:
        return json.load(f)


ids = get_ids()
meta = get_meta()
filter_config = get_filter_config()
adm0_list = list(
    filter(lambda x: x['lvl_full'] is not None and x['lvl_full'] >= 1, meta)
)
if len(ids) > 0:
    adm0_list = list(filter(lambda x: x['id'] in ids, adm0_list))
