import logging
import requests
from configparser import ConfigParser
from pathlib import Path
from psycopg2 import connect


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

DATABASE = 'edge_matcher'
DATA_URL = 'https://data.fieldmaps.io'


def apply_funcs(src, name, level, ids, *args):
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    for func in args:
        func(cur, src, name, level, ids)
    cur.close()
    con.close()


def get_ids(level):
    ids = []
    for l in range(level, -1, -1):
        ids.extend([
            f'adm{l}_id',
            f'adm{l}_src',
            f'adm{l}_name',
            f'adm{l}_name1',
            f'adm{l}_name2',
        ])
    ids.extend([
        'iso3', 'iso2',
        'lvl_full', 'lvl_part',
        'lang', 'lang1', 'lang2',
        'src_date', 'src_update',
        'src_name', 'src_org', 'src_lic', 'src_url', 'src_grp',
    ])
    return ids


def get_clip_ids(src):
    cwd = Path(__file__).parent
    cfg = ConfigParser()
    cfg.read((cwd / '../config.ini'))
    config = cfg['default']
    ids = config[src].split(',')
    return list(filter(lambda x: x != '', ids))


geoms = ['lines', 'points', 'polygons']
srcs = ['cod', 'geoboundaries']
dests = ['humanitarian', 'open']

src_list = {}
for src in srcs:
    meta = requests.get(f'{DATA_URL}/{src}.json').json()
    src_list[src] = list(
        filter(lambda x: x['lvl_full'] is not None and x['lvl_full'] >= 1,
               meta)
    )
    clip_ids = get_clip_ids(src)
    if len(clip_ids) > 0:
        src_list[f'{src}_clip'] = list(
            filter(lambda x: x['id'] in clip_ids, src_list[src]))
    else:
        src_list[f'{src}_clip'] = src_list[src]

dest_list = {}
dest_list['open'] = {}
for row in src_list['geoboundaries']:
    row['src'] = 'geoboundaries'
    dest_list['open'][row['id']] = row
dest_list['humanitarian'] = {**dest_list['open']}
for row in src_list['cod']:
    row['src'] = 'cod'
    dest_list['humanitarian'][row['id']] = row

dest_list['open'] = dest_list['open'].values()
dest_list['humanitarian'] = dest_list['humanitarian'].values()
