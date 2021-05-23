import logging
import requests

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

DATABASE = 'edge_matcher'

geoms = ['lines', 'points', 'polygons']
srcs = ['cod', 'geoboundaries']
dests = ['open', 'humanitarian']

src_list = {}
for src in srcs:
    meta = requests.get(f'https://data.fieldmaps.io/{src}/meta.json').json()
    src_list[src] = list(
        filter(lambda x: x['adm_full'] is not None and x['adm_full'] >= 1,
               meta)
    )

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


def apply_funcs(src, name, level, *args):
    for func in args:
        func(src, name, level)


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
    return ids
