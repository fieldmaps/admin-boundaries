import logging
import requests

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

DATABASE = 'edge_matcher'

geoms = ['lines', 'points', 'polygons']
srcs = ['cod', 'geoboundaries']

adm0_list = {}
for src in srcs:
    meta = requests.get(f'https://data.fieldmaps.io/{src}/meta.json').json()
    adm0_list[src] = list(
        filter(lambda x: x['adm_full'] is not None and x['adm_full'] >= 1,
               meta)
    )


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
