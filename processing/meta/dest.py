import json
from datetime import date
from pathlib import Path
from .utils import DATA_URL, dests, world_views, geoms

cwd = Path(__file__).parent
outputs = cwd / '../../outputs'


def main(name):
    outputs.mkdir(exist_ok=True, parents=True)
    data = []
    for dest in dests:
        for wld in world_views:
            for l in range(5):
                row = {
                    'id': dest,
                    'adm': l,
                    'wld_view': wld,
                    'date': date.today().strftime('%Y-%m-%d'),
                }
                for geom in geoms:
                    row[geom] = {}
                    for ext in ['gpkg', 'shp', 'xlsx']:
                        row[geom][ext] = f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_{geom}.{ext}.zip',
                data.append(row)
    with open((outputs / f'{name}.json'), 'w') as f:
        json.dump(data, f, separators=(',', ':'))
