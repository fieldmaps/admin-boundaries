import json
from pathlib import Path
from .utils import DATA_URL, originals_list, extended_list

cwd = Path(__file__).parent
outputs = cwd / '../../outputs'


def main(name):
    outputs.mkdir(exist_ok=True, parents=True)
    data = []
    for row in originals_list[name]:
        row['type'] = 'originals'
        for ext in ['gpkg', 'shp', 'svg', 'xlsx']:
            row[ext] = f"{DATA_URL}/{name}/originals/{row['id']}.{ext}.zip"
        data.append(row)
    for row in extended_list[name]:
        row['type'] = 'extended'
        for ext in ['gpkg', 'shp']:
            row[ext] = f"{DATA_URL}/{name}/extended/{row['id']}.{ext}.zip"
        data.append(row)
    with open((outputs / f'{name}.json'), 'w') as f:
        json.dump(data, f, separators=(',', ':'))
