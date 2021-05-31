import json
from datetime import date
from pathlib import Path
from .utils import DATA_URL, dests, geoms

cwd = Path(__file__).parent
outputs = (cwd / '../data')


def main():
    outputs.mkdir(exist_ok=True, parents=True)
    data = []
    for dest in dests:
        row = {
            'id': dest,
            'date': date.today().strftime('%Y-%m-%d'),
        }
        for geom in geoms:
            row[geom] = {
                'gpkg': f'{DATA_URL}/{dest}/adm_{geom}.gpkg.zip',
                'xlsx': f'{DATA_URL}/{dest}/adm_{geom}.xlsx.zip',
            }
        data.append(row)
    with open((outputs / 'edge-matched.json'), 'w') as f:
        json.dump(data, f, separators=(',', ':'))
