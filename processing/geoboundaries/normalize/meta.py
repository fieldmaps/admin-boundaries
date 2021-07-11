import json
from pathlib import Path
from .utils import meta, DATA_URL

cwd = Path(__file__).parent
outputs = (cwd / '../../../data')


def main():
    outputs.mkdir(exist_ok=True, parents=True)
    data = meta.copy()
    for row in data:
        if row['lvl_full'] is not None and row['lvl_full'] >= 1:
            for group in ['normalized']:
                row[group] = {}
                for ext in ['gpkg', 'shp.zip', 'xlsx']:
                    row[group][ext] = f"{DATA_URL}/geoboundaries/{group}/{row['id']}.{ext}"
            for group in ['standardized', 'extended', 'clipped']:
                row[group] = {}
                for ext in ['gpkg']:
                    row[group][ext] = f"{DATA_URL}/geoboundaries/{group}/{row['id']}.{ext}"
    with open((outputs / 'geoboundaries.json'), 'w') as f:
        json.dump(data, f, separators=(',', ':'))
