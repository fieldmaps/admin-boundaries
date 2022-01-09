import json
import pandas as pd
from pathlib import Path
from .utils import DATA_URL, dests, world_views, geoms, land_date

cwd = Path(__file__).parent
outputs = cwd / '../../outputs'


def main(name):
    outputs.mkdir(exist_ok=True, parents=True)
    data = []
    for dest in dests:
        for wld in world_views:
            for l in range(4, -1, -1):
                for geom in geoms:
                    row = {
                        'id': f'{dest}_{wld}_adm{l}_{geom}',
                        'grp': dest,
                        'wld': wld,
                        'adm': l,
                        'geom': geom,
                        'date': land_date,
                        'url_gpkg': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_{geom}.gpkg.zip',
                        'url_shp': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_{geom}.shp.zip',
                        'url_xlsx': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_{geom}.xlsx',
                    }
                    data.append(row)
    with open((outputs / f'{name}.json'), 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    df = pd.DataFrame(data)
    df.to_csv(outputs / f'{name}.csv', index=False)
    df.to_excel(outputs / f'{name}.xlsx', index=False)
