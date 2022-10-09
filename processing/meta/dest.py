import json
import pandas as pd
from pathlib import Path
from processing.meta.utils import DATA_URL, dests, world_views, land_date

cwd = Path(__file__).parent
outputs = cwd / '../../outputs'


def main(name):
    outputs.mkdir(exist_ok=True, parents=True)
    data = []
    for dest in dests:
        for wld in world_views:
            for l in range(4, 0, -1):
                row = {
                    'id': f'{dest}_{wld}_adm{l}',
                    'grp': dest,
                    'wld': wld,
                    'adm': l,
                    'date': land_date,
                    'a_gpkg': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_polygons.gpkg.zip',
                    'a_shp': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_polygons.shp.zip',
                    'a_xlsx': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_polygons.xlsx',
                    'l_gpkg': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_lines.gpkg.zip',
                    'l_shp': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_lines.shp.zip',
                    'l_xlsx': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_lines.xlsx',
                    'p_gpkg': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_points.gpkg.zip',
                    'p_shp': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_points.shp.zip',
                    'p_xlsx': f'{DATA_URL}/{name}/{dest}/{wld}/adm{l}_points.xlsx',
                }
                data.append(row)
    with open((outputs / f'{name}.json'), 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.date
    df.to_csv(outputs / f'{name}.csv', index=False)
    df.to_excel(outputs / f'{name}.xlsx', index=False)
