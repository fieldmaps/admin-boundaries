import pandas as pd
from pathlib import Path
from .utils import DATA_URL, meta

cwd = Path(__file__).parent
outputs = cwd / '../../outputs'


def main(name):
    outputs.mkdir(exist_ok=True, parents=True)
    df = meta[name]
    df['e_gpkg'] = f'{DATA_URL}/{name}/extended/' + df['id'] + '.gpkg.zip'
    df['e_shp'] = f'{DATA_URL}/{name}/extended/' + df['id'] + '.shp.zip'
    df['e_xlsx'] = f'{DATA_URL}/{name}/extended/' + df['id'] + '.xlsx.zip'
    df['o_gpkg'] = f'{DATA_URL}/{name}/originals/' + \
        df['iso_3'].str.lower() + '.gpkg.zip'
    df['o_shp'] = f'{DATA_URL}/{name}/originals/' + \
        df['iso_3'].str.lower() + '.shp.zip'
    df['o_xlsx'] = f'{DATA_URL}/{name}/originals/' + \
        df['iso_3'].str.lower() + '.xlsx.zip'
    df.to_csv(outputs / f'{name}.csv', index=False)
    df.to_excel(outputs / f'{name}.xlsx', index=False)
    df['src_date'] = pd.to_datetime(df['src_date']).dt.strftime('%Y-%m-%d')
    df['src_update'] = pd.to_datetime(df['src_update']).dt.strftime('%Y-%m-%d')
    data = df.to_json(orient='records')
    with open((outputs / f'{name}.json'), 'w') as f:
        f.write(data)
