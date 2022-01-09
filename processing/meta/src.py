import pandas as pd
from pathlib import Path
from .utils import DATA_URL, meta

cwd = Path(__file__).parent
outputs = cwd / '../../outputs'


def main(name):
    outputs.mkdir(exist_ok=True, parents=True)
    df1 = meta[name]['originals']
    df1['adm0_name'] = df1['adm0_name1']
    df1['adm0_name1'] = None
    df1['grp'] = 'originals'
    df1['url_gpkg'] = f'{DATA_URL}/{name}/originals/' + df1['id'] + '.gpkg.zip'
    df1['url_shp'] = f'{DATA_URL}/{name}/originals/' + df1['id'] + '.shp.zip'
    df1['url_xlsx'] = f'{DATA_URL}/{name}/originals/' + df1['id'] + '.xlsx.zip'
    df2 = meta[name]['extended']
    df2['grp'] = 'extended'
    df2['url_gpkg'] = f'{DATA_URL}/{name}/extended/' + df2['id'] + '.gpkg.zip'
    df2['url_shp'] = f'{DATA_URL}/{name}/extended/' + df2['id'] + '.shp.zip'
    df2['url_xlsx'] = f'{DATA_URL}/{name}/extended/' + df2['id'] + '.xlsx.zip'
    df = pd.concat([df1, df2])
    df.to_csv(outputs / f'{name}.csv', index=False)
    df.to_excel(outputs / f'{name}.xlsx', index=False)
    df['src_date'] = pd.to_datetime(df['src_date']).dt.strftime('%Y-%m-%d')
    df['src_update'] = pd.to_datetime(df['src_update']).dt.strftime('%Y-%m-%d')
    data = df.to_json(orient='records')
    with open((outputs / f'{name}.json'), 'w') as f:
        f.write(data)
