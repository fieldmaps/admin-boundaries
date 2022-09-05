import json
import pandas as pd
from processing.cod.gb_humanitarian.utils import (
    DATA_URL, cwd, logging, adm0_list_original, land_date)

logger = logging.getLogger(__name__)
outputs = cwd / '../../../outputs'
name = 'gb-humanitarian'


def main():
    logger.info('starting')
    data = []
    for adm0 in adm0_list_original:
        for l in range(adm0['src_lvl'] + 1):
            id = f"{adm0['id']}_adm{l}"
            row = {
                'id': id,
                'iso_3': adm0['iso_3'],
                'adm': l,
                'wld_date': land_date,
                'src_date': adm0['src_update'],
                'shp': f'{DATA_URL}/{name}/originals/{id.upper()}.zip',
            }
            data.append(row)
    with open((outputs / f'{name}.json'), 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    df = pd.DataFrame(data)
    df['wld_date'] = pd.to_datetime(df['wld_date'])
    df['wld_date'] = df['wld_date'].dt.date
    df['src_date'] = pd.to_datetime(df['src_date'])
    df['src_date'] = df['src_date'].dt.date
    df.to_csv(outputs / f'{name}.csv', index=False)
    df.to_excel(outputs / f'{name}.xlsx', index=False)
