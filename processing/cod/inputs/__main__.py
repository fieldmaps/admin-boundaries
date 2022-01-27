from pathlib import Path
import pandas as pd
from .utils import get_cod_meta, get_hdx_metadata, join_hdx_meta

cwd = Path(__file__).parent

if __name__ == '__main__':
    cod_meta = get_cod_meta()
    for row in cod_meta:
        if str(row['src_url']).startswith('https://data.humdata.org/dataset/'):
            hdx_meta = get_hdx_metadata(row['src_url'])
            if hdx_meta is not None:
                row = join_hdx_meta(row, hdx_meta)
    df = pd.DataFrame(cod_meta)
    df['src_date'] = pd.to_datetime(df['src_date'])
    df['src_update'] = pd.to_datetime(df['src_update'])
    df = df.sort_values(by='iso_3')
    df.to_csv(cwd / '../../../inputs/cod.csv', index=False)
