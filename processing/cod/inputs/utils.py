import pandas as pd
from pathlib import Path
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

Configuration.create(hdx_read_only=True, user_agent='read_only')
cwd = Path(__file__).parent
# COD_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQLc4p08eCEGMUjCNt4rcVj4KExhgtFqD2fuXWseoRZtgF2eNl74qCk6Jhezb7xMjlCy1WZP4NaMKy-/pub?gid=2003220882&single=true&output=csv'
COD_URL = cwd / '../../../inputs/cod_tmp.csv'

fieldmap = {
    'ISO 3166-1 Alpha 3-Codes': 'iso_3',
    'Country name': 'name',
    'Country category': 'status',
    'COD-AB URL': 'src_url',
}


def get_cod_meta():
    df = pd.read_csv(COD_URL)
    df = df[fieldmap.keys()]
    df = df.rename(columns=fieldmap)
    return df.to_dict('records')


def get_hdx_metadata(url):
    dataset_id = url[33:]
    data = Dataset.read_from_hdx(dataset_id)
    return data


def join_hdx_meta(row, hdx):
    row['src_date'] = hdx['dataset_date'][1:11]
    row['src_update'] = hdx['last_modified'][:10]
    row['src_name'] = hdx['dataset_source']
    row['src_name1'] = hdx['organization']['title']
    row['src_lic'] = hdx['license_title']
    return row
