import logging
import pandas as pd
from pathlib import Path

API_URL = 'https://www.geoboundaries.org/api/current/gbOpen/ALL/ALL/'
cwd = Path(__file__).parent

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

fieldmap = {
    'boundaryISO': 'iso_3',
    'boundaryName': 'name',
    'boundaryType': 'src_lvl',
    'boundaryYearRepresented': 'src_date',
    'sourceDataUpdateDate': 'src_update',
    'boundarySource-1': 'src_name',
    'boundarySource-2': 'src_name1',
    'boundaryLicense': 'src_lic',
    'licenseDetail': 'src_lic1',
    'licenseSource': 'src_lic2',
    'gjDownloadURL': 'src_url1',
}


def get_meta():
    df = pd.read_json(API_URL)
    df = df.replace({'nan': None})
    df = df[fieldmap.keys()]
    df = df.rename(columns=fieldmap)
    df['src_date'] = pd.to_numeric(df['src_date'], downcast='signed')
    df['src_date'] = pd.to_datetime(df['src_date'], format='%Y').dt.date
    df['src_lvl'] = df['src_lvl'].str.extract('(\d+)').astype(int)
    df['src_update'] = pd.to_datetime(
        df['src_update'], format='%b %d, %Y').dt.date
    df['src_url'] = df['iso_3'].apply(
        lambda x: f'https://www.geoboundaries.org/api/current/gbOpen/{x}/ALL/')
    df = df.sort_values(by=['iso_3', 'src_lvl'])
    df.to_csv(cwd / '../../../inputs/geoboundaries.csv', index=False)
    return df.to_dict('records')


adm0_list = get_meta()
