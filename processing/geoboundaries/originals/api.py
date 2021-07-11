from multiprocessing import Pool
from pathlib import Path
import pandas as pd
from .constants import partial_layers
from .utils import logging, http, API_URL, DATABASE

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
inputs = (cwd / '../../inputs/geoboundaries_1').resolve()
inputs.mkdir(exist_ok=True, parents=True)


def save_meta():
    con = f'postgresql:///{DATABASE}'
    df = pd.read_json(API_URL)
    df = df[~df['boundaryID'].isin(partial_layers)]
    df['boundaryYear'] = pd.to_numeric(df['boundaryYear'], downcast='signed')
    df['boundaryYear'] = pd.to_datetime(df['boundaryYear'],
                                        format='%Y').dt.date
    df['boundaryType'] = df['boundaryType'].str.extract('(\d+)').astype(int)
    df['boundaryUpdate'] = pd.to_datetime(df['boundaryUpdate']).dt.date
    df.to_sql(f'geoboundaries_meta', con,
              if_exists='replace', index=False, method='multi')


def download(row):
    filename = f"{row['boundaryISO']}_{row['boundaryType']}.geojson".lower()
    logger.info(filename)
    r = http.get(row['gjDownloadURL'])
    open(inputs / filename, 'wb').write(r.content)


def main():
    save_meta()
    data = http.get(API_URL).json()
    rows = list(filter(lambda x: x['boundaryID'] not in partial_layers, data))
    results = []
    pool = Pool()
    for row in rows:
        result = pool.apply_async(download, args=[row])
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
