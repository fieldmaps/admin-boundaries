import pandas as pd
from pathlib import Path
from .utils import logging, get_cols, DATABASE

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def main(_, name, level, row):
    con = f'postgresql:///{DATABASE}'
    file = (cwd / f'../../../inputs/cod/{name}.xlsx')
    sheets = pd.ExcelFile(file)
    for l in range(level, -1, -1):
        sheet = list(filter(lambda x: x.endswith(str(l)),
                            sheets.sheet_names))[0]
        df = sheets.parse(sheet_name=sheet, keep_default_na=False,
                          na_values=['', '#N/A'], dtype=str)
        df['date'] = pd.to_datetime(row['src_date'])
        df['date'] = df['date'].dt.date
        df['validOn'] = pd.to_datetime(row['src_update'])
        df['validOn'] = df['validOn'].dt.date
        df['validTo'] = None
        cols = list(filter(lambda x: x in get_cols().keys(), df.columns))
        df = df[cols]
        df.to_sql(f'{name}_adm{l}_attr', con,
                  if_exists='replace', index=False, method='multi')
        shp_dict = {k: get_cols()[k] for k in cols}
        df = df.rename(columns=shp_dict)
        df.to_sql(f'{name}_adm{l}_attr_shp', con,
                  if_exists='replace', index=False, method='multi')
    logger.info(name)
