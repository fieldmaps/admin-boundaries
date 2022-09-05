import pandas as pd
from processing.cod.gb_humanitarian.utils import logging, DATABASE

logger = logging.getLogger(__name__)
con = f'postgresql:///{DATABASE}'


def main(_, name, level, langs, *__):
    query = f"""
        SELECT
            admin{level}name_{langs[0]} AS Name,
            admin{level}pcode AS PCode
        FROM {name}_adm{level}_00
    """
    df = pd.read_sql_query(query, con)
    df.to_sql(f'{name}_adm{level}_01', con,
              if_exists='replace', index=False, method='multi')
    logger.info(f'{name}_adm{level}')
