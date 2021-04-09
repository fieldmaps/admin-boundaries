import pandas as pd
from sqlalchemy import create_engine
import sqlite3
from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import attributes, logging

logger = logging.getLogger(__name__)
engine_string = 'postgresql://localhost/edge_matcher'


def parse_excel(file):
    engine = create_engine(engine_string)
    sheets = pd.ExcelFile(file)
    for level in range(6):
        table = attributes[f'adm{level}_table']
        if table in sheets.sheet_names:
            df = pd.read_excel(file, table).rename(
                columns={attributes[f'adm{level}_id']: f'adm{level}_id'})
            df.to_sql(f'attributes_adm{level}', con=engine,
                      if_exists='append', index=False, method='multi')


def sqlite_table_exists(table, cur):
    query = f"""
        SELECT count(name)
        FROM sqlite_master
        WHERE type='table' AND name='{table}'
    """
    return cur.execute(query).fetchone()[0] == 1


def parse_sqlite(file):
    engine = create_engine(engine_string)
    con = sqlite3.connect(file)
    cur = con.cursor()
    for level in range(6):
        table = attributes[f'adm{level}_table']
        if sqlite_table_exists(table, cur):
            df = pd.read_sql_query(f"SELECT * FROM {table}", con).rename(
                columns={attributes[f'adm{level}_id']: f'adm{level}_id'})
            df.to_sql(f'attributes_adm{level}', con=engine,
                      if_exists='append', index=False, method='multi')
    cur.close()
    con.close()


def main(files):
    logger.info(f'Starting attributes')
    con = connect(database='edge_matcher')
    cur = con.cursor()
    drop_tmp = """
        DROP TABLE IF EXISTS {table_adm0};
        DROP TABLE IF EXISTS {table_adm1};
        DROP TABLE IF EXISTS {table_adm2};
        DROP TABLE IF EXISTS {table_adm3};
        DROP TABLE IF EXISTS {table_adm4};
        DROP TABLE IF EXISTS {table_adm5};
    """
    cur.execute(SQL(drop_tmp).format(
        table_adm0=Identifier(f'attributes_adm0'),
        table_adm1=Identifier(f'attributes_adm1'),
        table_adm2=Identifier(f'attributes_adm2'),
        table_adm3=Identifier(f'attributes_adm3'),
        table_adm4=Identifier(f'attributes_adm4'),
        table_adm5=Identifier(f'attributes_adm5'),
    ))
    con.commit()
    cur.close()
    con.close()
    for file in files:
        if file.suffix == '.xlsx':
            parse_excel(file)
        else:
            parse_sqlite(file)
