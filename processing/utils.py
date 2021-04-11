import re
import logging
from psycopg2.sql import SQL, Literal
from subprocess import run
from configparser import ConfigParser
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

cwd = Path(__file__).parent
cfg = ConfigParser()
cfg.read((cwd / '../config.ini').resolve())
adm0 = cfg['adm0']
voronoi = cfg['voronoi']
attributes = cfg['attributes']


def table_exists(cur, table):
    query = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = {table}
    """
    cur.execute(SQL(query).format(table=Literal(table)))
    return cur.fetchone()[0] == 1


def is_polygon(file):
    regex = re.compile(r'\((Multi Polygon|Polygon)\)')
    result = run(['ogrinfo', file], capture_output=True)
    return regex.search(str(result.stdout))


def apply_funcs(name, file, *args):
    for func in args:
        func(name, file)
