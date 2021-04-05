import re
import logging
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


def is_polygon(file):
    regex = re.compile(r'\((Multi Polygon|Polygon)\)')
    result = run(['ogrinfo', file], capture_output=True)
    return regex.search(str(result.stdout))


def apply_funcs(name, file, *args):
    for func in args:
        func(name, file)
