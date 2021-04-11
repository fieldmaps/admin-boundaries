import subprocess
from pathlib import Path
from psycopg2 import connect
from .utils import logging, table_exists

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
outputs = (cwd / '../outputs').resolve()
outputs.mkdir(exist_ok=True, parents=True)


def main(levels, geometry, name):
    logger.info(f'Starting {name}')
    output = (outputs / name).resolve()
    output.unlink(missing_ok=True)
    con = connect(database='edge_matcher')
    cur = con.cursor()
    for level in levels:
        if level == 'x' or table_exists(cur, f'adm{level}_attributes'):
            subprocess.run([
                'ogr2ogr',
                '-overwrite',
                '-nln', f'adm{level}_{geometry}',
                output,
                'PG:dbname=edge_matcher', f'adm{level}_{geometry}',
            ])
    cur.close()
    con.close()
    logger.info(f'Finished {name}')
