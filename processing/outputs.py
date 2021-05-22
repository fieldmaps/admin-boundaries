import subprocess
from pathlib import Path
from psycopg2 import connect
from .utils import DATABASE, logging, geoms

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def main(src, name, level):
    outputs = (cwd / f'../outputs/{src}').resolve()
    outputs.mkdir(exist_ok=True, parents=True)
    output = (outputs / f'{name}.gpkg').resolve()
    output.unlink(missing_ok=True)
    con = connect(database=DATABASE)
    cur = con.cursor()
    for l in range(level+1):
        for geom in geoms:
            subprocess.run([
                'ogr2ogr',
                '-overwrite',
                '-makevalid',
                '-nln', f'{name}_adm{l}_{geom}',
                output,
                f'PG:dbname={DATABASE}', f'{src}_{name}_adm{l}_{geom}',
            ])
    cur.close()
    con.close()
    logger.info(f'{name}_{src}')
