import subprocess
from pathlib import Path
from .utils import logging

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
outputs = (cwd / '../outputs').resolve()
outputs.mkdir(exist_ok=True, parents=True)


def main(levels, geometry, file):
    logger.info(f'Starting {file}')
    for level in levels:
        subprocess.run([
            'ogr2ogr',
            '-overwrite',
            '-nln', f'adm{level}_{geometry}',
            (outputs / file).resolve(),
            'PG:dbname=edge_matcher', f'adm{level}_{geometry}',
        ])
    logger.info(f'Finished {file}')
