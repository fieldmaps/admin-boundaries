import subprocess
from pathlib import Path
from .utils import logging, dest_list

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent
inputs = (cwd / '../inputs').resolve()
outputs = (cwd / '../outputs').resolve()


def cleanup(dest, geom):
    output = (outputs / f'{dest}/adm_{geom}.gpkg')
    output.unlink(missing_ok=True)
    logger.info(f'{dest}_{geom}')


def main(dest, geom):
    file_adm0 = (inputs / f'adm0/adm0_{geom}.gpkg')
    output = (outputs / f'{dest}/adm_{geom}.gpkg')
    output.unlink(missing_ok=True)
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '-dim', 'XY',
        '-t_srs', 'EPSG:4326',
        output,
        file_adm0,
    ])
    for row in dest_list[dest]:
        file = (outputs / f"{row['src']}/{row['id']}.gpkg")
        for level in range(1, row['adm_full'] + 1):
            subprocess.run([
                'ogr2ogr',
                '-append',
                '-dim', 'XY',
                '-t_srs', 'EPSG:4326',
                '-nln', f'adm{level}_{geom}',
                output,
                file, f"{row['id']}_adm{level}_{geom}",
            ])
    logger.info(f'{dest}_{geom}')
