import subprocess
from pathlib import Path
from .utils import logging, dest_list

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent
data = (cwd / '../../data').resolve()


def cleanup(dest, geom):
    output = (data / f'edge-matched/{dest}/adm_{geom}.gpkg')
    output.unlink(missing_ok=True)
    logger.info(f'{dest}_{geom}')


def main(dest, geom):
    file_adm0 = (data / f'adm0/adm0_{geom}.gpkg')
    output = (data / f'edge-matched/{dest}/adm_{geom}.gpkg')
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
        file = (data / f"{row['src']}/clipped/{row['id']}.gpkg")
        for level in range(1, row['lvl_full'] + 1):
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
