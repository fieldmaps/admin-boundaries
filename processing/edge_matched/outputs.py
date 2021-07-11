import subprocess
from pathlib import Path
from .utils import DATABASE, logging, geoms

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def export_layer(output, src, name, layer, level):
    for l in range(level+1):
        for geom in geoms:
            subprocess.run([
                'ogr2ogr',
                '-append',
                '-makevalid',
                '-mapFieldType', 'DateTime=Date',
                '-nln', f'{name}_adm{l}_{geom}',
                output,
                f'PG:dbname={DATABASE}', f'{src}_{layer}_adm{l}_{geom}',
            ])


def main(cur, src, name, level, ids):
    outputs = (cwd / f'../../data/{src}/clipped').resolve()
    outputs.mkdir(exist_ok=True, parents=True)
    output = (outputs / f'{name}.gpkg').resolve()
    output.unlink(missing_ok=True)
    if ids is not None:
        for num in range(1, ids+1):
            export_layer(output, src, name, f'{name}_{num}', level)
    else:
        export_layer(output, src, name, name, level)
    logger.info(f'{name}_{src}')
