from pathlib import Path
from multiprocessing import Pool
from . import inputs_admx, inputs_adm0, clip, merge, dissolve
from .utils import apply_funcs, is_polygon, adm0

ext = ['.shp', '.geojson', '.gpkg']
cwd = Path(__file__).parent
files = sorted((cwd / '../inputs_admx').iterdir())
pre_funcs = [inputs_admx.main, clip.main]
post_funcs = [dissolve.main]


def pre_processing():
    inputs_adm0.main('adm0', cwd / f"../inputs_adm0/{adm0['polygons']}")
    results = []
    pool = Pool()
    for file in files:
        if file.is_file() and file.suffix in ext and is_polygon(file):
            args = [file.name.replace('.', '_'), file, *pre_funcs]
            result = pool.apply_async(apply_funcs, args=args)
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def merging():
    layers = []
    for file in files:
        if file.is_file() and file.suffix in ext and is_polygon(file):
            layers.append(file.name.replace('.', '_'))
    merge.main(layers)


def post_processing():
    results = []
    pool = Pool()
    for level in range(5, 0, -1):
        args = [level, None, *post_funcs]
        result = pool.apply_async(apply_funcs, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


if __name__ == '__main__':
    pre_processing()
    merging()
    post_processing()
