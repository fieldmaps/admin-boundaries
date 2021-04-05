from pathlib import Path
from multiprocessing import Pool
from . import (inputs_admx, inputs_adm0, clip, merge, merge_cleanup,
               polygons, lines, points, outputs, outputs_cleanup)
from .utils import apply_funcs, is_polygon, adm0

ext = ['.shp', '.geojson', '.gpkg']
cwd = Path(__file__).parent
files = sorted((cwd / '../inputs_admx').iterdir())
pre_funcs = [inputs_admx.main, clip.main]
geometries = ['polygons', 'lines', 'points']


def import_adm0():
    results = []
    pool = Pool()
    for geometry in geometries:
        args = [f'adm0_{geometry}', cwd / f'../inputs_adm0/{adm0[geometry]}']
        result = pool.apply_async(inputs_adm0.main, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def import_admx():
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


def merge_all():
    layers = []
    for file in files:
        if file.is_file() and file.suffix in ext and is_polygon(file):
            layers.append(file.name.replace('.', '_'))
    merge.main(layers)
    merge_cleanup.main(layers)


def polygon_processing():
    results = []
    pool = Pool()
    for level in range(1, 6):
        args = [level]
        result = pool.apply_async(polygons.main, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def line_point_processing():
    results = []
    pool = Pool()
    for level in range(1, 6):
        args = [level]
        result_1 = pool.apply_async(lines.main, args=args)
        result_2 = pool.apply_async(points.main, args=args)
        results.append(result_1)
        results.append(result_2)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def export_all():
    results = []
    pool = Pool()
    for geometry in geometries:
        args = [range(6), geometry, f'wld_{geometry}.gpkg']
        result = pool.apply_async(outputs.main, args=args)
        results.append(result)
    for geometry in ['polygons']:
        args = [['x'], geometry, f'admx_{geometry}.gpkg']
        result = pool.apply_async(outputs.main, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
    outputs_cleanup.main(range(6), geometries)
    outputs_cleanup.main(['x'], ['polygons'])


if __name__ == '__main__':
    import_adm0()
    import_admx()
    merge_all()
    polygon_processing()
    line_point_processing()
    export_all()
