from pathlib import Path
from multiprocessing import Pool
from . import (inputs_adm0, inputs_voronoi, inputs_attributes, clip, merge,
               merge_cleanup, polygons, lines, points, outputs, outputs_cleanup)
from .utils import apply_funcs, is_polygon, adm0

geo_ext = ['.gpkg', '.shp', '.geojson']
db_ext = ['.db', '.sqlite', '.sqlite3', '.xlsx']
cwd = Path(__file__).parent
files_voronoi = sorted((cwd / '../inputs/voronoi').iterdir())
files_attributes = sorted((cwd / '../inputs/attributes').iterdir())
voronoi_funcs = [inputs_voronoi.main, clip.main]
geometries = ['polygons', 'lines', 'points']


def import_adm0():
    results = []
    pool = Pool()
    for geometry in geometries:
        args = [f'adm0_{geometry}', cwd / f'../inputs/adm0/{adm0[geometry]}']
        result = pool.apply_async(inputs_adm0.main, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def import_voronoi():
    results = []
    pool = Pool()
    for file in files_voronoi:
        if file.is_file() and file.suffix in geo_ext and is_polygon(file):
            args = [file.name.replace('.', '_'), file, *voronoi_funcs]
            result = pool.apply_async(apply_funcs, args=args)
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def import_attributes():
    files = []
    for file in files_attributes:
        if file.is_file() and file.suffix in db_ext:
            files.append(file)
    inputs_attributes.main(files)


def merge_all():
    layers = []
    for file in files_voronoi:
        if file.is_file() and file.suffix in geo_ext and is_polygon(file):
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
    outputs_cleanup.main(range(6), geometries + ['attributes'])
    outputs_cleanup.main(['x'], ['polygons'])


if __name__ == '__main__':
    import_adm0()
    import_voronoi()
    import_attributes()
    merge_all()
    polygon_processing()
    line_point_processing()
    export_all()
