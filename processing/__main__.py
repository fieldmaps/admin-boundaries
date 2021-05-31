from pathlib import Path
from multiprocessing import Pool
from . import (inputs_adm0, inputs, polygons, lines, points,
               outputs, cleanup, cleanup_adm0, merge, formats, meta)
from .utils import (logging, apply_funcs, srcs, src_list, dests, geoms)

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent
funcs = [inputs.main, polygons.main, lines.main,
         points.main, outputs.main, cleanup.main]


def import_boundaries():
    results = []
    pool = Pool()
    for src in srcs:
        for row in src_list[f'{src}_clip']:
            args = [src, row['id'], row['lvl_full'], *funcs]
            result = pool.apply_async(apply_funcs, args=args)
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def outputs_merge(func):
    results = []
    pool = Pool()
    for dest in dests:
        for geom in geoms:
            result = pool.apply_async(func, args=[dest, geom])
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


if __name__ == '__main__':
    logger.info('starting')
    inputs_adm0.main()
    import_boundaries()
    cleanup_adm0.main()
    outputs_merge(merge.main)
    outputs_merge(formats.main)
    outputs_merge(merge.cleanup)
    meta.main()
