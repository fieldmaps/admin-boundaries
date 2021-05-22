from pathlib import Path
from multiprocessing import Pool
from . import (inputs_adm0, inputs, polygons,
               lines, points, outputs, cleanup, cleanup_adm0)
from .utils import logging, apply_funcs, adm0_list, srcs

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent
funcs = [inputs.main, polygons.main, lines.main,
         points.main, outputs.main, cleanup.main]


def import_boundaries():
    results = []
    pool = Pool()
    for src in srcs:
        for row in adm0_list[src]:
            args = [src, row['id'], row['adm_full'], *funcs]
            result = pool.apply_async(apply_funcs, args=args)
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
