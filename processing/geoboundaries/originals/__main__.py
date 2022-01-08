import shutil
from multiprocessing import Pool
from . import inputs, overlay, dissolve, outputs, cleanup
from .utils import logging, adm0_list, apply_funcs

logger = logging.getLogger(__name__)
funcs = [inputs.main, overlay.main, dissolve.main, outputs.main, cleanup.main]

if __name__ == '__main__':
    logger.info('starting')
    shutil.rmtree(outputs.data, ignore_errors=True)
    shutil.rmtree(outputs.outputs, ignore_errors=True)
    results = []
    pool = Pool()
    for row in adm0_list:
        args = [row['id'], row['src_lvl'], *funcs]
        result = pool.apply_async(apply_funcs, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
