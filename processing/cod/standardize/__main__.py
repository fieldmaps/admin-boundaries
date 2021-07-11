from multiprocessing import Pool
from . import inputs, recode, outputs, cleanup
from .utils import logging, adm0_list, apply_funcs

logger = logging.getLogger(__name__)
funcs = [inputs.main, recode.main, outputs.main, cleanup.main]

if __name__ == '__main__':
    logger.info('starting')
    results = []
    pool = Pool()
    for row in adm0_list:
        langs_all = [row['lang'], row['lang1'], row['lang2']]
        langs = list(filter(lambda x: x is not None, langs_all))
        args = [row['id'], row['lvl_full'], langs, row, *funcs]
        result = pool.apply_async(apply_funcs, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
