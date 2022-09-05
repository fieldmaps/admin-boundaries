from multiprocessing import Pool
from processing.cod.gb_humanitarian import inputs, outputs, cleanup, meta
from processing.cod.gb_humanitarian.utils import logging, adm0_list, apply_funcs

logger = logging.getLogger(__name__)
funcs = [inputs.main, outputs.main, cleanup.main]

if __name__ == '__main__':
    logger.info('starting')
    results = []
    pool = Pool()
    for row in adm0_list:
        langs_all = [row['src_lang'], row['src_lang1'], row['src_lang2']]
        langs = list(filter(lambda x: x is not None, langs_all))
        for level in range(row['src_lvl'] + 1):
            args = [row['id'], level, langs, row, *funcs]
            result = pool.apply_async(apply_funcs, args=args)
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
    meta.main()
