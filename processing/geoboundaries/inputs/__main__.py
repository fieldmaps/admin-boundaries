from multiprocessing import Pool
from . import api
from .utils import logging, adm0_list

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info('starting')
    results = []
    pool = Pool()
    for row in adm0_list:
        result = pool.apply_async(api.main, args=[row])
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
