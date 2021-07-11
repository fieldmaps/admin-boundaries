from multiprocessing import Pool
from . import api_nightly
from .utils import logging, meta

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info('starting')
    results = []
    pool = Pool()
    for row in meta:
        for level in range(5):
            args = [row['iso3'], level]
            result = pool.apply_async(api_nightly.main, args=args)
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
