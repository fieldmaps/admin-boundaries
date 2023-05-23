from multiprocessing import Pool

from . import outputs
from .utils import input_list, logging, srcs

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("starting")
    results = []
    pool = Pool()
    for src in srcs:
        for row in input_list[src]:
            result = pool.apply_async(outputs.main, args=[src, row["id"]])
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
