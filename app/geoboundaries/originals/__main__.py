from multiprocessing import Pool

from . import cleanup, dissolve, inputs, outputs, overlay
from .utils import adm0_list, apply_funcs, logging

logger = logging.getLogger(__name__)
funcs = [inputs.main, overlay.main, dissolve.main, outputs.main, cleanup.main]

if __name__ == "__main__":
    logger.info("starting")
    results = []
    pool = Pool()
    for row in adm0_list:
        args = [row["id"], row["src_lvl"], *funcs]
        result = pool.apply_async(apply_funcs, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
