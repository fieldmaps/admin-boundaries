from multiprocessing import Pool
from pathlib import Path

from . import cleanup, inputs, outputs, recode
from .utils import adm0_list, apply_funcs, logging

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent
data = cwd / "../../../data/geoboundaries/standardized"
funcs = [inputs.main, recode.main, outputs.main, cleanup.main]

if __name__ == "__main__":
    logger.info("starting")
    data.mkdir(exist_ok=True, parents=True)
    results = []
    pool = Pool()
    for row in adm0_list:
        args = [row["id"], row["src_lvl"], row, *funcs]
        result = pool.apply_async(apply_funcs, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
