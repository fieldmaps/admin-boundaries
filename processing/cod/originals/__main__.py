from multiprocessing import Pool
from . import attributes, inputs, dissolve, outputs, cleanup
from .utils import adm0_list, apply_funcs

funcs = [cleanup.main, attributes.main, inputs.main, dissolve.main,
         outputs.main, cleanup.main]

if __name__ == '__main__':
    results = []
    pool = Pool()
    for row in adm0_list:
        args = [row['id'], row['src_lvl'], row, *funcs]
        result = pool.apply_async(apply_funcs, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
