from multiprocessing import Pool

from . import (
    area,
    attributes,
    cleanup,
    formats,
    inputs,
    lines,
    merge,
    outputs,
    points,
    polygons,
    template,
)
from .utils import (
    apply_funcs,
    dest_list,
    dests,
    geoms,
    geoms_clip,
    input_list,
    logging,
    srcs,
    world_views,
)

logger = logging.getLogger(__name__)
funcs = [attributes.main, polygons.main, lines.main, points.main]
funcs_cleanup = [cleanup.main]


def src_admx(func):
    results = []
    pool = Pool()
    for src in srcs:
        for row in input_list[src]:
            result = pool.apply_async(func, args=[src, row])
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def src_adm0(func):
    results = []
    pool = Pool()
    for dest in dests:
        for wld in world_views:
            for geom in geoms_clip:
                result = pool.apply_async(func, args=[dest, wld, geom])
                results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def process_boundaries(funcs):
    results = []
    pool = Pool()
    for dest in dests:
        for wld in world_views:
            for row in dest_list[f"{dest}_{wld}"]:
                args = [dest, wld, row, *funcs]
                result = pool.apply_async(apply_funcs, args=args)
                results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def dest_adm0(func):
    results = []
    pool = Pool()
    for dest in dests:
        for wld in world_views:
            result = pool.apply_async(func, args=[dest, wld])
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def dest_admx(func, end):
    results = []
    pool = Pool()
    for dest in dests:
        for wld in world_views:
            for lvl in range(4, end - 1, -1):
                for geom in geoms:
                    args = [dest, wld, lvl, geom]
                    result = pool.apply_async(func, args=args)
                    results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def dest_admx_sync(func, end):
    for dest in dests:
        for wld in world_views:
            for lvl in range(4, end - 1, -1):
                for geom in geoms:
                    func(dest, wld, lvl, geom)


if __name__ == "__main__":
    logger.info("starting")
    src_adm0(inputs.adm0)
    src_admx(inputs.admx)
    process_boundaries(funcs)
    dest_adm0(template.main)
    dest_admx(merge.main, 1)
    src_admx(cleanup.admx)
    process_boundaries(funcs_cleanup)
    dest_admx(outputs.main, 0)
    dest_adm0(area.main)
    src_adm0(cleanup.adm0)
    dest_admx(cleanup.dest_admx, 1)
    dest_admx_sync(formats.main, 1)
    dest_admx(formats.cleanup, 0)
