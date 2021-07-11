import requests
from pathlib import Path
from .utils import logging, GIT_URL, GIT_LFS_URL

logger = logging.getLogger(__name__)

cwd = Path(__file__).parent
outputs = (cwd / '../../../data/geoboundaries/originals/boundaries')
outputs.mkdir(exist_ok=True, parents=True)


def main(iso3, level):
    for ext in ['prj', 'shx', 'dbf', 'shp']:
        filename = f"{iso3}_adm{level}.{ext}".lower()
        r = requests.get(
            f'{GIT_URL}/{iso3}/ADM{level}/geoBoundaries-{iso3}-ADM{level}.{ext}')
        if r.headers['content-type'] == 'text/plain; charset=utf-8':
            if r.text == '404: Not Found':
                continue
            elif ext == 'prj':
                open(outputs / filename, 'wb').write(r.content)
            else:
                r = requests.get(
                    f'{GIT_LFS_URL}/{iso3}/ADM{level}/geoBoundaries-{iso3}-ADM{level}.{ext}')
                open(outputs / filename, 'wb').write(r.content)
        else:
            open(outputs / filename, 'wb').write(r.content)
    logger.info(f'{iso3}_adm{level}'.lower())
