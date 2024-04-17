import logging

from . import api, csv
from .utils import adm0_list

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("starting")
    for row in adm0_list:
        api.main(row)
    csv.main()
    logger.info("finished")
