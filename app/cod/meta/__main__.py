import logging

from . import csv

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("starting")
    csv.main()
    logger.info("finished")
