from . import dest, pcodes, src
from .utils import logging

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("starting")
    src.main("cod")
    src.main("geoboundaries")
    dest.main("edge-matched")
    pcodes.main()
    logger.info("finished")
