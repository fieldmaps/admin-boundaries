"""Pipeline entry point — runs the configured step(s) sequentially."""

from argparse import ArgumentParser
from logging import getLogger
from os import getenv

import app.config as _config
from app import _01_download, _02_prepare, _03_build, _04_export

logger = getLogger(__name__)

_STEPS = (
    ("_01_download", _01_download.main),
    ("_02_prepare", _02_prepare.main),
    ("_03_build", _03_build.main),
    ("_04_export", _04_export.main),
)

_BOOL_VALS = ("YES", "ON", "TRUE", "1")


def main() -> None:
    """Run the requested stage(s) of the admin-boundaries pipeline."""
    step = _parse()
    logger.info("--step=%s --debug=%s", step or "all", _config.DEBUG)
    for name, fn in _STEPS:
        if step in (None, name):
            logger.info("starting %s", name)
            fn()
            logger.info("finished %s", name)


def _parse() -> str | None:
    parser = ArgumentParser(description="Admin boundaries pipeline.")
    parser.add_argument(
        "--step",
        default=getenv("STEP"),
        choices=[name for name, _ in _STEPS],
        help="Run only the named stage (default: run all).",
    )
    parser.add_argument(
        "--debug",
        default=getenv("DEBUG", "").upper() in _BOOL_VALS,
        type=lambda v: v is None or v.upper() in _BOOL_VALS,
        nargs="?",
        const=True,
        help="Enable profiling and write parquet snapshots to tmp/.",
    )
    args = parser.parse_args()
    _config.DEBUG = args.debug or bool(args.step)
    return args.step


if __name__ == "__main__":
    main()
