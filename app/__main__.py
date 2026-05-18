"""Pipeline entry point — runs the configured step(s) sequentially."""

from argparse import ArgumentParser
from logging import getLogger
from os import getenv

from app import _01_download, _02_prepare, _03_build, _04_export

logger = getLogger(__name__)

_STEPS = (
    ("_01_download", _01_download.main),
    ("_02_prepare", _02_prepare.main),
    ("_03_build", _03_build.main),
    ("_04_export", _04_export.main),
)


def main() -> None:
    """Run the requested stage(s) of the admin-boundaries pipeline."""
    step = _parse()
    logger.info("--step=%s", step or "all")
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
    return parser.parse_args().step


if __name__ == "__main__":
    main()
