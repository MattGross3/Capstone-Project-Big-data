"""Central logging configuration shared by CLI, pipelines, and tests."""

from __future__ import annotations

import sys
from typing import Any

from loguru import logger


def configure_logging(level: str = "INFO") -> None:
    """Route loguru output to stderr with a concise structured format."""

    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        enqueue=True,
    )


def get_logger(**extra: Any):
    """Return a child logger pre-populated with extra context."""

    return logger.bind(**extra)
