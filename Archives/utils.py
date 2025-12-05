"""Utility helpers used across the wrapper package."""
from typing import Any
import logging

logger = logging.getLogger(__name__)


def ensure_list(x: Any):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]
