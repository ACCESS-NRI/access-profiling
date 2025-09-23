"""
access-profiling package.
"""

__version__ = "0.1.0"
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("access-profiling")
except PackageNotFoundError:
    # package is not installed
    pass

from .parser import ProfilingParser
from .fms_parser import FMSProfilingParser

__all__ = [
    "ProfilingParser",
    "FMSProfilingParser",
]
