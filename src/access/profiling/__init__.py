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

from access.profiling.parser import ProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.um_parser import UMProfilingParser
from access.profiling.payujson_parser import PayuJSONProfilingParser
from access.profiling.cice5_parser import CICE5ProfilingParser

__all__ = [
    "ProfilingParser",
    "FMSProfilingParser",
    "UMProfilingParser",
    "CICE5ProfilingParser",
    "PayuJSONProfilingParser",
]
