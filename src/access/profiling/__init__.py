"""
access-profiling package.
"""

from contextlib import suppress
from importlib.metadata import PackageNotFoundError, version

__version__ = "unknown"
with suppress(PackageNotFoundError):
    __version__ = version("access-profiling")

from access.profiling.access_models import ESM16Profiling
from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.esmf_parser import ESMFSummaryProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.parser import ProfilingParser
from access.profiling.payujson_parser import PayuJSONProfilingParser
from access.profiling.um_parser import UMProfilingParser

__all__ = [
    "ProfilingParser",
    "FMSProfilingParser",
    "UMProfilingParser",
    "CICE5ProfilingParser",
    "PayuJSONProfilingParser",
    "ESMFSummaryProfilingParser",
    "ESM16Profiling",
    "CylcProfilingParser",
    "CylcDBReader",
]
