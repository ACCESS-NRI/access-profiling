"""
access-profiling package.
"""

from contextlib import suppress
from importlib.metadata import PackageNotFoundError, version

__version__ = "unknown"
with suppress(PackageNotFoundError):
    __version__ = version("access-profiling")

from access.profiling.application import Application
from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.control import ControlSource, ExistingDirectoryControlSource, GitControlSource
from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.esmf_parser import ESMFSummaryProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.models.cice import CICEPartitioning, CICEPartitioningMode
from access.profiling.models.esm16 import ESM16_PI_CONTROL, ESM16_PI_CONTROL_SOURCE, ESM16Configuration
from access.profiling.models.om3 import OM3_MC_25KM, OM3_MC_25KM_SOURCE, OM3Configuration
from access.profiling.models.um_suites import AM3_N96E, AM3_N96E_SOURCE, RAM3
from access.profiling.parser import ProfilingParser
from access.profiling.payu_configuration import PayuConfiguration
from access.profiling.payu_manager import PayuManager
from access.profiling.payujson_parser import PayuJSONProfilingParser
from access.profiling.rose_configuration import RoseSuiteConfiguration
from access.profiling.um_parser import UMProfilingParser

__all__ = [
    "AM3_N96E",
    "AM3_N96E_SOURCE",
    "CICEPartitioning",
    "CICEPartitioningMode",
    "ControlSource",
    "CylcRoseManager",
    "ESM16_PI_CONTROL",
    "ESM16_PI_CONTROL_SOURCE",
    "ESM16Configuration",
    "ExistingDirectoryControlSource",
    "GitControlSource",
    "Application",
    "OM3_MC_25KM",
    "OM3_MC_25KM_SOURCE",
    "PayuConfiguration",
    "PayuManager",
    "RAM3",
    "RoseSuiteConfiguration",
    "ProfilingParser",
    "FMSProfilingParser",
    "UMProfilingParser",
    "CICE5ProfilingParser",
    "PayuJSONProfilingParser",
    "ESMFSummaryProfilingParser",
    "OM3Configuration",
    "CylcProfilingParser",
    "CylcDBReader",
]
