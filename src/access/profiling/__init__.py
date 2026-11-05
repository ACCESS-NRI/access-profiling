"""Tools to profile and optimise parallel applications, with implementations for the ACCESS climate models.

A study is put together from three parts. A manager drives one workflow engine and runs the study, from generating
its experiments to plotting their profiling data: the package provides `PayuManager` for Payu and `CylcRoseManager`
for a Cylc Rose suite, the workflow engines of the ACCESS models. An application says what is being profiled; the
package ships presets for the ACCESS models in `access.profiling.models`, such as `ESM16_PI_CONTROL`,
`OM3_MC_25KM` or `AM3_N96E`. A control source says where the configuration every experiment perturbs comes from;
most presets have one to go with them, such as `ESM16_PI_CONTROL_SOURCE`. The names a study uses most are importable
from the package itself; everything else is imported from the module that defines it.
"""

from contextlib import suppress
from importlib.metadata import PackageNotFoundError, version

__version__ = "unknown"
with suppress(PackageNotFoundError):
    __version__ = version("access-profiling")

from access.profiling.application import Application, LogSpec, log_at
from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.control import ControlSource, ExistingDirectoryControlSource, GitControlSource
from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.esmf_parser import ESMFSummaryProfilingParser
from access.profiling.experiment import ProfilingExperiment, ProfilingExperimentStatus, ProfilingLog
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.manager import ProfilingManager
from access.profiling.metrics import ProfilingMetric
from access.profiling.models.cice import CICEPartitioning, CICEPartitioningMode
from access.profiling.models.components import CICEComponent, MOM5Component, UMComponent
from access.profiling.models.esm15 import ESM15Configuration
from access.profiling.models.esm16 import ESM16_PI_CONTROL, ESM16_PI_CONTROL_SOURCE, ESM16Configuration
from access.profiling.models.om2 import OM2Configuration
from access.profiling.models.om3 import OM3_MC_25KM, OM3_MC_25KM_SOURCE, OM3Configuration
from access.profiling.models.um_suites import AM3_N96E, AM3_N96E_SOURCE, RAM3
from access.profiling.parser import ProfilingParser
from access.profiling.payu_configuration import (
    FixedRanksConstraint,
    PayuComponent,
    PayuConfiguration,
    PayuSubmodel,
    PlainComponent,
    SubmodelsConfiguration,
)
from access.profiling.payu_manager import PayuManager
from access.profiling.payujson_parser import PayuJSONProfilingParser
from access.profiling.plotting_utils import RegionGroup
from access.profiling.rose_configuration import RoseSuiteConfiguration
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

__all__ = [
    "AM3_N96E",
    "AM3_N96E_SOURCE",
    "Application",
    "CICE5ProfilingParser",
    "CICEComponent",
    "CICEPartitioning",
    "CICEPartitioningMode",
    "ControlSource",
    "CylcDBReader",
    "CylcProfilingParser",
    "CylcRoseManager",
    "ESM15Configuration",
    "ESM16_PI_CONTROL",
    "ESM16_PI_CONTROL_SOURCE",
    "ESM16Configuration",
    "ESMFSummaryProfilingParser",
    "ExistingDirectoryControlSource",
    "FixedRanksConstraint",
    "FMSProfilingParser",
    "GitControlSource",
    "log_at",
    "LogSpec",
    "MOM5Component",
    "OM2Configuration",
    "OM3_MC_25KM",
    "OM3_MC_25KM_SOURCE",
    "OM3Configuration",
    "PayuComponent",
    "PayuConfiguration",
    "PayuJSONProfilingParser",
    "PayuManager",
    "PayuSubmodel",
    "PlainComponent",
    "ProfilingExperiment",
    "ProfilingExperimentStatus",
    "ProfilingLog",
    "ProfilingManager",
    "ProfilingMetric",
    "ProfilingParser",
    "RAM3",
    "RegionGroup",
    "RoseSuiteConfiguration",
    "SubmodelsConfiguration",
    "UMComponent",
    "UMProfilingParser",
    "UMTotalRuntimeParser",
]
