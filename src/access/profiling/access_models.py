# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Deprecated. The ACCESS models now live in `access.profiling.models`.

A model used to be a manager subclass, one per model, each supplying the layout members the base declared
abstract. There is no longer anything model-shaped on a manager to supply: the model and its configuration are
data, and the manager is the workflow engine that runs them. So `ESM16Profiling` and the rest are no longer
classes, and what used to be a subclass of one is now a value passed to `PayuManager` or `CylcRoseManager`:

    from access.profiling.models.esm16 import ESM16_PI_CONTROL, ESM16_PI_CONTROL_SOURCE
    from access.profiling.payu_manager import PayuManager

    manager = PayuManager(work_dir, archive_dir, ESM16_PI_CONTROL, ESM16_PI_CONTROL_SOURCE)

The factories below build exactly that, and are kept for one release so that existing notebooks go on working.
Everything else here is re-exported from `access.profiling.models` unchanged.
"""

import warnings
from pathlib import Path

from access.profiling.control import ControlSource
from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.models.cice import CICEPartitioning, CICEPartitioningMode
from access.profiling.models.esm16 import (
    ESM16_1DEG_OCEAN,
    ESM16_AMIP_SUBMODELS,
    ESM16_CICE5,
    ESM16_CICE5_NAME,
    ESM16_CICE5_NX_GLOBAL,
    ESM16_CICE5_NY_GLOBAL,
    ESM16_CONFIGS_REPOSITORY,
    ESM16_COUPLED_SUBMODELS,
    ESM16_MAX_SUBDOMAIN_ASPECT_RATIO,
    ESM16_MAX_WASTED_CORE_FRACTION,
    ESM16_MOM5_NAME,
    ESM16_N96_ATMOSPHERE,
    ESM16_PI_CONTROL,
    ESM16_PI_CONTROL_CORES,
    ESM16_PI_CONTROL_SOURCE,
    ESM16_UM7_NAME,
    ESM16Configuration,
)
from access.profiling.models.om3 import (
    OM3_25KM_CICE6,
    OM3_25KM_GRID,
    OM3_ATMOSPHERE_NAME,
    OM3_CICE6_GHOST_WIDTH,
    OM3_CONFIGS_REPOSITORY,
    OM3_MAX_WASTED_CORE_FRACTION,
    OM3_MC_25KM,
    OM3_MC_25KM_RELEASE_CORES,
    OM3_MC_25KM_SOURCE,
    OM3_MEDIATOR_NAME,
    OM3_MOM6_HALO_WIDTH,
    OM3_OCEAN_NAME,
    OM3_RUNOFF_NAME,
    OM3_SEA_ICE_NAME,
    OM3_SHARED_NAME,
    OM3_WAVE_NAME,
    OM3Configuration,
)
from access.profiling.models.um_suites import AM3_N96E, AM3_N96E_SOURCE, RAM3, UM_SUITE_PARSERS
from access.profiling.payu_manager import PayuManager

__all__ = [
    "AM3_N96E",
    "AM3_N96E_SOURCE",
    "AM3Profiling",
    "CICEPartitioning",
    "CICEPartitioningMode",
    "ESM16_1DEG_OCEAN",
    "ESM16_AMIP_SUBMODELS",
    "ESM16_CICE5",
    "ESM16_CICE5_NAME",
    "ESM16_CICE5_NX_GLOBAL",
    "ESM16_CICE5_NY_GLOBAL",
    "ESM16_CONFIGS_REPOSITORY",
    "ESM16_COUPLED_SUBMODELS",
    "ESM16_MAX_SUBDOMAIN_ASPECT_RATIO",
    "ESM16_MAX_WASTED_CORE_FRACTION",
    "ESM16_MOM5_NAME",
    "ESM16_N96_ATMOSPHERE",
    "ESM16_PI_CONTROL",
    "ESM16_PI_CONTROL_CORES",
    "ESM16_PI_CONTROL_SOURCE",
    "ESM16_UM7_NAME",
    "ESM16Configuration",
    "ESM16Profiling",
    "OM3_25KM_CICE6",
    "OM3_25KM_GRID",
    "OM3_ATMOSPHERE_NAME",
    "OM3_CICE6_GHOST_WIDTH",
    "OM3_CONFIGS_REPOSITORY",
    "OM3_MAX_WASTED_CORE_FRACTION",
    "OM3_MC_25KM",
    "OM3_MC_25KM_RELEASE_CORES",
    "OM3_MC_25KM_SOURCE",
    "OM3_MEDIATOR_NAME",
    "OM3_MOM6_HALO_WIDTH",
    "OM3_OCEAN_NAME",
    "OM3_RUNOFF_NAME",
    "OM3_SEA_ICE_NAME",
    "OM3_SHARED_NAME",
    "OM3_WAVE_NAME",
    "OM3Configuration",
    "OM3Profiling",
    "RAM3",
    "RAM3Profiling",
    "UM_SUITE_PARSERS",
]


def _deprecated(old: str, new: str) -> None:
    """Warns that a model class has become a configuration passed to a manager.

    Args:
        old (str): The name that is going away.
        new (str): What to write instead.
    """
    warnings.warn(
        f"{old} is deprecated and will be removed in a future release. Use {new} instead.",
        DeprecationWarning,
        stacklevel=3,
    )


def ESM16Profiling(  # noqa: N802 - was a class, and is called like one.
    work_dir: Path,
    archive_dir: Path,
    configuration: ESM16Configuration = ESM16_PI_CONTROL,
    control: ControlSource | None = None,
) -> PayuManager:
    """Deprecated. Returns a PayuManager profiling an ACCESS-ESM1.6 configuration.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        configuration (ESM16Configuration): The configuration being profiled. Defaults to the released
            pre-industrial control, which is what this class used to assume.
        control (ControlSource | None): Where the control configuration comes from.

    Returns:
        PayuManager: The manager.
    """
    _deprecated("ESM16Profiling", "PayuManager(work_dir, archive_dir, ESM16_PI_CONTROL, control)")
    return PayuManager(work_dir, archive_dir, configuration, control)


def OM3Profiling(  # noqa: N802 - was a class, and is called like one.
    work_dir: Path,
    archive_dir: Path,
    configuration: OM3Configuration = OM3_MC_25KM,
    control: ControlSource | None = None,
) -> PayuManager:
    """Deprecated. Returns a PayuManager profiling an ACCESS-OM3 configuration.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        configuration (OM3Configuration): The configuration being profiled. Defaults to the released 25 km
            configuration.
        control (ControlSource | None): Where the control configuration comes from.

    Returns:
        PayuManager: The manager.
    """
    _deprecated("OM3Profiling", "PayuManager(work_dir, archive_dir, OM3_MC_25KM, control)")
    return PayuManager(work_dir, archive_dir, configuration, control)


def AM3Profiling(  # noqa: N802 - was a class, and is called like one.
    work_dir: Path,
    archive_dir: Path,
    *args,
    control: ControlSource | None = None,
    **kwargs,
) -> CylcRoseManager:
    """Deprecated. Returns a CylcRoseManager profiling the ACCESS-AM3 suite.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        *args: The variable names this used to take, which AM3_N96E now holds. Ignored.
        control (ControlSource | None): Where the control suite comes from.
        **kwargs: As for *args. Ignored.

    Returns:
        CylcRoseManager: The manager.
    """
    _deprecated("AM3Profiling", "CylcRoseManager(work_dir, archive_dir, AM3_N96E, control)")
    return CylcRoseManager(work_dir, archive_dir, AM3_N96E, control)


def RAM3Profiling(  # noqa: N802 - was a class, and is called like one.
    work_dir: Path,
    archive_dir: Path,
    *args,
    control: ControlSource | None = None,
    **kwargs,
) -> CylcRoseManager:
    """Deprecated. Returns a CylcRoseManager profiling the ACCESS-rAM3 suite.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        *args: The variable names this used to take, which RAM3 now holds. Ignored.
        control (ControlSource | None): Where the control suite comes from.
        **kwargs: As for *args. Ignored.

    Returns:
        CylcRoseManager: The manager.
    """
    _deprecated("RAM3Profiling", "CylcRoseManager(work_dir, archive_dir, RAM3, control)")
    return CylcRoseManager(work_dir, archive_dir, RAM3, control)
