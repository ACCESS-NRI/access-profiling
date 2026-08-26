# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import math
from pathlib import Path

from access.config import YAMLParser
from access.config.parallel_allocation_strategies import FixedAllocation, FreeAllocation, RootAllocation
from access.config.parallel_component import ComponentLayout, ParallelComponent
from access.config.parallel_constraints import (
    FixedThreadsPerRankConstraint,
    MaxWastedCoreFractionConstraint,
    ProcessGridDimEvenConstraint,
    SubdomainAspectRatioConstraint,
    UniformSubdomainConstraint,
)
from access.config.parallel_domain import Domain

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.experiment import ProfilingLog
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.payu_manager import PayuManager
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

logger = logging.getLogger(__name__)


ESM16_UM7_NAME: str = "UM7"
ESM16_MOM5_NAME: str = "MOM5"
ESM16_CICE5_NAME: str = "CICE5"
ESM16_CICE5_NBLOCKS: int = 360  # Number of blocks the CICE5 grid is split into in ACCESS-ESM1.6.
# Cores each component receives in the released ACCESS-ESM1.6 pre-industrial control configuration.
ESM16_PI_CONTROL_CORES: dict[str, int] = {ESM16_UM7_NAME: 208, ESM16_MOM5_NAME: 196, ESM16_CICE5_NAME: 12}

# Component tree of ACCESS-ESM1.6. It carries only the requirements that hold for every ACCESS-ESM1.6 layout,
# whatever is being studied. Constraints are cumulative and cannot be relaxed by a caller, so anything that is a
# choice rather than a requirement belongs in the allocation strategy instead.
ESM16_COMPONENT: ParallelComponent = ParallelComponent(
    name="ACCESS-ESM1.6",
    subcomponents=(
        ParallelComponent(
            name=ESM16_UM7_NAME,
            domain=Domain(shape=(192, 144)),  # N96 atmosphere grid.
            local_constraints=(
                ProcessGridDimEvenConstraint(dim=0),  # The UM requires an even number of processes along x.
                FixedThreadsPerRankConstraint(n_threads=1),  # ACCESS-ESM1.6 is built without OpenMP support.
            ),
        ),
        ParallelComponent(
            name=ESM16_MOM5_NAME,
            domain=Domain(shape=(360, 300)),  # 1 degree tripolar ocean grid.
            local_constraints=(FixedThreadsPerRankConstraint(n_threads=1),),
        ),
        ParallelComponent(
            name=ESM16_CICE5_NAME,
            domain=Domain(shape=(ESM16_CICE5_NBLOCKS,)),  # CICE5 blocks, over a one-dimensional process grid.
            local_constraints=(
                # Executables are only available for an exact number of blocks per rank.
                UniformSubdomainConstraint(),
                FixedThreadsPerRankConstraint(n_threads=1),
            ),
        ),
    ),
)


def esm16_scaling_allocations(
    total_cores: int,
    core_fraction_tolerance: float = 0.05,
    max_wasted_core_fraction: float = 0.02,
    max_subdomain_aspect_ratio: float = 1.5,
) -> RootAllocation:
    """Returns an allocation strategy that follows the proportions of the ACCESS-ESM1.6 PI control configuration.

    UM7 and MOM5 are given a range of core counts around the number of cores they would receive if the proportions
    of the PI control configuration were kept at the requested total number of cores. CICE5 is instead pinned to the
    number of cores closest to its own proportional share that divides the number of CICE5 blocks exactly, as
    required by the available executables. A range would often contain no such value at all, in which case no layout
    would be found.

    This is only one possible strategy, provided for convenience. Any other allocation strategy naming the
    subcomponents of ESM16_COMPONENT can be used instead.

    Args:
        total_cores (int): Total number of cores to distribute among the components.
        core_fraction_tolerance (float): Relative tolerance around the PI control proportions. Defaults to 0.05.
        max_wasted_core_fraction (float): Largest fraction of the total number of cores that may be left unused.
            Defaults to 0.02.
        max_subdomain_aspect_ratio (float): Largest aspect ratio allowed for the subdomains of the components with a
            two-dimensional domain. Defaults to 1.5.
    Returns:
        RootAllocation: Allocation strategy to pass to the layout search.
    """
    pi_control_cores = sum(ESM16_PI_CONTROL_CORES.values())

    subcomponents: dict = {}
    for name in (ESM16_UM7_NAME, ESM16_MOM5_NAME):
        target = ESM16_PI_CONTROL_CORES[name] * total_cores / pi_control_cores
        min_cores = max(1, math.floor(target * (1.0 - core_fraction_tolerance)))
        max_cores = max(min_cores, math.ceil(target * (1.0 + core_fraction_tolerance)))
        subcomponents[name] = FreeAllocation(
            min_cores=min_cores,
            max_cores=max_cores,
            local_constraints=(SubdomainAspectRatioConstraint(max_ratio=max_subdomain_aspect_ratio),),
        )

    ice_target = ESM16_PI_CONTROL_CORES[ESM16_CICE5_NAME] * total_cores / pi_control_cores
    divisors = [n for n in range(1, ESM16_CICE5_NBLOCKS + 1) if ESM16_CICE5_NBLOCKS % n == 0]
    subcomponents[ESM16_CICE5_NAME] = FixedAllocation(min(divisors, key=lambda n: abs(n - ice_target)))

    return RootAllocation(
        subcomponents=subcomponents,
        local_constraints=(MaxWastedCoreFractionConstraint(max_fraction=max_wasted_core_fraction),),
    )


def _esm16_sub_layouts(layout: ComponentLayout) -> tuple[ComponentLayout, ComponentLayout, ComponentLayout]:
    """Returns the UM7, MOM5 and CICE5 sub-layouts of an ACCESS-ESM1.6 layout.

    Args:
        layout (ComponentLayout): Layout of the ACCESS-ESM1.6 components.
    Returns:
        tuple[ComponentLayout, ComponentLayout, ComponentLayout]: The UM7, MOM5 and CICE5 sub-layouts.
    Raises:
        ValueError: If a component is missing from the layout, or if one of them has no domain decomposition.
    """
    sub_layouts = {sub.name: sub for sub in layout.sub_layouts}
    try:
        components = tuple(sub_layouts[name] for name in (ESM16_UM7_NAME, ESM16_MOM5_NAME, ESM16_CICE5_NAME))
    except KeyError as error:
        raise ValueError(f"Layout {layout.name!r} is not an ACCESS-ESM1.6 layout: {error} is missing.") from error
    for component in components:
        if component.decomposition is None:
            raise ValueError(f"Component {component.name!r} has no domain decomposition.")
    return components


class ESM16Profiling(PayuManager):
    """Handles profiling of ACCESS-ESM1.6 configurations."""

    _branch_name_prefix: str = "esm1p6-layout"  # Prefix of the branch names of the generated layout experiments.

    @property
    def model_type(self) -> str:
        return "access-esm1.6"

    def get_component_logs(self, path: Path) -> dict[str, ProfilingLog]:
        """Returns available profiling logs for the components in ACCESS-ESM1.6.

        Args:
            path (Path): Path to the output directory.
        Returns:
            dict[str, ProfilingLog]: Dictionary mapping component names to their ProfilingLog instances.
        """
        logs = {}
        parser = YAMLParser()

        um_env_path = path / "atmosphere" / "um_env.yaml"
        um_env = parser.parse(um_env_path.read_text())
        um_logfile = path / "atmosphere" / f"{um_env['UM_STDOUT_FILE']}0"
        if um_logfile.is_file():
            logger.debug(f"Found UM log file: {um_logfile}")
            logs["UM"] = ProfilingLog(um_logfile, UMProfilingParser())
            logs["UM_Total_Walltime"] = ProfilingLog(um_logfile, UMTotalRuntimeParser())

        config_path = path / "config.yaml"
        payu_config = parser.parse(config_path.read_text())
        mom5_logfile = path / f"{payu_config['model']}.out"
        if mom5_logfile.is_file():
            logger.debug(f"Found MOM5 log file: {mom5_logfile}")
            logs["MOM5"] = ProfilingLog(mom5_logfile, FMSProfilingParser(has_hits=False))

        cice5_logfile = path / "ice" / "ice_diag.d"
        if cice5_logfile.is_file():
            logger.debug(f"Found CICE5 log file: {cice5_logfile}")
            logs["CICE5"] = ProfilingLog(cice5_logfile, CICE5ProfilingParser())

        return logs

    @property
    def parallel_component(self) -> ParallelComponent:
        return ESM16_COMPONENT

    def layout_branch_name(self, layout: ComponentLayout) -> str:
        """Returns the name of the branch holding the experiment for a given ACCESS-ESM1.6 layout.

        Args:
            layout (ComponentLayout): Layout of the ACCESS-ESM1.6 components.
        Returns:
            str: Branch name.
        """
        um7, mom5, cice5 = _esm16_sub_layouts(layout)
        atm_nx, atm_ny = um7.decomposition.grid.shape
        mom_nx, mom_ny = mom5.decomposition.grid.shape
        return f"{self._branch_name_prefix}_atm_{atm_nx}x{atm_ny}_mom_{mom_nx}x{mom_ny}_ice_{cice5.n_ranks}x1"

    def layout_config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run ACCESS-ESM1.6 with a given layout.

        Args:
            layout (ComponentLayout): Layout of the ACCESS-ESM1.6 components.
        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control directory.
        """
        um7, mom5, cice5 = _esm16_sub_layouts(layout)
        atm_nx, atm_ny = um7.decomposition.grid.shape
        mom_nx, mom_ny = mom5.decomposition.grid.shape
        ice_nblocks_per_rank = ESM16_CICE5_NBLOCKS // cice5.n_ranks
        return {
            "config.yaml": {
                "submodels": [
                    [
                        {"ncpus": um7.n_cores},
                        {"ncpus": mom5.n_cores},
                        {
                            "ncpus": cice5.n_cores,
                            "exe": [f"cice_access_360x300_{ice_nblocks_per_rank}x300.exe"],
                        },
                    ]
                ]
            },
            "atmosphere/um_env.yaml": {
                "UM_ATM_NPROCX": str(atm_nx),
                "UM_ATM_NPROCY": str(atm_ny),
                "UM_NPES": str(um7.n_ranks),
            },
            "ocean/input.nml": {"ocean_model_nml": {"layout": [f"{mom_nx},{mom_ny}"]}},
            "ice/cice_in.nml": {"domain_nml": {"nprocs": [f"{cice5.n_ranks}"]}},
        }


class RAM3Profiling(CylcRoseManager):
    """Handles profiling of ACCESS-rAM3 configurations."""

    @property
    def known_parsers(self):
        return {
            "UM_regions": UMProfilingParser(),
            "UM_total": UMTotalRuntimeParser(),
        }
