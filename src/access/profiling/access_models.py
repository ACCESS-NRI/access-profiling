# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path

from access.config import YAMLParser
from access.config.esm1p6_parallel_config import (
    ESM1P6_CICE5_NAME,
    ESM1P6_CICE5_NBLOCKS,
    ESM1P6_MOM5_NAME,
    ESM1P6_UM7_NAME,
    esm1p6_layout_branch_name,
    esm1p6_layout_config_changes,
)
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


# The names and the CICE5 block count are those the layout conventions in access-config-utils expect, so the
# component tree below must name its subcomponents after them for its layouts to be understood there.
ESM16_UM7_NAME: str = ESM1P6_UM7_NAME
ESM16_MOM5_NAME: str = ESM1P6_MOM5_NAME
ESM16_CICE5_NAME: str = ESM1P6_CICE5_NAME
ESM16_CICE5_NBLOCKS: int = ESM1P6_CICE5_NBLOCKS
# Cores each component receives in the released ACCESS-ESM1.6 pre-industrial control configuration. These are not
# used to build any layout, and are provided as the reference a caller writing an allocation strategy is usually
# working from.
ESM16_PI_CONTROL_CORES: dict[str, int] = {ESM16_UM7_NAME: 208, ESM16_MOM5_NAME: 196, ESM16_CICE5_NAME: 12}

# Ceilings on what counts as a reasonable ACCESS-ESM1.6 layout at all, rather than the tolerances of any
# particular study. Constraints are cumulative and a caller can only tighten them, so these are set loosely: a
# study that wants near-square subdomains or no waste at all says so in its own allocation strategy.
ESM16_MAX_SUBDOMAIN_ASPECT_RATIO: float = 4.0
ESM16_MAX_WASTED_CORE_FRACTION: float = 0.1

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
                SubdomainAspectRatioConstraint(max_ratio=ESM16_MAX_SUBDOMAIN_ASPECT_RATIO),
            ),
        ),
        ParallelComponent(
            name=ESM16_MOM5_NAME,
            domain=Domain(shape=(360, 300)),  # 1 degree tripolar ocean grid.
            local_constraints=(
                FixedThreadsPerRankConstraint(n_threads=1),
                SubdomainAspectRatioConstraint(max_ratio=ESM16_MAX_SUBDOMAIN_ASPECT_RATIO),
            ),
        ),
        ParallelComponent(
            name=ESM16_CICE5_NAME,
            domain=Domain(shape=(ESM16_CICE5_NBLOCKS,)),  # CICE5 blocks, over a one-dimensional process grid.
            local_constraints=(
                # Executables are only available for an exact number of blocks per rank, so the number of CICE5
                # cores must divide ESM16_CICE5_NBLOCKS. An allocation strategy that pins CICE5 to such a divisor
                # with a FixedAllocation therefore finds layouts far faster than one that gives it a range, which
                # will often contain no valid value at all.
                UniformSubdomainConstraint(),
                FixedThreadsPerRankConstraint(n_threads=1),
            ),
        ),
    ),
    local_constraints=(MaxWastedCoreFractionConstraint(max_fraction=ESM16_MAX_WASTED_CORE_FRACTION),),
)


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
        return esm1p6_layout_branch_name(layout, self._branch_name_prefix)

    def layout_config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run ACCESS-ESM1.6 with a given layout.

        Args:
            layout (ComponentLayout): Layout of the ACCESS-ESM1.6 components.
        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control directory.
        """
        return esm1p6_layout_config_changes(layout)


class RAM3Profiling(CylcRoseManager):
    """Handles profiling of ACCESS-rAM3 configurations."""

    @property
    def known_parsers(self):
        return {
            "UM_regions": UMProfilingParser(),
            "UM_total": UMTotalRuntimeParser(),
        }
