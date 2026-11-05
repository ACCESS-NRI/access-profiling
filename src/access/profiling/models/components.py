# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The components of the ACCESS codes that Payu runs: what each decomposes, writes and logs.

A component is one code as the ACCESS models configure it: the UM decomposing its grid over the process grid
`um_env.yaml` states, MOM5 over the layout of `input.nml`, CICE as its partitioning says. ACCESS-ESM1.5,
ACCESS-ESM1.6 and ACCESS-OM2 share them, each pairing them with the submodels its control declares, and
ACCESS-OM3 runs CICE6 through the same CICE component. What a component writes is what its own code reads,
whichever model it is coupled into, which is why each is described once here rather than once per model.
"""

from dataclasses import dataclass

from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import (
    LocalConstraint,
    ProcessGridDimEvenConstraint,
    SubdomainAspectRatioConstraint,
)
from access.config.parallel_domain import Domain

from access.profiling.application import LogSpec, log_at
from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.models.cice import CICEPartitioning
from access.profiling.payu_configuration import PayuComponent, payu_model_stdout, require_process_grid
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser, um_stdout

MAX_SUBDOMAIN_ASPECT_RATIO: float = 4.0
"""Ceiling on the aspect ratio of a UM or MOM5 rank's subdomain, its longest extent over its shortest.

Like `MAX_WASTED_CORE_FRACTION`, this bounds what counts as a reasonable layout at all, rather than the tolerances
of any particular study. Constraints are cumulative and a caller can only tighten them, so it is set loosely: a
study that wants near-square subdomains says so in its own allocation strategy.
"""

# Where Payu's drivers of the UM and of MOM5 read their files, relative to the control directory: the files are
# the codes' own, and the directories are the names Payu gives their submodels in every ACCESS model.
_UM_ENV_PATH: str = "atmosphere/um_env.yaml"
_MOM5_NAMELIST_PATH: str = "ocean/input.nml"


def _require_two_dimensional(name: str, grid: Domain) -> None:
    """Refuses a grid that is not two-dimensional, for a component whose decomposition is written as a process grid.

    Checked when the component is built rather than where the grid is decomposed, so that a configuration that
    cannot be written out is refused when it is built instead of once a layout has been found for it.

    Args:
        name (str): The component, for the error message.
        grid (Domain): Its grid.

    Raises:
        ValueError: If the grid is not two-dimensional.
    """
    if grid.ndim != 2:
        raise ValueError(
            f"{name} is given a {grid.ndim}-dimensional grid {grid.shape}. Its decomposition is written as a "
            "two-dimensional process grid, so the grid it decomposes has to be one too."
        )


@dataclass(frozen=True)
class UMComponent(PayuComponent):
    """The UM, decomposing its grid over the two-dimensional process grid its environment file states.

    The UM requires an even number of processes along x. Its timers are read twice from its standard output, once
    for its regions and once for its total run time, so two logs name it.

    Args:
        grid (Domain): The UM's grid. Must be two-dimensional, as the process grid written for it is.

    Raises:
        ValueError: If the grid is not two-dimensional.
    """

    grid: Domain
    name = "UM7"
    token = "atm"

    def __post_init__(self) -> None:
        _require_two_dimensional(self.name, self.grid)

    @property
    def domain(self) -> Domain:
        return self.grid

    @property
    def local_constraints(self) -> tuple[LocalConstraint, ...]:
        return (
            ProcessGridDimEvenConstraint(dim=0),
            SubdomainAspectRatioConstraint(max_ratio=MAX_SUBDOMAIN_ASPECT_RATIO),
        )

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        return (
            LogSpec("UM", um_stdout(), UMProfilingParser(), component=self.name),
            LogSpec("UM_Total_Walltime", um_stdout(), UMTotalRuntimeParser(), component=self.name),
        )

    def config_changes(self, layout: ComponentLayout) -> dict:
        nx, ny = require_process_grid(self.name, layout)
        return {_UM_ENV_PATH: {"UM_ATM_NPROCX": str(nx), "UM_ATM_NPROCY": str(ny), "UM_NPES": str(layout.n_ranks)}}


@dataclass(frozen=True)
class MOM5Component(PayuComponent):
    """MOM5, decomposing its grid over the two-dimensional process grid its namelist states as `layout`.

    Its FMS clocks are read from the standard output Payu captured from the model.

    Args:
        grid (Domain): MOM5's grid. Must be two-dimensional, as `layout` is.

    Raises:
        ValueError: If the grid is not two-dimensional.
    """

    grid: Domain
    name = "MOM5"
    token = "mom"

    def __post_init__(self) -> None:
        _require_two_dimensional(self.name, self.grid)

    @property
    def domain(self) -> Domain:
        return self.grid

    @property
    def local_constraints(self) -> tuple[LocalConstraint, ...]:
        return (SubdomainAspectRatioConstraint(max_ratio=MAX_SUBDOMAIN_ASPECT_RATIO),)

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        return (LogSpec("MOM5", payu_model_stdout(), FMSProfilingParser(has_hits=False), component=self.name),)

    def config_changes(self, layout: ComponentLayout) -> dict:
        nx, ny = require_process_grid(self.name, layout)
        # A string holding the whole right-hand side, not a list of the two extents: the experiment generator
        # renders a namelist value by str(), so a Python list writes its own brackets into the file and MOM5 is
        # handed `layout = [16, 15]`, which is not Fortran.
        return {_MOM5_NAMELIST_PATH: {"ocean_model_nml": {"layout": f"{nx}, {ny}"}}}


@dataclass(frozen=True)
class CICEComponent(PayuComponent):
    """CICE, dividing its grid as its partitioning says: CICE5 in ACCESS-ESM1.6 and ACCESS-OM2, CICE6 in ACCESS-OM3.

    What the search may choose about it, and what is written into its namelist to realise a layout, both follow
    from the partitioning, so neither is stated here. CICE6 prints its timers as CICE5 does, so one parser reads
    both.

    Args:
        partitioning (CICEPartitioning): How it divides its grid among its ranks.
        name (str): How the component tree names it. ACCESS-OM3 names its realm `ice`.
        token (str): How `experiment_name` abbreviates it.
        log_name (str): What its profiling log is called in the parsed data.
        log_path (str): Where that log is, relative to the output directory: `ice/ice_diag.d` where Payu runs
            CICE as a submodel, `log/ice.log` in ACCESS-OM3.
        optional (bool): Whether a run may legitimately not write the log.
    """

    partitioning: CICEPartitioning
    name: str = "CICE5"
    token: str = "ice"
    log_name: str = "CICE5"
    log_path: str = "ice/ice_diag.d"
    optional: bool = False

    @property
    def domain(self) -> Domain | None:
        return self.partitioning.domain

    @property
    def local_constraints(self) -> tuple[LocalConstraint, ...]:
        return self.partitioning.local_constraints

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        return (
            LogSpec(
                self.log_name,
                log_at(self.log_path),
                CICE5ProfilingParser(),
                component=self.name,
                optional=self.optional,
            ),
        )

    def config_changes(self, layout: ComponentLayout) -> dict:
        changes = self.partitioning.namelist_changes(layout.n_ranks, layout.decomposition)
        if not changes:
            return {}
        return {self.partitioning.namelist_path: {self.partitioning.namelist_group: changes}}
