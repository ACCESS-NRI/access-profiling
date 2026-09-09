# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from access.config import YAMLParser
from access.config.parallel_component import ComponentLayout, CoreSharing, ParallelComponent
from access.config.parallel_constraints import (
    DomainDivisibleByRanksConstraint,
    FixedThreadsPerRankConstraint,
    MaxWastedCoreFractionConstraint,
    ProcessGridDimEvenConstraint,
    SubdomainAspectRatioConstraint,
)
from access.config.parallel_domain import Domain

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.esmf_parser import ESMFSummaryProfilingParser
from access.profiling.experiment import ProfilingLog
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.payu_manager import PayuManager
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

logger = logging.getLogger(__name__)


ESM16_UM7_NAME: str = "UM7"
ESM16_MOM5_NAME: str = "MOM5"
ESM16_CICE5_NAME: str = "CICE5"
# The CICE5 global grid in ACCESS-ESM1.6. The layout search splits the x extent over a one-dimensional process
# grid, and layout_config_changes gives each rank a single block spanning the full y extent, so the number of
# CICE5 ranks has to divide ESM16_CICE5_NX_GLOBAL exactly. DomainDivisibleByRanksConstraint on the tree enforces it.
ESM16_CICE5_NX_GLOBAL: int = 360
ESM16_CICE5_NY_GLOBAL: int = 300
# Cores each component receives in the released ACCESS-ESM1.6 pre-industrial control configuration. These are not
# used to build any layout, and are provided as the reference a caller writing an allocation strategy is usually
# working from.
ESM16_PI_CONTROL_CORES: dict[str, int] = {ESM16_UM7_NAME: 256, ESM16_MOM5_NAME: 240, ESM16_CICE5_NAME: 12}

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
            domain=Domain(shape=(ESM16_CICE5_NX_GLOBAL,)),  # CICE5 x extent, over a 1-D process grid.
            local_constraints=(
                # Each rank is given a single block spanning the full y extent, so the blocks tile the x extent
                # only if every rank gets the same number of columns: the CICE5 core count has to divide
                # ESM16_CICE5_NX_GLOBAL. The admissible counts thin out as the total grows, so a strategy giving
                # CICE5 a narrow band of cores will often find no layout at all.
                DomainDivisibleByRanksConstraint(),
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

        The name records the process grid of each component, so it is distinct for every distinct layout and the
        same layout always produces the same name. This is what lets the manager tell whether it already has an
        experiment for a layout before building one.

        Args:
            layout (ComponentLayout): Layout of the ACCESS-ESM1.6 components, as returned by the layout search.
        Returns:
            str: Branch name.
        Raises:
            ValueError: If the layout is not a layout of ESM16_COMPONENT.
        """
        # ComponentLayout.sub_layouts is documented to come in the order of ParallelComponent.subcomponents, so
        # unpacking positionally is safe here and a layout of any other model simply does not unpack.
        um7, mom5, cice5 = layout.sub_layouts
        atm_nx, atm_ny = um7.decomposition.grid.shape
        mom_nx, mom_ny = mom5.decomposition.grid.shape
        # The trailing x1 on CICE5 is the same assumption layout_config_changes writes into cice_in.nml: one
        # block per rank, spanning the full y extent. Were the block distribution ever swept, CICE5 would get a
        # two-dimensional domain in ESM16_COMPONENT and the name would follow its process grid like the others.
        return f"{self._branch_name_prefix}_atm_{atm_nx}x{atm_ny}_mom_{mom_nx}x{mom_ny}_ice_{cice5.n_ranks}x1"

    def layout_config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run ACCESS-ESM1.6 with a given layout.

        Args:
            layout (ComponentLayout): Layout of the ACCESS-ESM1.6 components, as returned by the layout search.
        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control directory.
        Raises:
            ValueError: If the layout is not a layout of ESM16_COMPONENT.
        """
        um7, mom5, cice5 = layout.sub_layouts
        atm_nx, atm_ny = um7.decomposition.grid.shape
        mom_nx, mom_ny = mom5.decomposition.grid.shape
        # One block per rank spanning the full y extent, which is what block_size_y and max_blocks below say and
        # what the x1 in the branch name records. The distribution itself is left to the control configuration.
        ice_block_size_x = ESM16_CICE5_NX_GLOBAL // cice5.n_ranks
        return {
            "config.yaml": {
                "submodels": [
                    [
                        {"ncpus": um7.n_cores},
                        {"ncpus": mom5.n_cores},
                        {
                            "ncpus": cice5.n_cores,
                            "exe": ["cice_access.exe"],
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
            "ice/cice_in.nml": {
                "domain_nml": {
                    "nprocs": f"{cice5.n_ranks}",
                    "nx_global": str(ESM16_CICE5_NX_GLOBAL),
                    "ny_global": str(ESM16_CICE5_NY_GLOBAL),
                    "block_size_x": str(ice_block_size_x),
                    "block_size_y": str(ESM16_CICE5_NY_GLOBAL),
                    "max_blocks": "1",
                }
            },
        }


# NUOPC realm prefixes, as they appear in the PELAYOUT_attributes block of nuopc.runconfig. They are also the
# component names of the tree below, and so the keys a caller uses in an allocation strategy.
OM3_MEDIATOR_NAME: str = "cpl"
OM3_ATMOSPHERE_NAME: str = "atm"
OM3_SEA_ICE_NAME: str = "ice"
OM3_RUNOFF_NAME: str = "rof"
OM3_OCEAN_NAME: str = "ocn"
OM3_WAVE_NAME: str = "wav"
# The pool of cores the mediator, the data components and the sea ice take turns on. It is a component of the
# tree but not of the model, so its name is not a NUOPC realm.
OM3_SHARED_NAME: str = "shared"

# Ceiling on what counts as a reasonable ACCESS-OM3 layout at all, rather than the budget of any particular
# study. Constraints are cumulative and a caller can only tighten this one.
OM3_MAX_WASTED_CORE_FRACTION: float = 0.1


@dataclass(frozen=True)
class OM3Configuration:
    """An ACCESS-OM3 configuration: which components it runs, and the grid each one runs on.

    ACCESS-OM3 always runs the CMEPS mediator, and payu additionally requires a data atmosphere and a data
    runoff. The ocean, the sea ice and the waves are optional, and a configuration has to have at least one of
    them to be worth profiling. Every component carries its own grid: the configurations released so far all
    happen to put every component on the tripolar ocean grid, but that is a property of those configurations
    rather than of the model.

    The grids are not used by the layout search yet, which distributes cores without deciding how each
    component decomposes its domain. They are recorded because the configuration files state them, and because
    enumerating decompositions later must not change this class's shape.

    Args:
        name (str): Identifies the configuration in the names of the experiments generated for it.
        ocean (Domain | None): MOM6's grid, or None if the configuration has no ocean. MOM6 takes a range of
            cores of its own.
        sea_ice (Domain | None): CICE6's grid, or None if the configuration has no sea ice. CICE6 shares its
            cores with the mediator and the data components.
        waves (Domain | None): WW3's grid, or None if the configuration has no waves. WW3 takes a range of
            cores of its own.
        atmosphere (Domain | None): The data atmosphere's grid, on the shared cores.
        runoff (Domain | None): The data runoff's grid, on the shared cores.
        shared_core_offsets (Mapping[str, int]): The first core each component on the shared range occupies,
            counted from the start of that range. A component absent from the mapping starts at the beginning,
            which is what every released configuration does. Keyed by realm, so one of OM3_MEDIATOR_NAME,
            OM3_ATMOSPHERE_NAME, OM3_SEA_ICE_NAME or OM3_RUNOFF_NAME.

    Raises:
        ValueError: If the configuration has no ocean, sea ice or waves; or if an offset is negative or names
            something other than a component on the shared range.
    """

    name: str
    ocean: Domain | None = None
    sea_ice: Domain | None = None
    waves: Domain | None = None
    atmosphere: Domain | None = None
    runoff: Domain | None = None
    shared_core_offsets: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.ocean is None and self.sea_ice is None and self.waves is None:
            raise ValueError(
                f"ACCESS-OM3 configuration {self.name!r} has no ocean, sea ice or waves. A configuration of "
                "nothing but the mediator and the data components has no model to profile."
            )
        for realm, offset in self.shared_core_offsets.items():
            if realm not in self.shared_realms:
                raise ValueError(
                    f"ACCESS-OM3 configuration {self.name!r} gives a core offset for {realm!r}, which is not "
                    f"one of the components sharing its cores: {self.shared_realms}."
                )
            if offset < 0:
                raise ValueError(
                    f"ACCESS-OM3 configuration {self.name!r} gives {realm!r} a negative core offset {offset}."
                )

    @property
    def shared_realms(self) -> tuple[str, ...]:
        """Returns the components taking turns on the shared range of cores, in NUOPC realm order."""
        realms = [OM3_MEDIATOR_NAME]
        if self.atmosphere is not None:
            realms.append(OM3_ATMOSPHERE_NAME)
        if self.sea_ice is not None:
            realms.append(OM3_SEA_ICE_NAME)
        if self.runoff is not None:
            realms.append(OM3_RUNOFF_NAME)
        return tuple(realms)

    @property
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how this configuration is parallelised.

        The mediator, the data components and the sea ice draw on one range of cores and run on it in turn, so
        they are sub-components of a shared parent: the range is as large as the largest of them, not as large
        as their total. The ocean and the waves each take a range of their own, so they are siblings of that
        parent and their cores add to it.

        The leaves carry no domain. The search distributes cores and leaves each component to decompose its own
        grid, which is what the configuration files do today.
        """
        no_threads = (FixedThreadsPerRankConstraint(n_threads=1),)  # ACCESS-OM3 sets *_nthreads = 1 throughout.
        subcomponents = [
            ParallelComponent(
                name=OM3_SHARED_NAME,
                core_sharing=CoreSharing.SHARED,
                subcomponents=tuple(
                    ParallelComponent(
                        name=realm,
                        local_constraints=no_threads,
                        core_offset=self.shared_core_offsets.get(realm, 0),
                    )
                    for realm in self.shared_realms
                ),
            )
        ]
        if self.ocean is not None:
            subcomponents.append(ParallelComponent(name=OM3_OCEAN_NAME, local_constraints=no_threads))
        if self.waves is not None:
            subcomponents.append(ParallelComponent(name=OM3_WAVE_NAME, local_constraints=no_threads))
        return ParallelComponent(
            name=f"ACCESS-OM3 {self.name}",
            subcomponents=tuple(subcomponents),
            local_constraints=(MaxWastedCoreFractionConstraint(max_fraction=OM3_MAX_WASTED_CORE_FRACTION),),
        )


# Every ACCESS-OM3 configuration released so far is this one: MOM6 and CICE6 at 25 km, every component on the
# same tripolar grid. The releases differ in their forcing, their biogeochemistry and the core counts they were
# run with, none of which changes how the model is parallelised.
#
# Note that the ocean core count of this configuration cannot be varied on its own. MOM_input pins MOM6's
# decomposition with an explicit LAYOUT and a MASKTABLE naming a file of masked blocks, and the two must agree
# with ocn_ntasks. Searching over ocean core counts here means regenerating that mask table as well, which
# layout_config_changes does not do.
OM3_MC_25KM: OM3Configuration = OM3Configuration(
    name="MC-25km",
    ocean=Domain(shape=(1440, 1152)),
    sea_ice=Domain(shape=(1440, 1152)),
    atmosphere=Domain(shape=(1440, 1152)),
    runoff=Domain(shape=(1440, 1152)),
)
# Cores each component receives in release-MC_25km_jra_ryf-2.0-beta. These build no layout, and are provided as
# the reference a caller writing an allocation strategy is usually working from.
OM3_MC_25KM_RELEASE_CORES: dict[str, int] = {
    OM3_MEDIATOR_NAME: 275,
    OM3_ATMOSPHERE_NAME: 275,
    OM3_SEA_ICE_NAME: 275,
    OM3_RUNOFF_NAME: 275,
    OM3_OCEAN_NAME: 2429,
}


class OM3Profiling(PayuManager):
    """Handles profiling of ACCESS-OM3 configurations.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        configuration (OM3Configuration): The configuration being profiled. ACCESS-OM3 runs different
            components on different grids depending on the configuration, so unlike ACCESS-ESM1.6 there is no
            one component tree for the model.
    """

    _branch_name_prefix: str = "om3-layout"  # Prefix of the branch names of the generated layout experiments.

    _configuration: OM3Configuration

    def __init__(self, work_dir: Path, archive_dir: Path, configuration: OM3Configuration):
        super().__init__(work_dir, archive_dir)
        self._configuration = configuration

    @property
    def configuration(self) -> OM3Configuration:
        """Returns the ACCESS-OM3 configuration being profiled."""
        return self._configuration

    @property
    def model_type(self) -> str:
        return "access-om3"

    @property
    def parallel_component(self) -> ParallelComponent:
        return self._configuration.parallel_component

    def get_component_logs(self, path: Path) -> dict[str, ProfilingLog]:
        """Returns available profiling logs for the components in ACCESS-OM3.

        Every log is optional. ACCESS-OM3 writes the ESMF summary only when the run asked for it, through
        ESMF_RUNTIME_PROFILE, and a configuration without an ocean or without sea ice writes neither of the
        other two.

        Args:
            path (Path): Path to the output directory.
        Returns:
            dict[str, ProfilingLog]: Dictionary mapping component names to their ProfilingLog instances.
        """
        logs = {}

        config_path = path / "config.yaml"
        payu_config = YAMLParser().parse(config_path.read_text())
        mom6_logfile = path / f"{payu_config['model']}.out"
        if mom6_logfile.is_file():
            logger.debug(f"Found MOM6 log file: {mom6_logfile}")
            logs["MOM6"] = ProfilingLog(mom6_logfile, FMSProfilingParser(has_hits=True), optional=True)

        cice6_logfile = path / "log" / "ice.log"
        if cice6_logfile.is_file():
            logger.debug(f"Found CICE6 log file: {cice6_logfile}")
            logs["CICE6"] = ProfilingLog(cice6_logfile, CICE5ProfilingParser(), optional=True)

        # The name ESMF gives its summary when ESMF_RUNTIME_PROFILE_OUTPUT is SUMMARY. This has not been
        # checked against an ACCESS-OM3 archive; the log is optional, so a wrong name costs the ESMF timings
        # rather than the whole parse.
        esmf_logfile = path / "ESMF_Profile.summary"
        if esmf_logfile.is_file():
            logger.debug(f"Found ESMF profile summary: {esmf_logfile}")
            logs["ESMF"] = ProfilingLog(esmf_logfile, ESMFSummaryProfilingParser(), optional=True)

        return logs

    def _core_ranges(self, layout: ComponentLayout) -> dict[str, tuple[int, int]]:
        """Returns the cores each ACCESS-OM3 component occupies, as a (rootpe, ntasks) pair per realm.

        The components sharing a range are given that range's first core plus the offset the layout carries
        for each of them; the ocean and the waves follow the shared range in the order the tree declares them.
        A layout states where a shared component starts but not where a concurrent one does, since that
        follows from the sizes of the ones before it, so the running total here is what places those.

        Args:
            layout (ComponentLayout): Layout of the ACCESS-OM3 components, as returned by the layout search.
        Returns:
            dict[str, tuple[int, int]]: The first core and the number of cores of each realm.
        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        ranges: dict[str, tuple[int, int]] = {}
        rootpe = 0
        for component in layout.sub_layouts:
            if component.name == OM3_SHARED_NAME:
                for shared in component.sub_layouts:
                    ranges[shared.name] = (rootpe + shared.core_offset, shared.n_cores)
            else:
                ranges[component.name] = (rootpe, component.n_cores)
            rootpe += component.n_cores
        return ranges

    def layout_branch_name(self, layout: ComponentLayout) -> str:
        """Returns the name of the branch holding the experiment for a given ACCESS-OM3 layout.

        The name records the cores each component receives, so it is distinct for every distinct layout and the
        same layout always produces the same name. This is what lets the manager tell whether it already has an
        experiment for a layout before building one.

        Args:
            layout (ComponentLayout): Layout of the ACCESS-OM3 components, as returned by the layout search.
        Returns:
            str: Branch name.
        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        ranges = self._core_ranges(layout)
        cores = "_".join(f"{realm}_{ranges[realm][1]}" for realm in sorted(ranges))
        return f"{self._branch_name_prefix}_{self._configuration.name}_{cores}"

    def layout_config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run ACCESS-OM3 with a given layout.

        Args:
            layout (ComponentLayout): Layout of the ACCESS-OM3 components, as returned by the layout search.
        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control
                directory.
        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        pelayout: dict[str, int] = {}
        for realm, (rootpe, ntasks) in self._core_ranges(layout).items():
            pelayout[f"{realm}_ntasks"] = ntasks
            pelayout[f"{realm}_rootpe"] = rootpe
        return {
            # The whole budget, including any cores left idle: this is what the job occupies.
            "config.yaml": {"ncpus": layout.n_cores},
            "nuopc.runconfig": {"PELAYOUT_attributes": pelayout},
        }


class RAM3Profiling(CylcRoseManager):
    """Handles profiling of ACCESS-rAM3 configurations."""

    @property
    def known_parsers(self):
        return {
            "UM_regions": UMProfilingParser(),
            "UM_total": UMTotalRuntimeParser(),
        }


class AM3Profiling(CylcRoseManager):
    """Handles profiling of ACCESS-AM3 configurations."""

    @property
    def known_parsers(self):
        return {
            "UM_regions": UMProfilingParser(),
            "UM_total": UMTotalRuntimeParser(),
        }
