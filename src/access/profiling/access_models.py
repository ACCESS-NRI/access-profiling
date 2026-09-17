# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from access.config import YAMLParser
from access.config.parallel_component import ComponentLayout, CoreSharing, ParallelComponent
from access.config.parallel_constraints import (
    DomainDivisibleByRanksConstraint,
    FixedThreadsPerRankConstraint,
    MaxWastedCoreFractionConstraint,
    MinSubdomainSizeConstraint,
    ProcessGridDimEvenConstraint,
    SubdomainAspectRatioConstraint,
)
from access.config.parallel_domain import Domain, DomainDecompositionSpec

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

# Floors on how thin a rank's sub-domain may get, one per model that decomposes one. A MOM6 rank must hold at
# least its halo width along every dimension, and NIHALO and NJHALO are 4 throughout ACCESS-OM3's MOM_input;
# a CICE6 block must hold at least its ghost width. Neither is a tolerance of any particular study: a layout
# that breaks them is not one the model can run. Anything beyond them - near-square sub-domains, blocks that
# tile the sea ice grid exactly - is a choice, and belongs in the allocation strategy.
OM3_MOM6_HALO_WIDTH: int = 4
OM3_CICE6_GHOST_WIDTH: int = 1


@dataclass(frozen=True)
class OM3Configuration:
    """An ACCESS-OM3 configuration: which components it runs, and the grid each one runs on.

    ACCESS-OM3 always runs the CMEPS mediator, and payu additionally requires a data atmosphere and a data
    runoff. The ocean, the sea ice and the waves are optional, and a configuration has to have at least one of
    them to be worth profiling. Every component carries its own grid: the configurations released so far all
    happen to put every component on the tripolar ocean grid, but that is a property of those configurations
    rather than of the model.

    Of those grids, only the ones a control directory states as a process grid reach the component tree and
    are decomposed by the layout search, since a decomposition nothing reads is not one a search can be said
    to have chosen. The sea ice's always does, as CICE6's domain_nml. The ocean's does only where the
    configuration pins MOM6's LAYOUT: MOM6 otherwise works its own decomposition out from the ranks it is
    given, which is what auto_ocean_layout says and what ACCESS-OM3 does. The mediator, the data components
    and the waves are handed their meshes by ESMF, which decomposes them over whatever ranks they are given,
    and state no process grid either. Every grid is recorded whether or not it reaches the tree, because the
    configuration files state them, and because a grid is how this class says which components a
    configuration runs.

    Args:
        name (str): Identifies the configuration in the names of the experiments generated for it.
        ocean (Domain | None): MOM6's grid, or None if the configuration has no ocean. MOM6 takes a range of
            cores of its own and decomposes this grid over them; whether the search chooses how is
            auto_ocean_layout's business. Must be two-dimensional, as LAYOUT is.
        sea_ice (Domain | None): CICE6's grid, or None if the configuration has no sea ice. CICE6 shares its
            cores with the mediator and the data components, and decomposes this grid over them: the search
            chooses the process grid and layout_config_changes turns it into the blocks ice_in states. Must be
            two-dimensional, as nx_global and ny_global are.
        waves (Domain | None): WW3's grid, or None if the configuration has no waves. WW3 takes a range of
            cores of its own. Recorded only: WW3 decomposes its own spectral grid and states no process grid.
        atmosphere (Domain | None): The data atmosphere's grid, on the shared cores. Recorded only: CDEPS
            hands the mesh to ESMF, which decomposes it over the ranks datm is given.
        runoff (Domain | None): The data runoff's grid, on the shared cores. Recorded only, as for the
            atmosphere.
        shared_core_offsets (Mapping[str, int]): The first core each component on the shared range occupies,
            counted from the start of that range. A component absent from the mapping starts at the beginning,
            which is what every released configuration does. Keyed by realm, so one of OM3_MEDIATOR_NAME,
            OM3_ATMOSPHERE_NAME, OM3_SEA_ICE_NAME or OM3_RUNOFF_NAME.
        auto_ocean_layout (bool): Whether MOM6 works its own decomposition out from the ranks it is given,
            rather than being handed a LAYOUT. True by default, which is how ACCESS-OM3 runs: its MOM_input
            sets AUTO_MASKTABLE and states no LAYOUT at all. The ocean then carries no grid into the
            component tree - there is no process grid for the search to choose, so it enumerates none and
            offers one layout per ocean core count instead of one per factorisation of it - and
            layout_config_changes writes no MOM_input, leaving the control directory to say how MOM6
            decomposes. Note that this leaves a control directory that *does* pin a LAYOUT still using it,
            the way one carrying a MASKTABLE keeps that: a configuration whose ocean layout is really fixed
            should set this False and have the search choose one. Set False, the ocean carries its grid as
            the sea ice does, the search picks a process grid subject to MOM6's halo width, and
            layout_config_changes writes it to MOM_input.

    Raises:
        ValueError: If the configuration has no ocean, sea ice or waves; if the ocean's or the sea ice's grid
            is not two-dimensional; or if an offset is negative or names something other than a component on
            the shared range.
    """

    name: str
    ocean: Domain | None = None
    sea_ice: Domain | None = None
    waves: Domain | None = None
    atmosphere: Domain | None = None
    runoff: Domain | None = None
    shared_core_offsets: Mapping[str, int] = field(default_factory=dict)
    auto_ocean_layout: bool = True

    def __post_init__(self) -> None:
        if self.ocean is None and self.sea_ice is None and self.waves is None:
            raise ValueError(
                f"ACCESS-OM3 configuration {self.name!r} has no ocean, sea ice or waves. A configuration of "
                "nothing but the mediator and the data components has no model to profile."
            )
        for realm, domain in ((OM3_OCEAN_NAME, self.ocean), (OM3_SEA_ICE_NAME, self.sea_ice)):
            # Checked here rather than where the grid is decomposed, so a configuration that cannot be
            # written out is refused when it is built instead of once a layout has been found for it.
            if domain is not None and domain.ndim != 2:
                raise ValueError(
                    f"ACCESS-OM3 configuration {self.name!r} gives {realm!r} a {domain.ndim}-dimensional "
                    f"grid {domain.shape}. Its decomposition is written as a two-dimensional process grid, "
                    "MOM6's LAYOUT or CICE6's blocks, so the grid it decomposes has to be one too."
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

        The sea ice carries its grid, so the search decomposes it and layout_config_changes writes the
        decomposition it chose, and the ocean does too where the configuration pins MOM6's LAYOUT. The other
        leaves carry none, and nor does the ocean under auto_ocean_layout: ESMF decomposes the data
        components' meshes over the ranks they are given and MOM6 works its own decomposition out from them,
        so in neither case is there a process grid to choose or to write.
        """
        no_threads = (FixedThreadsPerRankConstraint(n_threads=1),)  # ACCESS-OM3 sets *_nthreads = 1 throughout.
        ocean_constraints = no_threads + (MinSubdomainSizeConstraint(min_size=OM3_MOM6_HALO_WIDTH),)
        ice_constraints = no_threads + (MinSubdomainSizeConstraint(min_size=OM3_CICE6_GHOST_WIDTH),)
        subcomponents = [
            ParallelComponent(
                name=OM3_SHARED_NAME,
                core_sharing=CoreSharing.SHARED,
                subcomponents=tuple(
                    ParallelComponent(
                        name=realm,
                        domain=self.sea_ice if realm == OM3_SEA_ICE_NAME else None,
                        local_constraints=ice_constraints if realm == OM3_SEA_ICE_NAME else no_threads,
                        core_offset=self.shared_core_offsets.get(realm, 0),
                    )
                    for realm in self.shared_realms
                ),
            )
        ]
        if self.ocean is not None:
            # Under auto_ocean_layout MOM6 chooses the decomposition, so the grid stays off the tree and the
            # search enumerates no process grid for it. MinSubdomainSizeConstraint goes with it rather than
            # being left to raise: it reads a decomposition, and a sub-domain shape nobody chose here is not
            # one this can speak for. MOM6 answers for the shape it picks.
            subcomponents.append(
                ParallelComponent(
                    name=OM3_OCEAN_NAME,
                    domain=None if self.auto_ocean_layout else self.ocean,
                    local_constraints=no_threads if self.auto_ocean_layout else ocean_constraints,
                )
            )
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
# Note what the mask table costs this configuration. MOM_input pins MOM6's decomposition with an explicit
# LAYOUT and a MASKTABLE naming a file of masked blocks, and ocn_ntasks is the layout's ranks less the masked
# ones. layout_config_changes writes LAYOUT but never touches MASKTABLE - dropping it silently would change
# what is computed, not just how it is divided - so a control directory carrying one has to have it removed or
# regenerated before a generated layout means anything.
#
# It also puts the released core counts out of the search's reach in all but name. ocn_ntasks is 2429, a
# masked count and so 7 x 347 as a process grid, and the sea ice's 275 tiles neither extent of the grid: the
# search still finds layouts on those counts, because nothing here rules them out, but the decompositions it
# writes for them are not the ones the release runs. Treat OM3_MC_25KM_RELEASE_CORES as the budget the release
# occupied rather than as a layout to reproduce.
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


def _om3_cice6_domain_nml(decomposition: DomainDecompositionSpec) -> dict[str, str]:
    """Returns the CICE6 domain_nml entries realising a given sea ice decomposition.

    CICE6 is not given a process grid. It is given a block size, and distributes the blocks covering the grid
    over its ranks, so the grid the search chose has to be turned into blocks. Each rank holds one block of
    ``shape[i] // grid[i]`` points, rounded *down*: rounding up can leave fewer blocks than there are ranks -
    1440 points over 275 ranks gives blocks of 6 and only 240 of them - and a rank with no block to work on
    aborts the run. Rounding down always leaves at least as many blocks as ranks, and leaves exactly one each,
    with max_blocks of 1, when the process grid tiles the extents.

    The distribution itself is left to the control configuration, as it is for ACCESS-ESM1.6's CICE5. Note
    that a configuration distributing its blocks with distribution_type = "cartesian" needs them to tile the
    grid, which only a decomposition whose process grid divides both extents gives: a study on such a
    configuration says so with a DomainDivisibleByRanksConstraint in its allocation strategy.

    Args:
        decomposition (DomainDecompositionSpec): CICE6's grid and the process grid it is split over.
    Returns:
        dict[str, str]: The domain_nml entries, as the strings a Fortran namelist is written from.
    """
    extents = decomposition.domain.shape
    block_sizes = tuple(extent // ranks for extent, ranks in zip(extents, decomposition.grid, strict=True))
    n_blocks = math.prod(math.ceil(extent / size) for extent, size in zip(extents, block_sizes, strict=True))
    return {
        "nprocs": str(decomposition.n_ranks),
        "nx_global": str(extents[0]),
        "ny_global": str(extents[1]),
        "block_size_x": str(block_sizes[0]),
        "block_size_y": str(block_sizes[1]),
        "max_blocks": str(math.ceil(n_blocks / decomposition.n_ranks)),
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

    def _decompositions(self, layout: ComponentLayout) -> dict[str, DomainDecompositionSpec]:
        """Returns the decomposition the search chose for each ACCESS-OM3 component that decomposes one.

        Only the ocean and the sea ice do, so only those appear here, and only when the configuration has
        them. The mediator, the data components and the waves are decomposed by ESMF over the ranks they are
        given, and carry no grid for the search to split.

        Args:
            layout (ComponentLayout): Layout of the ACCESS-OM3 components, as returned by the layout search.
        Returns:
            dict[str, DomainDecompositionSpec]: The grid and process grid of each realm that has one.
        """
        decompositions: dict[str, DomainDecompositionSpec] = {}
        for component in layout.sub_layouts:
            # The shared range is a component of the tree and not of the model, so it is its children that
            # name realms; everything else is a realm in its own right.
            realms = component.sub_layouts if component.name == OM3_SHARED_NAME else (component,)
            for realm in realms:
                if realm.decomposition is not None:
                    decompositions[realm.name] = realm.decomposition
        return decompositions

    def layout_branch_name(self, layout: ComponentLayout) -> str:
        """Returns the name of the branch holding the experiment for a given ACCESS-OM3 layout.

        The name records what each component received: its process grid where it decomposes one, and its cores
        otherwise. A grid says both, since its product is the component's rank count and so its core count
        too, every component running one thread per rank. The name is therefore distinct for every distinct
        layout - including two that divide the cores the same way and decompose them differently - and the
        same layout always produces the same name. This is what lets the manager tell whether it already has
        an experiment for a layout before building one.

        Args:
            layout (ComponentLayout): Layout of the ACCESS-OM3 components, as returned by the layout search.
        Returns:
            str: Branch name.
        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        ranges = self._core_ranges(layout)
        decompositions = self._decompositions(layout)
        components = []
        for realm in sorted(ranges):
            decomposition = decompositions.get(realm)
            if decomposition is None:
                components.append(f"{realm}_{ranges[realm][1]}")
            else:
                components.append(f"{realm}_" + "x".join(str(ranks) for ranks in decomposition.grid))
        return f"{self._branch_name_prefix}_{self._configuration.name}_{'_'.join(components)}"

    def layout_config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run ACCESS-OM3 with a given layout.

        Every component's cores go into the PELAYOUT_attributes block of nuopc.runconfig, and the ocean and
        the sea ice additionally have the decomposition the search chose for them written out: MOM6's as a
        LAYOUT in MOM_input, CICE6's as the blocks of ice_in's domain_nml. A configuration pinning MOM6's
        decomposition with a MASKTABLE needs that table removed or regenerated for the LAYOUT written here to
        mean anything; this does not touch it.

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
        changes = {
            # The whole budget, including any cores left idle: this is what the job occupies.
            "config.yaml": {"ncpus": layout.n_cores},
            "nuopc.runconfig": {"PELAYOUT_attributes": pelayout},
        }
        decompositions = self._decompositions(layout)
        ocean = decompositions.get(OM3_OCEAN_NAME)
        if ocean is not None:
            changes["MOM_input"] = {"LAYOUT": ", ".join(str(ranks) for ranks in ocean.grid)}
        sea_ice = decompositions.get(OM3_SEA_ICE_NAME)
        if sea_ice is not None:
            changes["ice_in"] = {"domain_nml": _om3_cice6_domain_nml(sea_ice)}
        return changes


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
