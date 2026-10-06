# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""ACCESS-OM3 configurations.

ACCESS-OM3 always runs the CMEPS mediator, and Payu additionally requires a data atmosphere and a data runoff.
The ocean, the sea ice and the waves are optional, and a configuration has to have at least one of them to be
worth profiling. Every component carries its own grid: the configurations released so far all happen to put
every component on the tripolar ocean grid, but that is a property of those configurations rather than of the
model.
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from access.config import NUOPCParser
from access.config.parallel_component import ComponentLayout, CoreSharing, ParallelComponent
from access.config.parallel_constraints import (
    FixedThreadsPerRankConstraint,
    MaxWastedCoreFractionConstraint,
    MinSubdomainSizeConstraint,
)
from access.config.parallel_domain import Domain, DomainDecompositionSpec

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.configuration import LogSpec, PayuConfiguration, log_at, payu_model_stdout
from access.profiling.control import GitControlSource
from access.profiling.esmf_parser import ESMFSummaryProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.models.cice import CICEPartitioning

logger = logging.getLogger(__name__)

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

# Floor on how thin a MOM6 rank's sub-domain may get: it must hold at least its halo width along every
# dimension, and NIHALO and NJHALO are 4 throughout ACCESS-OM3's MOM_input. Not a tolerance of any particular
# study - a layout that breaks it is not one the model can run - so anything beyond it, near-square
# sub-domains for one, is a choice and belongs in the allocation strategy. The sea ice's own floor is the
# ghost width its CICEPartitioning states.
OM3_MOM6_HALO_WIDTH: int = 4
OM3_CICE6_GHOST_WIDTH: int = 1

# The 25 km tripolar grid every ACCESS-OM3 configuration released so far runs every component on.
OM3_25KM_GRID: Domain = Domain(shape=(1440, 1152))


@dataclass(frozen=True)
class OM3Configuration(PayuConfiguration):
    """One configuration of ACCESS-OM3: which components it runs, and the grid each one runs on.

    Of those grids, only the ones a control directory states as a process grid reach the component tree and
    are decomposed by the layout search, since a decomposition nothing reads is not one a search can be said
    to have chosen. The sea ice's does where it is CICE6 *and* the scheme its ice_in configures forms a
    process grid at all, which is what its CICEPartitioning says; a configuration running CDEPS's data sea ice
    instead says so with data_sea_ice, and then it does not. The ocean's does only where the configuration
    pins MOM6's LAYOUT: MOM6 otherwise works its own decomposition out from the ranks it is given, which is
    what auto_ocean_layout says and what ACCESS-OM3 does. The mediator, the data components - the data sea ice
    among them when there is one - and the waves are handed their meshes by ESMF, which decomposes them over
    whatever ranks they are given, and state no process grid either. Every grid is recorded whether or not it
    reaches the tree, because the configuration files state them, and because a grid is how this class says
    which components a configuration runs.

    Args:
        name (str): Identifies the configuration in the names of the experiments generated for it.
        ocean (Domain | None): MOM6's grid, or None if the configuration has no ocean. MOM6 takes a range of
            cores of its own and decomposes this grid over them; whether the search chooses how is
            auto_ocean_layout's business. Must be two-dimensional, as LAYOUT is.
        sea_ice (CICEPartitioning | None): How CICE6 divides its grid, or None if the configuration has no sea
            ice. CICE6 shares its cores with the mediator and the data components; what the search may choose
            about its decomposition follows from the partitioning rather than being assumed here.
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
            offers one layout per ocean core count instead of one per factorisation of it - and config_changes
            writes no MOM_input, leaving the control directory to say how MOM6 decomposes. Note that this
            leaves a control directory that *does* pin a LAYOUT still using it, the way one carrying a
            MASKTABLE keeps that: a configuration whose ocean layout is really fixed should set this False and
            have the search choose one. Set False, the ocean carries its grid, the search picks a process grid
            subject to MOM6's halo width, and config_changes writes it to MOM_input.
        data_sea_ice (bool): Whether the sea ice is CDEPS's data component rather than CICE6. False by
            default, which is CICE6. Set True and the sea ice is treated as the data atmosphere and the data
            runoff are: it still takes its turn on the shared range and still receives cores, but ESMF
            decomposes its mesh over whatever ranks it is given, so it carries no grid into the component
            tree, the search chooses no process grid for it, and config_changes writes no ice_in. Its grid is
            still recorded in sea_ice, which is also what says the configuration has a sea ice at all.
        model_type (str): The model type identifier, as Payu defines it.
        experiment_prefix (str): Prefix for the names of experiments generated for this configuration.

    Raises:
        ValueError: If the configuration has no ocean, sea ice or waves; if the ocean's grid is not
            two-dimensional; or if an offset is negative or names something other than a component on the
            shared range.
    """

    # These three are dataclass fields rather than properties because that is all it takes to satisfy the
    # abstract properties of the base class: a field with a default leaves a value in the class namespace, and
    # a plain value is not a data descriptor, so each instance reads back its own. A field with no default
    # would leave the member abstract and the class uninstantiable, which is why name is defaulted and then
    # checked below rather than simply required.
    name: str = ""
    ocean: Domain | None = None
    sea_ice: CICEPartitioning | None = None
    waves: Domain | None = None
    atmosphere: Domain | None = None
    runoff: Domain | None = None
    shared_core_offsets: Mapping[str, int] = field(default_factory=dict)
    auto_ocean_layout: bool = True
    data_sea_ice: bool = False
    model_type: str = "access-om3"
    experiment_prefix: str = "om3-layout"

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError(
                "OM3Configuration.name must be non-empty: it is what tells one configuration's experiments "
                "from another's, and ACCESS-OM3's differ in which components they run."
            )
        if self.ocean is None and self.sea_ice is None and self.waves is None:
            raise ValueError(
                f"ACCESS-OM3 configuration {self.name!r} has no ocean, sea ice or waves. A configuration of "
                "nothing but the mediator and the data components has no model to profile."
            )
        # Checked here rather than where the grid is decomposed, so a configuration that cannot be written
        # out is refused when it is built instead of once a layout has been found for it. The sea ice's grid
        # is checked by CICEPartitioning itself.
        if self.ocean is not None and self.ocean.ndim != 2:
            raise ValueError(
                f"ACCESS-OM3 configuration {self.name!r} gives {OM3_OCEAN_NAME!r} a {self.ocean.ndim}-"
                f"dimensional grid {self.ocean.shape}. Its decomposition is written as MOM6's LAYOUT, a "
                "two-dimensional process grid, so the grid it decomposes has to be one too."
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
    def _cice6(self) -> CICEPartitioning | None:
        """Returns the CICE6 partitioning the search may choose from, or None where there is none to choose.

        A configuration running CDEPS's data sea ice has none, whatever grid it records for it.
        """
        return None if self.data_sea_ice else self.sea_ice

    @property
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how this configuration is parallelised.

        The mediator, the data components and the sea ice draw on one range of cores and run on it in turn, so
        they are sub-components of a shared parent: the range is as large as the largest of them, not as large
        as their total. The ocean and the waves each take a range of their own, so they are siblings of that
        parent and their cores add to it.

        The sea ice carries a domain where its own partitioning gives the search something to choose, and the
        ocean does where the configuration pins MOM6's LAYOUT. The other leaves carry none, and nor does the
        ocean under auto_ocean_layout: ESMF decomposes the data components' meshes over the ranks they are
        given and MOM6 works its own decomposition out from them, so in neither case is there a process grid
        to choose or to write.

        Returns:
            ParallelComponent: Root of the component tree.
        """
        no_threads = (FixedThreadsPerRankConstraint(n_threads=1),)  # ACCESS-OM3 sets *_nthreads = 1 throughout.
        cice6 = self._cice6
        subcomponents = [
            ParallelComponent(
                name=OM3_SHARED_NAME,
                core_sharing=CoreSharing.SHARED,
                subcomponents=tuple(
                    ParallelComponent(
                        name=realm,
                        domain=cice6.domain if realm == OM3_SEA_ICE_NAME and cice6 is not None else None,
                        local_constraints=(
                            no_threads + cice6.local_constraints
                            if realm == OM3_SEA_ICE_NAME and cice6 is not None
                            else no_threads
                        ),
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
                    local_constraints=(
                        no_threads
                        if self.auto_ocean_layout
                        else no_threads + (MinSubdomainSizeConstraint(min_size=OM3_MOM6_HALO_WIDTH),)
                    ),
                )
            )
        if self.waves is not None:
            subcomponents.append(ParallelComponent(name=OM3_WAVE_NAME, local_constraints=no_threads))
        return ParallelComponent(
            name=f"ACCESS-OM3 {self.name}",
            subcomponents=tuple(subcomponents),
            local_constraints=(MaxWastedCoreFractionConstraint(max_fraction=OM3_MAX_WASTED_CORE_FRACTION),),
        )

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        """Returns the profiling logs this configuration writes.

        Every one is optional. ACCESS-OM3 writes the ESMF summary only when the run asked for it, through
        ESMF_RUNTIME_PROFILE, and that summary covers the whole model rather than any one component of it, so
        it names no realm and has no cores of its own to be read against.
        """
        specs: list[LogSpec] = []
        if self.ocean is not None:
            specs.append(
                LogSpec(
                    "MOM6",
                    payu_model_stdout(),
                    FMSProfilingParser(has_hits=True),
                    component=OM3_OCEAN_NAME,
                    optional=True,
                )
            )
        if self._cice6 is not None:
            specs.append(
                LogSpec(
                    "CICE6", log_at("log/ice.log"), CICE5ProfilingParser(), component=OM3_SEA_ICE_NAME, optional=True
                )
            )
        # The name ESMF gives its summary when ESMF_RUNTIME_PROFILE_OUTPUT is SUMMARY. This has not been
        # checked against an ACCESS-OM3 archive; the log is optional, so a wrong name costs the ESMF timings
        # rather than the whole parse.
        specs.append(LogSpec("ESMF", log_at("ESMF_Profile.summary"), ESMFSummaryProfilingParser(), optional=True))
        return tuple(specs)

    def _core_ranges(self, layout: ComponentLayout) -> dict[str, tuple[int, int]]:
        """Returns the cores each component occupies, as a (rootpe, ntasks) pair per realm.

        The components sharing a range are given that range's first core plus the offset the layout carries
        for each of them; the ocean and the waves follow the shared range in the order the tree declares them.
        A layout states where a shared component starts but not where a concurrent one does, since that
        follows from the sizes of the ones before it, so the running total here is what places those.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

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
        """Returns the decomposition the search chose for each component that decomposes one.

        Only the ocean and the sea ice do, so only those appear here, and only when the configuration has
        them and leaves the choice to the search. The mediator, the data components and the waves are
        decomposed by ESMF over the ranks they are given, and carry no grid for the search to split.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

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

    def experiment_name(self, layout: ComponentLayout) -> str:
        """Returns the name of the experiment holding a given layout.

        The name records the configuration and then what each component received: its process grid where it
        decomposes one, and its cores otherwise. A grid says both, since its product is the component's rank
        count and so its core count too, every component running one thread per rank. The name is therefore
        distinct for every distinct layout - including two that divide the cores the same way and decompose
        them differently - and the same layout always produces the same name. This is what lets a manager tell
        whether it already has an experiment for a layout before building one.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            str: The experiment name.

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
        return f"{self.experiment_prefix}_{self.name}_{'_'.join(components)}"

    def config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run this configuration with a given layout.

        Every component's cores go into the PELAYOUT_attributes block of nuopc.runconfig, and the ocean and
        the sea ice additionally have the decomposition the search chose for them written out: MOM6's as a
        LAYOUT in MOM_input, CICE6's as whatever its partitioning says realises it. A configuration pinning
        MOM6's decomposition with a MASKTABLE needs that table removed or regenerated for the LAYOUT written
        here to mean anything; this does not touch it.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control
                directory.

        Raises:
            ValueError: If the layout is not a layout of this configuration, or cannot be realised by it.
        """
        ranges = self._core_ranges(layout)
        pelayout: dict[str, int] = {}
        for realm, (rootpe, ntasks) in ranges.items():
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
        cice6 = self._cice6
        if cice6 is not None:
            ice_changes = cice6.namelist_changes(ranges[OM3_SEA_ICE_NAME][1], decompositions.get(OM3_SEA_ICE_NAME))
            if ice_changes:
                changes[cice6.namelist_path] = {cice6.namelist_group: ice_changes}
        return changes

    def parse_layout(self, output_dir: Path) -> ComponentLayout | None:
        """Returns the layout an experiment ran, from the PE layout its NUOPC configuration states.

        This is the inverse of what config_changes writes, and reads the same block: every realm the
        configuration has states the first core it runs on and how many it is given. The shape has to come
        back with it, since cores alone do not say it - the realms taking turns on one range belong under the
        parent that holds that range, and the ones running beside it are its siblings - and which realms those
        are is what the configuration being profiled says.

        What comes back carries no decompositions: the PE layout says how many ranks each realm has, not what
        grid they are arranged in.

        Args:
            output_dir (Path): Directory holding the configuration files. Must contain a nuopc.runconfig.

        Returns:
            ComponentLayout | None: The layout, or None if the configuration does not say.

        Raises:
            ValueError: If the cores the configuration states are not a layout of this component tree - a
                shared range left partly idle, or a gap between two ranges. Better said here than quietly
                plotted.
        """
        runconfig_path = output_dir / "nuopc.runconfig"
        ranges = self._parse_core_ranges(runconfig_path)
        if ranges is None:
            return None

        try:
            return self._layout_from_core_ranges(ranges)
        except ValueError as error:
            raise ValueError(f"The PE layout in {runconfig_path} is not a layout of this model: {error}") from error

    def _parse_core_ranges(self, runconfig_path: Path) -> dict[str, tuple[int, int]] | None:
        """Returns the first core and the core count of each realm, as nuopc.runconfig states them.

        Args:
            runconfig_path (Path): Path to the experiment's nuopc.runconfig.

        Returns:
            dict[str, tuple[int, int]] | None: The first core and the number of cores of each realm this
                configuration has, or None if the file does not say.
        """
        if not runconfig_path.is_file():
            logger.debug(f"No NUOPC configuration at {runconfig_path}.")
            return None

        pelayout = NUOPCParser().parse(runconfig_path.read_text()).get("PELAYOUT_attributes")
        if not pelayout:
            logger.debug(f"The NUOPC configuration at {runconfig_path} states no PE layout.")
            return None

        realms = list(self.shared_realms)
        if self.ocean is not None:
            realms.append(OM3_OCEAN_NAME)
        if self.waves is not None:
            realms.append(OM3_WAVE_NAME)

        ranges = {}
        for realm in realms:
            rootpe, ntasks = pelayout.get(f"{realm}_rootpe"), pelayout.get(f"{realm}_ntasks")
            if rootpe is None or ntasks is None:
                logger.debug(f"The PE layout in {runconfig_path} does not say what {realm!r} was given.")
                return None
            ranges[realm] = (rootpe, ntasks)
        return ranges

    def _layout_from_core_ranges(self, ranges: dict[str, tuple[int, int]]) -> ComponentLayout:
        """Returns the layout the given core ranges describe, the inverse of _core_ranges.

        Args:
            ranges (dict[str, tuple[int, int]]): The first core and the number of cores of each realm.

        Returns:
            ComponentLayout: The layout those ranges describe.

        Raises:
            ValueError: If they do not describe a layout of this component tree.
        """
        shared_realms = self.shared_realms
        # The realms sharing a range are placed within it, so the range starts where the earliest of them
        # does and reaches as far as the furthest of them reaches.
        shared_start = min(ranges[realm][0] for realm in shared_realms)
        shared_end = max(ranges[realm][0] + ranges[realm][1] for realm in shared_realms)
        shared = ComponentLayout(
            name=OM3_SHARED_NAME,
            n_cores=shared_end - shared_start,
            # A shared parent counts the cores its components occupy between them, and they have to occupy
            # all of them, so this is the size of the range itself.
            n_ranks=shared_end - shared_start,
            threads_per_rank=None,
            decomposition=None,
            sub_layouts=tuple(
                _om3_leaf(realm, ranges[realm][1], core_offset=ranges[realm][0] - shared_start)
                for realm in shared_realms
            ),
            core_sharing=CoreSharing.SHARED,
        )

        # The realms with a range of their own follow it, in the order the tree declares them.
        placed = [(shared_start, shared)]
        if self.ocean is not None:
            placed.append((ranges[OM3_OCEAN_NAME][0], _om3_leaf(OM3_OCEAN_NAME, ranges[OM3_OCEAN_NAME][1])))
        if self.waves is not None:
            placed.append((ranges[OM3_WAVE_NAME][0], _om3_leaf(OM3_WAVE_NAME, ranges[OM3_WAVE_NAME][1])))

        # Each range has to start where the one before it ended, and the first of them at the first core.
        # Anything else leaves cores between the ranges that no component accounts for, which a tree of
        # components running side by side has no way to express.
        expected = 0
        for rootpe, sub_layout in placed:
            if rootpe != expected:
                raise ValueError(
                    f"{sub_layout.name!r} starts at core {rootpe}, but the components before it end at "
                    f"{expected}, so {rootpe - expected} core(s) belong to nothing."
                )
            expected += sub_layout.n_cores

        sub_layouts = tuple(sub_layout for _, sub_layout in placed)
        return ComponentLayout(
            name=f"ACCESS-OM3 {self.name}",
            n_cores=sum(sub_layout.n_cores for sub_layout in sub_layouts),
            n_ranks=sum(sub_layout.n_ranks for sub_layout in sub_layouts),
            threads_per_rank=None,
            decomposition=None,
            sub_layouts=sub_layouts,
        )


def _om3_leaf(name: str, n_cores: int, core_offset: int = 0) -> ComponentLayout:
    """Returns the layout of one ACCESS-OM3 realm, which runs one thread per rank throughout."""

    return ComponentLayout(
        name=name,
        n_cores=n_cores,
        n_ranks=n_cores,
        threads_per_rank=1,
        decomposition=None,
        core_offset=core_offset,
    )


# CICE6 as the 25 km releases configure it: square-ice blocks distributed roundrobin. Neither forms a process
# grid - roundrobin maps blocks to ranks without one, and square-ice is not a shape a one-dimensional domain
# could express anyway - so the search chooses how many ranks the sea ice gets and nothing else, and nothing
# is written into ice_in. That is the whole of what the release leaves open.
OM3_25KM_CICE6: CICEPartitioning = CICEPartitioning(
    grid=OM3_25KM_GRID.shape,
    distribution_type="roundrobin",
    processor_shape="square-ice",
    nghost=OM3_CICE6_GHOST_WIDTH,
)

# Every ACCESS-OM3 configuration released so far is this one: MOM6 and CICE6 at 25 km, every component on the
# same tripolar grid. The releases differ in their forcing, their biogeochemistry and the core counts they
# were run with, none of which changes how the model is parallelised.
#
# Note what the mask table costs this configuration. MOM_input pins MOM6's decomposition with an explicit
# LAYOUT and a MASKTABLE naming a file of masked blocks, and ocn_ntasks is the layout's ranks less the masked
# ones. config_changes writes LAYOUT but never touches MASKTABLE - dropping it silently would change what is
# computed, not just how it is divided - so a control directory carrying one has to have it removed or
# regenerated before a generated layout means anything.
#
# It also puts the released core counts out of the search's reach in all but name: ocn_ntasks is 2429, a
# masked count and so 7 x 347 as a process grid. The search still finds layouts on those counts, because
# nothing here rules them out, but a decomposition it writes for them is not the one the release runs. Treat
# OM3_MC_25KM_RELEASE_CORES as the budget the release occupied rather than as a layout to reproduce.
OM3_MC_25KM: OM3Configuration = OM3Configuration(
    name="MC-25km",
    ocean=OM3_25KM_GRID,
    sea_ice=OM3_25KM_CICE6,
    atmosphere=OM3_25KM_GRID,
    runoff=OM3_25KM_GRID,
)

# The release that configuration describes, which is a convenience rather than part of it: the other 25 km
# releases are the same configuration against a different control, so a study profiling one passes its own
# and keeps OM3_MC_25KM.
OM3_CONFIGS_REPOSITORY: str = "git@github.com:ACCESS-NRI/access-om3-configs.git"
OM3_MC_25KM_SOURCE: GitControlSource = GitControlSource(
    repository=OM3_CONFIGS_REPOSITORY,
    start_point="release-MC_25km_jra_ryf-2.0-beta",
)
# Cores each component receives in release-MC_25km_jra_ryf-2.0-beta. These build no layout, and are provided
# as the reference a caller writing an allocation strategy is usually working from.
OM3_MC_25KM_RELEASE_CORES: dict[str, int] = {
    OM3_MEDIATOR_NAME: 275,
    OM3_ATMOSPHERE_NAME: 275,
    OM3_SEA_ICE_NAME: 275,
    OM3_RUNOFF_NAME: 275,
    OM3_OCEAN_NAME: 2429,
}
