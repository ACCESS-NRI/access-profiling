# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""ACCESS-OM3 configurations.

ACCESS-OM3 couples MOM6 and CICE6 through the CMEPS mediator, with the data atmosphere and the data runoff Payu
requires, and optionally WW3. Every configuration released or under development runs MOM6, CICE6 and the data
components; the waves are what the MCW configurations add. All of a configuration's components run on one grid,
the sea ice's, which is what the configuration records.

The components are named after their NUOPC realm prefixes in nuopc.runconfig - `cpl`, `atm`, `ice`, `rof`, `ocn`
and `wav` - and the mediator, the data components and the sea ice sit under a parent named `shared`, the range of
cores they take turns on. Those names are the keys of an allocation strategy.

A configuration is put together from the components of `access.profiling.models.components` and of
`access.profiling.payu_configuration`: CICE6 through the CICE component, and the rest as plain components, since
ESMF decomposes the data components' meshes over whatever ranks they are given, MOM6 works its own decomposition
out under AUTO_MASKTABLE and WW3 decomposes its own spectral grid. The arrangement is NUOPC's rather than Payu's,
so the configuration writes and reads the PE layout itself.
"""

import logging
from dataclasses import dataclass, replace
from pathlib import Path

from access.config import NUOPCParser
from access.config.parallel_component import ComponentLayout, CoreSharing, ParallelComponent

from access.profiling.application import LogSpec, log_at
from access.profiling.control import GitControlSource
from access.profiling.esmf_parser import ESMFSummaryProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.models.cice import CICEPartitioning
from access.profiling.models.components import CICEComponent
from access.profiling.payu_configuration import PayuComponent, PayuConfiguration, PlainComponent, payu_model_stdout

logger = logging.getLogger(__name__)

# NUOPC realm prefixes, as they appear in the PELAYOUT_attributes block of nuopc.runconfig. They are also the
# component names of the tree below, and so the keys a caller uses in an allocation strategy.
OM3_MEDIATOR_NAME: str = "cpl"
"""Component name of the CMEPS mediator."""
OM3_ATMOSPHERE_NAME: str = "atm"
"""Component name of the data atmosphere."""
OM3_SEA_ICE_NAME: str = "ice"
"""Component name of the sea ice."""
OM3_RUNOFF_NAME: str = "rof"
"""Component name of the data runoff."""
OM3_OCEAN_NAME: str = "ocn"
"""Component name of the ocean, MOM6."""
OM3_WAVE_NAME: str = "wav"
"""Component name of the waves, WW3."""
OM3_SHARED_NAME: str = "shared"
"""Component name of the pool of cores the mediator, the data components and the sea ice take turns on.

It is a component of the tree but not of the model, so its name is not a NUOPC realm.
"""

OM3_CICE6_GHOST_WIDTH: int = 1
"""CICE6's ghost width, the floor on how thin the sea ice's blocks may get."""

OM3_CICE6_BLOCKS_PER_RANK_RANGE: tuple[int, int] = (5, 8)
"""How many blocks each CICE6 rank should hold, land blocks included.

Several per rank is how CICE6 balances its work, and the 25 km release holds about 7.5 of its 30 x 27 blocks on
each of its 275 ranks. Every preset chooses its block size from the rank count to fall in this range, with the
distribution and the processor shape its control states.
"""

OM3_MOM6: PlainComponent = PlainComponent(
    name=OM3_OCEAN_NAME,
    token=OM3_OCEAN_NAME,
    logs=(
        LogSpec(
            "MOM6", payu_model_stdout(), FMSProfilingParser(has_hits=True), component=OM3_OCEAN_NAME, optional=True
        ),
    ),
)
"""MOM6, as every ACCESS-OM3 configuration runs it: decomposing its own grid, and logging its FMS clocks.

MOM6 works its own decomposition out from the ranks it is given, under AUTO_MASKTABLE, so the search chooses its
cores alone and nothing is written to MOM_input. The log is in the standard output Payu captures, and optional,
since a run may be configured not to write clocks.
"""

# The name ESMF gives its summary when ESMF_RUNTIME_PROFILE_OUTPUT is SUMMARY, at the top of the output directory
# Payu archives a run to. That is where the ACCESS-OM3 25 km scaling study of access-model-scaling (commit 2108c21)
# read its ESMF timings from. The log is optional, since a run without those settings writes none.
_ESMF_SUMMARY: LogSpec = LogSpec("ESMF", log_at("ESMF_Profile.summary"), ESMFSummaryProfilingParser(), optional=True)


@dataclass(frozen=True, kw_only=True)
class OM3Configuration(PayuConfiguration):
    """One configuration of ACCESS-OM3: how its sea ice divides its grid, and whether it runs waves.

    The mediator, the data atmosphere and the data runoff are always there, on the range of cores they share with
    the sea ice, and so is the ocean, on a range of its own. MOM6 works its own decomposition out from the ranks
    it is given, under AUTO_MASKTABLE, so the search chooses its cores alone and config_changes writes no
    MOM_input: a control that pins a LAYOUT and a MASKTABLE in MOM_input needs them removed, and AUTO_MASKTABLE
    set, through control_options before a generated ocean core count means anything. Another arrangement - a
    pinned MOM6 layout, a data sea ice, a shared realm starting at an offset - is a subclass returning the
    components it wants from `shared` and `concurrent`.

    Args:
        name (str): Identifies the configuration in the names of the experiments generated for it.
        sea_ice (CICEPartitioning): How CICE6 divides its grid, which is every component's grid. What the search
            may choose about it follows from the partitioning: the presets choose the block size from the rank
            count, so the search chooses that count alone.
        waves (bool): Whether the configuration runs WW3, as the MCW configurations do. WW3 takes a range of
            cores of its own after the ocean's and decomposes its own spectral grid, so the search chooses its
            cores alone.
        model_type (str): The model type identifier, as Payu defines it.
        model_name (str): The model's name, which names the root of the component tree with `name`.
        experiment_prefix (str): Prefix for the names of experiments generated for this configuration.

    Raises:
        ValueError: If the name is empty.
    """

    name: str
    sea_ice: CICEPartitioning
    waves: bool = False
    model_type: str = "access-om3"
    model_name: str = "ACCESS-OM3"
    experiment_prefix: str = "om3-layout"

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError(
                "OM3Configuration.name must be non-empty: it is what tells one configuration's experiments "
                "from another's, and ACCESS-OM3's differ in their grids and in which components they run."
            )

    @property
    def shared(self) -> tuple[PayuComponent, ...]:
        """Returns the components taking turns on the shared range of cores, in NUOPC realm order."""
        return (
            PlainComponent(name=OM3_MEDIATOR_NAME, token=OM3_MEDIATOR_NAME),
            PlainComponent(name=OM3_ATMOSPHERE_NAME, token=OM3_ATMOSPHERE_NAME),
            CICEComponent(
                self.sea_ice,
                name=OM3_SEA_ICE_NAME,
                token=OM3_SEA_ICE_NAME,
                log_name="CICE6",
                log_path="log/ice.log",
                optional=True,
            ),
            PlainComponent(name=OM3_RUNOFF_NAME, token=OM3_RUNOFF_NAME),
        )

    @property
    def concurrent(self) -> tuple[PayuComponent, ...]:
        """Returns the components with a range of their own, in the order those ranges follow the shared one."""
        components: list[PayuComponent] = [OM3_MOM6]
        if self.waves:
            components.append(PlainComponent(name=OM3_WAVE_NAME, token=OM3_WAVE_NAME))
        return tuple(components)

    @property
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how this configuration is parallelised.

        The mediator, the data components and the sea ice draw on one range of cores and run on it in turn, so
        they are sub-components of a shared parent: the range is as large as the largest of them, not as large
        as their total. The ocean and the waves each take a range of their own, so they are siblings of that
        parent and their cores add to it.

        Only the sea ice can carry a domain, where its partitioning gives the search something to choose. The
        other leaves carry none: ESMF decomposes the data components' meshes over the ranks they are given, and
        MOM6 and WW3 work their own decompositions out, so for none of them is there a process grid to choose or
        to write.

        Returns:
            ParallelComponent: Root of the component tree.
        """
        shared = ParallelComponent(
            name=OM3_SHARED_NAME,
            core_sharing=CoreSharing.SHARED,
            subcomponents=tuple(self._leaf(component) for component in self.shared),
        )
        return self._root([shared, *(self._leaf(component) for component in self.concurrent)])

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        """Returns the profiling logs this configuration writes, every one of them optional.

        Besides its components' logs, ACCESS-OM3 writes the ESMF summary when the run asked for it, through
        ESMF_RUNTIME_PROFILE, and that summary covers the whole model rather than any one component of it, so it
        names no realm and has no cores of its own to be read against.
        """
        return (*self._component_logs(self.shared), *self._component_logs(self.concurrent), _ESMF_SUMMARY)

    def _realm_layouts(self, layout: ComponentLayout) -> dict[str, ComponentLayout]:
        """Returns what each realm was given, keyed by realm.

        The shared range is a component of the tree and not of the model, so it is its children that name
        realms; everything else is a realm in its own right.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            dict[str, ComponentLayout]: The layout of each realm this configuration runs.

        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        realms: dict[str, ComponentLayout] = {}
        for component in layout.sub_layouts:
            for realm in component.sub_layouts if component.name == OM3_SHARED_NAME else (component,):
                realms[realm.name] = realm
        expected = [component.name for component in (*self.shared, *self.concurrent)]
        if sorted(realms) != sorted(expected):
            raise ValueError(
                f"The layout {layout.name!r} has the components {sorted(realms)}, but {self.model_name} "
                f"configuration {self.name!r} runs {sorted(expected)}."
            )
        return realms

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
        self._realm_layouts(layout)
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
        realms = self._realm_layouts(layout)
        components = []
        for realm in sorted(realms):
            decomposition = realms[realm].decomposition
            if decomposition is None:
                components.append(f"{realm}_{realms[realm].n_cores}")
            else:
                components.append(f"{realm}_" + "x".join(str(ranks) for ranks in decomposition.grid))
        return f"{self.experiment_prefix}_{self.name}_{'_'.join(components)}"

    def config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run this configuration with a given layout.

        Every component's cores go into the PELAYOUT_attributes block of nuopc.runconfig, and each component
        additionally has its own files changed as it says: the sea ice's block size, as its partitioning
        realises its rank count, goes into ice_in.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control
                directory.

        Raises:
            ValueError: If the layout is not a layout of this configuration, or cannot be realised by it.
        """
        pelayout: dict[str, int] = {}
        for realm, (rootpe, ntasks) in self._core_ranges(layout).items():
            pelayout[f"{realm}_ntasks"] = ntasks
            pelayout[f"{realm}_rootpe"] = rootpe
        changes: dict = {
            # The whole budget, including any cores left idle: this is what the job occupies.
            "config.yaml": {"ncpus": layout.n_cores},
            "nuopc.runconfig": {"PELAYOUT_attributes": pelayout},
        }
        realms = self._realm_layouts(layout)
        for component in (*self.shared, *self.concurrent):
            changes.update(component.config_changes(realms[component.name]))
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

        ranges = {}
        for component in (*self.shared, *self.concurrent):
            realm = component.name
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
        shared_realms = [component.name for component in self.shared]
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
        for component in self.concurrent:
            placed.append((ranges[component.name][0], _om3_leaf(component.name, ranges[component.name][1])))

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
            name=f"{self.model_name} {self.name}",
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


OM3_25KM_GRID: tuple[int, int] = (1440, 1152)
"""The 25 km tripolar grid, as nx_global and ny_global, which every component of the MC_25km configurations runs on."""
OM3_100KM_GRID: tuple[int, int] = (360, 324)
"""The 100 km tripolar grid of the MC_100km and MCW_100km configurations."""
OM3_8KM_GRID: tuple[int, int] = (4320, 3360)
"""The 8 km tripolar grid of the MC_8km configuration."""
OM3_PANAN4KM_GRID: tuple[int, int] = (4320, 1442)
"""The 4 km pan-Antarctic regional grid of the MC_panan4km configurations."""

OM3_25KM_CICE6: CICEPartitioning = CICEPartitioning(
    grid=OM3_25KM_GRID,
    distribution_type="roundrobin",
    processor_shape="square-ice",
    blocks_per_rank_range=OM3_CICE6_BLOCKS_PER_RANK_RANGE,
    nghost=OM3_CICE6_GHOST_WIDTH,
)
"""CICE6 as the 25 km releases configure it: square-ice blocks distributed roundrobin.

Neither forms a process grid - roundrobin maps blocks to ranks without one, and square-ice is not a shape a
one-dimensional domain could express anyway - so the search chooses how many ranks the sea ice gets and nothing
else. The block size follows from that rank count, so that each rank keeps holding several blocks as the count
changes, and is written into ice_in with max_blocks left for CICE6 to work out.
"""

OM3_MC_25KM: OM3Configuration = OM3Configuration(name="MC-25km", sea_ice=OM3_25KM_CICE6)
"""ACCESS-OM3 with MOM6 and CICE6 at 25 km, every component on the same tripolar grid.

Every ACCESS-OM3 configuration released so far is this one. The releases differ in their forcing, their
biogeochemistry and the core counts they were run with, none of which changes how the model is parallelised.

Note what the mask table costs this configuration. Its control, `OM3_MC_25KM_SOURCE`, pins MOM6's decomposition in
MOM_input with an explicit LAYOUT of 72 x 48 and a MASKTABLE naming a file of 1027 masked blocks, and ocn_ntasks is
the layout's ranks less the masked ones: 2429. This configuration leaves the ocean's decomposition to MOM6, so a
study has to remove LAYOUT, IO_LAYOUT and MASKTABLE and set AUTO_MASKTABLE through control_options before a
generated ocean core count means anything, and the released core counts are then the budget the release occupied
rather than a layout to reproduce.
"""
OM3_MC_25KM_CORES: dict[str, int] = {
    OM3_MEDIATOR_NAME: 275,
    OM3_ATMOSPHERE_NAME: 275,
    OM3_SEA_ICE_NAME: 275,
    OM3_RUNOFF_NAME: 275,
    OM3_OCEAN_NAME: 2429,
}
"""Cores each component receives in release-MC_25km_jra_ryf-2.0-beta, keyed by component name.

These build no layout, and are provided as the reference a caller writing an allocation strategy is usually
working from.
"""

OM3_100KM_CICE6: CICEPartitioning = CICEPartitioning(
    grid=OM3_100KM_GRID,
    blocks_per_rank_range=OM3_CICE6_BLOCKS_PER_RANK_RANGE,
    nghost=OM3_CICE6_GHOST_WIDTH,
)
"""CICE6 as dev-MC_100km_jra_ryf configures it: blocks distributed cartesian with one block row.

The scheme forms a process grid, so the block size chosen from the rank count is one whose blocks split evenly
over it.
"""
OM3_MC_100KM: OM3Configuration = OM3Configuration(name="MC-100km", sea_ice=OM3_100KM_CICE6)
"""ACCESS-OM3 with MOM6 and CICE6 at 100 km: the dev-MC_100km_jra_* configurations, as of November 2026.

They differ in their forcing and their biogeochemistry, none of which changes how the model is parallelised.
MOM_input sets AUTO_MASKTABLE, so a generated ocean core count runs as it is.
"""
OM3_MC_100KM_CORES: dict[str, int] = {
    OM3_MEDIATOR_NAME: 24,
    OM3_ATMOSPHERE_NAME: 24,
    OM3_SEA_ICE_NAME: 24,
    OM3_RUNOFF_NAME: 24,
    OM3_OCEAN_NAME: 288,
}
"""Cores each component receives in dev-MC_100km_jra_ryf, keyed by component name: 3 nodes of 104 cores."""

OM3_MCW_100KM_CICE6: CICEPartitioning = CICEPartitioning(
    grid=OM3_100KM_GRID,
    distribution_type="roundrobin",
    blocks_per_rank_range=OM3_CICE6_BLOCKS_PER_RANK_RANGE,
    nghost=OM3_CICE6_GHOST_WIDTH,
)
"""CICE6 as dev-MCW_100km_era_iaf configures it: blocks distributed roundrobin, in one block row."""
OM3_MCW_100KM: OM3Configuration = OM3Configuration(name="MCW-100km", sea_ice=OM3_MCW_100KM_CICE6, waves=True)
"""ACCESS-OM3 with MOM6, CICE6 and WW3 at 100 km: the dev-MCW_100km_era_iaf configuration, as of November 2026.

MOM_input sets AUTO_MASKTABLE, so a generated ocean core count runs as it is.
"""
OM3_MCW_100KM_CORES: dict[str, int] = {
    OM3_MEDIATOR_NAME: 56,
    OM3_ATMOSPHERE_NAME: 56,
    OM3_SEA_ICE_NAME: 56,
    OM3_RUNOFF_NAME: 56,
    OM3_OCEAN_NAME: 96,
    OM3_WAVE_NAME: 56,
}
"""Cores each component receives in dev-MCW_100km_era_iaf, keyed by component name: 2 nodes of 104 cores."""

OM3_8KM_CICE6: CICEPartitioning = CICEPartitioning(
    grid=OM3_8KM_GRID,
    distribution_type="roundrobin",
    processor_shape="square-ice",
    blocks_per_rank_range=OM3_CICE6_BLOCKS_PER_RANK_RANGE,
    nghost=OM3_CICE6_GHOST_WIDTH,
)
"""CICE6 as dev-MC_8km_jra_ryf configures it: square-ice blocks distributed roundrobin, as at 25 km."""
OM3_MC_8KM: OM3Configuration = OM3Configuration(name="MC-8km", sea_ice=OM3_8KM_CICE6)
"""ACCESS-OM3 with MOM6 and CICE6 at 8 km: the dev-MC_8km_jra_ryf configuration, as of November 2026.

Its control pins a LAYOUT of 108 x 96 and a MASKTABLE in MOM_input, which a study removes as for `OM3_MC_25KM`.
Choosing the sea ice's block size on this grid is slow: every candidate block size of a 4320 x 3360 grid is
weighed for each rank count the search visits.
"""
OM3_MC_8KM_CORES: dict[str, int] = {
    OM3_MEDIATOR_NAME: 1749,
    OM3_ATMOSPHERE_NAME: 1749,
    OM3_SEA_ICE_NAME: 1749,
    OM3_RUNOFF_NAME: 1749,
    OM3_OCEAN_NAME: 6675,
}
"""Cores each component receives in dev-MC_8km_jra_ryf, keyed by component name: 81 nodes of 104 cores."""

OM3_PANAN4KM_CICE6: CICEPartitioning = CICEPartitioning(
    grid=OM3_PANAN4KM_GRID,
    distribution_type="sectrobin",
    processor_shape="square-ice",
    blocks_per_rank_range=OM3_CICE6_BLOCKS_PER_RANK_RANGE,
    nghost=OM3_CICE6_GHOST_WIDTH,
)
"""CICE6 as the dev-MC_panan4km_jra_ryf configurations configure it: square-ice blocks distributed sectrobin."""
OM3_MC_PANAN4KM: OM3Configuration = OM3Configuration(name="MC-panan4km", sea_ice=OM3_PANAN4KM_CICE6)
"""The regional pan-Antarctic ACCESS-OM3 at 4 km: the dev-MC_panan4km_jra_ryf configuration, as of November 2026.

Its control pins a LAYOUT of 70 x 35 and a MASKTABLE in MOM_input, which a study removes as for `OM3_MC_25KM`.
"""
OM3_MC_PANAN4KM_CORES: dict[str, int] = {
    OM3_MEDIATOR_NAME: 256,
    OM3_ATMOSPHERE_NAME: 256,
    OM3_SEA_ICE_NAME: 256,
    OM3_RUNOFF_NAME: 256,
    OM3_OCEAN_NAME: 1408,
}
"""Cores each component receives in dev-MC_panan4km_jra_ryf, keyed by component name: 16 nodes of 104 cores."""
OM3_MC_PANAN4KM_ISF: OM3Configuration = replace(OM3_MC_PANAN4KM, name="MC-panan4km-isf")
"""The pan-Antarctic ACCESS-OM3 at 4 km with ice shelves: dev-MC_panan4km_jra_ryf+isf, as of November 2026.

The same grid and sea ice scheme as `OM3_MC_PANAN4KM`, so the same configuration under another name; its control
pins a LAYOUT of 84 x 49 and a MASKTABLE in MOM_input, which a study removes as for `OM3_MC_25KM`.
"""
OM3_MC_PANAN4KM_ISF_CORES: dict[str, int] = {
    OM3_MEDIATOR_NAME: 364,
    OM3_ATMOSPHERE_NAME: 364,
    OM3_SEA_ICE_NAME: 364,
    OM3_RUNOFF_NAME: 364,
    OM3_OCEAN_NAME: 2626,
}
"""Cores each component receives in dev-MC_panan4km_jra_ryf+isf, keyed by component name.

Its control asks for 2990 cores, which is not a whole number of 104-core nodes.
"""

OM3_CONFIGS_REPOSITORY: str = "git@github.com:ACCESS-NRI/access-om3-configs.git"
"""The repository holding the released ACCESS-OM3 configurations."""
OM3_MC_25KM_SOURCE: GitControlSource = GitControlSource(
    repository=OM3_CONFIGS_REPOSITORY,
    start_point="release-MC_25km_jra_ryf-2.0-beta",
)
"""The release `OM3_MC_25KM` describes, as a control to profile it against.

This is a convenience rather than part of the configuration: the other 25 km releases are the same configuration
against a different control, so a study profiling one passes its own and keeps `OM3_MC_25KM`.
"""
