# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""ACCESS-ESM1.6 configurations.

`access-esm1.6-configs` releases a dozen of these - piControl, historical, amip, esm-piControl, esm-historical
and the scenario runs - and they are not interchangeable for profiling purposes. Most share the N96 atmosphere
and the one-degree ocean, but `amip` runs the atmosphere alone, and the coupled ones are indistinguishable by
their layout: piControl-2.1 and esm-historical-1.3 have the same grids, the same components *and* the same
core counts, so a name derived from a layout is one string for two different experiments. Which configuration
is being profiled therefore has to be part of what identifies an experiment, and it is.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

from access.config import YAMLParser
from access.config.parallel_component import ComponentLayout, ParallelComponent
from access.config.parallel_constraints import (
    FixedThreadsPerRankConstraint,
    MaxWastedCoreFractionConstraint,
    ProcessGridDimEvenConstraint,
    SubdomainAspectRatioConstraint,
)
from access.config.parallel_domain import Domain

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.configuration import LogSpec, PayuConfiguration, log_at, payu_model_stdout, um_stdout
from access.profiling.control import GitControlSource
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.models.cice import CICEPartitioning
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

logger = logging.getLogger(__name__)

ESM16_UM7_NAME: str = "UM7"
ESM16_MOM5_NAME: str = "MOM5"
ESM16_CICE5_NAME: str = "CICE5"

# The N96 atmosphere grid and the one-degree tripolar ocean grid, which every released ACCESS-ESM1.6
# configuration that has the component in question runs it on.
ESM16_N96_ATMOSPHERE: Domain = Domain(shape=(192, 144))
ESM16_1DEG_OCEAN: Domain = Domain(shape=(360, 300))
# CICE5's grid is the ocean's. ACCESS-ESM1.6 builds CICE5 with runtime grid and block namelist entries and
# with nprocs stated there, distributes its blocks cartesian with one block row, and pins no block size, so
# the search chooses how the x extent is split and each rank receives one block spanning the full y extent.
ESM16_CICE5: CICEPartitioning = CICEPartitioning(
    grid=(360, 300),
    writes_nprocs=True,
    grid_is_runtime=True,
    namelist_path="ice/cice_in.nml",
)

# Kept for callers that wrote them out before CICEPartitioning held them.
ESM16_CICE5_NX_GLOBAL: int = ESM16_CICE5.grid[0]
ESM16_CICE5_NY_GLOBAL: int = ESM16_CICE5.grid[1]

# Cores each component receives in the released pre-industrial control configuration. These are not used to
# build any layout, and are provided as the reference a caller writing an allocation strategy is usually
# working from. Note that the other coupled releases run on the same counts, which is the whole difficulty.
ESM16_PI_CONTROL_CORES: dict[str, int] = {ESM16_UM7_NAME: 256, ESM16_MOM5_NAME: 240, ESM16_CICE5_NAME: 12}

# Ceilings on what counts as a reasonable ACCESS-ESM1.6 layout at all, rather than the tolerances of any
# particular study. Constraints are cumulative and a caller can only tighten them, so these are set loosely: a
# study that wants near-square subdomains or no waste at all says so in its own allocation strategy.
ESM16_MAX_SUBDOMAIN_ASPECT_RATIO: float = 4.0
ESM16_MAX_WASTED_CORE_FRACTION: float = 0.1

# The submodels the coupled releases declare in config.yaml, in the order they declare them. The experiment
# generator merges list edits positionally and then truncates to the length of the edit, so a three-element
# submodels change against this four-element list would silently delete the coupler: every slot has to be
# accounted for, and the one this package sets nothing in is marked PRESERVE. OASIS takes no cores and
# changes no layout, so it is a slot here rather than a component of the tree.
ESM16_COUPLED_SUBMODELS: tuple[tuple[str, str | None], ...] = (
    ("atmosphere", ESM16_UM7_NAME),
    ("ocean", ESM16_MOM5_NAME),
    ("ice", ESM16_CICE5_NAME),
    ("coupler", None),
)
# What release-amip-1.0 declares: the atmosphere alone, with no ocean, no sea ice and nothing to couple.
ESM16_AMIP_SUBMODELS: tuple[tuple[str, str | None], ...] = (("atmosphere", ESM16_UM7_NAME),)

# The marker the experiment generator reads as "leave this slot as the control has it".
_PRESERVE: str = "PRESERVE"


@dataclass(frozen=True)
class ESM16Configuration(PayuConfiguration):
    """One configuration of ACCESS-ESM1.6: which components it runs, and the grid each one runs on.

    Args:
        name (str): Identifies the configuration in the names of the experiments generated for it. Two
            released configurations can have the same grids, components and core counts, so this is what
            keeps their experiments apart.
        atmosphere (Domain | None): The UM's grid, or None if the configuration has no atmosphere. The search
            decomposes it over a two-dimensional process grid, which um_env.yaml states.
        ocean (Domain | None): MOM5's grid, or None if the configuration has no ocean, as AMIP does not. The
            search decomposes it over a two-dimensional process grid, which input.nml states.
        sea_ice (CICEPartitioning | None): How CICE5 divides its grid, or None if the configuration has no
            sea ice. What the search may choose about it follows from the partitioning rather than being
            assumed here.
        submodels (tuple[tuple[str, str | None], ...]): The Payu submodels the control declares, in order,
            each paired with the component it is or with None where it is one this package sets nothing in.
            Both the order and the length matter: they are what a layout's core counts are merged against.
        model_type (str): The model type identifier, as Payu defines it.
        experiment_prefix (str): Prefix for the names of experiments generated for this configuration.
        sea_ice_executable (str | None): The executable the sea ice submodel is to run, where the
            configuration names one. None leaves the control's own.

    Raises:
        ValueError: If the configuration has no component at all, if a grid that is decomposed over a process
            grid is not two-dimensional, or if the submodels name a component the configuration does not run.
    """

    _name: str
    atmosphere: Domain | None = None
    ocean: Domain | None = None
    sea_ice: CICEPartitioning | None = None
    submodels: tuple[tuple[str, str | None], ...] = ESM16_COUPLED_SUBMODELS
    _model_type: str = "access-esm1.6"
    _experiment_prefix: str = "esm1p6-layout"
    sea_ice_executable: str | None = "cice_access.exe"

    def __post_init__(self) -> None:
        if self.atmosphere is None and self.ocean is None and self.sea_ice is None:
            raise ValueError(
                f"ACCESS-ESM1.6 configuration {self._name!r} runs no component. There is nothing to profile."
            )
        for component, domain in ((ESM16_UM7_NAME, self.atmosphere), (ESM16_MOM5_NAME, self.ocean)):
            # Checked here rather than where the grid is decomposed, so a configuration that cannot be
            # written out is refused when it is built instead of once a layout has been found for it.
            if domain is not None and domain.ndim != 2:
                raise ValueError(
                    f"ACCESS-ESM1.6 configuration {self._name!r} gives {component!r} a {domain.ndim}-"
                    f"dimensional grid {domain.shape}. Its decomposition is written as a two-dimensional "
                    "process grid, so the grid it decomposes has to be one too."
                )
        running = set(self._components)
        declared = {component for _, component in self.submodels if component is not None}
        if not declared <= running:
            raise ValueError(
                f"ACCESS-ESM1.6 configuration {self._name!r} declares submodels for {sorted(declared - running)}, "
                f"which it does not run. It runs {sorted(running)}."
            )

    @property
    def name(self) -> str:
        return self._name

    @property
    def model_type(self) -> str:
        return self._model_type

    @property
    def experiment_prefix(self) -> str:
        return self._experiment_prefix

    @property
    def _components(self) -> tuple[str, ...]:
        """Returns the components this configuration runs, in the order the component tree declares them."""
        present = ((ESM16_UM7_NAME, self.atmosphere), (ESM16_MOM5_NAME, self.ocean), (ESM16_CICE5_NAME, self.sea_ice))
        return tuple(name for name, component in present if component is not None)

    @property
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how this configuration is parallelised.

        The components run side by side, each on a range of cores of its own, so they are siblings and their
        cores add up. Only the ones the configuration runs are there: AMIP's tree is the atmosphere alone.

        The tree carries only the requirements that hold for every layout of this configuration, whatever is
        being studied. Constraints are cumulative and cannot be relaxed by a caller, so anything that is a
        choice rather than a requirement belongs in the allocation strategy instead.

        Returns:
            ParallelComponent: Root of the component tree.
        """
        # ACCESS-ESM1.6 is built without OpenMP support, so every component runs one thread per rank.
        no_threads = (FixedThreadsPerRankConstraint(n_threads=1),)
        subcomponents = []
        if self.atmosphere is not None:
            subcomponents.append(
                ParallelComponent(
                    name=ESM16_UM7_NAME,
                    domain=self.atmosphere,
                    local_constraints=no_threads
                    + (
                        ProcessGridDimEvenConstraint(dim=0),  # The UM requires an even number of processes along x.
                        SubdomainAspectRatioConstraint(max_ratio=ESM16_MAX_SUBDOMAIN_ASPECT_RATIO),
                    ),
                )
            )
        if self.ocean is not None:
            subcomponents.append(
                ParallelComponent(
                    name=ESM16_MOM5_NAME,
                    domain=self.ocean,
                    local_constraints=no_threads
                    + (SubdomainAspectRatioConstraint(max_ratio=ESM16_MAX_SUBDOMAIN_ASPECT_RATIO),),
                )
            )
        if self.sea_ice is not None:
            subcomponents.append(
                ParallelComponent(
                    name=ESM16_CICE5_NAME,
                    domain=self.sea_ice.domain,
                    local_constraints=no_threads + self.sea_ice.local_constraints,
                )
            )
        return ParallelComponent(
            name=f"ACCESS-ESM1.6 {self._name}",
            subcomponents=tuple(subcomponents),
            local_constraints=(MaxWastedCoreFractionConstraint(max_fraction=ESM16_MAX_WASTED_CORE_FRACTION),),
        )

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        """Returns the profiling logs this configuration writes, one per component that writes one.

        The UM's is read twice, once for its regions and once for its total run time, so two logs name the
        same component.
        """
        specs: list[LogSpec] = []
        if self.atmosphere is not None:
            specs.append(LogSpec("UM", um_stdout(), UMProfilingParser(), component=ESM16_UM7_NAME))
            specs.append(LogSpec("UM_Total_Walltime", um_stdout(), UMTotalRuntimeParser(), component=ESM16_UM7_NAME))
        if self.ocean is not None:
            specs.append(
                LogSpec("MOM5", payu_model_stdout(), FMSProfilingParser(has_hits=False), component=ESM16_MOM5_NAME)
            )
        if self.sea_ice is not None:
            specs.append(LogSpec("CICE5", log_at("ice/ice_diag.d"), CICE5ProfilingParser(), component=ESM16_CICE5_NAME))
        return tuple(specs)

    def _component_layouts(self, layout: ComponentLayout) -> dict[str, ComponentLayout]:
        """Returns what each component was given, keyed by component.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            dict[str, ComponentLayout]: The layout of each component this configuration runs.

        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        components = self._components
        if len(layout.sub_layouts) != len(components):
            raise ValueError(
                f"The layout {layout.name!r} has {len(layout.sub_layouts)} component(s), but ACCESS-ESM1.6 "
                f"configuration {self._name!r} runs {len(components)}: {list(components)}."
            )
        # ComponentLayout.sub_layouts is documented to come in the order of ParallelComponent.subcomponents,
        # which is the order _components states, so zipping is the inverse of how the tree was built.
        return dict(zip(components, layout.sub_layouts, strict=True))

    def experiment_name(self, layout: ComponentLayout) -> str:
        """Returns the name of the experiment holding a given layout.

        The name records the configuration and then what each component received: its process grid where it
        decomposes one, and its cores otherwise. It is therefore distinct for every distinct layout, and the
        same layout always produces the same name, which is what lets a manager tell whether it already has
        an experiment for a layout before building one. The configuration is part of it because the layout is
        not enough: two released configurations run the same components, on the same grids, on the same
        number of cores.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            str: The experiment name.

        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        tokens = {ESM16_UM7_NAME: "atm", ESM16_MOM5_NAME: "mom", ESM16_CICE5_NAME: "ice"}
        parts = []
        for component, sub_layout in self._component_layouts(layout).items():
            if sub_layout.decomposition is None:
                parts.append(f"{tokens[component]}_{sub_layout.n_ranks}")
            else:
                parts.append(f"{tokens[component]}_" + "x".join(str(ranks) for ranks in sub_layout.decomposition.grid))
        return f"{self._experiment_prefix}_{self._name}_{'_'.join(parts)}"

    def config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run this configuration with a given layout.

        Every component's cores go into config.yaml's submodels, in the order the control declares them and
        with a slot for every one of them, and each component that decomposes a grid additionally has the
        decomposition the search chose written into its own configuration file.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control
                directory.

        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        layouts = self._component_layouts(layout)
        changes: dict = {"config.yaml": {"submodels": [self._submodel_changes(layouts)]}}

        atmosphere = layouts.get(ESM16_UM7_NAME)
        if atmosphere is not None:
            nx, ny = atmosphere.decomposition.grid
            changes["atmosphere/um_env.yaml"] = {
                "UM_ATM_NPROCX": str(nx),
                "UM_ATM_NPROCY": str(ny),
                "UM_NPES": str(atmosphere.n_ranks),
            }

        ocean = layouts.get(ESM16_MOM5_NAME)
        if ocean is not None:
            nx, ny = ocean.decomposition.grid
            changes["ocean/input.nml"] = {"ocean_model_nml": {"layout": [f"{nx},{ny}"]}}

        sea_ice = layouts.get(ESM16_CICE5_NAME)
        if sea_ice is not None:
            changes[self.sea_ice.namelist_path] = {
                self.sea_ice.namelist_group: self.sea_ice.namelist_changes(sea_ice.n_ranks, sea_ice.decomposition)
            }
        return changes

    def _submodel_changes(self, layouts: dict[str, ComponentLayout]) -> list:
        """Returns the submodels list a layout comes to, one slot per submodel the control declares.

        A slot this configuration sets nothing in is marked PRESERVE rather than left out: the experiment
        generator merges list edits positionally and truncates to the length of the edit, so a shorter list
        would silently delete whatever the control declared after it - the OASIS coupler, for one.

        Args:
            layouts (dict[str, ComponentLayout]): What each component was given.

        Returns:
            list: The submodels list, ready to merge against the control's own.
        """
        slots: list = []
        for _, component in self.submodels:
            sub_layout = layouts.get(component) if component is not None else None
            if sub_layout is None:
                slots.append(_PRESERVE)
                continue
            slot = {"ncpus": sub_layout.n_cores}
            if component == ESM16_CICE5_NAME and self.sea_ice_executable is not None:
                slot["exe"] = [self.sea_ice_executable]
            slots.append(slot)
        return slots

    def parse_layout(self, output_dir: Path) -> ComponentLayout | None:
        """Returns the layout an experiment ran, from the Payu submodels its config.yaml declares.

        Payu gives each submodel its cores outright, so the configuration states this directly. What it does
        not state is how a component divides its domain, so the layout that comes back carries no
        decompositions.

        A submodel this configuration does not declare is left out rather than guessed at, and a file naming
        none of them produces nothing at all.

        Args:
            output_dir (Path): Directory holding the configuration files. Must contain a config.yaml.

        Returns:
            ComponentLayout | None: The layout, or None if config.yaml names no component of this
                configuration.
        """
        config_path = output_dir / "config.yaml"
        payu_config = YAMLParser().parse(config_path.read_text())
        components = {name: component for name, component in self.submodels if component is not None}

        sub_layouts = []
        for submodel in payu_config.get("submodels", []):
            component = components.get(submodel.get("name"))
            if component is None:
                logger.debug(f"Submodel {submodel.get('name')!r} of {config_path} is not a component of this model.")
                continue
            n_cores = submodel.get("ncpus")
            if not n_cores:
                logger.debug(f"Submodel {submodel.get('name')!r} of {config_path} states no core count.")
                continue
            # One thread per rank throughout this model, as the tree requires, so ranks are cores.
            sub_layouts.append(
                ComponentLayout(
                    name=component, n_cores=n_cores, n_ranks=n_cores, threads_per_rank=1, decomposition=None
                )
            )

        if not sub_layouts:
            logger.debug(f"No component of this model is named among the submodels of {config_path}.")
            return None

        # The components run side by side, so the root spends what they spend between them. Any cores left
        # idle to fill a node are not the layout's; the manager's parse_ncpus is what reports those.
        used = sum(sub_layout.n_cores for sub_layout in sub_layouts)
        return ComponentLayout(
            name=f"ACCESS-ESM1.6 {self._name}",
            n_cores=used,
            n_ranks=used,
            threads_per_rank=None,
            decomposition=None,
            sub_layouts=tuple(sub_layouts),
        )


# The one ACCESS-ESM1.6 configuration this package ships. The other released configurations - historical,
# amip, esm-piControl, the scenario runs - are reached by replacing what differs, which for the coupled ones
# is the name alone:
#
#     replace(ESM16_PI_CONTROL, _name="historical")
#
# paired with a control of their own:
#
#     GitControlSource(ESM16_CONFIGS_REPOSITORY, "release-historical-1.1")
#
# Shipping one rather than a dozen is deliberate: a preset per config-repo release would tie a release of this
# package to every release of that one.
ESM16_PI_CONTROL: ESM16Configuration = ESM16Configuration(
    _name="piControl",
    atmosphere=ESM16_N96_ATMOSPHERE,
    ocean=ESM16_1DEG_OCEAN,
    sea_ice=ESM16_CICE5,
)

# The release that configuration describes, which is a convenience rather than part of it: a study profiling
# a fork, or a later release, passes its own control to the manager and keeps this configuration. Kept beside
# the preset rather than on it so that choosing a different release does not mean building a different
# configuration.
ESM16_CONFIGS_REPOSITORY: str = "git@github.com:ACCESS-NRI/access-esm1.6-configs.git"
ESM16_PI_CONTROL_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-piControl-2.1",
)
