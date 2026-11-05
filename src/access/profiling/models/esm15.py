# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""ACCESS-ESM1.5 configurations.

`access-esm1.5-configs` releases two of these, `preindustrial+concentrations` and `historical+concentrations`, with
the same grids, the same components and the same core counts, so the configuration is part of what names an
experiment, as for ACCESS-ESM1.6.

What sets ACCESS-ESM1.5 apart, for profiling, is its sea ice. It runs CICE4, whose grid, blocks and rank count are
compiled into its executable - `cice_access_360x300_12x1_12p.exe` runs on 12 ranks and nothing else - so the search
can choose nothing about it and nothing is written for it: it is a plain component pinned to the rank count its
executable is built for. The UM and MOM5 are the components ACCESS-ESM1.6 runs, on the same grids, and the coupler
is the slot OASIS takes.
"""

from dataclasses import dataclass, replace

from access.config.parallel_domain import Domain

from access.profiling.application import LogSpec, log_at
from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.control import GitControlSource
from access.profiling.models.components import MOM5Component, UMComponent
from access.profiling.payu_configuration import PayuSubmodel, PlainComponent, SubmodelsConfiguration

ESM15_UM7_NAME: str = UMComponent.name
"""Component name of the atmosphere, UM7."""
ESM15_MOM5_NAME: str = MOM5Component.name
"""Component name of the ocean, MOM5."""
ESM15_CICE4_NAME: str = "CICE4"
"""Component name of the sea ice, CICE4."""

ESM15_N96_ATMOSPHERE: Domain = Domain(shape=(192, 144))
"""The N96 atmosphere grid, which both released ACCESS-ESM1.5 configurations run the UM on."""
ESM15_1DEG_OCEAN: Domain = Domain(shape=(360, 300))
"""The one-degree tripolar ocean grid, which both released ACCESS-ESM1.5 configurations run MOM5 on."""
ESM15_CICE4_RANKS: int = 12
"""The ranks the released CICE4 executable is built for.

`cice_access_360x300_12x1_12p.exe` divides the ocean's grid into 12 blocks of 30 x 300, one per rank, and runs on
no other number of ranks.
"""

ESM15_PREINDUSTRIAL_CORES: dict[str, int] = {ESM15_UM7_NAME: 240, ESM15_MOM5_NAME: 180, ESM15_CICE4_NAME: 12}
"""Cores each component receives in the released pre-industrial configuration, keyed by component name.

These are not used to build any layout, and are provided as the reference a caller writing an allocation strategy
is usually working from. The historical release runs on the same counts. The release was run on 48-core nodes, 9
of them exactly; on the 104-core nodes of Gadi's Sapphire Rapids queue the same counts leave 88 of 520 idle.
"""


@dataclass(frozen=True, kw_only=True)
class ESM15Configuration(SubmodelsConfiguration):
    """One configuration of ACCESS-ESM1.5: which components it runs, and the grid each one runs on.

    Args:
        name (str): Identifies the configuration in the names of the experiments generated for it. The two
            released configurations have the same grids, components and core counts, so this is what keeps
            their experiments apart.
        atmosphere (Domain | None): The UM's grid, or None if the configuration has no atmosphere. The search
            decomposes it over a two-dimensional process grid, which um_env.yaml states.
        ocean (Domain | None): MOM5's grid, or None if the configuration has no ocean. The search decomposes it
            over a two-dimensional process grid, which input.nml states.
        sea_ice (int | None): The number of ranks CICE4's executable is built for, or None if the configuration
            has no sea ice. The search gives CICE4 that many ranks and chooses nothing else about it, and nothing
            is written for it.
        model_type (str): The model type identifier, as Payu defines it: `access`, the name Payu gives ACCESS-ESM1.5.
        model_name (str): The model's name, which names the root of the component tree with `name`.
        experiment_prefix (str): Prefix for the names of experiments generated for this configuration.

    Raises:
        ValueError: If the name is empty, if the configuration has no component at all, if a grid is not
            two-dimensional, or if the sea ice is built for no ranks.
    """

    name: str
    atmosphere: Domain | None = None
    ocean: Domain | None = None
    sea_ice: int | None = None
    model_type: str = "access"
    model_name: str = "ACCESS-ESM1.5"
    experiment_prefix: str = "esm1p5-layout"

    @property
    def submodels(self) -> tuple[PayuSubmodel, ...]:
        """Returns the submodels the control declares, in its order, each with the component that runs in it.

        The order is the one both ACCESS-ESM1.5 controls declare: atmosphere, ocean, ice, coupler. OASIS takes
        no cores and changes no layout, so it is a slot marked PRESERVE rather than a component, and it is there
        whenever there is something to couple, that is, more than one component.
        """
        submodels = []
        if self.atmosphere is not None:
            submodels.append(PayuSubmodel("atmosphere", "um", UMComponent(self.atmosphere)))
        if self.ocean is not None:
            submodels.append(PayuSubmodel("ocean", "mom", MOM5Component(self.ocean)))
        if self.sea_ice is not None:
            cice4 = PlainComponent(
                name=ESM15_CICE4_NAME,
                token="ice",
                n_ranks=self.sea_ice,
                logs=(LogSpec("CICE4", log_at("ice/ice_diag.d"), CICE5ProfilingParser(), component=ESM15_CICE4_NAME),),
            )
            submodels.append(PayuSubmodel("ice", "cice", cice4))
        if len(submodels) > 1:
            submodels.append(PayuSubmodel("coupler", "oasis"))
        return tuple(submodels)


ESM15_PREINDUSTRIAL: ESM15Configuration = ESM15Configuration(
    name="preindustrial+concentrations",
    atmosphere=ESM15_N96_ATMOSPHERE,
    ocean=ESM15_1DEG_OCEAN,
    sea_ice=ESM15_CICE4_RANKS,
)
"""The pre-industrial configuration, with concentration-driven greenhouse gases.

CICE4 prints its timers as CICE5 does, so its log is read with the CICE5 parser.
"""
ESM15_HISTORICAL: ESM15Configuration = replace(ESM15_PREINDUSTRIAL, name="historical+concentrations")
"""The historical configuration, with concentration-driven greenhouse gases: the same layout under another name."""

ESM15_CONFIGS_REPOSITORY: str = "git@github.com:ACCESS-NRI/access-esm1.5-configs.git"
"""The repository holding the released ACCESS-ESM1.5 configurations."""
ESM15_PREINDUSTRIAL_SOURCE: GitControlSource = GitControlSource(
    repository=ESM15_CONFIGS_REPOSITORY,
    start_point="release-preindustrial+concentrations-2.0",
)
"""The release `ESM15_PREINDUSTRIAL` describes, as a control to profile it against."""
ESM15_HISTORICAL_SOURCE: GitControlSource = GitControlSource(
    repository=ESM15_CONFIGS_REPOSITORY,
    start_point="release-historical+concentrations-2.0",
)
"""The release `ESM15_HISTORICAL` describes, as a control to profile it against."""
