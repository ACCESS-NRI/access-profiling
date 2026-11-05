# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""ACCESS-ESM1.6 configurations.

`access-esm1.6-configs` releases eighteen of these - piControl, historical, amip, esm-piControl, esm-historical,
the idealised CO2 runs and the scenario runs - and they are not interchangeable for profiling purposes. Most share
the N96 atmosphere and the one-degree ocean, but `amip` runs the atmosphere alone, and the coupled ones are
indistinguishable by their layout: piControl-2.1 and esm-historical-1.3 have the same grids, the same components
*and* the same core counts, so a name derived from a layout is one string for two different experiments. Which
configuration is being profiled therefore has to be part of what identifies an experiment, and it is.

A configuration is put together from the components of `access.profiling.models.components`, the UM, MOM5 and
CICE5, each the Payu submodel the control declares it as, with OASIS as a slot this package sets nothing in.
"""

from dataclasses import dataclass, replace
from pathlib import Path

from access.config.parallel_domain import Domain

from access.profiling.control import GitControlSource
from access.profiling.models.cice import CICEPartitioning
from access.profiling.models.components import CICEComponent, MOM5Component, UMComponent
from access.profiling.payu_configuration import PayuSubmodel, SubmodelsConfiguration, control_name, read_submodels

ESM16_UM7_NAME: str = UMComponent.name
"""Component name of the atmosphere, UM7."""
ESM16_MOM5_NAME: str = MOM5Component.name
"""Component name of the ocean, MOM5."""
ESM16_CICE5_NAME: str = CICEComponent.name
"""Component name of the sea ice, CICE5."""

ESM16_N96_ATMOSPHERE: Domain = Domain(shape=(192, 144))
"""The N96 atmosphere grid, which every released ACCESS-ESM1.6 configuration with an atmosphere runs it on."""
ESM16_1DEG_OCEAN: Domain = Domain(shape=(360, 300))
"""The one-degree tripolar ocean grid, which every released ACCESS-ESM1.6 configuration with an ocean runs it on."""
ESM16_CICE5: CICEPartitioning = CICEPartitioning(
    grid=(360, 300),
    writes_nprocs=True,
    grid_is_runtime=True,
    namelist_path="ice/cice_in.nml",
)
"""How ACCESS-ESM1.6's CICE5 divides its grid.

CICE5's grid is the ocean's. ACCESS-ESM1.6 builds CICE5 with runtime grid and block namelist entries and with
nprocs stated there, distributes its blocks cartesian with one block row, and pins no block size, so the search
chooses how the x extent is split and each rank receives one block spanning the full y extent.
"""

ESM16_PI_CONTROL_CORES: dict[str, int] = {ESM16_UM7_NAME: 256, ESM16_MOM5_NAME: 240, ESM16_CICE5_NAME: 12}
"""Cores each component receives in the released pre-industrial control configuration, keyed by component name.

These are not used to build any layout, and are provided as the reference a caller writing an allocation strategy
is usually working from. Note that the other coupled releases run on the same counts, which is the whole
difficulty.
"""


@dataclass(frozen=True, kw_only=True)
class ESM16Configuration(SubmodelsConfiguration):
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
        sea_ice_executable (str | None): The executable the sea ice submodel is to run, where the
            configuration names one. None leaves the control's own.
        model_type (str): The model type identifier, as Payu defines it.
        model_name (str): The model's name, which names the root of the component tree with `name`.
        experiment_prefix (str): Prefix for the names of experiments generated for this configuration.

    Raises:
        ValueError: If the name is empty, if the configuration has no component at all, or if a grid that is
            decomposed over a process grid is not two-dimensional.
    """

    name: str
    atmosphere: Domain | None = None
    ocean: Domain | None = None
    sea_ice: CICEPartitioning | None = None
    sea_ice_executable: str | None = "cice_access.exe"
    model_type: str = "access-esm1.6"
    model_name: str = "ACCESS-ESM1.6"
    experiment_prefix: str = "esm1p6-layout"

    @property
    def submodels(self) -> tuple[PayuSubmodel, ...]:
        """Returns the submodels the control declares, in its order, each with the component that runs in it.

        The order is the one every ACCESS-ESM1.6 control declares: atmosphere, ocean, ice, coupler. OASIS takes
        no cores and changes no layout, so it is a slot marked PRESERVE rather than a component, and it is there
        whenever there is something to couple, that is, more than one component.
        """
        submodels = []
        if self.atmosphere is not None:
            submodels.append(PayuSubmodel("atmosphere", "um", UMComponent(self.atmosphere)))
        if self.ocean is not None:
            submodels.append(PayuSubmodel("ocean", "mom", MOM5Component(self.ocean)))
        if self.sea_ice is not None:
            submodels.append(PayuSubmodel("ice", "cice5", CICEComponent(self.sea_ice), exe=self.sea_ice_executable))
        if len(submodels) > 1:
            submodels.append(PayuSubmodel("coupler", "oasis"))
        return tuple(submodels)

    @classmethod
    def from_control(cls, directory: Path, name: str | None = None) -> "ESM16Configuration":
        """Returns the configuration a control directory holds, read from its own files.

        The components are the submodels `config.yaml` declares, by their Payu model types: `um` is the
        atmosphere on the N96 grid, `mom` the ocean on the one-degree grid, `cice5` the sea ice, whose
        distribution and processor shape come from `ice/cice_in.nml`, and `oasis` the coupler. The grids are the
        model's, since no ACCESS-ESM1.6 configuration runs others. The sea ice is read for the runtime-grid build
        the configuration writes for, whether or not the control's executable is one: the releases name a CICE5
        built for their grid, which `sea_ice_executable` replaces.

        Args:
            directory (Path): The control directory, as `clone_control` returns it or as an
                `ExistingDirectoryControlSource` names it.
            name (str | None): The configuration's name. Defaults to the `jobname` of `config.yaml`.

        Returns:
            ESM16Configuration: The configuration.

        Raises:
            ValueError: If the control is not an ACCESS-ESM1.6 one: no `config.yaml`, a submodel of a type the
                model does not run, or a `cice` sea ice, which is CICE4 and ACCESS-ESM1.5's.
        """
        types = {submodel.model for submodel in read_submodels(directory)}
        if "cice" in types:
            raise ValueError(
                f"The control at {directory} runs CICE4 (`model: cice`), which is ACCESS-ESM1.5's sea ice: read it "
                "with ESM15Configuration.from_control."
            )
        unknown = types - {"um", "mom", "cice5", "oasis"}
        if unknown:
            raise ValueError(
                f"The control at {directory} declares submodels of types {sorted(unknown)}, which ACCESS-ESM1.6 "
                "does not run."
            )
        sea_ice = None
        if "cice5" in types:
            partitioning = CICEPartitioning.from_control(directory, "ice/cice_in.nml", grid=(360, 300))
            sea_ice = replace(partitioning, grid_is_runtime=True)
        return cls(
            name=control_name(directory, name),
            atmosphere=ESM16_N96_ATMOSPHERE if "um" in types else None,
            ocean=ESM16_1DEG_OCEAN if "mom" in types else None,
            sea_ice=sea_ice,
        )


ESM16_PI_CONTROL: ESM16Configuration = ESM16Configuration(
    name="piControl",
    atmosphere=ESM16_N96_ATMOSPHERE,
    ocean=ESM16_1DEG_OCEAN,
    sea_ice=ESM16_CICE5,
)
"""The pre-industrial control: the coupled configuration, which most of ACCESS-ESM1.6's releases share.

The other coupled configurations - historical, esm-piControl, the scenario runs - have the same grids, the same
components and the same core counts, and are reached by replacing the name alone:

```python
replace(ESM16_PI_CONTROL, name="historical")
```

paired with their own control, such as `ESM16_HISTORICAL_SOURCE`: the package ships one per released
configuration, at its latest release. `ESM16_AMIP` is the one released configuration with other components.
"""

ESM16_AMIP: ESM16Configuration = ESM16Configuration(name="amip", atmosphere=ESM16_N96_ATMOSPHERE)
"""The AMIP configuration: the UM on the N96 grid alone, with no ocean, no sea ice and nothing to couple.

`release-amip-1.0` runs it on 208 cores, as a 16 x 13 process grid.
"""

ESM16_CONFIGS_REPOSITORY: str = "git@github.com:ACCESS-NRI/access-esm1.6-configs.git"
"""The repository holding the released ACCESS-ESM1.6 configurations."""
ESM16_PI_CONTROL_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-piControl-2.1",
)
"""The latest release of `piControl`, the pre-industrial control, which `ESM16_PI_CONTROL` describes. A control is a
convenience rather than part of the configuration: a study profiling a fork, or another release, passes its own
control to the manager and keeps the preset. Kept beside the presets rather than on them so that choosing a different
release does not mean building a different configuration.
"""
ESM16_HISTORICAL_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-historical-1.3",
)
"""The latest release of `historical`, the historical configuration, which `ESM16_PI_CONTROL` under the name
`historical` describes.
"""
ESM16_AMIP_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-amip-1.0",
)
"""The latest release of `amip`, the AMIP configuration, which `ESM16_AMIP` describes."""
ESM16_ESM_PICONTROL_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-esm-piControl-2.1",
)
"""The latest release of `esm-piControl`, the emissions-driven pre-industrial control, which `ESM16_PI_CONTROL` under
the name `esm-piControl` describes.
"""
ESM16_ESM_HISTORICAL_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-esm-historical-1.3",
)
"""The latest release of `esm-historical`, the emissions-driven historical configuration, which `ESM16_PI_CONTROL` under
the name `esm-historical` describes.
"""
ESM16_1PCTCO2_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-1pctCO2-1.0",
)
"""The latest release of `1pctCO2`, the 1% per year CO2 increase, which `ESM16_PI_CONTROL` under the name `1pctCO2`
describes.
"""
ESM16_1PCTCO2_BGC_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-1pctCO2-bgc-1.0",
)
"""The latest release of `1pctCO2-bgc`, the 1% per year CO2 increase felt by the biogeochemistry alone, which
`ESM16_PI_CONTROL` under the name `1pctCO2-bgc` describes.
"""
ESM16_1PCTCO2_RAD_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-1pctCO2-rad-1.0",
)
"""The latest release of `1pctCO2-rad`, the 1% per year CO2 increase felt by the radiation alone, which
`ESM16_PI_CONTROL` under the name `1pctCO2-rad` describes.
"""
ESM16_ABRUPT_0P5XCO2_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-abrupt-0p5xCO2-2.0",
)
"""The latest release of `abrupt-0p5xCO2`, the abrupt halving of CO2, which `ESM16_PI_CONTROL` under the name
`abrupt-0p5xCO2` describes.
"""
ESM16_ABRUPT_2XCO2_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-abrupt-2xCO2-2.0",
)
"""The latest release of `abrupt-2xCO2`, the abrupt doubling of CO2, which `ESM16_PI_CONTROL` under the name
`abrupt-2xCO2` describes.
"""
ESM16_ABRUPT_4XCO2_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-abrupt-4xCO2-1.0",
)
"""The latest release of `abrupt-4xCO2`, the abrupt quadrupling of CO2, which `ESM16_PI_CONTROL` under the name
`abrupt-4xCO2` describes.
"""
ESM16_ESM_FLAT10_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-esm-flat10-1.0",
)
"""The latest release of `esm-flat10`, the flat 10 GtC per year emissions, which `ESM16_PI_CONTROL` under the name `esm-
flat10` describes.
"""
ESM16_ESM_FLAT10_CDR_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-esm-flat10-cdr-1.0",
)
"""The latest release of `esm-flat10-cdr`, the flat 10 GtC per year emissions followed by carbon dioxide removal, which
`ESM16_PI_CONTROL` under the name `esm-flat10-cdr` describes.
"""
ESM16_ESM_FLAT10_ZEC_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-esm-flat10-zec-1.0",
)
"""The latest release of `esm-flat10-zec`, the flat 10 GtC per year emissions followed by zero emissions, which
`ESM16_PI_CONTROL` under the name `esm-flat10-zec` describes.
"""
ESM16_SCEN7_H_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-scen7-h-1.0",
)
"""The latest release of `scen7-h`, the high ScenarioMIP scenario, which `ESM16_PI_CONTROL` under the name `scen7-h`
describes.
"""
ESM16_SCEN7_VL_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-scen7-vl-1.0",
)
"""The latest release of `scen7-vl`, the very low ScenarioMIP scenario, which `ESM16_PI_CONTROL` under the name
`scen7-vl` describes.
"""
ESM16_ESM_SCEN7_H_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-esm-scen7-h-1.0",
)
"""The latest release of `esm-scen7-h`, the emissions-driven high ScenarioMIP scenario, which `ESM16_PI_CONTROL` under
the name `esm-scen7-h` describes.
"""
ESM16_ESM_SCEN7_VL_SOURCE: GitControlSource = GitControlSource(
    repository=ESM16_CONFIGS_REPOSITORY,
    start_point="release-esm-scen7-vl-1.0",
)
"""The latest release of `esm-scen7-vl`, the emissions-driven very low ScenarioMIP scenario, which `ESM16_PI_CONTROL`
under the name `esm-scen7-vl` describes.
"""
