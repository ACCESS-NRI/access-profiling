# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""ACCESS-OM2 configurations.

`access-om2-configs` releases eighteen of these: three resolutions, `1deg`, `025deg` and `01deg`, each forced by
JRA55 as a repeat year or interannually, and each with or without biogeochemistry. The forcing and the
biogeochemistry change the core counts a release runs with, and not how the model is parallelised, so there is one
configuration per resolution, and its name is what keeps the experiments of one release apart from another's.

ACCESS-OM2 couples MOM5 and CICE5 through OASIS3-MCT, built into both as libaccessom2, with YATM, the data
atmosphere, which is serial. The submodels are the three of them, in the order atmosphere, ocean, ice, and there
is no coupler submodel. MOM5 runs on a masked layout: the releases pin a process grid in `ocean/input.nml` and list
a mask table of the land-only ranks among the ocean's inputs, so the ocean's cores are the grid's product less the
masked ranks. The search writes an unmasked process grid, so a study takes the mask table out of the inputs, or
sets the `mask` option of the ocean submodel with a reference table for every layout, before a generated ocean core
count runs. CICE5 is the runtime-grid build of `access-om2/2026.08.002` and later, `cice_auscom.exe`, which reads
its grid and its blocks from `ice/cice_in.nml`; the `-4.1` and `_bgc` releases run executables compiled for their
layout, and a study of one of them loads that module through `control_options`.
"""

from dataclasses import dataclass

from access.config.parallel_domain import Domain

from access.profiling.models.cice import CICEPartitioning
from access.profiling.models.components import CICEComponent, MOM5Component
from access.profiling.payu_configuration import PayuSubmodel, PlainComponent, SubmodelsConfiguration

OM2_YATM_NAME: str = "YATM"
"""Component name of the data atmosphere, YATM."""
OM2_MOM5_NAME: str = MOM5Component.name
"""Component name of the ocean, MOM5."""
OM2_CICE5_NAME: str = CICEComponent.name
"""Component name of the sea ice, CICE5."""

OM2_1DEG_GRID: Domain = Domain(shape=(360, 300))
"""The one-degree tripolar grid, which the ocean and the sea ice of the 1deg configurations run on."""
OM2_025DEG_GRID: Domain = Domain(shape=(1440, 1080))
"""The quarter-degree tripolar grid of the 025deg configurations."""
OM2_01DEG_GRID: Domain = Domain(shape=(3600, 2700))
"""The tenth-degree tripolar grid of the 01deg configurations."""

OM2_1DEG_CICE5: CICEPartitioning = CICEPartitioning(
    grid=(360, 300),
    block_shape=(15, 300),
    writes_nprocs=True,
    grid_is_runtime=True,
    namelist_path="ice/cice_in.nml",
)
"""How the 1deg configurations' CICE5 divides its grid: 24 blocks of 15 x 300, distributed cartesian in one row.

The block size is the control's, so the search chooses how the 24 block columns are shared out, and a rank count
has to divide 24. The runtime-grid build reads the grid, the blocks and nprocs from the namelist.
"""
OM2_025DEG_CICE5: CICEPartitioning = CICEPartitioning(
    grid=(1440, 1080),
    distribution_type="roundrobin",
    processor_shape="square-ice",
    block_shape=(30, 27),
    writes_nprocs=True,
    grid_is_runtime=True,
    namelist_path="ice/cice_in.nml",
)
"""How the 025deg configurations' CICE5 divides its grid: 30 x 27 blocks distributed roundrobin.

Roundrobin forms no process grid, so the search chooses the rank count and nothing else, and max_blocks is left
for CICE5 to work out.
"""
OM2_01DEG_CICE5: CICEPartitioning = CICEPartitioning(
    grid=(3600, 2700),
    distribution_type="sectrobin",
    processor_shape="square-ice",
    block_shape=(40, 30),
    writes_nprocs=True,
    grid_is_runtime=True,
    namelist_path="ice/cice_in.nml",
)
"""How the 01deg configurations' CICE5 divides its grid: 40 x 30 blocks distributed sectrobin.

Sectrobin forms no process grid either, so the search chooses the rank count and nothing else.
"""


@dataclass(frozen=True, kw_only=True)
class OM2Configuration(SubmodelsConfiguration):
    """One configuration of ACCESS-OM2: the grid its ocean and its sea ice run on, and how the sea ice divides it.

    Args:
        name (str): Identifies the configuration in the names of the experiments generated for it.
        ocean (Domain): MOM5's grid. The search decomposes it over a two-dimensional process grid, which
            input.nml states, unmasked: see the module docstring on the mask table.
        sea_ice (CICEPartitioning): How CICE5 divides its grid, the ocean's. What the search may choose about it
            follows from the partitioning rather than being assumed here.
        sea_ice_executable (str | None): The executable the sea ice submodel is to run: the runtime-grid build,
            since the partitioning writes the grid and the blocks into the namelist. None leaves the control's own.
        model_type (str): The model type identifier, as Payu defines it.
        model_name (str): The model's name, which names the root of the component tree with `name`.
        experiment_prefix (str): Prefix for the names of experiments generated for this configuration.

    Raises:
        ValueError: If the name is empty or the ocean's grid is not two-dimensional.
    """

    name: str
    ocean: Domain
    sea_ice: CICEPartitioning
    sea_ice_executable: str | None = "cice_auscom.exe"
    model_type: str = "access-om2"
    model_name: str = "ACCESS-OM2"
    experiment_prefix: str = "om2-layout"

    @property
    def submodels(self) -> tuple[PayuSubmodel, ...]:
        """Returns the submodels every ACCESS-OM2 control declares, in its order: atmosphere, ocean, ice.

        YATM is serial, so it is pinned to one rank; OASIS is a library of the other two rather than a submodel.
        """
        return (
            PayuSubmodel("atmosphere", "yatm", PlainComponent(name=OM2_YATM_NAME, token="atm", n_ranks=1)),
            PayuSubmodel("ocean", "mom", MOM5Component(self.ocean)),
            PayuSubmodel("ice", "cice5", CICEComponent(self.sea_ice), exe=self.sea_ice_executable),
        )


OM2_1DEG: OM2Configuration = OM2Configuration(name="1deg", ocean=OM2_1DEG_GRID, sea_ice=OM2_1DEG_CICE5)
"""ACCESS-OM2 at one degree: the `1deg_jra55_*` configurations.

`release-1deg_jra55_ryf-4.1` runs MOM5 on a 16 x 15 process grid with 24 ranks masked, 216 cores, and CICE5 on 24;
the `+wombatlite` releases give MOM5 an 18 x 25 grid masked to 390.
"""
OM2_1DEG_CORES: dict[str, int] = {OM2_YATM_NAME: 1, OM2_MOM5_NAME: 216, OM2_CICE5_NAME: 24}
"""Cores each component receives in release-1deg_jra55_ryf-4.1, keyed by component name.

These build no layout, and are provided as the reference a caller writing an allocation strategy is usually working
from. The ocean's count is a masked one: 16 x 15 ranks less the 24 the mask table removes.
"""
OM2_025DEG: OM2Configuration = OM2Configuration(name="025deg", ocean=OM2_025DEG_GRID, sea_ice=OM2_025DEG_CICE5)
"""ACCESS-OM2 at a quarter of a degree: the `025deg_jra55_*` configurations.

`release-025deg_jra55_ryf-4.1` runs MOM5 on a 48 x 40 process grid masked to 1456 cores, and CICE5 on 361; the
`+wombatlite` releases give MOM5 a 48 x 60 grid masked to 2129.
"""
OM2_025DEG_CORES: dict[str, int] = {OM2_YATM_NAME: 1, OM2_MOM5_NAME: 1456, OM2_CICE5_NAME: 361}
"""Cores each component receives in release-025deg_jra55_ryf-4.1, keyed by component name; the ocean's is masked."""
OM2_01DEG: OM2Configuration = OM2Configuration(name="01deg", ocean=OM2_01DEG_GRID, sea_ice=OM2_01DEG_CICE5)
"""ACCESS-OM2 at a tenth of a degree: the `01deg_jra55_*` configurations.

`release-01deg_jra55_ryf-4.1` runs MOM5 on an 80 x 75 process grid masked to 4358 cores, and CICE5 on 722; the
`+wombatlite` releases give MOM5 an 80 x 100 grid masked to 5725.
"""
OM2_01DEG_CORES: dict[str, int] = {OM2_YATM_NAME: 1, OM2_MOM5_NAME: 4358, OM2_CICE5_NAME: 722}
"""Cores each component receives in release-01deg_jra55_ryf-4.1, keyed by component name; the ocean's is masked."""

OM2_CONFIGS_REPOSITORY: str = "git@github.com:ACCESS-NRI/access-om2-configs.git"
"""The repository holding the released ACCESS-OM2 configurations.

A study passes a `GitControlSource` of this repository at the release it profiles, such as
`release-1deg_jra55_ryf-4.1`, with the preset of that release's resolution.
"""
