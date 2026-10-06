# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import dataclasses
from pathlib import Path

import pytest
from access.config.parallel_allocation_strategies import FixedAllocation, FreeAllocation, RootAllocation
from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import DomainDivisibleByRanksConstraint, EqualCoresGroupConstraint
from access.config.parallel_domain import Domain

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.esmf_parser import ESMFSummaryProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.manager import find_component
from access.profiling.models.cice import CICEPartitioning
from access.profiling.models.om3 import (
    OM3_ATMOSPHERE_NAME,
    OM3_MC_25KM,
    OM3_MC_25KM_RELEASE_CORES,
    OM3_MEDIATOR_NAME,
    OM3_MOM6_HALO_WIDTH,
    OM3_OCEAN_NAME,
    OM3_RUNOFF_NAME,
    OM3_SEA_ICE_NAME,
    OM3_SHARED_NAME,
    OM3_WAVE_NAME,
    OM3Configuration,
)
from access.profiling.payu_manager import PayuManager

# The two development configurations below are not shipped with the package: only released ones are. They are
# built here the way a caller builds their own, which is what OM3Configuration exists for.
OM3_100KM_GRID = Domain(shape=(360, 324))
# dev-MC_100km_jra_ryf distributes its blocks cartesian with one block row, so the search chooses how the x
# extent is split and each rank takes one block spanning the full y extent. The released 25 km configuration
# does not, which is what OM3_25KM_CICE6 says and what every test below that expects no sea ice grid is about.
OM3_100KM_CICE6 = CICEPartitioning(grid=OM3_100KM_GRID.shape)
OM3_MC_100KM = OM3Configuration(
    name="MC-100km",
    ocean=OM3_100KM_GRID,
    sea_ice=OM3_100KM_CICE6,
    atmosphere=OM3_100KM_GRID,
    runoff=OM3_100KM_GRID,
)
# MOM6 works out its own decomposition unless a configuration says otherwise, so the cases below that are
# about a process grid the search chose - and the LAYOUT it writes - need one that says otherwise.
OM3_MC_100KM_PINNED = dataclasses.replace(OM3_MC_100KM, name="MC-100km-pinned", auto_ocean_layout=False)
OM3_MCW_100KM = OM3Configuration(
    name="MCW-100km",
    ocean=OM3_100KM_GRID,
    sea_ice=OM3_100KM_CICE6,
    waves=OM3_100KM_GRID,
    atmosphere=OM3_100KM_GRID,
    runoff=OM3_100KM_GRID,
)


def _om3_pinned(pool_cores: int | None = None, **cores: int) -> RootAllocation:
    """An allocation pinning every ACCESS-OM3 component, given its cores keyed by realm.

    The components sharing a range are gathered under the shared parent, which is pinned to the largest of
    them unless *pool_cores* says otherwise - a range has to reach past its last component, so one placed at
    an offset needs a larger range than its own core count. Pinning is the usual way to allocate a shared
    parent: its children are enumerated as a product, so leaving several of them free is expensive.
    """
    shared = {realm: FixedAllocation(n) for realm, n in cores.items() if realm not in (OM3_OCEAN_NAME, OM3_WAVE_NAME)}
    if pool_cores is None:
        pool_cores = max(cores[realm] for realm in shared)
    subcomponents: dict = {OM3_SHARED_NAME: FixedAllocation(pool_cores, subcomponents=shared)}
    for realm in (OM3_OCEAN_NAME, OM3_WAVE_NAME):
        if realm in cores:
            subcomponents[realm] = FixedAllocation(cores[realm])
    return RootAllocation(subcomponents=subcomponents)


def _om3_grids(layout: ComponentLayout) -> dict[str, tuple[int, ...]]:
    """The process grid of every ACCESS-OM3 component that decomposes one, keyed by realm."""
    grids = {}
    for component in layout.sub_layouts:
        realms = component.sub_layouts if component.name == OM3_SHARED_NAME else (component,)
        for realm in realms:
            if realm.decomposition is not None:
                grids[realm.name] = realm.decomposition.grid.shape
    return grids


def _select(configuration: OM3Configuration, total_cores: int, **kwargs) -> list[ComponentLayout]:
    """The layouts of a configuration at a size, through the manager that searches for them.

    Args:
        configuration (OM3Configuration): The configuration to search over.
        total_cores (int): Cores the layouts must distribute among the components.
        **kwargs: As for ProfilingManager.select_layouts.

    Returns:
        list[ComponentLayout]: The layouts found, fewest idle cores first.
    """
    manager = PayuManager(Path("/fake/test_path"), Path("/fake/archive_path"), configuration)
    return manager.select_layouts(total_cores, **kwargs)


def _om3_layout(
    configuration: OM3Configuration, total_cores: int, cores: dict[str, int], **grids: tuple[int, ...]
) -> ComponentLayout:
    """The one layout with the given process grids, out of those the search finds for the given cores.

    Pinning the cores does not always pick out a single layout: a component the configuration leaves to the
    search to decompose contributes one layout per process grid it admits. The grids wanted are given by
    realm, and a configuration leaving every component to decompose itself needs none.
    """
    found = [
        layout
        for layout in _select(configuration, total_cores, allocations=_om3_pinned(**cores))
        if _om3_grids(layout) == grids
    ]
    (layout,) = found
    return layout


@pytest.fixture(scope="function")
def om3():
    """The released 25 km configuration, which is the one this package ships."""
    return OM3_MC_25KM


def _pelayout(configuration: OM3Configuration, layout) -> dict:
    return configuration.config_changes(layout)["nuopc.runconfig"]["PELAYOUT_attributes"]


def test_om3_names_the_model_payu_runs(om3):
    """Payu is told which model this is, whichever configuration of it is being profiled."""

    assert om3.model_type == "access-om3"
    assert om3.name == "MC-25km"


def test_om3_reproduces_the_released_layout(om3):
    """Test that the search reproduces the core counts of release-MC_25km_jra_ryf-2.0-beta exactly.

    Only the core counts: this release leaves both decompositions to the components themselves. MOM6 works
    its own out from the 2429 cores, and CICE6 distributes square-ice blocks roundrobin, which forms no
    process grid for a search to choose. See the note on OM3_MC_25KM.
    """

    layouts = _select(om3, 2704, allocations=_om3_pinned(**OM3_MC_25KM_RELEASE_CORES))
    assert len(layouts) == 1, "nothing here decomposes, so one set of core counts is one layout"
    assert all(layout.idle_cores == 0 for layout in layouts)
    assert all(_om3_grids(layout) == {} for layout in layouts), (
        "both decompositions are the components' own, so the search enumerates a process grid for neither"
    )

    changes = [om3.config_changes(layout) for layout in layouts]
    assert all(change["config.yaml"] == {"ncpus": 2704} for change in changes)
    assert all(
        change["nuopc.runconfig"]["PELAYOUT_attributes"]
        == {
            "cpl_ntasks": 275,
            "cpl_rootpe": 0,
            "atm_ntasks": 275,
            "atm_rootpe": 0,
            "ice_ntasks": 275,
            "ice_rootpe": 0,
            "rof_ntasks": 275,
            "rof_rootpe": 0,
            "ocn_ntasks": 2429,
            "ocn_rootpe": 275,
        }
        for change in changes
    ), "however the components decompose their grids, they receive the cores the release gave them"


def test_om3_experiment_name_records_cores_where_nothing_decomposes(om3):
    """The released configuration decomposes nothing, so every component is named by its cores."""

    layout = _om3_layout(om3, 2704, OM3_MC_25KM_RELEASE_CORES)
    assert om3.experiment_name(layout) == "om3-layout_MC-25km_atm_275_cpl_275_ice_275_ocn_2429_rof_275"


def test_om3_experiment_name_records_a_grid_where_there_is_one():
    """Two layouts dividing the cores identically and decomposing them differently are different experiments,
    so the name has to tell them apart."""

    cores = {"cpl": 24, "atm": 24, "ice": 24, "rof": 24, "ocn": 216}
    tall = _om3_layout(OM3_MC_100KM_PINNED, 240, cores, ice=(24,), ocn=(18, 12))
    wide = _om3_layout(OM3_MC_100KM_PINNED, 240, cores, ice=(24,), ocn=(12, 18))

    assert OM3_MC_100KM_PINNED.experiment_name(tall).endswith("_ice_24_ocn_18x12_rof_24")
    assert OM3_MC_100KM_PINNED.experiment_name(wide).endswith("_ice_24_ocn_12x18_rof_24")
    assert OM3_MC_100KM_PINNED.experiment_name(tall).startswith("om3-layout_MC-100km-pinned_atm_24_cpl_24_")


def test_om3_shared_components_need_not_be_the_same_size(om3):
    """Test that the shared range is as large as its largest component, not as large as their total."""

    layout = _om3_layout(om3, 2704, {"cpl": 275, "atm": 100, "ice": 275, "rof": 50, "ocn": 2429})
    shared = layout.sub_layouts[0]
    assert shared.n_cores == 275, "the range holds its largest component"
    assert sum(component.n_cores for component in shared.sub_layouts) == 700, "which is not their total"
    assert layout.n_cores == 2704, "so the job still occupies the cores the release asked for"

    pelayout = _pelayout(om3, layout)
    assert (pelayout["atm_ntasks"], pelayout["rof_ntasks"]) == (100, 50)
    assert pelayout["ocn_rootpe"] == 275


def test_om3_shared_components_may_start_at_different_cores():
    """Test that a configuration can place the components sharing a range at offsets within it."""

    configuration = OM3Configuration(
        name="offset",
        sea_ice=OM3_100KM_CICE6,
        atmosphere=OM3_100KM_GRID,
        runoff=OM3_100KM_GRID,
        shared_core_offsets={OM3_RUNOFF_NAME: 12},
    )
    layout = _om3_layout(configuration, 24, {"cpl": 24, "atm": 12, "ice": 24, "rof": 12}, ice=(24,))

    pelayout = _pelayout(configuration, layout)
    assert pelayout["rof_rootpe"] == 12, "the runoff starts where the configuration put it"
    assert pelayout["atm_rootpe"] == 0, "and everything else at the start of the range"
    assert layout.sub_layouts[0].n_cores == 24


def test_om3_finds_no_layout_for_a_component_running_past_its_range():
    """Test that the search rules out a component whose offset leaves no room for it.

    The offsets go into the component tree, so a range too small to reach past the last component simply has
    no layout. The search never offers one that would then have to be rejected when the configuration is
    written.
    """

    configuration = OM3Configuration(
        name="overrun",
        sea_ice=OM3_100KM_CICE6,
        atmosphere=OM3_100KM_GRID,
        shared_core_offsets={OM3_ATMOSPHERE_NAME: 12},
    )
    # The atmosphere starts 12 cores in and wants 24, so it reaches 36 and a 24-core range cannot hold it.
    assert _select(configuration, 24, allocations=_om3_pinned(cpl=24, atm=24, ice=24)) == []
    # Give it a range that reaches far enough and the layout appears.
    layout = _om3_layout(configuration, 36, {"pool_cores": 36, "cpl": 24, "atm": 24, "ice": 24}, ice=(24,))
    assert _pelayout(configuration, layout)["atm_rootpe"] == 12


def test_om3_finds_no_layout_for_a_range_its_components_leave_idle():
    """Test that the search rules out a range with cores no component ever runs on.

    The components sharing a range have to sit on all of it. Offsetting every one of them
    leaves cores at the front that belong to nobody, and since the range is placed by the
    running total over its siblings, starting it later is what such a configuration means -
    so there is no layout rather than one that pays for cores it never uses.
    """

    def manager_for(offsets: dict) -> OM3Configuration:
        configuration = OM3Configuration(
            name="idle",
            sea_ice=OM3_100KM_CICE6,
            atmosphere=OM3_100KM_GRID,
            shared_core_offsets=offsets,
        )
        return configuration

    offsets = dict.fromkeys((OM3_MEDIATOR_NAME, OM3_ATMOSPHERE_NAME, OM3_SEA_ICE_NAME), 4)
    pinned = _om3_pinned(pool_cores=28, cpl=24, atm=24, ice=24)
    assert _select(manager_for(offsets), 28, allocations=pinned) == []
    # The same components with nothing between them and the start of the range do have one.
    layout = _om3_layout(manager_for({}), 24, {"cpl": 24, "atm": 24, "ice": 24}, ice=(24,))
    assert layout.sub_layouts[0].idle_cores == 0


def test_om3_equal_cores_puts_every_shared_component_on_the_whole_range():
    """Test that requiring equal cores leaves each component on all of the range they share.

    The components sharing a range are enumerated as a product, so leaving them free is expensive and
    returns a layout for every way of giving them different slices of it. EqualCoresGroupConstraint says
    in one rule what a FixedAllocation per component otherwise has to say once each, and rewrite for every
    core count a scaling study visits. Since they must also cover the range between them, matching counts
    at a common offset put every one of them on all of it.
    """

    manager = OM3_MC_100KM
    realms = OM3_MC_100KM.shared_realms

    def allocations_with(*group_constraints) -> RootAllocation:
        return RootAllocation(
            subcomponents={
                OM3_SHARED_NAME: FixedAllocation(
                    12,
                    subcomponents=dict.fromkeys(realms, FreeAllocation()),
                    group_constraints=group_constraints,
                ),
                OM3_OCEAN_NAME: FixedAllocation(12),
            }
        )

    layouts = _select(manager, 24, allocations=allocations_with(EqualCoresGroupConstraint()))
    assert layouts
    for layout in layouts:
        pool = layout.sub_layouts[0]
        assert [realm.n_cores for realm in pool.sub_layouts] == [pool.n_cores] * len(realms)
        assert pool.n_cores == 12

    # Without the rule the same search still offers every uneven division of the range.
    uneven = _select(manager, 24, allocations=allocations_with(), max_layouts=500)
    assert any(len({realm.n_cores for realm in layout.sub_layouts[0].sub_layouts}) > 1 for layout in uneven)


@pytest.mark.parametrize(
    ("configuration", "total_cores", "cores", "grids", "expected_rootpes"),
    [
        pytest.param(
            OM3_MC_100KM,
            240,
            {"cpl": 24, "atm": 24, "ice": 24, "rof": 24, "ocn": 216},
            {"ice": (24,)},
            {"cpl_rootpe": 0, "atm_rootpe": 0, "ice_rootpe": 0, "rof_rootpe": 0, "ocn_rootpe": 24},
            id="dev-MC_100km_jra_ryf",
        ),
        pytest.param(
            OM3_MCW_100KM,
            208,
            {"cpl": 24, "atm": 24, "ice": 24, "rof": 24, "ocn": 96, "wav": 88},
            {"ice": (24,)},
            {
                "cpl_rootpe": 0,
                "atm_rootpe": 0,
                "ice_rootpe": 0,
                "rof_rootpe": 0,
                "ocn_rootpe": 24,
                "wav_rootpe": 120,
            },
            id="dev-MCW_100km_era_iaf",
        ),
    ],
)
def test_om3_reproduces_caller_built_configurations(configuration, total_cores, cores, grids, expected_rootpes):
    """Test that a configuration a caller builds reproduces the layout it was taken from."""

    layout = _om3_layout(configuration, total_cores, cores, **grids)
    assert layout.idle_cores == 0

    pelayout = _pelayout(configuration, layout)
    assert {key: value for key, value in pelayout.items() if key.endswith("_rootpe")} == expected_rootpes
    assert configuration.config_changes(layout)["config.yaml"] == {"ncpus": total_cores}


def test_om3_omits_the_components_a_configuration_does_not_have():
    """Test that a configuration without an ocean, or without sea ice, writes no keys for it."""

    ocean_only = OM3Configuration(name="M", ocean=OM3_100KM_GRID, atmosphere=OM3_100KM_GRID, auto_ocean_layout=False)
    manager = ocean_only
    layout = _om3_layout(manager, 20, {"cpl": 4, "atm": 4, "ocn": 16}, ocn=(4, 4))
    assert not any(key.startswith("ice_") for key in _pelayout(manager, layout))
    assert "ice_in" not in manager.config_changes(layout), "and nothing for CICE6 to decompose"

    # Pinned as well, so that the missing MOM_input is the missing ocean rather than MOM6 choosing.
    ice_only = OM3Configuration(name="C", sea_ice=OM3_100KM_CICE6, runoff=OM3_100KM_GRID, auto_ocean_layout=False)
    manager = ice_only
    layout = _om3_layout(manager, 8, {"cpl": 8, "ice": 8, "rof": 8}, ice=(8,))
    assert not any(key.startswith("ocn_") for key in _pelayout(manager, layout))
    assert "MOM_input" not in manager.config_changes(layout), "and no MOM6 layout to write"


def test_om3_assigns_the_domains_it_records():
    """Test that the grids a configuration records reach the leaves that decompose them, and only those."""

    leaves = {}
    for component in OM3_MCW_100KM.parallel_component.subcomponents:
        realms = component.subcomponents if component.name == OM3_SHARED_NAME else (component,)
        leaves.update({realm.name: realm for realm in realms})

    assert leaves[OM3_SEA_ICE_NAME].domain == OM3_MCW_100KM.sea_ice.domain, (
        "CICE6 carries what its own partitioning leaves the search to choose"
    )
    # ESMF decomposes the data components' and the waves' meshes over whatever ranks they are given, and MOM6
    # works its own decomposition out from them, so none of these states a process grid for the search to
    # choose or for config_changes to write - whatever grid the configuration records for them.
    absent = (OM3_MEDIATOR_NAME, OM3_ATMOSPHERE_NAME, OM3_RUNOFF_NAME, OM3_WAVE_NAME, OM3_OCEAN_NAME)
    assert all(leaves[realm].domain is None for realm in absent)
    assert OM3_MCW_100KM.ocean is not None, "though the configuration still records the ocean's grid"

    pinned = dataclasses.replace(OM3_MCW_100KM, auto_ocean_layout=False)
    pinned_leaves = {sub.name: sub for sub in pinned.parallel_component.subcomponents}
    assert pinned_leaves[OM3_OCEAN_NAME].domain == pinned.ocean, "unless the configuration pins MOM6's LAYOUT"


def test_om3_rejects_a_grid_it_cannot_decompose():
    """A one-dimensional ocean grid is refused when the configuration is built, rather than once a layout has
    been found for it. The sea ice's is CICEPartitioning's to check, and it does."""

    with pytest.raises(ValueError, match="1-dimensional grid"):
        OM3Configuration(name="flat", ocean=Domain(shape=(360,)))
    with pytest.raises(ValueError, match="two positive extents"):
        CICEPartitioning(grid=(360,))


def test_om3_needs_a_name():
    with pytest.raises(ValueError, match="name must be non-empty"):
        OM3Configuration(ocean=OM3_100KM_GRID)


def test_om3_enumerates_no_ocean_decomposition_by_default():
    """Test that MOM6 choosing its own layout costs the search a dimension rather than a factor.

    Every factorisation of the ocean's cores used to be a layout of its own, differing only in a LAYOUT that
    is now not written at all. What is left is one layout per sea ice process grid, and the ocean named by
    its core count.
    """

    auto = OM3_MC_100KM
    pinned = OM3_MC_100KM_PINNED
    cores = {"cpl": 24, "atm": 24, "ice": 24, "rof": 24, "ocn": 216}

    auto_layouts = _select(auto, 240, allocations=_om3_pinned(**cores))
    pinned_layouts = _select(pinned, 240, allocations=_om3_pinned(**cores))
    assert len(auto_layouts) < len(pinned_layouts), "one layout per ocean process grid is what goes away"
    assert all(set(_om3_grids(layout)) == {OM3_SEA_ICE_NAME} for layout in auto_layouts), (
        "the sea ice is the only component left with a decomposition the search chose"
    )

    layout = auto_layouts[0]
    assert "MOM_input" not in auto.config_changes(layout), "MOM6 is left to decompose its own grid"
    assert auto.config_changes(layout)["nuopc.runconfig"]["PELAYOUT_attributes"]["ocn_ntasks"] == 216
    assert "ocn_216" in auto.experiment_name(layout), "so the name records its cores, not a grid"


def test_om3_pinning_the_ocean_layout_restores_the_search():
    """Test that a configuration pinning MOM6's LAYOUT gets the grid, the halo rule and the written LAYOUT."""

    manager = OM3_MC_100KM_PINNED
    layouts = _select(manager, 240, allocations=_om3_pinned(cpl=24, atm=24, ice=24, rof=24, ocn=216))

    grids = {_om3_grids(layout)[OM3_OCEAN_NAME] for layout in layouts}
    assert grids, "the ocean decomposes its grid again"
    for nx_ranks, ny_ranks in grids:
        assert nx_ranks * ny_ranks == 216
        # MinSubdomainSizeConstraint applies again, so no rank holds less than MOM6's halo width.
        assert min(360 // nx_ranks, 324 // ny_ranks) >= OM3_MOM6_HALO_WIDTH

    layout = next(lay for lay in layouts if _om3_grids(lay)[OM3_OCEAN_NAME] == (18, 12))
    assert manager.config_changes(layout)["MOM_input"] == {"LAYOUT": "18, 12"}


def test_om3_writes_the_chosen_decomposition():
    """Test that MOM6's LAYOUT and CICE6's blocks follow the decomposition the search chose."""

    manager = OM3_MC_100KM_PINNED
    layout = _om3_layout(
        manager, 240, {"cpl": 24, "atm": 24, "ice": 24, "rof": 24, "ocn": 216}, ice=(24,), ocn=(18, 12)
    )

    changes = manager.config_changes(layout)
    assert changes["MOM_input"] == {"LAYOUT": "18, 12"}
    # One block per rank spanning the full y extent, 24 ranks splitting the 360-point x extent between them.
    # Neither nprocs nor the grid extents are written: this configuration states them nowhere the search
    # decides, so CICE6 and the control configuration settle them between themselves.
    assert changes["ice_in"] == {"domain_nml": {"block_size_x": "15", "block_size_y": "324", "max_blocks": "1"}}


def test_om3_writes_nothing_for_a_sea_ice_that_decomposes_itself(om3):
    """The released configuration distributes square-ice blocks roundrobin, so there is nothing to write.

    A process grid chosen here would mean nothing to a scheme that never forms one, and a block size derived
    from it would change how the sea ice is divided on no evidence at all. The search picks its rank count,
    which is the thing a scaling study is after, and leaves ice_in alone.
    """

    layout = _om3_layout(om3, 2704, OM3_MC_25KM_RELEASE_CORES)

    assert "ice_in" not in om3.config_changes(layout)
    assert _pelayout(om3, layout)["ice_ntasks"] == 275, "though it still receives the cores it was given"


def test_om3_blocks_tile_the_grid_wherever_the_search_chooses_them():
    """A configuration whose blocks are distributed cartesian needs them to tile the grid exactly.

    That is not a study's choice but the scheme's, so CICEPartitioning puts it in the component tree rather
    than leaving each study to add it: a rank left without a block aborts the run. An allocation strategy
    stating it again changes nothing, which is what this checks.
    """

    manager = OM3_MC_100KM
    allocations = RootAllocation(
        subcomponents={
            OM3_SHARED_NAME: FixedAllocation(
                24,
                subcomponents={
                    OM3_MEDIATOR_NAME: FixedAllocation(24),
                    OM3_ATMOSPHERE_NAME: FixedAllocation(24),
                    OM3_SEA_ICE_NAME: FixedAllocation(24, local_constraints=(DomainDivisibleByRanksConstraint(),)),
                    OM3_RUNOFF_NAME: FixedAllocation(24),
                },
            ),
            OM3_OCEAN_NAME: FixedAllocation(216),
        }
    )

    layouts = _select(manager, 240, allocations=allocations)
    assert layouts, "the 100 km grid is tiled by 24 block columns"
    nx_global, ny_global = OM3_100KM_GRID.shape
    for layout in layouts:
        (nx_ranks,) = _om3_grids(layout)[OM3_SEA_ICE_NAME]
        assert nx_global % nx_ranks == 0
        domain_nml = manager.config_changes(layout)["ice_in"]["domain_nml"]
        assert int(domain_nml["block_size_x"]) * nx_ranks == nx_global, "so the blocks tile the x extent"
        assert int(domain_nml["block_size_y"]) == ny_global, "each one spanning the full y extent"

    # The same search without the strategy's rule finds the same layouts: the requirement is the scheme's.
    without = RootAllocation(
        subcomponents={
            OM3_SHARED_NAME: FixedAllocation(
                24, subcomponents=dict.fromkeys(OM3_MC_100KM.shared_realms, FixedAllocation(24))
            ),
            OM3_OCEAN_NAME: FixedAllocation(216),
        }
    )
    assert len(_select(manager, 240, allocations=without)) == len(layouts)


def test_om3_data_sea_ice_is_not_decomposed():
    """Test that a configuration running CDEPS's data sea ice leaves its decomposition to ESMF.

    It still takes its turn on the shared range and still receives cores, but no configuration file states a
    process grid for it, so the search chooses none and writes no ice_in - as for the data atmosphere and the
    data runoff. Its grid is still recorded, which is what says the configuration has a sea ice at all.
    """

    configuration = dataclasses.replace(OM3_MC_100KM, name="MD-100km", data_sea_ice=True)
    cores = {"cpl": 24, "atm": 24, "ice": 24, "rof": 24, "ocn": 216}

    layouts = _select(configuration, 240, allocations=_om3_pinned(**cores))
    (layout,) = layouts  # with no process grid to choose, one set of core counts is one layout

    assert OM3_SEA_ICE_NAME not in _om3_grids(layout), "ESMF decomposes its mesh over the ranks it is given"
    assert OM3_SEA_ICE_NAME in configuration.shared_realms, "but it still takes its turn on the shared range"
    assert configuration.sea_ice is not None, "and the configuration still records its grid"

    changes = configuration.config_changes(layout)
    assert "ice_in" not in changes, "and there is no CICE6 namelist to write"
    assert _pelayout(configuration, layout)["ice_ntasks"] == 24, "though it still receives its cores"
    assert "ice_24" in configuration.experiment_name(layout), "so the name records them, not a grid"


def test_om3_configuration_needs_something_to_profile():
    """Test that a configuration of nothing but the mediator and the data components is rejected."""

    with pytest.raises(ValueError, match="no ocean, sea ice or waves"):
        OM3Configuration(name="empty", atmosphere=OM3_100KM_GRID, runoff=OM3_100KM_GRID)


@pytest.mark.parametrize(
    ("offsets", "match"),
    [
        ({OM3_OCEAN_NAME: 4}, "not one of the components sharing its cores"),
        ({OM3_MEDIATOR_NAME: -1}, "negative core offset"),
    ],
)
def test_om3_configuration_rejects_bad_offsets(offsets, match):
    """Test that an offset for a concurrent component, or a negative one, is rejected."""

    with pytest.raises(ValueError, match=match):
        OM3Configuration(name="bad", ocean=OM3_100KM_GRID, shared_core_offsets=offsets)


class TestOM3Logs:
    """Which profiling logs a configuration says it writes, and where they are."""

    @staticmethod
    def _output(tmp_path: Path, *, mom6: bool = True, cice6: bool = True, esmf: bool = True) -> Path:
        """Writes an output directory holding whichever of the three logs is asked for."""

        (tmp_path / "config.yaml").write_text("model: access-om3\n")
        if mom6:
            (tmp_path / "access-om3.out").write_text("")
        if cice6:
            (tmp_path / "log").mkdir()
            (tmp_path / "log" / "ice.log").write_text("")
        if esmf:
            (tmp_path / "ESMF_Profile.summary").write_text("")
        return tmp_path

    def test_every_log_is_found_and_read_with_its_own_parser(self, om3, tmp_path):
        logs = om3.component_logs(self._output(tmp_path))

        assert set(logs) == {"MOM6", "CICE6", "ESMF"}
        assert isinstance(logs["MOM6"].parser, FMSProfilingParser)
        assert isinstance(logs["CICE6"].parser, CICE5ProfilingParser)
        assert isinstance(logs["ESMF"].parser, ESMFSummaryProfilingParser)
        assert all(log.optional for log in logs.values()), "every ACCESS-OM3 log is optional"

    def test_a_run_that_wrote_no_profiling_data_at_all(self, om3, tmp_path):
        assert om3.component_logs(self._output(tmp_path, mom6=False, cice6=False, esmf=False)) == {}

    def test_each_log_names_the_component_it_was_run_on(self, om3):
        """Except the ESMF summary, which covers the whole model and so has no cores of its own."""

        assert om3.component_for_log("MOM6") == OM3_OCEAN_NAME
        assert om3.component_for_log("CICE6") == OM3_SEA_ICE_NAME
        assert om3.component_for_log("ESMF") is None

    def test_a_configuration_declares_only_the_logs_its_components_write(self):
        waves_only = OM3Configuration(name="W", waves=OM3_100KM_GRID, atmosphere=OM3_100KM_GRID)
        data_ice = dataclasses.replace(OM3_MC_100KM, name="MD-100km", data_sea_ice=True)

        assert {spec.name for spec in waves_only.logs} == {"ESMF"}, "neither MOM6 nor CICE6 is there to write one"
        assert {spec.name for spec in data_ice.logs} == {"MOM6", "ESMF"}, "CDEPS writes no CICE6 log"


def _om3_ranges(layout: ComponentLayout) -> dict[str, tuple[int, int]]:
    """The first core and the core count of each realm of a layout, however it is nested."""

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


def _write_runconfig(path: Path, pelayout: dict[str, int]) -> Path:
    """Writes a nuopc.runconfig stating the given PE layout, and returns its directory."""

    lines = ["PELAYOUT_attributes::"] + [f"     {key} = {value}" for key, value in pelayout.items()] + ["::"]
    (path / "nuopc.runconfig").write_text("\n".join(lines) + "\n")
    return path


class TestOM3ParseLayout:
    """Reading an ACCESS-OM3 layout back off the PE layout its configuration states."""

    def test_it_is_the_inverse_of_what_generation_writes(self, tmp_path):
        """The round trip that matters: what config_changes wrote is what this reads back."""

        configuration = dataclasses.replace(OM3_MC_100KM, name="MD-100km", data_sea_ice=True)
        # No component of this one carries a grid, so these core counts pick out a single layout.
        (generated,) = _select(configuration, 240, allocations=_om3_pinned(cpl=24, atm=24, ice=24, rof=24, ocn=216))
        path = _write_runconfig(tmp_path, _pelayout(configuration, generated))

        parsed = configuration.parse_layout(path)

        assert _om3_ranges(parsed) == _om3_ranges(generated)
        assert parsed.n_cores == generated.n_cores
        assert find_component(parsed, OM3_OCEAN_NAME).n_cores == 216

    def test_the_shared_realms_keep_their_places_on_the_range(self, om3, tmp_path):
        """Their offsets are what a shared range is, and the range has to be covered exactly."""

        path = _write_runconfig(
            tmp_path,
            {
                "cpl_ntasks": 48,
                "cpl_rootpe": 0,
                "atm_ntasks": 24,
                "atm_rootpe": 0,
                "ice_ntasks": 24,
                "ice_rootpe": 24,
                "rof_ntasks": 24,
                "rof_rootpe": 24,
                "ocn_ntasks": 192,
                "ocn_rootpe": 48,
            },
        )

        parsed = om3.parse_layout(path)

        (shared, ocean) = parsed.sub_layouts
        assert shared.name == OM3_SHARED_NAME
        assert shared.n_cores == 48
        assert {sub.name: (sub.core_offset, sub.n_cores) for sub in shared.sub_layouts} == {
            "cpl": (0, 48),
            "atm": (0, 24),
            "ice": (24, 24),
            "rof": (24, 24),
        }
        assert (ocean.name, ocean.n_cores) == (OM3_OCEAN_NAME, 192)
        assert parsed.n_cores == 240
        assert parsed.idle_cores == 0

    def test_the_sea_ice_can_be_found_under_the_range_it_shares(self, om3, tmp_path):
        """Which is the whole reason find_component looks at any depth."""

        path = _write_runconfig(
            tmp_path,
            {
                "cpl_ntasks": 48,
                "cpl_rootpe": 0,
                "atm_ntasks": 48,
                "atm_rootpe": 0,
                "ice_ntasks": 48,
                "ice_rootpe": 0,
                "rof_ntasks": 48,
                "rof_rootpe": 0,
                "ocn_ntasks": 192,
                "ocn_rootpe": 48,
            },
        )

        assert find_component(om3.parse_layout(path), OM3_SEA_ICE_NAME).n_cores == 48

    def test_cores_between_the_ranges_that_belong_to_nothing(self, om3, tmp_path):
        """The shared range ends at 24 and the ocean starts at 48, so 24 cores answer to no component."""

        path = _write_runconfig(
            tmp_path,
            {
                "cpl_ntasks": 24,
                "cpl_rootpe": 0,
                "atm_ntasks": 24,
                "atm_rootpe": 0,
                "ice_ntasks": 24,
                "ice_rootpe": 0,
                "rof_ntasks": 24,
                "rof_rootpe": 0,
                "ocn_ntasks": 192,
                "ocn_rootpe": 48,
            },
        )

        with pytest.raises(ValueError, match=r"24 core\(s\) belong to nothing"):
            om3.parse_layout(path)

    def test_a_shared_range_its_realms_leave_partly_idle(self, om3, tmp_path):
        """The range reaches core 48, but nothing runs on cores 24 to 35: they are idle in every realm."""

        path = _write_runconfig(
            tmp_path,
            {
                "cpl_ntasks": 24,
                "cpl_rootpe": 0,
                "atm_ntasks": 24,
                "atm_rootpe": 0,
                "ice_ntasks": 12,
                "ice_rootpe": 36,
                "rof_ntasks": 12,
                "rof_rootpe": 36,
                "ocn_ntasks": 192,
                "ocn_rootpe": 48,
            },
        )

        with pytest.raises(ValueError, match="is not a layout of this model"):
            om3.parse_layout(path)

    def test_no_configuration_file(self, om3, tmp_path):
        assert om3.parse_layout(tmp_path) is None

    def test_a_configuration_stating_no_pe_layout(self, om3, tmp_path):
        (tmp_path / "nuopc.runconfig").write_text("ALLCOMP_attributes::\n     ATM_model = datm\n::\n")
        assert om3.parse_layout(tmp_path) is None

    def test_a_pe_layout_missing_a_realm(self, om3, tmp_path):
        path = _write_runconfig(tmp_path, {"cpl_ntasks": 48, "cpl_rootpe": 0})
        assert om3.parse_layout(path) is None

    def test_a_configuration_without_an_ocean(self, tmp_path):
        ice_only = OM3Configuration(name="ice-only", sea_ice=OM3_100KM_GRID, atmosphere=OM3_100KM_GRID)
        manager = ice_only
        path = _write_runconfig(
            tmp_path,
            {"cpl_ntasks": 24, "cpl_rootpe": 0, "atm_ntasks": 24, "atm_rootpe": 0, "ice_ntasks": 24, "ice_rootpe": 0},
        )

        parsed = manager.parse_layout(path)

        assert [sub.name for sub in parsed.sub_layouts] == [OM3_SHARED_NAME]
        assert parsed.n_cores == 24

    def test_a_configuration_with_waves(self, tmp_path):
        """WW3 takes a range of its own after the ocean, so it is read back beside it."""

        manager = OM3_MCW_100KM
        path = _write_runconfig(
            tmp_path,
            {
                "cpl_ntasks": 24,
                "cpl_rootpe": 0,
                "atm_ntasks": 24,
                "atm_rootpe": 0,
                "ice_ntasks": 24,
                "ice_rootpe": 0,
                "rof_ntasks": 24,
                "rof_rootpe": 0,
                "ocn_ntasks": 192,
                "ocn_rootpe": 24,
                "wav_ntasks": 24,
                "wav_rootpe": 216,
            },
        )

        parsed = manager.parse_layout(path)

        assert [sub.name for sub in parsed.sub_layouts] == [OM3_SHARED_NAME, OM3_OCEAN_NAME, OM3_WAVE_NAME]
        assert find_component(parsed, OM3_WAVE_NAME).n_cores == 24
        assert parsed.n_cores == 240
