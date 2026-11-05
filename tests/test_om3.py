# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import dataclasses
from pathlib import Path

import pytest
from access.config.parallel_allocation_strategies import FixedAllocation, FreeAllocation, RootAllocation
from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import DomainDivisibleByRanksConstraint, EqualCoresGroupConstraint
from access.config.parallel_domain import Domain
from conftest import component_of, layout_of, select_layouts

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.esmf_parser import ESMFSummaryProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.models.cice import CICEPartitioning
from access.profiling.models.om3 import (
    OM3_100KM_GRID,
    OM3_ATMOSPHERE_NAME,
    OM3_CICE6_BLOCKS_PER_RANK_RANGE,
    OM3_MC_8KM,
    OM3_MC_8KM_CORES,
    OM3_MC_25KM,
    OM3_MC_25KM_CORES,
    OM3_MC_100KM,
    OM3_MC_100KM_CORES,
    OM3_MC_PANAN4KM,
    OM3_MC_PANAN4KM_CORES,
    OM3_MC_PANAN4KM_ISF,
    OM3_MC_PANAN4KM_ISF_CORES,
    OM3_MCW_100KM,
    OM3_MCW_100KM_CORES,
    OM3_MEDIATOR_NAME,
    OM3_OCEAN_NAME,
    OM3_RUNOFF_NAME,
    OM3_SEA_ICE_NAME,
    OM3_SHARED_NAME,
    OM3_WAVE_NAME,
    OM3Configuration,
)

# A configuration whose sea ice leaves the search a process grid to choose, which no preset does: the 100 km
# scheme, cartesian with one block row, with no block size chosen from the rank count, so the search splits the x
# extent and each rank takes one block spanning the full y extent.
OM3_MC_100KM_TILED = dataclasses.replace(
    OM3_MC_100KM, name="MC-100km-tiled", sea_ice=CICEPartitioning(grid=OM3_100KM_GRID)
)
TILED_CORES = {"cpl": 24, "atm": 24, "ice": 24, "rof": 24, "ocn": 216}


def _om3_pinned(**cores: int) -> RootAllocation:
    """An allocation pinning every ACCESS-OM3 component, given its cores keyed by realm.

    The components sharing a range are gathered under the shared parent, which is pinned to the largest of
    them. Pinning is the usual way to allocate a shared parent: its children are enumerated as a product, so
    leaving several of them free is expensive.
    """
    shared = {realm: FixedAllocation(n) for realm, n in cores.items() if realm not in (OM3_OCEAN_NAME, OM3_WAVE_NAME)}
    subcomponents: dict = {
        OM3_SHARED_NAME: FixedAllocation(
            max(shared_cores.n_cores or 0 for shared_cores in shared.values()), subcomponents=shared
        )
    }
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
        for layout in select_layouts(configuration, total_cores, allocations=_om3_pinned(**cores))
        if _om3_grids(layout) == grids
    ]
    (layout,) = found
    return layout


@pytest.fixture(scope="function")
def om3():
    """The released 25 km configuration."""
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

    layouts = select_layouts(om3, 2704, allocations=_om3_pinned(**OM3_MC_25KM_CORES))
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

    layout = _om3_layout(om3, 2704, OM3_MC_25KM_CORES)
    assert om3.experiment_name(layout) == "om3-layout_MC-25km_atm_275_cpl_275_ice_275_ocn_2429_rof_275"


def test_om3_experiment_name_records_a_grid_where_there_is_one():
    """A sea ice the search decomposes is named by its process grid, which says its ranks as well."""

    layout = _om3_layout(OM3_MC_100KM_TILED, 240, TILED_CORES, ice=(24,))

    assert OM3_SEA_ICE_NAME in _om3_grids(layout), "the search chose a process grid for the sea ice"
    assert OM3_MC_100KM_TILED.experiment_name(layout) == "om3-layout_MC-100km-tiled_atm_24_cpl_24_ice_24_ocn_216_rof_24"


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


def test_om3_refuses_a_layout_of_another_model(om3):
    """Both methods read the realms off the layout, so a layout of another model is refused rather than read as
    if it were one of these."""

    other_model = ComponentLayout(
        name="other-model",
        n_cores=4,
        n_ranks=4,
        threads_per_rank=None,
        decomposition=None,
        sub_layouts=(ComponentLayout(name="other", n_cores=4, n_ranks=4, threads_per_rank=1, decomposition=None),),
    )
    with pytest.raises(ValueError, match=r"configuration 'MC-25km' runs \['atm', 'cpl', 'ice', 'ocn', 'rof'\]"):
        om3.experiment_name(other_model)
    with pytest.raises(ValueError, match="has the components \\['other'\\]"):
        om3.config_changes(other_model)


def test_om3_equal_cores_puts_every_shared_component_on_the_whole_range():
    """Test that requiring equal cores leaves each component on all of the range they share.

    The components sharing a range are enumerated as a product, so leaving them free is expensive and
    returns a layout for every way of giving them different slices of it. EqualCoresGroupConstraint says
    in one rule what a FixedAllocation per component otherwise has to say once each, and rewrite for every
    core count a scaling study visits. Since they must also cover the range between them, matching counts
    at a common offset put every one of them on all of it.
    """

    configuration = OM3_MC_100KM
    realms = [component.name for component in configuration.shared]

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

    layouts = select_layouts(configuration, 24, allocations=allocations_with(EqualCoresGroupConstraint()))
    assert layouts
    for layout in layouts:
        pool = layout.sub_layouts[0]
        assert [realm.n_cores for realm in pool.sub_layouts] == [pool.n_cores] * len(realms)
        assert pool.n_cores == 12

    # Without the rule the same search still offers every uneven division of the range.
    uneven = select_layouts(configuration, 24, allocations=allocations_with(), max_layouts=500)
    assert any(len({realm.n_cores for realm in layout.sub_layouts[0].sub_layouts}) > 1 for layout in uneven)


@pytest.mark.parametrize(
    ("configuration", "total_cores", "cores", "expected_rootpes"),
    [
        pytest.param(
            OM3_MC_100KM,
            312,
            OM3_MC_100KM_CORES,
            {"cpl_rootpe": 0, "atm_rootpe": 0, "ice_rootpe": 0, "rof_rootpe": 0, "ocn_rootpe": 24},
            id="dev-MC_100km_jra_ryf",
        ),
        pytest.param(
            OM3_MCW_100KM,
            208,
            OM3_MCW_100KM_CORES,
            {
                "cpl_rootpe": 0,
                "atm_rootpe": 0,
                "ice_rootpe": 0,
                "rof_rootpe": 0,
                "ocn_rootpe": 56,
                "wav_rootpe": 152,
            },
            id="dev-MCW_100km_era_iaf",
        ),
    ],
)
def test_om3_reproduces_the_development_configurations(configuration, total_cores, cores, expected_rootpes):
    """Test that each 100 km preset reproduces the layout its control runs, as of November 2026."""

    layout = _om3_layout(configuration, total_cores, cores)
    assert layout.idle_cores == 0

    pelayout = _pelayout(configuration, layout)
    assert {key: value for key, value in pelayout.items() if key.endswith("_rootpe")} == expected_rootpes
    assert configuration.config_changes(layout)["config.yaml"] == {"ncpus": total_cores}


def test_om3_writes_the_waves_only_where_there_are_waves():
    """Test that a configuration without waves writes no keys for them, and one with them does."""

    layout = _om3_layout(OM3_MC_100KM, 312, OM3_MC_100KM_CORES)
    assert not any(key.startswith("wav_") for key in _pelayout(OM3_MC_100KM, layout))
    assert [sub.name for sub in layout.sub_layouts] == [OM3_SHARED_NAME, OM3_OCEAN_NAME]

    layout = _om3_layout(OM3_MCW_100KM, 208, OM3_MCW_100KM_CORES)
    assert _pelayout(OM3_MCW_100KM, layout)["wav_ntasks"] == 56
    assert [sub.name for sub in layout.sub_layouts] == [OM3_SHARED_NAME, OM3_OCEAN_NAME, OM3_WAVE_NAME]


@pytest.mark.parametrize(
    ("configuration", "cores", "grid", "distribution_type", "processor_shape", "ncpus"),
    [
        pytest.param(OM3_MC_8KM, OM3_MC_8KM_CORES, (4320, 3360), "roundrobin", "square-ice", 8424, id="MC_8km"),
        pytest.param(
            OM3_MC_PANAN4KM, OM3_MC_PANAN4KM_CORES, (4320, 1442), "sectrobin", "square-ice", 1664, id="MC_panan4km"
        ),
        pytest.param(
            OM3_MC_PANAN4KM_ISF,
            OM3_MC_PANAN4KM_ISF_CORES,
            (4320, 1442),
            "sectrobin",
            "square-ice",
            2990,
            id="MC_panan4km+isf",
        ),
    ],
)
def test_om3_large_presets_state_the_schemes_of_their_controls(
    configuration, cores, grid, distribution_type, processor_shape, ncpus
):
    """Their layouts are not searched here: choosing a block size on a grid this large weighs millions of
    candidates for each rank count the search visits. What is checked is that each preset states the scheme
    and the budget its control runs, as of November 2026: the ncpus of its config.yaml, which is the shared
    range plus the ocean, and not always whole nodes."""

    assert configuration.sea_ice.grid == grid
    assert configuration.sea_ice.distribution_type == distribution_type
    assert configuration.sea_ice.processor_shape == processor_shape
    assert configuration.sea_ice.blocks_per_rank_range == OM3_CICE6_BLOCKS_PER_RANK_RANGE
    assert not configuration.waves
    shared = max(cores[realm] for realm in (OM3_MEDIATOR_NAME, OM3_ATMOSPHERE_NAME, OM3_SEA_ICE_NAME, OM3_RUNOFF_NAME))
    assert shared + cores[OM3_OCEAN_NAME] == ncpus


def test_om3_assigns_a_domain_only_where_the_sea_ice_leaves_one_to_choose():
    """Test that the sea ice carries a domain exactly when its partitioning gives the search one to choose.

    ESMF decomposes the data components' and the waves' meshes over whatever ranks they are given, and MOM6
    works its own decomposition out from them, so none of those states a process grid for the search to
    choose or for config_changes to write.
    """

    def leaves(configuration: OM3Configuration) -> dict:
        found = {}
        for component in configuration.parallel_component.subcomponents:
            realms = component.subcomponents if component.name == OM3_SHARED_NAME else (component,)
            found.update({realm.name: realm for realm in realms})
        return found

    assert all(leaf.domain is None for leaf in leaves(OM3_MCW_100KM).values()), (
        "the presets choose the sea ice's blocks from its rank count, so nothing is left to decompose"
    )
    tiled = leaves(OM3_MC_100KM_TILED)
    assert tiled[OM3_SEA_ICE_NAME].domain == Domain((360,)), "one block column per rank, which the search splits"
    assert all(tiled[realm].domain is None for realm in (OM3_MEDIATOR_NAME, OM3_ATMOSPHERE_NAME, OM3_RUNOFF_NAME))
    assert tiled[OM3_OCEAN_NAME].domain is None


def test_om3_needs_a_name(om3):
    with pytest.raises(ValueError, match="name must be non-empty"):
        OM3Configuration(name="", sea_ice=om3.sea_ice)


def test_om3_leaves_the_ocean_to_decompose_itself():
    """Test that MOM6 choosing its own layout costs the search a dimension rather than a factor.

    One layout per set of core counts, with the ocean named by its cores and no LAYOUT written for it.
    """

    layouts = select_layouts(OM3_MC_100KM, 312, allocations=_om3_pinned(**OM3_MC_100KM_CORES))
    (layout,) = layouts

    assert _om3_grids(layout) == {}, "neither the ocean nor the sea ice is left to the search to decompose"
    changes = OM3_MC_100KM.config_changes(layout)
    assert "MOM_input" not in changes, "MOM6 is left to decompose its own grid"
    assert changes["nuopc.runconfig"]["PELAYOUT_attributes"]["ocn_ntasks"] == 288
    assert "ocn_288" in OM3_MC_100KM.experiment_name(layout), "so the name records its cores, not a grid"


def test_om3_writes_the_chosen_decomposition():
    """Test that CICE6's blocks follow the decomposition the search chose, where it chose one."""

    layout = _om3_layout(OM3_MC_100KM_TILED, 240, TILED_CORES, ice=(24,))

    changes = OM3_MC_100KM_TILED.config_changes(layout)
    # One block per rank spanning the full y extent, 24 ranks splitting the 360-point x extent between them.
    # Neither nprocs nor the grid extents are written: this configuration states them nowhere the search
    # decides, so CICE6 and the control configuration settle them between themselves.
    assert changes["ice_in"] == {"domain_nml": {"block_size_x": "15", "block_size_y": "324", "max_blocks": "1"}}
    assert "MOM_input" not in changes


def test_om3_writes_the_blocks_its_rank_count_calls_for(om3):
    """The released configuration distributes square-ice blocks roundrobin, so there is no process grid to write.

    The search picks the sea ice's rank count, which is the thing a scaling study is after, and the block size
    follows from it, so that each rank holds several blocks whatever that count is.
    """

    layout = _om3_layout(om3, 2704, OM3_MC_25KM_CORES)

    # 45 x 36 blocks of 32 x 32 tile the 1440 x 1152 grid exactly, 1620 of them over 275 ranks.
    assert om3.config_changes(layout)["ice_in"] == {
        "domain_nml": {
            "nx_global": "1440",
            "ny_global": "1152",
            "block_size_x": "32",
            "block_size_y": "32",
            "max_blocks": "-1",
        }
    }
    assert _pelayout(om3, layout)["ice_ntasks"] == 275
    assert OM3_SEA_ICE_NAME not in _om3_grids(layout), "and the search chose no process grid for it"


def test_om3_searches_only_sea_ice_rank_counts_some_block_size_suits():
    """A rank count no block size gives OM3_CICE6_BLOCKS_PER_RANK_RANGE blocks each is never a layout at all.

    A cartesian distribution is where that bites: the blocks have to split evenly over the process grid CICE
    forms, and with one processor row 29 ranks would need 29 block columns or 29 block rows, which no block
    size on the 100 km grid gives.
    """

    configuration = OM3_MC_100KM
    leaves = {sub.name: sub for sub in configuration.parallel_component.subcomponents[0].subcomponents}
    assert leaves[OM3_SEA_ICE_NAME].domain is None, "the search chooses a rank count and nothing else"

    assert not select_layouts(configuration, 240, allocations=_om3_pinned(cpl=29, atm=29, ice=29, rof=29, ocn=211))

    (layout,) = select_layouts(configuration, 240, allocations=_om3_pinned(**TILED_CORES))
    domain_nml = configuration.config_changes(layout)["ice_in"]["domain_nml"]
    n_blocks = (360 // int(domain_nml["block_size_x"])) * (324 // int(domain_nml["block_size_y"]))
    assert 5 * 24 <= n_blocks <= 8 * 24
    assert domain_nml["max_blocks"] == "-1"


def test_om3_blocks_tile_the_grid_wherever_the_search_chooses_them():
    """A configuration whose blocks are distributed cartesian needs them to tile the grid exactly.

    That is not a study's choice but the scheme's, so CICEPartitioning puts it in the component tree rather
    than leaving each study to add it: a rank left without a block aborts the run. An allocation strategy
    stating it again changes nothing, which is what this checks.
    """

    configuration = OM3_MC_100KM_TILED
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

    layouts = select_layouts(configuration, 240, allocations=allocations)
    assert layouts, "the 100 km grid is tiled by 24 block columns"
    nx_global, ny_global = OM3_100KM_GRID
    for layout in layouts:
        (nx_ranks,) = _om3_grids(layout)[OM3_SEA_ICE_NAME]
        assert nx_global % nx_ranks == 0
        domain_nml = configuration.config_changes(layout)["ice_in"]["domain_nml"]
        assert int(domain_nml["block_size_x"]) * nx_ranks == nx_global, "so the blocks tile the x extent"
        assert int(domain_nml["block_size_y"]) == ny_global, "each one spanning the full y extent"

    # The same search without the strategy's rule finds the same layouts: the requirement is the scheme's.
    without = RootAllocation(
        subcomponents={
            OM3_SHARED_NAME: FixedAllocation(
                24, subcomponents={component.name: FixedAllocation(24) for component in configuration.shared}
            ),
            OM3_OCEAN_NAME: FixedAllocation(216),
        }
    )
    assert len(select_layouts(configuration, 240, allocations=without)) == len(layouts)


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

    def test_the_waves_write_no_log(self):
        """WW3 writes no timers this package reads, so a configuration with waves declares the same three logs."""

        assert {spec.name for spec in OM3_MCW_100KM.logs} == {"MOM6", "CICE6", "ESMF"}


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

        configuration = OM3_MC_100KM
        # No component of a preset carries a grid, so these core counts pick out a single layout.
        (generated,) = select_layouts(configuration, 312, allocations=_om3_pinned(**OM3_MC_100KM_CORES))
        path = _write_runconfig(tmp_path, _pelayout(configuration, generated))

        parsed = layout_of(configuration.parse_layout(path))

        assert _om3_ranges(parsed) == _om3_ranges(generated)
        assert parsed.n_cores == generated.n_cores
        assert parsed.name == "ACCESS-OM3 MC-100km"
        assert component_of(parsed, OM3_OCEAN_NAME).n_cores == 288

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

        parsed = layout_of(om3.parse_layout(path))

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

        assert component_of(layout_of(om3.parse_layout(path)), OM3_SEA_ICE_NAME).n_cores == 48

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

    def test_a_configuration_with_waves(self, tmp_path):
        """WW3 takes a range of its own after the ocean, so it is read back beside it."""

        configuration = OM3_MCW_100KM
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

        parsed = layout_of(configuration.parse_layout(path))

        assert [sub.name for sub in parsed.sub_layouts] == [OM3_SHARED_NAME, OM3_OCEAN_NAME, OM3_WAVE_NAME]
        assert component_of(parsed, OM3_WAVE_NAME).n_cores == 24
        assert parsed.n_cores == 240


def _om3_control(
    path: Path,
    *,
    ice_model: str = "cice",
    wav_model: str = "swav",
    ocn_model: str = "mom",
    ice_in: str = "",
    mom_input: str = "AUTO_MASKTABLE = True           !   [Boolean] default = False\n",
    jobname: str | None = "MC_100km_jra_ryf",
) -> Path:
    """Writes a control the way the ACCESS-OM3 configurations are laid out, as far as the reader looks."""

    path.mkdir(parents=True, exist_ok=True)
    (path / "nuopc.runconfig").write_text(
        "ALLCOMP_attributes::\n"
        f"     ATM_model = datm\n     ICE_model = {ice_model}\n     OCN_model = {ocn_model}\n"
        f"     ROF_model = drof\n     WAV_model = {wav_model}\n::\n"
    )
    (path / "ice_in").write_text(
        ice_in
        or '&domain_nml\n  block_size_x = 15\n  block_size_y = 20\n  distribution_type = "cartesian"\n'
        '  max_blocks = -1\n  nx_global = 360\n  ny_global = 324\n  processor_shape = "slenderX1"\n/\n'
    )
    (path / "MOM_input").write_text(mom_input)
    (path / "config.yaml").write_text("model: access-om3\n" + (f"jobname: {jobname}\n" if jobname else ""))
    return path


class TestOM3FromControl:
    """Reading a configuration off a control's own files, by the rules Payu's ACCESS-OM3 driver reads them with."""

    def test_a_development_configuration_reads_as_its_preset_once_the_blocks_are_chosen(self, tmp_path):
        """The reader pins the block size the control states; choosing it from the rank count is the preset's."""

        read = OM3Configuration.from_control(_om3_control(tmp_path), name="MC-100km")

        assert read.sea_ice == CICEPartitioning(grid=(360, 324), block_shape=(15, 20), grid_is_runtime=True)
        assert not read.waves
        chosen = dataclasses.replace(
            read.sea_ice, block_shape=None, blocks_per_rank_range=OM3_CICE6_BLOCKS_PER_RANK_RANGE
        )
        assert dataclasses.replace(read, sea_ice=chosen) == OM3_MC_100KM

    def test_the_name_is_the_jobname_unless_one_is_given(self, tmp_path):
        assert OM3Configuration.from_control(_om3_control(tmp_path)).name == "MC_100km_jra_ryf"

        with pytest.raises(ValueError, match="states no jobname"):
            OM3Configuration.from_control(_om3_control(tmp_path / "unnamed", jobname=None))

    def test_the_waves_follow_the_wave_model(self, tmp_path):
        assert OM3Configuration.from_control(_om3_control(tmp_path / "ww3", wav_model="ww3")).waves
        assert OM3Configuration.from_control(_om3_control(tmp_path / "dev", wav_model="ww3dev")).waves
        assert not OM3Configuration.from_control(_om3_control(tmp_path / "stub", wav_model="swav")).waves

    def test_a_pinned_ocean_layout_is_warned_about(self, tmp_path, caplog):
        """The 25 km release pins LAYOUT and MASKTABLE, which a generated ocean core count cannot run with."""

        release = _om3_control(
            tmp_path,
            mom_input='LAYOUT = 72, 48                 !\nMASKTABLE = "./auxiliary_input/mask_table.1027.72x48" !\n',
        )

        with caplog.at_level("WARNING"):
            read = OM3Configuration.from_control(release, name="MC-25km")

        assert "pins LAYOUT and MASKTABLE without AUTO_MASKTABLE" in caplog.text
        assert read.sea_ice.block_shape == (15, 20)

    def test_a_control_setting_auto_masktable_is_not_warned_about(self, tmp_path, caplog):
        with caplog.at_level("WARNING"):
            OM3Configuration.from_control(
                _om3_control(tmp_path, mom_input="LAYOUT = 72, 48 !\nAUTO_MASKTABLE = True !\n")
            )

        assert "AUTO_MASKTABLE" not in caplog.text

    def test_a_control_without_a_mom_input_is_read_without_a_word(self, tmp_path, caplog):
        control = _om3_control(tmp_path)
        (control / "MOM_input").unlink()

        with caplog.at_level("WARNING"):
            OM3Configuration.from_control(control)

        assert caplog.text == ""

    @pytest.mark.parametrize("ice_model", ["sice", "dice"])
    def test_a_sea_ice_that_is_not_cice_is_refused(self, tmp_path, ice_model):
        with pytest.raises(ValueError, match=f"is '{ice_model}', not CICE"):
            OM3Configuration.from_control(_om3_control(tmp_path, ice_model=ice_model))

    def test_an_ocean_that_is_not_mom6_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="is 'docn', not MOM6"):
            OM3Configuration.from_control(_om3_control(tmp_path, ocn_model="docn"))

    def test_a_control_without_a_nuopc_configuration_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="not an ACCESS-OM3 control"):
            OM3Configuration.from_control(tmp_path)
