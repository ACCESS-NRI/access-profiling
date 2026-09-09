# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import math
from pathlib import Path
from unittest import mock

import pytest
from access.config import YAMLParser
from access.config.parallel_allocation_strategies import FixedAllocation, FreeAllocation, RootAllocation
from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import DomainDivisibleByRanksConstraint, SubdomainAspectRatioConstraint
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid

from access.profiling.access_models import (
    ESM16_CICE5_NAME,
    ESM16_CICE5_NX_GLOBAL,
    ESM16_MAX_SUBDOMAIN_ASPECT_RATIO,
    ESM16_MAX_WASTED_CORE_FRACTION,
    ESM16_MOM5_NAME,
    ESM16_PI_CONTROL_CORES,
    ESM16_UM7_NAME,
    OM3_ATMOSPHERE_NAME,
    OM3_MC_25KM,
    OM3_MC_25KM_RELEASE_CORES,
    OM3_MEDIATOR_NAME,
    OM3_OCEAN_NAME,
    OM3_RUNOFF_NAME,
    OM3_SEA_ICE_NAME,
    OM3_SHARED_NAME,
    OM3_WAVE_NAME,
    AM3Profiling,
    ESM16Profiling,
    OM3Configuration,
    OM3Profiling,
    RAM3Profiling,
)
from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.esmf_parser import ESMFSummaryProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser


@mock.patch.object(YAMLParser, "parse", return_value={"UM_STDOUT_FILE": "file", "model": "file"})
@mock.patch.object(Path, "read_text", return_value="some text")
@mock.patch.object(Path, "is_file")
def test_esm16_config_profiling(mock_is_file, mock_read_text, mock_yaml_parse):
    """Test the ESM16ConfigProfiling class."""

    # Instantiate ESM16ConfigProfiling
    config_profiling = ESM16Profiling(Path("/fake/test_path"), Path("/fake/archive_path"))

    # Mock the presence of all log files
    mock_is_file.side_effect = [True, True, True]
    logs = config_profiling.get_component_logs(Path("/fake/path"))
    assert "UM" in logs
    assert "MOM5" in logs
    assert "CICE5" in logs
    assert isinstance(logs["UM"].parser, UMProfilingParser)
    assert isinstance(logs["MOM5"].parser, FMSProfilingParser)
    assert isinstance(logs["CICE5"].parser, CICE5ProfilingParser)

    # Mock the absence of UM log file
    mock_is_file.side_effect = [False, True, True]
    logs = config_profiling.get_component_logs(Path("/fake/path"))
    assert "UM" not in logs
    assert "MOM5" in logs
    assert "CICE5" in logs

    # Mock the absence of MOM5 log file
    mock_is_file.side_effect = [True, False, True]
    logs = config_profiling.get_component_logs(Path("/fake/path"))
    assert "UM" in logs
    assert "MOM5" not in logs
    assert "CICE5" in logs

    # Mock the absence of CICE5 log file
    mock_is_file.side_effect = [True, True, False]
    logs = config_profiling.get_component_logs(Path("/fake/path"))
    assert "UM" in logs
    assert "MOM5" in logs
    assert "CICE5" not in logs

    assert config_profiling.model_type == "access-esm1.6"


def test_ram3_config_profiling():
    """Test the rAM3Profiling class."""

    # Instantiate rAM3Profiling
    config_profiling = RAM3Profiling(Path("/fake/path"), Path("/fake/archive_path"), layout_variable="um_layout")
    assert "UM_regions" in config_profiling.known_parsers, '"UM_regions" key not in known_parsers.'
    assert isinstance(config_profiling.known_parsers["UM_regions"], UMProfilingParser), (
        "UM_regions known_parser not UMProfilingParser type."
    )
    assert "UM_total" in config_profiling.known_parsers, '"UM_total" key not in known_parsers.'
    assert isinstance(config_profiling.known_parsers["UM_total"], UMTotalRuntimeParser), (
        "UM_total known parser not UMTotalRuntimeParser type."
    )


def test_am3_config_profiling():
    """Test the AM3Profiling class."""

    # Instantiate AM3Profiling with AM3's split PROCX/PROCY layout variables
    config_profiling = AM3Profiling(
        Path("/fake/path"), Path("/fake/archive_path"), layout_variable=("MAIN_ATM_PROCX", "MAIN_ATM_PROCY")
    )
    assert "UM_regions" in config_profiling.known_parsers, '"UM_regions" key not in known_parsers.'
    assert isinstance(config_profiling.known_parsers["UM_regions"], UMProfilingParser), (
        "UM_regions known_parser not UMProfilingParser type."
    )
    assert "UM_total" in config_profiling.known_parsers, '"UM_total" key not in known_parsers.'
    assert isinstance(config_profiling.known_parsers["UM_total"], UMTotalRuntimeParser), (
        "UM_total known parser not UMTotalRuntimeParser type."
    )


# The released ACCESS-ESM1.6 pre-industrial control configuration: 508 of the 5 x 104 cores it is given go to the
# components, and the remaining 12 are left idle.
PI_CONTROL_NODES = 5.0
PI_CONTROL_CORES_PER_NODE = 104
PI_CONTROL_TOTAL_CORES = int(PI_CONTROL_NODES * PI_CONTROL_CORES_PER_NODE)
PI_CONTROL_UM7_GRID = (16, 16)
PI_CONTROL_MOM5_GRID = (16, 15)
PI_CONTROL_CICE5_RANKS = 12
PI_CONTROL_IDLE_CORES = 12
PI_CONTROL_BRANCH = "esm1p6-layout_atm_16x16_mom_16x15_ice_12x1"
PI_CONTROL_ALLOCATIONS = RootAllocation(
    subcomponents={
        ESM16_UM7_NAME: FixedAllocation(256, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
        ESM16_MOM5_NAME: FixedAllocation(240, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
        ESM16_CICE5_NAME: FixedAllocation(12),
    },
)


@pytest.fixture(scope="function")
def esm16():
    return ESM16Profiling(Path("/fake/test_path"), Path("/fake/archive_path"))


@pytest.fixture(scope="function")
def pi_control_layout(esm16):
    """The released ACCESS-ESM1.6 PI control layout, picked out of the ones the layout search returns.

    Its core split does not determine the layout on its own: three layouts share it at this size, all leaving
    the same 12 cores idle. What matters here is that the released one is among them.
    """

    layouts = esm16.select_layouts(PI_CONTROL_TOTAL_CORES, allocations=PI_CONTROL_ALLOCATIONS)
    released = [
        layout
        for layout in layouts
        if (layout.sub_layouts[0].decomposition.grid.shape, layout.sub_layouts[1].decomposition.grid.shape)
        == (PI_CONTROL_UM7_GRID, PI_CONTROL_MOM5_GRID)
    ]
    assert len(released) == 1, "The layout search should find the released PI control layout exactly once."
    return released[0]


def test_esm16_pi_control_layout(pi_control_layout):
    """Test that the layout search reproduces the released ACCESS-ESM1.6 PI control configuration."""

    um7, mom5, cice5 = pi_control_layout.sub_layouts
    assert um7.decomposition.grid.shape == PI_CONTROL_UM7_GRID
    assert mom5.decomposition.grid.shape == PI_CONTROL_MOM5_GRID
    assert cice5.n_ranks == PI_CONTROL_CICE5_RANKS
    assert pi_control_layout.idle_cores == PI_CONTROL_IDLE_CORES


def test_esm16_layout_branch_name(esm16, pi_control_layout):
    """Test the layout_branch_name method of ESM16Profiling."""

    assert esm16.layout_branch_name(pi_control_layout) == PI_CONTROL_BRANCH


def test_esm16_layout_config_changes(esm16, pi_control_layout):
    """Test the layout_config_changes method of ESM16Profiling."""

    changes = esm16.layout_config_changes(pi_control_layout)
    assert changes["config.yaml"]["submodels"] == [
        [
            {"ncpus": 256},
            {"ncpus": 240},
            {"ncpus": 12, "exe": ["cice_access.exe"]},
        ]
    ]
    assert changes["atmosphere/um_env.yaml"] == {
        "UM_ATM_NPROCX": "16",
        "UM_ATM_NPROCY": "16",
        "UM_NPES": "256",
    }
    assert changes["ocean/input.nml"] == {"ocean_model_nml": {"layout": ["16,15"]}}
    assert changes["ice/cice_in.nml"] == {
        "domain_nml": {
            "nprocs": "12",
            "nx_global": "360",
            "ny_global": "300",
            "block_size_x": "30",
            "block_size_y": "300",
            "max_blocks": "1",
        }
    }


def test_esm16_layout_requires_esm16_layout(esm16):
    """Test that the ESM1.6 layout methods reject layouts of other models.

    Both methods read the components positionally, in the order of ESM16_COMPONENT.subcomponents, so a layout
    of any other model simply does not unpack.
    """

    other_model = ComponentLayout(
        name="other-model",
        n_cores=4,
        n_ranks=4,
        threads_per_rank=None,
        decomposition=None,
        sub_layouts=(
            ComponentLayout(
                name="other-component",
                n_cores=4,
                n_ranks=4,
                threads_per_rank=1,
                decomposition=DomainDecompositionSpec(Domain((8, 8)), MPICartesianGrid((2, 2))),
            ),
        ),
    )
    with pytest.raises(ValueError):
        esm16.layout_branch_name(other_model)
    with pytest.raises(ValueError):
        esm16.layout_config_changes(other_model)


def _esm16_scaling_allocations(atm_ocn_tolerance: float = 0.05, ice_tolerance: float = 0.25) -> RootAllocation:
    """An allocation strategy following the proportions of the ACCESS-ESM1.6 PI control configuration.

    This is the kind of strategy a caller supplies to the layout search: it is the study's own choice, not a
    requirement of ACCESS-ESM1.6, which is why it lives here rather than in access.profiling.access_models.

    Every bound is a fraction of the total, so the strategy this returns is a single object usable at every core
    count of a scaling study - which is the whole reason the layout search understands fractions.

    CICE5 gets a wider band than the other two. Each of its ranks takes one block spanning the full y extent, so
    the core counts it admits are the divisors of ESM16_CICE5_NX_GLOBAL, and they thin out as the count grows: a
    +/-5% band around its proportional share is [142, 158] at 5200 cores, which contains no divisor of 360 at all,
    and no layout would be found.
    """
    pi_control_cores = sum(ESM16_PI_CONTROL_CORES.values())

    def band(name: str, tolerance: float, **kwargs) -> FreeAllocation:
        share = ESM16_PI_CONTROL_CORES[name] / pi_control_cores
        return FreeAllocation(
            min_core_fraction=share * (1.0 - tolerance),
            max_core_fraction=min(1.0, share * (1.0 + tolerance)),
            **kwargs,
        )

    aspect_ratio = (SubdomainAspectRatioConstraint(max_ratio=1.5),)
    return RootAllocation(
        subcomponents={
            ESM16_UM7_NAME: band(ESM16_UM7_NAME, atm_ocn_tolerance, local_constraints=aspect_ratio),
            ESM16_MOM5_NAME: band(ESM16_MOM5_NAME, atm_ocn_tolerance, local_constraints=aspect_ratio),
            ESM16_CICE5_NAME: band(ESM16_CICE5_NAME, ice_tolerance),
        },
    )


# One strategy for the whole scaling study, built once and reused at every core count below.
ESM16_SCALING_ALLOCATIONS = _esm16_scaling_allocations()


@pytest.mark.parametrize("total_cores", [PI_CONTROL_TOTAL_CORES, 1040, 5200])
def test_esm16_caller_supplied_allocations(esm16, total_cores):
    """Test that one fractional allocation strategy generates usable ACCESS-ESM1.6 layouts at every size."""

    layouts = esm16.select_layouts(total_cores, allocations=ESM16_SCALING_ALLOCATIONS)
    assert layouts, f"No layout found for {total_cores} cores."

    for layout in layouts:
        um7, _, cice5 = layout.sub_layouts
        # Executables are only available for an exact number of CICE5 blocks per rank
        assert ESM16_CICE5_NX_GLOBAL % cice5.n_ranks == 0
        # The UM requires an even number of processes along x
        assert um7.decomposition.grid.shape[0] % 2 == 0
        # UM_NPES is written from n_ranks and must match the process grid, or the UM hangs at startup
        atm_nx, atm_ny = um7.decomposition.grid.shape
        assert um7.n_ranks == atm_nx * atm_ny


def test_esm16_component_tree_bounds_layouts_on_its_own(esm16):
    """Test that the component tree rules out unreasonable layouts without any strategy constraints.

    The bounds in the tree are the ones a caller cannot relax, so they must hold for every layout the search
    returns when the caller states no preferences at all.
    """

    layouts = esm16.select_layouts(PI_CONTROL_TOTAL_CORES, max_layouts=200)
    assert layouts, "The component tree alone should still admit layouts."

    for layout in layouts:
        assert layout.idle_cores / layout.n_cores <= ESM16_MAX_WASTED_CORE_FRACTION
        for component in (layout.sub_layouts[0], layout.sub_layouts[1]):
            local_shape = component.decomposition.mean_local_shape
            assert max(local_shape) / min(local_shape) <= ESM16_MAX_SUBDOMAIN_ASPECT_RATIO
        assert ESM16_CICE5_NX_GLOBAL % layout.sub_layouts[2].n_ranks == 0


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_esm16_generate_scaling_experiments(mock_experiment_generator, esm16):
    """Test that ACCESS-ESM1.6 scaling experiments can be generated end to end."""

    esm16.set_control("https://example.com/repo", "commit")
    esm16.generate_scaling_experiments(
        num_nodes_list=[PI_CONTROL_NODES],
        control_options={},
        cores_per_node=PI_CONTROL_CORES_PER_NODE,
        walltime=2.0,  # hrs
        allocations=PI_CONTROL_ALLOCATIONS,
    )

    config = mock_experiment_generator.call_args[0][0]
    assert config["model_type"] == "access-esm1.6"

    # One perturbation experiment per layout, numbered sequentially. The released core split leaves the process
    # grids open, so this is every layout that fits it, not the released one alone.
    perturbations = config["Perturbation_Experiment"]
    assert list(perturbations) == [f"Experiment_{n}" for n in range(1, len(perturbations) + 1)]

    released = [block for block in perturbations.values() if block["branches"] == [PI_CONTROL_BRANCH]]
    assert len(released) == 1, "The released PI control layout should generate exactly one experiment."
    assert released[0]["config.yaml"]["walltime"] == "2:00:00"
    assert PI_CONTROL_BRANCH in esm16.experiments


# The two development configurations below are not shipped with the package: only released ones are. They are
# built here the way a caller builds their own, which is what OM3Configuration exists for.
OM3_100KM_GRID = Domain(shape=(360, 324))
OM3_MC_100KM = OM3Configuration(
    name="MC-100km",
    ocean=OM3_100KM_GRID,
    sea_ice=OM3_100KM_GRID,
    atmosphere=OM3_100KM_GRID,
    runoff=OM3_100KM_GRID,
)
OM3_MCW_100KM = OM3Configuration(
    name="MCW-100km",
    ocean=OM3_100KM_GRID,
    sea_ice=OM3_100KM_GRID,
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


def _om3_layout(
    manager: OM3Profiling, total_cores: int, cores: dict[str, int], **grids: tuple[int, int]
) -> ComponentLayout:
    """The one layout with the given process grids, out of those the search finds for the given cores.

    Pinning the cores no longer picks out a single layout: the ocean and the sea ice decompose their grids, so
    the search returns one layout per process grid each of them admits. The grids wanted are given by realm.
    """
    found = [
        layout
        for layout in manager.select_layouts(total_cores, allocations=_om3_pinned(**cores))
        if _om3_grids(layout) == grids
    ]
    (layout,) = found
    return layout


@pytest.fixture(scope="function")
def om3():
    return OM3Profiling(Path("/fake/test_path"), Path("/fake/archive_path"), OM3_MC_25KM)


def _pelayout(manager: OM3Profiling, layout) -> dict:
    return manager.layout_config_changes(layout)["nuopc.runconfig"]["PELAYOUT_attributes"]


def test_om3_reproduces_the_released_layout(om3):
    """Test that the search reproduces the core counts of release-MC_25km_jra_ryf-2.0-beta exactly.

    Only the core counts: the decompositions it offers for them are not the released ones. 2429 ocean cores
    are a masked count, and 275 sea ice cores tile neither extent of the grid. See the note on OM3_MC_25KM.
    """

    layouts = om3.select_layouts(2704, allocations=_om3_pinned(**OM3_MC_25KM_RELEASE_CORES))
    assert len(layouts) == 6, "one layout per process grid the sea ice admits on 275 cores"
    assert all(layout.idle_cores == 0 for layout in layouts)
    assert {_om3_grids(layout)[OM3_OCEAN_NAME] for layout in layouts} == {(347, 7)}, (
        "the only factorisation of 2429 whose sub-domains clear MOM6's halo"
    )

    changes = [om3.layout_config_changes(layout) for layout in layouts]
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


def test_om3_layout_branch_name(om3):
    """Test that the branch name records a grid where there is one and a core count otherwise.

    Two layouts dividing the cores identically and decomposing them differently are different experiments, so
    the name has to tell them apart.
    """

    layout = _om3_layout(om3, 2704, OM3_MC_25KM_RELEASE_CORES, ice=(25, 11), ocn=(347, 7))
    assert om3.layout_branch_name(layout) == "om3-layout_MC-25km_atm_275_cpl_275_ice_25x11_ocn_347x7_rof_275"

    other = _om3_layout(om3, 2704, OM3_MC_25KM_RELEASE_CORES, ice=(55, 5), ocn=(347, 7))
    assert om3.layout_branch_name(other) == "om3-layout_MC-25km_atm_275_cpl_275_ice_55x5_ocn_347x7_rof_275"


def test_om3_shared_components_need_not_be_the_same_size(om3):
    """Test that the shared range is as large as its largest component, not as large as their total."""

    layout = _om3_layout(
        om3,
        2704,
        {"cpl": 275, "atm": 100, "ice": 275, "rof": 50, "ocn": 2429},
        ice=(25, 11),
        ocn=(347, 7),
    )
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
        sea_ice=OM3_100KM_GRID,
        atmosphere=OM3_100KM_GRID,
        runoff=OM3_100KM_GRID,
        shared_core_offsets={OM3_RUNOFF_NAME: 12},
    )
    manager = OM3Profiling(Path("/fake/test_path"), Path("/fake/archive_path"), configuration)
    layout = _om3_layout(manager, 24, {"cpl": 24, "atm": 12, "ice": 24, "rof": 12}, ice=(4, 6))

    pelayout = _pelayout(manager, layout)
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
        sea_ice=OM3_100KM_GRID,
        atmosphere=OM3_100KM_GRID,
        shared_core_offsets={OM3_ATMOSPHERE_NAME: 12},
    )
    manager = OM3Profiling(Path("/fake/test_path"), Path("/fake/archive_path"), configuration)
    # The atmosphere starts 12 cores in and wants 24, so it reaches 36 and a 24-core range cannot hold it.
    assert manager.select_layouts(24, allocations=_om3_pinned(cpl=24, atm=24, ice=24)) == []
    # Give it a range that reaches far enough and the layout appears.
    layout = _om3_layout(manager, 36, {"pool_cores": 36, "cpl": 24, "atm": 24, "ice": 24}, ice=(4, 6))
    assert _pelayout(manager, layout)["atm_rootpe"] == 12


@pytest.mark.parametrize(
    ("configuration", "total_cores", "cores", "grids", "expected_rootpes"),
    [
        pytest.param(
            OM3_MC_100KM,
            240,
            {"cpl": 24, "atm": 24, "ice": 24, "rof": 24, "ocn": 216},
            {"ice": (4, 6), "ocn": (18, 12)},
            {"cpl_rootpe": 0, "atm_rootpe": 0, "ice_rootpe": 0, "rof_rootpe": 0, "ocn_rootpe": 24},
            id="dev-MC_100km_jra_ryf",
        ),
        pytest.param(
            OM3_MCW_100KM,
            208,
            {"cpl": 24, "atm": 24, "ice": 24, "rof": 24, "ocn": 96, "wav": 88},
            {"ice": (4, 6), "ocn": (12, 8)},
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

    manager = OM3Profiling(Path("/fake/test_path"), Path("/fake/archive_path"), configuration)
    layout = _om3_layout(manager, total_cores, cores, **grids)
    assert layout.idle_cores == 0

    pelayout = _pelayout(manager, layout)
    assert {key: value for key, value in pelayout.items() if key.endswith("_rootpe")} == expected_rootpes
    assert manager.layout_config_changes(layout)["config.yaml"] == {"ncpus": total_cores}


def test_om3_omits_the_components_a_configuration_does_not_have():
    """Test that a configuration without an ocean, or without sea ice, writes no keys for it."""

    ocean_only = OM3Configuration(name="M", ocean=OM3_100KM_GRID, atmosphere=OM3_100KM_GRID)
    manager = OM3Profiling(Path("/fake/test_path"), Path("/fake/archive_path"), ocean_only)
    layout = _om3_layout(manager, 20, {"cpl": 4, "atm": 4, "ocn": 16}, ocn=(4, 4))
    assert not any(key.startswith("ice_") for key in _pelayout(manager, layout))
    assert "ice_in" not in manager.layout_config_changes(layout), "and nothing for CICE6 to decompose"

    ice_only = OM3Configuration(name="C", sea_ice=OM3_100KM_GRID, runoff=OM3_100KM_GRID)
    manager = OM3Profiling(Path("/fake/test_path"), Path("/fake/archive_path"), ice_only)
    layout = _om3_layout(manager, 8, {"cpl": 8, "ice": 8, "rof": 8}, ice=(2, 4))
    assert not any(key.startswith("ocn_") for key in _pelayout(manager, layout))
    assert "MOM_input" not in manager.layout_config_changes(layout), "and no MOM6 layout to write"


def test_om3_assigns_the_domains_it_records():
    """Test that the grids a configuration records reach the leaves that decompose them, and only those."""

    leaves = {}
    for component in OM3_MCW_100KM.parallel_component.subcomponents:
        realms = component.subcomponents if component.name == OM3_SHARED_NAME else (component,)
        leaves.update({realm.name: realm for realm in realms})

    assert leaves[OM3_OCEAN_NAME].domain == OM3_MCW_100KM.ocean, "MOM6 decomposes the grid it was given"
    assert leaves[OM3_SEA_ICE_NAME].domain == OM3_MCW_100KM.sea_ice, "and so does CICE6"
    # ESMF decomposes the rest over whatever ranks they are given, and no configuration file states a process
    # grid for them, so there is nothing for the search to choose or for layout_config_changes to write.
    absent = (OM3_MEDIATOR_NAME, OM3_ATMOSPHERE_NAME, OM3_RUNOFF_NAME, OM3_WAVE_NAME)
    assert all(leaves[realm].domain is None for realm in absent)


def test_om3_rejects_a_grid_it_cannot_decompose():
    """Test that a one-dimensional ocean or sea ice grid is refused when the configuration is built."""

    with pytest.raises(ValueError, match="1-dimensional grid"):
        OM3Configuration(name="flat", ocean=Domain(shape=(360,)))
    with pytest.raises(ValueError, match="1-dimensional grid"):
        OM3Configuration(name="flat", sea_ice=Domain(shape=(360,)))


def test_om3_writes_the_chosen_decomposition():
    """Test that MOM6's LAYOUT and CICE6's blocks follow the decomposition the search chose."""

    manager = OM3Profiling(Path("/fake/test_path"), Path("/fake/archive_path"), OM3_MC_100KM)
    layout = _om3_layout(
        manager, 240, {"cpl": 24, "atm": 24, "ice": 24, "rof": 24, "ocn": 216}, ice=(4, 6), ocn=(18, 12)
    )

    changes = manager.layout_config_changes(layout)
    assert changes["MOM_input"] == {"LAYOUT": "18, 12"}
    # A 4x6 process grid tiles the 360x324 grid exactly, so every rank holds one 90x54 block.
    assert changes["ice_in"] == {
        "domain_nml": {
            "nprocs": "24",
            "nx_global": "360",
            "ny_global": "324",
            "block_size_x": "90",
            "block_size_y": "54",
            "max_blocks": "1",
        }
    }


def test_om3_blocks_cover_the_grid_when_it_does_not_tile(om3):
    """Test that a sea ice grid not dividing the extents still leaves every rank a block to work on."""

    layout = _om3_layout(om3, 2704, OM3_MC_25KM_RELEASE_CORES, ice=(25, 11), ocn=(347, 7))

    domain_nml = om3.layout_config_changes(layout)["ice_in"]["domain_nml"]
    # 1440 // 25 and 1152 // 11, so the 26x12 blocks covering the grid outnumber the 275 ranks sharing them.
    assert (domain_nml["block_size_x"], domain_nml["block_size_y"]) == ("57", "104")
    n_blocks = math.ceil(1440 / 57) * math.ceil(1152 / 104)
    assert n_blocks >= 275, "the blocks covering the grid are at least as many as the ranks"
    assert domain_nml["max_blocks"] == str(math.ceil(n_blocks / 275))

    # Rounding down is what keeps that true. This is the grid it matters on: blocks of ceil(1440 / 275) = 6
    # would cover the x extent with 240 of them, leaving 35 of the 275 ranks nothing to work on.
    layout = _om3_layout(om3, 2704, OM3_MC_25KM_RELEASE_CORES, ice=(275, 1), ocn=(347, 7))

    domain_nml = om3.layout_config_changes(layout)["ice_in"]["domain_nml"]
    assert (domain_nml["block_size_x"], domain_nml["block_size_y"]) == ("5", "1152")
    assert math.ceil(1440 / 5) * math.ceil(1152 / 1152) >= 275, "every rank still has a block"


def test_om3_caller_can_require_blocks_that_tile():
    """Test that a study whose control configuration needs tiling sea ice blocks can ask for them.

    The component tree does not require it, since the released core counts admit no such grid at all, so it is
    the allocation strategy that adds the constraint - which is the only direction a constraint can be changed
    in. A configuration distributing its blocks with distribution_type = "cartesian" needs this.
    """

    manager = OM3Profiling(Path("/fake/test_path"), Path("/fake/archive_path"), OM3_MC_100KM)
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

    layouts = manager.select_layouts(240, allocations=allocations)
    assert layouts, "the 100 km grid admits sea ice grids that tile it on 24 cores"
    nx_global, ny_global = OM3_100KM_GRID.shape
    for layout in layouts:
        nx_ranks, ny_ranks = _om3_grids(layout)[OM3_SEA_ICE_NAME]
        assert (nx_global % nx_ranks, ny_global % ny_ranks) == (0, 0)
        domain_nml = manager.layout_config_changes(layout)["ice_in"]["domain_nml"]
        assert domain_nml["max_blocks"] == "1", "blocks that tile the grid are one per rank"


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


@mock.patch.object(YAMLParser, "parse", return_value={"model": "access-om3"})
@mock.patch.object(Path, "read_text", return_value="some text")
@mock.patch.object(Path, "is_file")
def test_om3_config_profiling(mock_is_file, mock_read_text, mock_yaml_parse, om3):
    """Test the discovery of the ACCESS-OM3 component logs."""

    # All three logs present, probed in order: MOM6, CICE6, ESMF.
    mock_is_file.side_effect = [True, True, True]
    logs = om3.get_component_logs(Path("/fake/output000"))
    assert set(logs) == {"MOM6", "CICE6", "ESMF"}
    assert isinstance(logs["MOM6"].parser, FMSProfilingParser)
    assert isinstance(logs["CICE6"].parser, CICE5ProfilingParser)
    assert isinstance(logs["ESMF"].parser, ESMFSummaryProfilingParser)
    assert all(log.optional for log in logs.values()), "every ACCESS-OM3 log is optional"

    # None present: the run wrote no profiling data at all.
    mock_is_file.side_effect = [False, False, False]
    assert om3.get_component_logs(Path("/fake/output000")) == {}
