# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest
from access.config import YAMLParser
from access.config.parallel_allocation_strategies import FixedAllocation, FreeAllocation, RootAllocation
from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import SubdomainAspectRatioConstraint
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid

from access.profiling.access_models import (
    ESM16_CICE5_NAME,
    ESM16_CICE5_NBLOCKS,
    ESM16_MAX_SUBDOMAIN_ASPECT_RATIO,
    ESM16_MAX_WASTED_CORE_FRACTION,
    ESM16_MOM5_NAME,
    ESM16_PI_CONTROL_CORES,
    ESM16_UM7_NAME,
    ESM16Profiling,
    RAM3Profiling,
)
from access.profiling.cice5_parser import CICE5ProfilingParser
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


# Cores of each component in the released ACCESS-ESM1.6 pre-industrial control configuration, which uses the whole
# of its 4 x 104 core allocation.
PI_CONTROL_NODES = 4.0
PI_CONTROL_CORES_PER_NODE = 104
PI_CONTROL_TOTAL_CORES = int(PI_CONTROL_NODES * PI_CONTROL_CORES_PER_NODE)
PI_CONTROL_ALLOCATIONS = RootAllocation(
    subcomponents={
        ESM16_UM7_NAME: FixedAllocation(208, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
        ESM16_MOM5_NAME: FixedAllocation(196, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
        ESM16_CICE5_NAME: FixedAllocation(12),
    },
)


@pytest.fixture(scope="function")
def esm16():
    return ESM16Profiling(Path("/fake/test_path"), Path("/fake/archive_path"))


@pytest.fixture(scope="function")
def pi_control_layout(esm16):
    """The layout of the released ACCESS-ESM1.6 pre-industrial control configuration."""

    layouts = esm16.select_layouts(PI_CONTROL_TOTAL_CORES, allocations=PI_CONTROL_ALLOCATIONS)
    assert len(layouts) == 1, "The PI control allocation should determine the layout uniquely."
    return layouts[0]


def test_esm16_pi_control_layout(pi_control_layout):
    """Test that the layout search reproduces the released ACCESS-ESM1.6 PI control configuration."""

    um7, mom5, cice5 = pi_control_layout.sub_layouts
    assert um7.decomposition.grid.shape == (16, 13)
    assert mom5.decomposition.grid.shape == (14, 14)
    assert cice5.n_ranks == 12
    assert pi_control_layout.idle_cores == 0


def test_esm16_layout_branch_name(esm16, pi_control_layout):
    """Test the layout_branch_name method of ESM16Profiling."""

    assert esm16.layout_branch_name(pi_control_layout) == "esm1p6-layout_atm_16x13_mom_14x14_ice_12x1"


def test_esm16_layout_config_changes(esm16, pi_control_layout):
    """Test the layout_config_changes method of ESM16Profiling."""

    changes = esm16.layout_config_changes(pi_control_layout)
    assert changes["config.yaml"]["submodels"] == [
        [
            {"ncpus": 208},
            {"ncpus": 196},
            {"ncpus": 12, "exe": ["cice_access_360x300_30x300.exe"]},
        ]
    ]
    assert changes["atmosphere/um_env.yaml"] == {
        "UM_ATM_NPROCX": "16",
        "UM_ATM_NPROCY": "13",
        "UM_NPES": "208",
    }
    assert changes["ocean/input.nml"] == {"ocean_model_nml": {"layout": ["14,14"]}}
    assert changes["ice/cice_in.nml"] == {"domain_nml": {"nprocs": ["12"]}}


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

    CICE5 gets a wider band than the other two. Its executables are only available for an exact number of blocks
    per rank, so the core counts it admits are the divisors of ESM16_CICE5_NBLOCKS, and they thin out as the count
    grows: a +/-5% band around its proportional share is [142, 158] at 5200 cores, which contains no divisor of 360
    at all, and no layout would be found.
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


@pytest.mark.parametrize("total_cores", [PI_CONTROL_TOTAL_CORES, 520, 5200])
def test_esm16_caller_supplied_allocations(esm16, total_cores):
    """Test that one fractional allocation strategy generates usable ACCESS-ESM1.6 layouts at every size."""

    layouts = esm16.select_layouts(total_cores, allocations=ESM16_SCALING_ALLOCATIONS)
    assert layouts, f"No layout found for {total_cores} cores."

    for layout in layouts:
        um7, _, cice5 = layout.sub_layouts
        # Executables are only available for an exact number of CICE5 blocks per rank
        assert ESM16_CICE5_NBLOCKS % cice5.n_ranks == 0
        # The UM requires an even number of processes along x
        assert um7.decomposition.grid.shape[0] % 2 == 0


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
        assert ESM16_CICE5_NBLOCKS % layout.sub_layouts[2].n_ranks == 0


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_esm16_generate_scaling_experiments(mock_experiment_generator, esm16):
    """Test that ACCESS-ESM1.6 scaling experiments can be generated end to end."""

    esm16.set_control("https://example.com/repo", "commit")
    esm16.generate_scaling_experiments(
        num_nodes_list=[PI_CONTROL_NODES],
        control_options={},
        cores_per_node=PI_CONTROL_CORES_PER_NODE,
        walltime=2.0,
        allocations=PI_CONTROL_ALLOCATIONS,
    )

    config = mock_experiment_generator.call_args[0][0]
    assert config["model_type"] == "access-esm1.6"
    assert list(config["Perturbation_Experiment"]) == ["Experiment_1"]
    block = config["Perturbation_Experiment"]["Experiment_1"]
    assert block["branches"] == ["esm1p6-layout_atm_16x13_mom_14x14_ice_12x1"]
    assert block["config.yaml"]["walltime"] == "2:00:00"
    assert "esm1p6-layout_atm_16x13_mom_14x14_ice_12x1" in esm16.experiments
