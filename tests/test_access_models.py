# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest
from access.config import YAMLParser
from access.config.parallel_allocation_strategies import FixedAllocation, RootAllocation
from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import SubdomainAspectRatioConstraint
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid

from access.profiling.access_models import (
    ESM16_CICE5_NAME,
    ESM16_CICE5_NBLOCKS,
    ESM16_MOM5_NAME,
    ESM16_UM7_NAME,
    ESM16Profiling,
    RAM3Profiling,
    esm16_scaling_allocations,
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
PI_CONTROL_TOTAL_CORES = 416
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


def test_esm16_layout_requires_esm16_layout(esm16, pi_control_layout):
    """Test that the ESM1.6 layout methods reject layouts of other models."""

    # A layout that does not have the ACCESS-ESM1.6 components
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

    # A layout whose components have no domain decomposition
    no_decomposition = ComponentLayout(
        name=pi_control_layout.name,
        n_cores=pi_control_layout.n_cores,
        n_ranks=pi_control_layout.n_ranks,
        threads_per_rank=None,
        decomposition=None,
        sub_layouts=tuple(
            ComponentLayout(
                name=sub.name, n_cores=sub.n_cores, n_ranks=sub.n_ranks, threads_per_rank=1, decomposition=None
            )
            for sub in pi_control_layout.sub_layouts
        ),
    )
    with pytest.raises(ValueError):
        esm16.layout_config_changes(no_decomposition)


@pytest.mark.parametrize("total_cores", [PI_CONTROL_TOTAL_CORES, 520, 5200])
def test_esm16_scaling_allocations(esm16, total_cores):
    """Test that the ACCESS-ESM1.6 allocation strategy generates usable layouts."""

    layouts = esm16.select_layouts(total_cores, allocations=esm16_scaling_allocations(total_cores))
    assert layouts, f"No layout found for {total_cores} cores."

    for layout in layouts:
        um7, _, cice5 = layout.sub_layouts
        # Executables are only available for an exact number of CICE5 blocks per rank
        assert ESM16_CICE5_NBLOCKS % cice5.n_ranks == 0
        # The UM requires an even number of processes along x
        assert um7.decomposition.grid.shape[0] % 2 == 0
        # The allocation strategy bounds the number of cores left unused
        assert layout.idle_cores / layout.n_cores <= 0.02


def test_esm16_scaling_allocations_pins_ice_to_a_divisor(esm16):
    """Test that CICE5 is always given a number of cores that divides the number of CICE5 blocks.

    A range of core counts would often contain no such value, and the search would then find no layout at all.
    """

    for total_cores in (PI_CONTROL_TOTAL_CORES, 520, 1000, 2080, 5000, 5200, 10400):
        ice_cores = esm16_scaling_allocations(total_cores).subcomponents[ESM16_CICE5_NAME].n_cores
        assert ESM16_CICE5_NBLOCKS % ice_cores == 0, f"{ice_cores} cores does not divide {ESM16_CICE5_NBLOCKS}."


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_esm16_generate_scaling_experiments(mock_experiment_generator, esm16):
    """Test that ACCESS-ESM1.6 scaling experiments can be generated end to end."""

    esm16.set_control("https://example.com/repo", "commit")
    esm16.generate_scaling_experiments(
        total_cores_list=[PI_CONTROL_TOTAL_CORES],
        control_options={},
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
