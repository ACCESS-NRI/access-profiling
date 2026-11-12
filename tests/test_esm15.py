# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest
from access.config.parallel_allocation_strategies import FixedAllocation, FreeAllocation, RootAllocation
from access.config.parallel_constraints import SubdomainAspectRatioConstraint
from conftest import grid_of, layout_of, select_layouts

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.control import GitControlSource
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.models.esm15 import (
    ESM15_CICE4_NAME,
    ESM15_CICE4_RANKS,
    ESM15_CONFIGS_REPOSITORY,
    ESM15_HISTORICAL,
    ESM15_HISTORICAL_SOURCE,
    ESM15_MOM5_NAME,
    ESM15_N96_ATMOSPHERE,
    ESM15_PREINDUSTRIAL,
    ESM15_PREINDUSTRIAL_CORES,
    ESM15_PREINDUSTRIAL_SOURCE,
    ESM15_UM7_NAME,
    ESM15Configuration,
)
from access.profiling.payu_manager import PayuManager
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

# The release ran on 9 nodes of 48 cores, every one of them used.
PREINDUSTRIAL_TOTAL_CORES = 432
PREINDUSTRIAL_UM7_GRID = (16, 15)
PREINDUSTRIAL_MOM5_GRID = (18, 10)
PREINDUSTRIAL_BRANCH = "esm1p5-layout_preindustrial+concentrations_atm_16x15_mom_18x10_ice_12"
PREINDUSTRIAL_ALLOCATIONS = RootAllocation(
    subcomponents={
        ESM15_UM7_NAME: FixedAllocation(240, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
        ESM15_MOM5_NAME: FixedAllocation(180, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
        ESM15_CICE4_NAME: FixedAllocation(12),
    },
)


@pytest.fixture(scope="function")
def esm15():
    """The released pre-industrial configuration."""
    return ESM15_PREINDUSTRIAL


@pytest.fixture(scope="function")
def preindustrial_layout(esm15):
    """The released layout, picked out of the ones the search returns by its two process grids."""

    layouts = select_layouts(esm15, PREINDUSTRIAL_TOTAL_CORES, allocations=PREINDUSTRIAL_ALLOCATIONS)
    released = [
        layout
        for layout in layouts
        if (grid_of(layout.sub_layouts[0]), grid_of(layout.sub_layouts[1]))
        == (PREINDUSTRIAL_UM7_GRID, PREINDUSTRIAL_MOM5_GRID)
    ]
    assert len(released) == 1, "The layout search should find the released layout exactly once."
    return released[0]


def test_esm15_names_the_model_payu_runs(esm15):
    """Payu calls ACCESS-ESM1.5 `access`, and the tree's root is the model and the configuration."""

    assert esm15.model_type == "access"
    assert esm15.parallel_component.name == "ACCESS-ESM1.5 preindustrial+concentrations"
    assert sum(ESM15_PREINDUSTRIAL_CORES.values()) == PREINDUSTRIAL_TOTAL_CORES


def test_esm15_reproduces_the_released_layout(preindustrial_layout):
    """Test that the layout search reproduces the released ACCESS-ESM1.5 pre-industrial configuration."""

    um7, mom5, cice4 = preindustrial_layout.sub_layouts
    assert grid_of(um7) == PREINDUSTRIAL_UM7_GRID
    assert grid_of(mom5) == PREINDUSTRIAL_MOM5_GRID
    assert (cice4.n_ranks, cice4.decomposition) == (ESM15_CICE4_RANKS, None)
    assert preindustrial_layout.idle_cores == 0


def test_esm15_experiment_name(esm15, preindustrial_layout):
    assert esm15.experiment_name(preindustrial_layout) == PREINDUSTRIAL_BRANCH


def test_esm15_config_changes(esm15, preindustrial_layout):
    """What the released layout comes to in the configuration files: nothing for the sea ice."""

    changes = esm15.config_changes(preindustrial_layout)

    assert changes["config.yaml"] == {
        "ncpureq": PREINDUSTRIAL_TOTAL_CORES,
        "submodels": [[{"ncpus": 240}, {"ncpus": 180}, {"ncpus": 12}, "PRESERVE"]],
    }
    assert changes["atmosphere/um_env.yaml"] == {"UM_ATM_NPROCX": "16", "UM_ATM_NPROCY": "15", "UM_NPES": "240"}
    assert changes["ocean/input.nml"] == {"ocean_model_nml": {"layout": "18, 10"}}
    assert "ice/cice_in.nml" not in changes, "CICE4's decomposition is compiled in, so there is nothing to write"


def test_esm15_sea_ice_runs_only_on_the_ranks_its_executable_is_built_for(esm15):
    """A strategy leaving the sea ice free still gets 12 ranks, and nothing else, since the model requires it."""

    allocations = RootAllocation(
        subcomponents={
            ESM15_UM7_NAME: FixedAllocation(240),
            ESM15_MOM5_NAME: FixedAllocation(180),
            ESM15_CICE4_NAME: FreeAllocation(min_cores=6, max_cores=24),
        },
    )
    layouts = select_layouts(esm15, PREINDUSTRIAL_TOTAL_CORES, allocations=allocations)

    assert layouts
    assert {layout.sub_layouts[2].n_ranks for layout in layouts} == {ESM15_CICE4_RANKS}


def test_esm15_refuses_a_sea_ice_built_for_no_ranks():
    with pytest.raises(ValueError, match="must be positive"):
        ESM15Configuration(name="none", atmosphere=ESM15_N96_ATMOSPHERE, sea_ice=0)


def test_esm15_historical_is_the_same_layout_under_another_name(esm15, preindustrial_layout):
    """Both releases run the same components on the same grids and the same cores, so the name tells them apart."""

    assert ESM15_HISTORICAL.parallel_component.subcomponents == esm15.parallel_component.subcomponents
    assert ESM15_HISTORICAL.experiment_name(preindustrial_layout) == PREINDUSTRIAL_BRANCH.replace(
        "preindustrial", "historical"
    )


def test_esm15_sources_point_at_the_releases():
    for source, release in (
        (ESM15_PREINDUSTRIAL_SOURCE, "release-preindustrial+concentrations-2.0"),
        (ESM15_HISTORICAL_SOURCE, "release-historical+concentrations-2.0"),
    ):
        assert (source.repository, source.start_point) == (ESM15_CONFIGS_REPOSITORY, release)


class TestESM15Logs:
    """Which profiling logs a configuration says it writes, and where they are."""

    @staticmethod
    def _output(tmp_path: Path) -> Path:
        (tmp_path / "atmosphere" / "pe_output").mkdir(parents=True)
        (tmp_path / "atmosphere" / "um_env.yaml").write_text("UM_STDOUT_FILE: pe_output/atm.fort6.pe\n")
        (tmp_path / "atmosphere" / "pe_output" / "atm.fort6.pe0").write_text("")
        (tmp_path / "config.yaml").write_text("model: access\n")
        (tmp_path / "access.out").write_text("")
        (tmp_path / "ice").mkdir()
        (tmp_path / "ice" / "ice_diag.d").write_text("")
        return tmp_path

    def test_every_log_is_found_and_read_with_its_own_parser(self, esm15, tmp_path):
        """CICE4 prints its timers in CICE5's format, so the CICE5 parser reads its log."""

        logs = esm15.component_logs(self._output(tmp_path))

        assert set(logs) == {"UM", "UM_Total_Walltime", "MOM5", "CICE4"}
        assert isinstance(logs["UM"].parser, UMProfilingParser)
        assert isinstance(logs["UM_Total_Walltime"].parser, UMTotalRuntimeParser)
        assert isinstance(logs["MOM5"].parser, FMSProfilingParser)
        assert isinstance(logs["CICE4"].parser, CICE5ProfilingParser)
        assert logs["CICE4"].filepath == tmp_path / "ice" / "ice_diag.d"

    def test_each_log_names_the_component_it_was_run_on(self, esm15):
        assert esm15.component_for_log("UM") == ESM15_UM7_NAME
        assert esm15.component_for_log("MOM5") == ESM15_MOM5_NAME
        assert esm15.component_for_log("CICE4") == ESM15_CICE4_NAME


def test_esm15_parse_layout_reads_the_cores_of_every_submodel(esm15, tmp_path):
    (tmp_path / "config.yaml").write_text(
        "model: access\nsubmodels:\n  - name: atmosphere\n    ncpus: 240\n  - name: ocean\n    ncpus: 180\n"
        "  - name: ice\n    ncpus: 12\n  - name: coupler\n    ncpus: 0\n"
    )

    layout = layout_of(esm15.parse_layout(tmp_path))

    assert {sub.name: sub.n_cores for sub in layout.sub_layouts} == ESM15_PREINDUSTRIAL_CORES
    assert layout.n_cores == PREINDUSTRIAL_TOTAL_CORES


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_esm15_generate_scaling_experiments(mock_experiment_generator, esm15):
    """Test that ACCESS-ESM1.5 scaling experiments can be generated end to end, as Payu's `access` model."""

    manager = PayuManager(
        Path("/fake/test_path"),
        Path("/fake/archive_path"),
        esm15,
        GitControlSource("https://example.com/repo", "commit"),
    )
    manager.generate_scaling_experiments(
        num_nodes_list=[9.0], cores_per_node=48, walltime=2.0, allocations=PREINDUSTRIAL_ALLOCATIONS
    )

    config = mock_experiment_generator.call_args[0][0]
    assert config["model_type"] == "access"
    assert PREINDUSTRIAL_BRANCH in manager.experiments
