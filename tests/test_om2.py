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
from access.profiling.models.om2 import (
    OM2_01DEG,
    OM2_01DEG_CORES,
    OM2_025DEG,
    OM2_025DEG_CORES,
    OM2_1DEG,
    OM2_1DEG_CORES,
    OM2_CICE5_NAME,
    OM2_MOM5_NAME,
    OM2_YATM_NAME,
    OM2Configuration,
)
from access.profiling.payu_manager import PayuManager

# YATM on 1 core, MOM5 on its 16 x 15 process grid unmasked, CICE5 on 24: the release's layout, bar the mask.
ONE_DEGREE_TOTAL_CORES = 265
ONE_DEGREE_MOM5_GRID = (16, 15)
ONE_DEGREE_BRANCH = "om2-layout_1deg_atm_1_mom_16x15_ice_24"


def _pinned(mom5: int, cice5: int) -> RootAllocation:
    """An allocation pinning every component, with near-square ocean subdomains."""
    return RootAllocation(
        subcomponents={
            OM2_YATM_NAME: FixedAllocation(1),
            OM2_MOM5_NAME: FixedAllocation(mom5, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
            OM2_CICE5_NAME: FixedAllocation(cice5),
        },
    )


def _layout_with(configuration: OM2Configuration, total_cores: int, mom5: int, cice5: int, grid: tuple[int, int]):
    """The one layout of the given cores whose ocean is split over the given process grid."""
    layouts = select_layouts(configuration, total_cores, allocations=_pinned(mom5, cice5))
    picked = [layout for layout in layouts if grid_of(layout.sub_layouts[1]) == grid]
    assert len(picked) == 1, f"The layout search should find the ocean's {grid} process grid exactly once."
    return picked[0]


@pytest.fixture(scope="function")
def one_degree_layout():
    return _layout_with(OM2_1DEG, ONE_DEGREE_TOTAL_CORES, 240, 24, ONE_DEGREE_MOM5_GRID)


def test_om2_names_the_model_payu_runs():
    assert {preset.model_type for preset in (OM2_1DEG, OM2_025DEG, OM2_01DEG)} == {"access-om2"}
    assert [preset.name for preset in (OM2_1DEG, OM2_025DEG, OM2_01DEG)] == ["1deg", "025deg", "01deg"]
    assert OM2_1DEG.parallel_component.name == "ACCESS-OM2 1deg"


def test_om2_1deg_reproduces_the_released_layout(one_degree_layout):
    """Test that the search reproduces release-1deg_jra55_ryf-4.1, with its ocean unmasked."""

    yatm, mom5, cice5 = one_degree_layout.sub_layouts
    assert (yatm.n_ranks, yatm.decomposition) == (1, None)
    assert grid_of(mom5) == ONE_DEGREE_MOM5_GRID
    assert grid_of(cice5) == (24,), "the 24 block columns the control's block size makes, one per rank"
    assert one_degree_layout.idle_cores == 0
    assert OM2_1DEG_CORES[OM2_CICE5_NAME] == 24 and OM2_1DEG_CORES[OM2_YATM_NAME] == 1


def test_om2_1deg_experiment_name(one_degree_layout):
    assert OM2_1DEG.experiment_name(one_degree_layout) == ONE_DEGREE_BRANCH


def test_om2_1deg_config_changes(one_degree_layout):
    """What the released layout comes to: the ocean's grid, and the sea ice's rank count, grid and blocks per rank."""

    changes = OM2_1DEG.config_changes(one_degree_layout)

    assert changes["config.yaml"] == {
        "ncpureq": ONE_DEGREE_TOTAL_CORES,
        "submodels": [[{"ncpus": 1}, {"ncpus": 240}, {"ncpus": 24, "exe": ["cice_auscom.exe"]}]],
    }
    assert changes["ocean/input.nml"] == {"ocean_model_nml": {"layout": "16, 15"}}
    assert changes["ice/cice_in.nml"] == {
        "domain_nml": {"nprocs": "24", "nx_global": "360", "ny_global": "300", "max_blocks": "1"}
    }
    assert "atmosphere/um_env.yaml" not in changes, "YATM has no decomposition to write"


def test_om2_yatm_is_serial():
    """A strategy leaving the data atmosphere a band of cores still gets one, since the model requires it."""

    allocations = RootAllocation(
        subcomponents={
            OM2_YATM_NAME: FreeAllocation(min_cores=1, max_cores=4),
            OM2_MOM5_NAME: FixedAllocation(240),
            OM2_CICE5_NAME: FixedAllocation(24),
        },
    )
    layouts = select_layouts(OM2_1DEG, 268, allocations=allocations)

    assert layouts
    assert {layout.sub_layouts[0].n_ranks for layout in layouts} == {1}


def test_om2_1deg_sea_ice_shares_out_its_block_columns():
    """The control pins 15 x 300 blocks, 24 columns of them, so CICE5's ranks divide 24 and each holds its share."""

    allocations = RootAllocation(
        subcomponents={
            OM2_YATM_NAME: FixedAllocation(1),
            OM2_MOM5_NAME: FixedAllocation(240, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
            OM2_CICE5_NAME: FreeAllocation(min_cores=1, max_cores=30),
        },
    )
    layouts = select_layouts(OM2_1DEG, ONE_DEGREE_TOTAL_CORES, allocations=allocations)

    ranks = {layout.sub_layouts[2].n_ranks for layout in layouts}
    assert ranks == {1, 2, 3, 4, 6, 8, 12, 24}
    for layout in layouts:
        n_ranks = layout.sub_layouts[2].n_ranks
        domain_nml = OM2_1DEG.config_changes(layout)["ice/cice_in.nml"]["domain_nml"]
        assert domain_nml["max_blocks"] == str(24 // n_ranks)
        assert "block_size_x" not in domain_nml, "the block size is the control's"


@pytest.mark.parametrize(
    ("configuration", "cores", "total_cores", "mom5", "cice5", "grid"),
    [
        pytest.param(OM2_025DEG, OM2_025DEG_CORES, 2282, 1920, 361, (48, 40), id="025deg"),
        pytest.param(OM2_01DEG, OM2_01DEG_CORES, 6723, 6000, 722, (80, 75), id="01deg"),
    ],
)
def test_om2_finer_resolutions_choose_the_sea_ice_rank_count_alone(
    configuration, cores, total_cores, mom5, cice5, grid
):
    """Roundrobin and sectrobin form no process grid, so the sea ice is given its ranks and nothing else.

    The ocean runs on its release's process grid unmasked, which is more cores than the masked release count.
    """

    layout = _layout_with(configuration, total_cores, mom5, cice5, grid)

    assert layout.sub_layouts[2].decomposition is None
    assert layout.idle_cores == 0
    assert cores[OM2_CICE5_NAME] == cice5 and cores[OM2_MOM5_NAME] < mom5

    changes = configuration.config_changes(layout)
    nx, ny = configuration.ocean.shape
    assert changes["ice/cice_in.nml"] == {
        "domain_nml": {"nprocs": str(cice5), "nx_global": str(nx), "ny_global": str(ny), "max_blocks": "-1"}
    }
    assert configuration.experiment_name(layout).endswith(f"_mom_{grid[0]}x{grid[1]}_ice_{cice5}")


def test_om2_the_sea_ice_runs_on_the_ocean_grid():
    for preset in (OM2_1DEG, OM2_025DEG, OM2_01DEG):
        assert preset.sea_ice.grid == preset.ocean.shape


def test_om2_needs_a_name():
    with pytest.raises(ValueError, match="name must be non-empty"):
        OM2Configuration(name="", ocean=OM2_1DEG.ocean, sea_ice=OM2_1DEG.sea_ice)


class TestOM2Logs:
    """Which profiling logs a configuration says it writes, and where they are."""

    @staticmethod
    def _output(tmp_path: Path) -> Path:
        (tmp_path / "config.yaml").write_text("model: access-om2\n")
        (tmp_path / "access-om2.out").write_text("")
        (tmp_path / "ice").mkdir()
        (tmp_path / "ice" / "ice_diag.d").write_text("")
        return tmp_path

    def test_the_ocean_and_the_sea_ice_write_logs_and_the_data_atmosphere_none(self, tmp_path):
        logs = OM2_1DEG.component_logs(self._output(tmp_path))

        assert set(logs) == {"MOM5", "CICE5"}
        assert isinstance(logs["MOM5"].parser, FMSProfilingParser)
        assert logs["MOM5"].filepath == tmp_path / "access-om2.out"
        assert isinstance(logs["CICE5"].parser, CICE5ProfilingParser)

    def test_each_log_names_the_component_it_was_run_on(self):
        assert OM2_1DEG.component_for_log("MOM5") == OM2_MOM5_NAME
        assert OM2_1DEG.component_for_log("CICE5") == OM2_CICE5_NAME
        assert OM2_1DEG.component_for_log("YATM") is None


def test_om2_parse_layout_reads_the_cores_of_every_submodel(tmp_path):
    (tmp_path / "config.yaml").write_text(
        "model: access-om2\nsubmodels:\n  - name: atmosphere\n    ncpus: 1\n  - name: ocean\n    ncpus: 216\n"
        "  - name: ice\n    ncpus: 24\n"
    )

    layout = layout_of(OM2_1DEG.parse_layout(tmp_path))

    assert {sub.name: sub.n_cores for sub in layout.sub_layouts} == OM2_1DEG_CORES
    assert layout.n_cores == 241


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_om2_generate_scaling_experiments(mock_experiment_generator):
    """Test that ACCESS-OM2 scaling experiments can be generated end to end."""

    manager = PayuManager(
        Path("/fake/test_path"),
        Path("/fake/archive_path"),
        OM2_1DEG,
        GitControlSource("https://example.com/repo", "release-1deg_jra55_ryf-4.1"),
    )
    manager.generate_scaling_experiments(
        num_nodes_list=[5.0], cores_per_node=53, walltime=2.0, allocations=_pinned(240, 24)
    )

    config = mock_experiment_generator.call_args[0][0]
    assert config["model_type"] == "access-om2"
    assert ONE_DEGREE_BRANCH in manager.experiments


RUNTIME_CICE_IN = """&domain_nml
    block_size_x = 15
    block_size_y = 300
    distribution_type = 'cartesian'
    nprocs = 24
    nx_global = 360
    ny_global = 300
    processor_shape = 'slenderX1'
/
"""


def _om2_control(path: Path, cice_in: str, types: tuple[str, str, str] = ("yatm", "mom", "cice5")) -> Path:
    """Writes a control the way the ACCESS-OM2 releases are laid out, as far as the reader looks."""

    lines = ["jobname: 1deg_jra55_ryf", "model: access-om2", "submodels:"]
    for name, model, ncpus in zip(("atmosphere", "ocean", "ice"), types, (1, 216, 24), strict=True):
        lines += [f"  - name: {name}", f"    model: {model}", f"    ncpus: {ncpus}"]
    (path / "config.yaml").write_text("\n".join(lines) + "\n")
    (path / "ice").mkdir()
    (path / "ice" / "cice_in.nml").write_text(cice_in)
    return path


class TestOM2FromControl:
    """Reading a configuration off a control's own files."""

    def test_a_runtime_grid_control_reads_as_the_preset_of_its_resolution(self, tmp_path):
        control = _om2_control(tmp_path, RUNTIME_CICE_IN)

        assert OM2Configuration.from_control(control, name="1deg") == OM2_1DEG
        assert OM2Configuration.from_control(control).name == "1deg_jra55_ryf", "the jobname, with no name given"

    def test_a_control_whose_cice5_is_built_for_its_grid_is_refused(self, tmp_path):
        """The -4.1 and _bgc releases: their namelist states no grid, so nothing here can tell it."""

        control = _om2_control(
            tmp_path,
            "&domain_nml\n    distribution_type = 'cartesian'\n    nprocs = 24\n    processor_shape = 'slenderX1'\n/\n",
        )

        with pytest.raises(ValueError, match="access-om2/2026.08.002"):
            OM2Configuration.from_control(control)

    def test_a_control_of_another_model_is_refused(self, tmp_path):
        control = _om2_control(tmp_path, RUNTIME_CICE_IN, types=("um", "mom", "cice5"))

        with pytest.raises(ValueError, match="where ACCESS-OM2 runs yatm, mom and cice5"):
            OM2Configuration.from_control(control)
