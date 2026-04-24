# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest
from access.config.esm1p6_layout_input import LayoutSearchConfig
from access.config.layout_config import LayoutTuple

from access.profiling.experiment import ProfilingLog
from access.profiling.manager import ProfilingManager
from access.profiling.payu_manager import PayuManager, ProfilingExperimentStatus


class MockPayuManager(PayuManager):
    """Test class inheriting from PayuConfigProfiling to test its methods."""

    @property
    def model_type(self) -> str:
        return "mock-payu-model"

    def get_component_logs(self, path):
        return {"component": ProfilingLog(path, mock.MagicMock())}

    def generate_core_layouts_from_node_count(
        self, num_nodes: float, cores_per_node: int, layout_search_config: LayoutSearchConfig | None = None
    ) -> list:
        """This method is to be mocked in tests that call generate_scaling_experiments."""
        raise NotImplementedError()

    def generate_perturbation_block(self, layout, branch_name_prefix: str) -> dict:
        """This method is to be mocked in tests that call generate_scaling_experiments."""
        raise NotImplementedError()


@pytest.fixture(scope="function")
def manager():
    return MockPayuManager(Path("/fake/test_path"), Path("/fake/archive_path"))


def test_nruns(manager):
    """Test the nruns property of PayuManager."""
    # Default value
    assert manager.nruns == 1

    # Set valid value
    manager.nruns = 5
    assert manager.nruns == 5

    # Zero value is also valid
    manager.nruns = 0
    assert manager.nruns == 0

    # Set invalid value
    with pytest.raises(ValueError):
        manager.nruns = -1


def test_startfrom_restart(manager):
    """Test the startfrom_restart property of PayuManager."""
    # Default value
    assert manager.startfrom_restart == "cold"

    # Set value
    manager.startfrom_restart = "restart000"
    assert manager.startfrom_restart == "restart000"


def test_set_control(manager):
    """Test the set_control method of PayuManager."""
    repository = "https://github.com/example/repo.git"
    commit = "abc123def456"

    manager.set_control(repository, commit)

    assert manager._repository == repository
    assert manager._control_commit == commit


@mock.patch("access.profiling.payu_manager.YAMLParser")
@mock.patch("access.profiling.payu_manager.Path.read_text", return_value="mock config content")
def test_ncpus(mock_read_text, mock_yaml_parser, manager):
    """Test the parse_ncpus method of PayuManager."""

    # Mock the YAMLParser to return the number of cpus
    mock_yaml_parser().parse.return_value = {"ncpus": 4}
    ncpus = manager.parse_ncpus(Path("/fake/path"))
    assert mock_read_text.call_count == 1
    assert ncpus == 4

    # Mock the YAMLParser to return dictionary of submodels
    mock_yaml_parser().parse.return_value = {"submodels": [{"ncpus": 2}, {"ncpus": 3}]}
    ncpus = manager.parse_ncpus(Path("/fake/path"))
    assert mock_read_text.call_count == 2
    assert ncpus == 5


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_generate_scaling_experiments_basic(mock_experiment_generator, manager):
    """Test the generate_scaling_experiments method with basic inputs."""
    manager.set_control("https://github.com/example/repo.git", "abc123")

    with (
        mock.patch.object(manager, "generate_core_layouts_from_node_count") as mock_layout_generator,
        mock.patch.object(manager, "generate_perturbation_block") as mock_perturbation_block,
    ):
        mock_layout_generator.side_effect = [
            [LayoutTuple(1, 2, 3, 4, 5), LayoutTuple(6, 7, 8, 9, 10)],
            [LayoutTuple(11, 12, 13, 14, 15), LayoutTuple(1, 2, 3, 4, 5)],
        ]
        mock_perturbation_block.side_effect = [
            {"branches": ["pert1"], "config.yaml": {}},
            {"branches": ["pert2"], "config.yaml": {}},
            {"branches": ["pert3"], "config.yaml": {}},
            {"branches": ["pert4"], "config.yaml": {}},
        ]
        manager.generate_scaling_experiments(
            num_nodes_list=[2.0, 4.0],
            control_options={"option1": "value1"},
            cores_per_node=48,
            tol_around_ctrl_ratio=0.1,
            max_wasted_ncores_frac=0.2,
            walltime=5.0,
        )

    # Verify ExperimentGenerator was called
    assert mock_experiment_generator.call_count == 1

    # Verify the configuration passed to ExperimentGenerator
    call_args = mock_experiment_generator.call_args[0][0]
    assert call_args["model_type"] == "mock-payu-model"
    assert call_args["repository_url"] == "https://github.com/example/repo.git"
    assert call_args["start_point"] == "abc123"
    assert call_args["test_path"] == "/fake/test_path"
    assert call_args["repository_directory"] == "config"
    assert call_args["control_branch_name"] == "ctrl"
    assert call_args["Control_Experiment"] == {"option1": "value1"}

    # Verify experiments were added
    assert len(manager.experiments) == 3  # 2 layouts × 2 nodes miunus 1 duplicate


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_generate_scaling_experiments_callable_parameters(mock_experiment_generator, manager):
    """Test generate_scaling_experiments with callable walltime and max_wasted_ncores_frac."""
    manager.set_control("https://github.com/example/repo.git", "abc123")

    # Callable functions
    def walltime_func(num_nodes):
        return num_nodes * 2.5

    def max_wasted_func(num_nodes):
        return 0.1 + (num_nodes * 0.02)

    with (
        mock.patch.object(manager, "generate_core_layouts_from_node_count") as mock_layout_generator,
        mock.patch.object(manager, "generate_perturbation_block") as mock_perturbation_block,
        mock.patch(
            "access.profiling.payu_manager.LayoutSearchConfig", wraps=LayoutSearchConfig
        ) as mock_layout_search_config,
    ):
        mock_layout_generator.side_effect = [
            [LayoutTuple(1, 2, 3, 4, 5)],
            [LayoutTuple(11, 12, 13, 14, 15)],
        ]
        mock_perturbation_block.side_effect = [
            {"branches": ["pert1"], "config.yaml": {}},
            {"branches": ["pert2"], "config.yaml": {}},
        ]
        manager.generate_scaling_experiments(
            num_nodes_list=[2.0, 4.0],
            control_options={},
            cores_per_node=48,
            tol_around_ctrl_ratio=0.1,
            max_wasted_ncores_frac=max_wasted_func,
            walltime=walltime_func,
        )

    # Verify layout generation called with correct max_wasted_ncores_frac
    assert mock_layout_search_config.call_count == 2
    assert mock_layout_search_config.call_args_list[0][1]["max_wasted_ncores_frac"] == max_wasted_func(2.0)
    assert mock_layout_search_config.call_args_list[1][1]["max_wasted_ncores_frac"] == max_wasted_func(4.0)

    # Verify ExperimentGenerator was called
    assert mock_experiment_generator.call_count == 1

    # Verify the configuration passed to ExperimentGenerator has correct walltime
    call_args = mock_experiment_generator.call_args[0][0]
    assert (
        call_args["Perturbation_Experiment"]["Experiment_1"]["config.yaml"]["walltime"] == "5:00:00"
    )  # 2.0 nodes * 2.5 hrs
    assert (
        call_args["Perturbation_Experiment"]["Experiment_2"]["config.yaml"]["walltime"] == "10:00:00"
    )  # 4.0 nodes * 2.5 hrs


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_generate_scaling_experiments_no_layouts(mock_experiment_generator, manager):
    """Test generate_scaling_experiments when no layouts are found for some nodes."""
    manager.set_control("https://github.com/example/repo.git", "abc123")

    with (
        mock.patch.object(manager, "generate_core_layouts_from_node_count") as mock_layout_generator,
        mock.patch.object(manager, "generate_perturbation_block") as mock_perturbation_block,
    ):
        mock_layout_generator.side_effect = [
            [LayoutTuple(1, 2, 3, 4, 5), LayoutTuple(6, 7, 8, 9, 10)],
            [],
        ]
        mock_perturbation_block.side_effect = [
            {"branches": ["pert1"], "config.yaml": {}},
            {"branches": ["pert2"], "config.yaml": {}},
        ]
        manager.generate_scaling_experiments(
            num_nodes_list=[2.0, 4.0],
            control_options={},
            cores_per_node=48,
            tol_around_ctrl_ratio=0.1,
            max_wasted_ncores_frac=0.2,
            walltime=5.0,
        )

    # Verify ExperimentGenerator was called
    assert mock_experiment_generator.call_count == 1

    # Verify only experiments for nodes with layouts were added
    assert len(manager.experiments) == 2


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_run_experiments(mock_experiment_runner, manager):
    """Test the run_experiments method of PayuManager."""

    with mock.patch.dict(
        manager.experiments,
        {
            "branch1": mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1")),
            "branch2": mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch2")),
            "branch3": mock.MagicMock(status=ProfilingExperimentStatus.DONE, path=Path("branch3")),
        },
    ):
        manager.run_experiments()
        expected_call = {
            "test_path": Path("/fake/test_path"),
            "repository_directory": "config",
            "running_branches": ["branch1", "branch2"],
            "keep_uuid": True,
            "nruns": [1, 1],
            "startfrom_restart": ["cold", "cold"],
        }
        mock_experiment_runner.assert_called_once_with(expected_call)

    # Rerun again with no NEW experiments
    with mock.patch.dict(
        manager.experiments,
        {
            "branch1": mock.MagicMock(status=ProfilingExperimentStatus.DONE, path=Path("branch1")),
            "branch2": mock.MagicMock(status=ProfilingExperimentStatus.DONE, path=Path("branch2")),
            "branch3": mock.MagicMock(status=ProfilingExperimentStatus.RUNNING, path=Path("branch3")),
        },
    ):
        mock_experiment_runner.reset_mock()
        manager.run_experiments()
        mock_experiment_runner.assert_not_called()


@mock.patch.object(ProfilingManager, "archive_experiments")
def test_archive_experiments(mock_archive, manager):
    """Test the archive_experiments method of PayuManager.

    The only thing to test here is that the correct exclude files and dirs are passed to the parent method.
    """

    # No arguments passed
    manager.archive_experiments()
    assert mock_archive.call_count == 1
    mock_archive.assert_called_with(
        exclude_dirs=[".git", "restart*"], exclude_files=["*.nc"], follow_symlinks=True, overwrite=False
    )
    mock_archive.reset_mock()

    # Custom arguments passed
    manager.archive_experiments(exclude_dirs=["dir1"], exclude_files=["file1"])
    assert mock_archive.call_count == 1
    mock_archive.assert_called_with(
        exclude_dirs=["dir1"], exclude_files=["file1"], follow_symlinks=True, overwrite=False
    )


@mock.patch("access.profiling.payu_manager.Path.is_dir")
@mock.patch("access.profiling.payu_manager.Path.glob")
def test_profiling_logs_missing_directories(mock_glob, mock_is_dir, manager):
    """Test the profiling_logs method of PayuManager with missing directories."""

    # Missing archive directory
    mock_is_dir.return_value = False
    with pytest.raises(FileNotFoundError):
        manager.profiling_logs(Path("/fake/path"))
    mock_is_dir.assert_called_once()

    # Missing output directories
    mock_is_dir.return_value = True
    mock_glob.return_value = []
    with pytest.raises(FileNotFoundError):
        manager.profiling_logs(Path("/fake/path"))
    mock_glob.assert_called_with("output*")


def path_glob_side_effect(pattern):
    """Side effect function for Path.glob to simulate different directory contents."""

    if pattern == "payu_jobs/*/run/*.json":
        return [Path("payu_jobs/job1/run/log1.json"), Path("payu_jobs/job2/run/log2.json")]
    elif pattern == "output*":
        return [Path("output1"), Path("output2")]
    else:
        return []


@mock.patch.object(Path, "is_dir", return_value=True)
@mock.patch.object(Path, "glob", side_effect=path_glob_side_effect)
def test_profiling_logs(mock_glob, mock_is_dir, manager):
    """Test the profiling_logs method of PayuManager."""

    with mock.patch.object(manager, "get_component_logs", wraps=manager.get_component_logs) as mock_get_logs:
        logs = manager.profiling_logs(Path("/fake/path"))
        # Check correct path access
        assert mock_is_dir.call_count == 1  # Called to check archive directory
        assert mock_glob.call_count == 2  # Called for payu_jobs and output directories
        assert mock_get_logs.call_count == 1
        mock_get_logs.assert_called_with(Path("output1"))

        # Check returned datasets
        assert "payu" in logs
        assert isinstance(logs["payu"], ProfilingLog)
        assert "component" in logs
        assert isinstance(logs["component"], ProfilingLog)


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_rejects_all_branches_and_branches(mock_experiment_runner, manager):
    """delete_experiments raises an error if both all_branches and branches are provided."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))

    with pytest.raises(ValueError):
        manager.delete_experiments(branches=["branch1"], all_branches=True)

    mock_experiment_runner.assert_not_called()


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_no_branches_or_all_branches(mock_experiment_runner, manager):
    """delete_experiments raises an error if neither branches nor all_branches is provided."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))

    with pytest.raises(ValueError):
        manager.delete_experiments()

    mock_experiment_runner.assert_not_called()


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_all_branches_but_no_experiments(mock_experiment_runner, manager):
    """delete_experiments raises an error if all_branches is True but there are no experiments."""
    manager.experiments.clear()

    manager.delete_experiments(all_branches=True)
    mock_experiment_runner.assert_not_called()


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_rejects_unmanaged_branches(mock_experiment_runner, manager):
    """delete_experiments raises an error if branches are provided that are not in the manager experiments."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))
    manager.experiments["branch2"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch2"))

    with pytest.raises(KeyError):
        manager.delete_experiments(branches=["branch2", "branch3"])

    mock_experiment_runner.assert_not_called()


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_valid_branches(mock_experiment_runner, manager):
    """delete_experiments calls ExperimentRunner with correct branches when valid branches are provided."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))
    manager.experiments["branch2"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch2"))
    manager.experiments["branch3"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch3"))

    runner = mock_experiment_runner.return_value

    manager.delete_experiments(branches=["branch3", "branch1"])

    mock_experiment_runner.assert_called_once_with({"test_path": manager.work_dir, "repository_directory": "config"})

    assert runner.delete_experiments.call_count == 1
    _, kwargs = runner.delete_experiments.call_args
    assert set(kwargs["branches"]) == {"branch3", "branch1"}
    assert kwargs["dry_run"] is False
    assert kwargs["remove_repo_dir"] is False


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_dry_run_does_not_modify_state(mock_experiment_runner, manager):
    """delete_experiments with dry_run=True does not modify the manager state."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))
    manager.experiments["branch2"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch2"))

    manager.delete_experiments(branches=["branch1"], dry_run=True)

    assert set(manager.experiments.keys()) == {"branch1", "branch2"}


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_non_dry_run_removes_from_state(mock_experiment_runner, manager):
    """delete_experiments with dry_run=False removes deleted branches from the manager state."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))
    manager.experiments["branch2"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch2"))

    manager.delete_experiments(branches=["branch1"], dry_run=False)

    assert set(manager.experiments.keys()) == {"branch2"}
