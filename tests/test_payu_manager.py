# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest
from access.config.parallel_allocation_strategies import FixedAllocation, RootAllocation
from access.config.parallel_component import ParallelComponent
from access.config.parallel_constraints import FixedThreadsPerRankConstraint
from access.config.parallel_domain import Domain

from access.profiling.experiment import ProfilingLog
from access.profiling.manager import ProfilingManager
from access.profiling.payu_manager import PayuManager, ProfilingExperiment, ProfilingExperimentStatus

# A model that has nothing to do with any real one, so that the layout machinery of PayuManager is tested without
# involving the specifics of a particular model. On 4 cores split evenly it has exactly 4 layouts, which differ only
# in the shape of their process grids.
MOCK_COMPONENT = ParallelComponent(
    name="mock-model",
    subcomponents=(
        ParallelComponent("atm", domain=Domain((8, 8)), local_constraints=(FixedThreadsPerRankConstraint(1),)),
        ParallelComponent("ocn", domain=Domain((8, 8)), local_constraints=(FixedThreadsPerRankConstraint(1),)),
    ),
)
MOCK_ALLOCATIONS = RootAllocation(subcomponents={"atm": FixedAllocation(2), "ocn": FixedAllocation(2)})


class MockPayuManager(PayuManager):
    """Test class inheriting from PayuConfigProfiling to test its methods."""

    @property
    def model_type(self) -> str:
        return "mock-payu-model"

    def get_component_logs(self, path):
        return {"component": ProfilingLog(path, mock.MagicMock())}

    @property
    def parallel_component(self) -> ParallelComponent:
        return MOCK_COMPONENT

    def layout_branch_name(self, layout) -> str:
        atm, ocn = layout.sub_layouts
        atm_nx, atm_ny = atm.decomposition.grid.shape
        ocn_nx, ocn_ny = ocn.decomposition.grid.shape
        return f"mock_atm_{atm_nx}x{atm_ny}_ocn_{ocn_nx}x{ocn_ny}"

    def layout_config_changes(self, layout) -> dict:
        atm, ocn = layout.sub_layouts
        return {"config.yaml": {"submodels": [[{"ncpus": atm.n_cores}, {"ncpus": ocn.n_cores}]]}}


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


def test_select_layouts(manager):
    """Test the select_layouts method of PayuManager."""

    layouts = manager.select_layouts(4, allocations=MOCK_ALLOCATIONS)
    assert len(layouts) == 4
    assert all(layout.idle_cores == 0 for layout in layouts)
    assert all(layout.n_cores == 4 for layout in layouts)

    # Sorted by increasing number of idle cores
    assert [layout.idle_cores for layout in layouts] == sorted(layout.idle_cores for layout in layouts)

    # Enumeration is bounded, and the caller is told about it
    assert len(manager.select_layouts(4, allocations=MOCK_ALLOCATIONS, max_layouts=2)) == 2

    # An allocation that does not fit the budget yields nothing
    too_big = RootAllocation(subcomponents={"atm": FixedAllocation(100), "ocn": FixedAllocation(100)})
    assert manager.select_layouts(4, allocations=too_big) == []


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_generate_scaling_experiments(mock_experiment_generator, manager):
    """Test the generate_scaling_experiments method of PayuManager."""

    manager.set_control("https://example.com/repo", "commit")
    manager.generate_scaling_experiments(
        num_nodes_list=[1.0],
        control_options={"some": "option"},
        cores_per_node=4,
        walltime=2.0,
        allocations=MOCK_ALLOCATIONS,
    )

    mock_experiment_generator.assert_called_once()
    config = mock_experiment_generator.call_args[0][0]
    assert config["model_type"] == "mock-payu-model"
    assert config["repository_url"] == "https://example.com/repo"
    assert config["start_point"] == "commit"
    assert config["test_path"] == "/fake/test_path"
    assert config["repository_directory"] == "config"
    assert config["control_branch_name"] == "ctrl"
    assert config["Control_Experiment"] == {"some": "option"}

    # One perturbation experiment per layout, numbered sequentially
    perturbations = config["Perturbation_Experiment"]
    assert list(perturbations) == ["Experiment_1", "Experiment_2", "Experiment_3", "Experiment_4"]

    # Every experiment carries its branch, its walltime and the model's configuration changes
    branches = []
    for block in perturbations.values():
        assert len(block["branches"]) == 1
        branches.append(block["branches"][0])
        assert block["config.yaml"]["walltime"] == "2:00:00"
        assert block["config.yaml"]["submodels"] == [[{"ncpus": 2}, {"ncpus": 2}]]

    # Each branch is distinct and registered as a new experiment
    assert len(set(branches)) == len(branches)
    for branch in branches:
        assert isinstance(manager.experiments[branch], ProfilingExperiment)
        assert manager.experiments[branch].path == Path("/fake/test_path") / branch / "config"


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_generate_scaling_experiments_callables(mock_experiment_generator, manager):
    """Test that generate_scaling_experiments evaluates its callable arguments with the number of nodes."""

    manager.set_control("https://example.com/repo", "commit")
    walltime = mock.MagicMock(return_value=1.5)
    allocations = mock.MagicMock(return_value=MOCK_ALLOCATIONS)

    manager.generate_scaling_experiments([1.0], {}, 4, walltime, allocations=allocations)

    walltime.assert_called_once_with(1.0)
    allocations.assert_called_once_with(1.0)
    config = mock_experiment_generator.call_args[0][0]
    assert config["Perturbation_Experiment"]["Experiment_1"]["config.yaml"]["walltime"] == "1:30:00"


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_generate_scaling_experiments_duplicates(mock_experiment_generator, manager):
    """Test that generate_scaling_experiments skips layouts whose experiment already exists."""

    manager.set_control("https://example.com/repo", "commit")
    manager.generate_scaling_experiments([1.0], {}, 4, 2.0, allocations=MOCK_ALLOCATIONS)
    assert len(manager.experiments) == 4
    mock_experiment_generator.reset_mock()

    # The same layouts are found again, so there is nothing left to generate
    manager.generate_scaling_experiments([1.0], {}, 4, 2.0, allocations=MOCK_ALLOCATIONS)
    assert len(manager.experiments) == 4
    mock_experiment_generator.assert_not_called()


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_generate_scaling_experiments_no_layouts(mock_experiment_generator, manager):
    """Test that generate_scaling_experiments does nothing when no layout can be found."""

    manager.set_control("https://example.com/repo", "commit")
    too_big = RootAllocation(subcomponents={"atm": FixedAllocation(100), "ocn": FixedAllocation(100)})

    manager.generate_scaling_experiments([1.0], {}, 4, 2.0, allocations=too_big)

    assert manager.experiments == {}
    mock_experiment_generator.assert_not_called()


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_generate_scaling_experiments_fractional_nodes(mock_experiment_generator, manager):
    """Test that a fractional node count reaches the layout search as the truncated number of cores."""

    manager.set_control("https://example.com/repo", "commit")

    # Half of an 8 core node is the same 4 core budget as a whole 4 core one
    manager.generate_scaling_experiments([0.5], {}, 8, 2.0, allocations=MOCK_ALLOCATIONS)

    config = mock_experiment_generator.call_args[0][0]
    assert len(config["Perturbation_Experiment"]) == 4


def test_generate_scaling_experiments_invalid_inputs(manager):
    """Test that generate_scaling_experiments rejects node counts and node sizes it cannot use."""

    manager.set_control("https://example.com/repo", "commit")

    for cores_per_node in (0, -4, 4.0):
        with pytest.raises(ValueError):
            manager.generate_scaling_experiments([1.0], {}, cores_per_node, 2.0, allocations=MOCK_ALLOCATIONS)

    for num_nodes in (0.0, -1.0):
        with pytest.raises(ValueError):
            manager.generate_scaling_experiments([num_nodes], {}, 4, 2.0, allocations=MOCK_ALLOCATIONS)


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
    """Side effect function for Path.glob to simulate different directory contents.

    Run 10 is deliberately present: Payu does not zero-pad the payu_jobs directory names, so keying runs on a
    lexical sort of the paths would order it before run 2.
    """

    if pattern == "payu_jobs/*/run/*.json":
        return [Path("payu_jobs/0/run/log.json"), Path("payu_jobs/10/run/log.json")]
    elif pattern == "output*":
        return [Path("output000"), Path("output001"), Path("output010")]
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

        # Every output directory is visited, not just the first one
        assert mock_get_logs.call_args_list == [
            mock.call(Path("output000")),
            mock.call(Path("output001")),
            mock.call(Path("output010")),
        ]

        # Check returned logs are keyed by the run numbers found in the paths
        assert set(logs["payu"]) == {0, 10}
        assert set(logs["component"]) == {0, 1, 10}
        assert isinstance(logs["payu"][10], ProfilingLog)
        assert isinstance(logs["component"][10], ProfilingLog)


@mock.patch.object(Path, "is_dir", return_value=True)
@mock.patch.object(
    Path,
    "glob",
    side_effect=lambda pattern: {
        "payu_jobs/*/run/*.json": [Path("payu_jobs/7/run/log.json")],
        "output*": [Path("output007")],
    }.get(pattern, []),
)
def test_profiling_logs_single_run(mock_glob, mock_is_dir, manager):
    """A single run keeps the run number Payu gave it, rather than being renumbered."""

    logs = manager.profiling_logs(Path("/fake/path"))

    assert set(logs["payu"]) == {7}
    assert set(logs["component"]) == {7}


@mock.patch.object(Path, "is_dir", return_value=True)
@mock.patch.object(
    Path,
    "glob",
    side_effect=lambda pattern: {
        "payu_jobs/*/run/*.json": [Path("payu_jobs/0/run/log.json"), Path("payu_jobs/1/run/log.json")],
        "output*": [Path("output000")],
    }.get(pattern, []),
)
def test_profiling_logs_output_json_mismatch(mock_glob, mock_is_dir, manager):
    """Payu writes its telemetry log at submission, so a run may have a log but no output directory."""

    logs = manager.profiling_logs(Path("/fake/path"))

    assert set(logs["payu"]) == {0, 1}
    assert set(logs["component"]) == {0}


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_rejects_all_experiments_and_experiments(mock_experiment_runner, manager):
    """delete_experiments raises an error if both all_experiments and experiments are provided."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))

    with pytest.raises(ValueError):
        manager.delete_experiments(experiments=["branch1"], all_experiments=True)

    mock_experiment_runner.assert_not_called()


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_no_experiments_or_all_experiments(mock_experiment_runner, manager):
    """delete_experiments raises an error if neither experiments nor all_experiments is provided."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))

    with pytest.raises(ValueError):
        manager.delete_experiments()

    mock_experiment_runner.assert_not_called()


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_all_experiments_but_no_experiments(mock_experiment_runner, manager):
    """delete_experiments is a no-op if all_experiments is True but there are no experiments."""
    manager.experiments.clear()

    manager.delete_experiments(all_experiments=True)
    mock_experiment_runner.assert_not_called()


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_rejects_unmanaged_experiments(mock_experiment_runner, manager):
    """delete_experiments raises an error if experiments are provided that are not in the manager experiments."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))
    manager.experiments["branch2"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch2"))

    with pytest.raises(KeyError):
        manager.delete_experiments(experiments=["branch2", "branch3"])

    mock_experiment_runner.assert_not_called()


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_valid_experiments(mock_experiment_runner, manager):
    """delete_experiments deletes each selected branch individually via ExperimentRunner."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))
    manager.experiments["branch2"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch2"))
    manager.experiments["branch3"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch3"))

    runner = mock_experiment_runner.return_value

    manager.delete_experiments(experiments=["branch3", "branch1"])

    mock_experiment_runner.assert_called_with({"test_path": manager.work_dir, "repository_directory": "config"})

    assert runner.delete_experiments.call_count == 2
    deleted_branches = set()
    for _, kwargs in runner.delete_experiments.call_args_list:
        assert len(kwargs["branches"]) == 1
        deleted_branches.add(kwargs["branches"][0])
        assert kwargs["dry_run"] is False
        assert kwargs["remove_repo_dir"] is False
    assert deleted_branches == {"branch3", "branch1"}


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_forwards_remove_repo_dir(mock_experiment_runner, manager):
    """delete_experiments forwards remove_repo_dir to the runner for each branch."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))

    runner = mock_experiment_runner.return_value

    manager.delete_experiments(experiments=["branch1"], remove_repo_dir=True)

    _, kwargs = runner.delete_experiments.call_args
    assert kwargs["remove_repo_dir"] is True


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_dry_run_does_not_modify_state(mock_experiment_runner, manager):
    """delete_experiments with dry_run=True does not modify the manager state."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))
    manager.experiments["branch2"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch2"))

    manager.delete_experiments(experiments=["branch1"], dry_run=True)

    assert set(manager.experiments.keys()) == {"branch1", "branch2"}


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_delete_experiments_non_dry_run_removes_from_state(mock_experiment_runner, manager):
    """delete_experiments with dry_run=False removes deleted branches from the manager state."""
    manager.experiments.clear()
    manager.experiments["branch1"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1"))
    manager.experiments["branch2"] = mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch2"))

    manager.delete_experiments(experiments=["branch1"], dry_run=False)

    assert set(manager.experiments.keys()) == {"branch2"}
