# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json
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


def write_job_file(path: Path, run: int, job_info: dict) -> Path:
    """Writes a Payu job file for a run of the experiment at path, and returns it."""

    job_file = path / "archive" / "payu_jobs" / str(run) / "run" / "149764665.gadi-pbs.json"
    job_file.parent.mkdir(parents=True, exist_ok=True)
    job_file.write_text(json.dumps(job_info))
    return job_file


def pbs_job_info(ncpus: int, job_id: str = "149764665.gadi-pbs") -> dict:
    """A Payu job file recording a PBS job of the given size, shaped as `qstat -f -F json` returns it."""

    return {
        "scheduler_job_id": job_id,
        "scheduler_type": "pbs",
        "scheduler_job_info": {"Jobs": {job_id: {"Resource_List": {"ncpus": ncpus, "nodect": 4}}}},
        "timings": {"payu_total_duration_seconds": 6838.225644},
    }


class TestRequestedNcpus:
    """The core count Payu asks the scheduler for, mirroring payu.subcommands.run_cmd."""

    def test_the_submodels_are_summed(self):
        assert PayuManager._requested_ncpus({"submodels": [{"ncpus": 2}, {"ncpus": 3}]}) == 5

    def test_a_top_level_count_wins_over_the_submodels(self):
        # Payu's own precedence: an explicit ncpus suppresses the sum rather than adding to it.
        assert PayuManager._requested_ncpus({"ncpus": 8, "submodels": [{"ncpus": 2}, {"ncpus": 3}]}) == 8

    def test_a_submodel_without_a_count_contributes_nothing(self):
        assert PayuManager._requested_ncpus({"submodels": [{"ncpus": 2}, {"exe": "model.exe"}]}) == 2

    def test_a_configuration_stating_nothing_asks_for_one_core(self):
        assert PayuManager._requested_ncpus({}) == 1

    def test_ncpureq_overrides_everything_including_the_rounding(self):
        # A hard override, so the misaligned 100 is passed on as it stands rather than rounded to 104.
        config = {"ncpureq": 100, "ncpus": 8, "platform": {"nodesize": 52}}
        assert PayuManager._requested_ncpus(config) == 100

    def test_a_job_within_one_node_is_not_rounded_up(self):
        # Payu leaves these alone, so a 100 core job on 104 core nodes reports the 100 it asked for.
        assert PayuManager._requested_ncpus({"ncpus": 100, "platform": {"nodesize": 104}}) == 100

    def test_a_misaligned_request_fills_whole_nodes(self):
        # The case this whole change is about: 402 cores of work occupy, and cost, four whole nodes.
        config = {"submodels": [{"ncpus": 200}, {"ncpus": 190}, {"ncpus": 12}], "platform": {"nodesize": 104}}
        assert PayuManager._requested_ncpus(config) == 416

    def test_a_request_already_filling_whole_nodes_is_left_alone(self):
        config = {"submodels": [{"ncpus": 208}, {"ncpus": 196}, {"ncpus": 12}], "platform": {"nodesize": 104}}
        assert PayuManager._requested_ncpus(config) == 416

    def test_npernode_below_the_node_size_spreads_over_more_nodes(self):
        # 208 cores at 52 per node is four nodes, and four nodes cost 4 x 104 cores whatever runs on them.
        config = {"ncpus": 208, "npernode": 52, "platform": {"nodesize": 104}}
        assert PayuManager._requested_ncpus(config) == 416

    def test_the_node_size_defaults_to_payus_own(self):
        # Nothing declares a node size, so 48 applies and 100 cores become three nodes' worth.
        assert PayuManager._requested_ncpus({"ncpus": 100}) == 144


class TestRecordedNcpus:
    """The core count the scheduler recorded, read back from the Payu job files."""

    def test_it_reads_the_pbs_request(self, tmp_path):
        write_job_file(tmp_path, 0, pbs_job_info(416))
        assert PayuManager._recorded_ncpus(tmp_path) == 416

    def test_the_most_recent_run_is_used(self, tmp_path):
        # Numerically, not lexicographically: run 10 is the newest, not run 9.
        write_job_file(tmp_path, 9, pbs_job_info(208))
        write_job_file(tmp_path, 10, pbs_job_info(416))
        assert PayuManager._recorded_ncpus(tmp_path) == 416

    def test_no_archive_records_nothing(self, tmp_path):
        assert PayuManager._recorded_ncpus(tmp_path) is None

    def test_an_archive_without_job_files_records_nothing(self, tmp_path):
        (tmp_path / "archive" / "output000").mkdir(parents=True)
        assert PayuManager._recorded_ncpus(tmp_path) is None

    def test_a_job_file_that_is_not_json_records_nothing(self, tmp_path):
        job_file = write_job_file(tmp_path, 0, pbs_job_info(416))
        job_file.write_text("not json at all")
        assert PayuManager._recorded_ncpus(tmp_path) is None

    def test_another_scheduler_records_nothing(self, tmp_path):
        # Only PBS reports a Resource_List, so there is nothing to read for anything else.
        job_info = pbs_job_info(416)
        job_info["scheduler_type"] = "slurm"
        write_job_file(tmp_path, 0, job_info)
        assert PayuManager._recorded_ncpus(tmp_path) is None

    def test_a_job_file_without_job_information_records_nothing(self, tmp_path):
        # Payu writes the timings but omits the scheduler keys when the scheduler query fails.
        write_job_file(tmp_path, 0, {"timings": {"payu_total_duration_seconds": 1.0}})
        assert PayuManager._recorded_ncpus(tmp_path) is None

    def test_a_job_file_without_a_core_count_records_nothing(self, tmp_path):
        job_info = pbs_job_info(416)
        del job_info["scheduler_job_info"]["Jobs"]["149764665.gadi-pbs"]["Resource_List"]
        write_job_file(tmp_path, 0, job_info)
        assert PayuManager._recorded_ncpus(tmp_path) is None

    def test_a_job_file_naming_another_job_records_nothing(self, tmp_path):
        write_job_file(tmp_path, 0, pbs_job_info(416, job_id="different.gadi-pbs") | {"scheduler_job_id": "149764665"})
        assert PayuManager._recorded_ncpus(tmp_path) is None


class TestParseNcpus:
    """The two mechanisms together: what the scheduler recorded, else what Payu would request."""

    def test_the_record_is_preferred_over_the_configuration(self, manager, tmp_path):
        # The two agree in practice, since one is the result of submitting the other. Made to disagree here so
        # that the answer says which of them was used.
        (tmp_path / "config.yaml").write_text("ncpus: 8")
        write_job_file(tmp_path, 0, pbs_job_info(416))
        assert manager.parse_ncpus(tmp_path) == 416

    def test_the_configuration_is_used_when_nothing_was_recorded(self, manager, tmp_path):
        (tmp_path / "config.yaml").write_text("ncpus: 100\nplatform:\n  nodesize: 104\n")
        assert manager.parse_ncpus(tmp_path) == 100

    def test_layouts_filling_the_same_nodes_report_the_same_size(self, manager, tmp_path):
        """The point of counting occupied cores: these two are the same size, and must compete as one."""

        def experiment(name: str, cores: tuple[int, int, int]) -> Path:
            path = tmp_path / name
            path.mkdir()
            submodels = "".join(f"  - ncpus: {n}\n" for n in cores)
            (path / "config.yaml").write_text(f"platform:\n  nodesize: 104\nsubmodels:\n{submodels}")
            return path

        # 416 cores of work and 402 cores of work, both spread over the same four whole nodes.
        full = experiment("full", (208, 196, 12))
        wasteful = experiment("wasteful", (200, 190, 12))

        assert manager.parse_ncpus(full) == manager.parse_ncpus(wasteful) == 416


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
