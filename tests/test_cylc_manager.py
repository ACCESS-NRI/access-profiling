# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path
from unittest import mock

import pytest

from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.experiment import ProfilingExperiment, ProfilingExperimentStatus
from access.profiling.manager import ProfilingManager
from access.profiling.parser import ProfilingParser


class MockCylcManager(CylcRoseManager):
    """Test class inheriting from CylcRoseManager to test its methods."""

    @property
    def known_parsers(self) -> dict[str, ProfilingParser]:
        return {"fake-parser": mock.MagicMock()}


@pytest.fixture()
def manager():
    return MockCylcManager(Path("/fake/test_path"), Path("/fake/archive_path"), layout_variable="um_layout")


@mock.patch("access.profiling.cylc_manager.Path.glob")
def test_parse_profiling_logs(mock_path_glob, manager):
    """Test the parse_profiling_logs method of CylcRoseManager with missing directories."""

    run_path = Path("/fake/run_path")

    # no component log files
    mock_path_glob.return_value = []
    with pytest.raises(RuntimeError):
        manager.profiling_logs(Path("/fake/path"), run_path)
    mock_path_glob.assert_called_once()

    # component log files are present
    mock_path_glob.reset_mock()
    mock_path_glob.return_value = [Path("/fake/run_path/cycle1/task1/NN/job.out")]
    # return something "valid" for the cylc loc and db, but fail to read the component log.
    logs = manager.profiling_logs(Path("/fake/path"), run_path)
    mock_path_glob.assert_called_once()
    assert "cylc_suite_log" in logs
    assert isinstance(logs["cylc_suite_log"].parser, CylcProfilingParser)
    assert "cylc_tasks" in logs
    assert isinstance(logs["cylc_tasks"].parser, CylcDBReader)
    assert "task1_cyclecycle1_fake-parser" in logs
    assert isinstance(logs["task1_cyclecycle1_fake-parser"].parser, mock.MagicMock)


def test_profiling_logs_requires_run_path(manager):
    """Cylc profiling logs live in the run directory, so run_path is required."""

    with pytest.raises(ValueError, match="Cylc run_path is required"):
        manager.profiling_logs(Path("/fake/path"))


def test_profiling_logs_uses_run_path(tmp_path, manager):
    """Cylc runtime logs should be resolved from run_path when it is provided."""

    exp_path = tmp_path / "experiment"
    run_path = tmp_path / "runs"
    job_out = run_path / "log/job/cycle1/task1/NN/job.out"
    job_out.parent.mkdir(parents=True)
    job_out.touch()

    logs = manager.profiling_logs(exp_path, run_path)

    assert logs["cylc_suite_log"].filepath == run_path / "log/suite/log"
    assert logs["cylc_tasks"].filepath == run_path / "cylc-suite.db"
    assert logs["task1_cyclecycle1_fake-parser"].filepath == job_out


@mock.patch("access.profiling.access_models.Path.is_file")
@mock.patch("access.profiling.access_models.Path.read_text")
def test_parse_ncpus(mock_read_text, mock_is_file, manager):
    """Test the parse_ncpus method of CylcRoseManager."""

    # mock absence of rose-conf file
    mock_is_file.return_value = False
    with pytest.raises(FileNotFoundError):
        manager.parse_ncpus(Path("/fake/path"))

    # mock absence of layout variable
    mock_is_file.return_value = True
    mock_read_text.return_value = "another_var=another_value"
    with pytest.raises(ValueError):
        manager.parse_ncpus(Path("/fake/path"))

    # mock presence of layout variable
    mock_read_text.return_value += "\n um_layout = 2,3"
    manager.parse_ncpus(Path("/fake/path"))


def test_parse_ncpus_uses_run_path(tmp_path, manager):
    """The separate Cylc run directory is the source of truth when both configs exist."""

    exp_path = tmp_path / "experiment"
    run_path = tmp_path / "runs"
    exp_path.mkdir()
    (run_path / "log").mkdir(parents=True)
    (exp_path / "rose-suite.conf").write_text("um_layout = 9,9\n")
    (run_path / "log/rose-suite-run.conf").write_text("um_layout = 2,3\n")

    assert manager.parse_ncpus(exp_path, run_path) == 6


@mock.patch("access.profiling.cylc_manager.getpass.getuser", return_value="fake-user")
def test_add_rose_experiment_uses_new_experiment_api(mock_getuser, manager):
    """add_rose_experiment should populate ProfilingExperiment.path and run_path."""

    rose = "u-aa123"
    experiment_path = manager.work_dir / rose
    run_path = Path("/scratch") / "proj" / "fake-user" / "cylc-run" / rose

    with mock.patch("access.profiling.cylc_manager.Path.is_dir", autospec=True) as mock_is_dir:
        mock_is_dir.side_effect = lambda path: path in {experiment_path, run_path}
        manager.add_rose_experiment(rose, project="proj")

    assert manager.experiments[rose].path == experiment_path
    assert manager.experiments[rose].run_path == run_path
    assert manager.experiments[rose].status == ProfilingExperimentStatus.DONE
    mock_getuser.assert_called_once()


def test_add_rose_experiment_requires_project(monkeypatch, manager):
    """A project must be provided explicitly or through the PROJECT environment variable."""

    monkeypatch.delenv("PROJECT", raising=False)

    with pytest.raises(ValueError, match="No project specified and PROJECT environment variable is not set"):
        manager.add_rose_experiment("u-aa123")


def test_add_rose_experiment_rejects_missing_experiment_path(manager):
    """The Rose experiment directory must exist before it can be managed."""

    with (
        mock.patch("access.profiling.cylc_manager.Path.is_dir", return_value=False),
        pytest.raises(ValueError, match="does not exist or is not a directory"),
    ):
        manager.add_rose_experiment("u-aa123", project="proj")


@mock.patch("access.profiling.cylc_manager.getpass.getuser", return_value="fake-user")
def test_add_rose_experiment_missing_run_path(mock_getuser, caplog, manager):
    """Missing Cylc run directories are allowed, but stored as None."""

    rose = "u-aa123"
    experiment_path = manager.work_dir / rose
    run_path = Path("/scratch") / "proj" / "fake-user" / "cylc-run" / rose

    with mock.patch("access.profiling.cylc_manager.Path.is_dir", autospec=True) as mock_is_dir:
        mock_is_dir.side_effect = lambda path: path == experiment_path
        with caplog.at_level(logging.WARNING):
            manager.add_rose_experiment(rose, project="proj")

    assert manager.experiments[rose].path == experiment_path
    assert manager.experiments[rose].run_path is None
    assert f"Run path '{run_path}' does not exist" in caplog.text
    mock_getuser.assert_called_once()


def test_delete_experiments_removes_path_and_run_path(tmp_path):
    """delete_experiments should use ProfilingExperiment.path and run_path."""

    manager = MockCylcManager(tmp_path / "work", tmp_path / "archive", layout_variable="um_layout")
    exp_path = tmp_path / "work/u-aa123"
    run_path = tmp_path / "runs/u-aa123"
    exp_path.mkdir(parents=True)
    run_path.mkdir(parents=True)
    manager.experiments["u-aa123"] = ProfilingExperiment(path=exp_path, run_path=run_path)

    manager.delete_experiments(experiments=["u-aa123"])

    assert "u-aa123" not in manager.experiments
    assert not exp_path.exists()
    assert not run_path.exists()


def test_delete_experiments_rejects_invalid_selection(manager):
    """delete_experiments requires exactly one selection mode."""

    with pytest.raises(ValueError, match="Pass either experiments=\\[\\.\\.\\.\\] or all_experiments=True"):
        manager.delete_experiments(experiments=["u-aa123"], all_experiments=True)

    with pytest.raises(ValueError, match="No experiments specified"):
        manager.delete_experiments()


def test_delete_experiments_rejects_unmanaged_experiment(manager):
    """Only experiments tracked by this manager can be deleted."""

    with pytest.raises(KeyError, match="not managed by this CylcRoseManager"):
        manager.delete_experiments(experiments=["u-aa123"])


def test_delete_experiments_warns_for_missing_directories(tmp_path, caplog):
    """Missing experiment and run directories should warn but still remove manager state."""

    manager = MockCylcManager(tmp_path / "work", tmp_path / "archive", layout_variable="um_layout")
    exp_path = tmp_path / "work/u-aa123"
    run_path = tmp_path / "runs/u-aa123"
    manager.experiments["u-aa123"] = ProfilingExperiment(path=exp_path, run_path=run_path)

    with caplog.at_level(logging.WARNING):
        manager.delete_experiments(experiments=["u-aa123"])

    assert "u-aa123" not in manager.experiments
    assert f"Experiment directory '{exp_path}' does not exist. Skipping deletion." in caplog.text
    assert f"Run directory '{run_path}' does not exist. Skipping deletion." in caplog.text


def test_delete_experiments_dry_run_keeps_directories_and_manager_state(tmp_path, caplog):
    """Dry runs should report actions without deleting directories or manager entries."""

    manager = MockCylcManager(tmp_path / "work", tmp_path / "archive", layout_variable="um_layout")
    exp_path = tmp_path / "work/u-aa123"
    run_path = tmp_path / "runs/u-aa123"
    exp_path.mkdir(parents=True)
    run_path.mkdir(parents=True)
    manager.experiments["u-aa123"] = ProfilingExperiment(path=exp_path, run_path=run_path)

    with caplog.at_level(logging.INFO):
        manager.delete_experiments(experiments=["u-aa123"], dry_run=True)

    assert "u-aa123" in manager.experiments
    assert exp_path.is_dir()
    assert run_path.is_dir()
    assert f"Dry run: would delete experiment directory '{exp_path}' and run directory '{run_path}'." in caplog.text


@mock.patch("access.profiling.cylc_manager.subprocess.run")
def test_run_experiments_skips_when_no_new_experiments(mock_subprocess, caplog, manager):
    """run_experiments should do nothing when there are no NEW experiments."""

    manager.experiments["u-aa123"] = ProfilingExperiment(path=Path("/fake/path"))
    manager.experiments["u-aa123"].status = ProfilingExperimentStatus.DONE

    with caplog.at_level(logging.INFO):
        manager.run_experiments()

    mock_subprocess.assert_not_called()
    assert "No new experiments to run" in caplog.text


@mock.patch("access.profiling.cylc_manager.subprocess.run")
def test_run_experiments_calls_rose_suite_run(mock_subprocess, manager):
    """run_experiments should call rose suite-run in each NEW experiment's path."""

    exp_path = Path("/fake/u-aa123")
    manager.experiments["u-aa123"] = ProfilingExperiment(path=exp_path)
    manager.experiments["u-aa123"].status = ProfilingExperimentStatus.NEW

    manager.run_experiments()

    mock_subprocess.assert_called_once_with(["rose", "suite-run"], cwd=exp_path, check=True)


@mock.patch("access.profiling.cylc_manager.subprocess.run")
def test_run_experiments_only_runs_new_experiments(mock_subprocess, manager):
    """run_experiments should only submit experiments with NEW status."""

    manager.experiments["new"] = ProfilingExperiment(path=Path("/fake/new"))
    manager.experiments["new"].status = ProfilingExperimentStatus.NEW
    manager.experiments["done"] = ProfilingExperiment(path=Path("/fake/done"))
    manager.experiments["done"].status = ProfilingExperimentStatus.DONE

    manager.run_experiments()

    assert mock_subprocess.call_count == 1
    mock_subprocess.assert_called_once_with(["rose", "suite-run"], cwd=Path("/fake/new"), check=True)


@mock.patch("access.profiling.cylc_manager.subprocess.run", side_effect=Exception("rose suite-run failed"))
def test_run_experiments_propagates_subprocess_failure(mock_subprocess, manager):
    """A failed rose suite-run should propagate the exception."""

    manager.experiments["u-aa123"] = ProfilingExperiment(path=Path("/fake/path"))
    manager.experiments["u-aa123"].status = ProfilingExperimentStatus.NEW

    with pytest.raises(Exception, match="rose suite-run failed"):
        manager.run_experiments()

    mock_subprocess.assert_called_once()


@mock.patch.object(ProfilingManager, "archive_experiments")
def test_archive_experiments_defaults(mock_archive, manager):
    """Cylc archive defaults should be applied before calling the base manager."""

    manager.archive_experiments()
    mock_archive.assert_called_once_with(
        exclude_dirs=[".svn", "share"], exclude_files=["*.nc"], follow_symlinks=False, overwrite=False
    )
    mock_archive.reset_mock()

    manager.archive_experiments(exclude_dirs=["dir1"], exclude_files=["file1"], follow_symlinks=True, overwrite=True)
    mock_archive.assert_called_once_with(
        exclude_dirs=["dir1"], exclude_files=["file1"], follow_symlinks=True, overwrite=True
    )
