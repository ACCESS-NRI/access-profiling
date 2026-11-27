# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
import logging
import shutil
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path
from unittest import mock

import pytest
import xarray as xr
from access.config.parallel_allocation_strategies import FixedAllocation, RootAllocation
from access.config.parallel_domain import Domain
from conftest import write_suite_conf

from access.profiling.control import ExistingDirectoryControlSource, GitControlSource
from access.profiling.cylc_manager import CylcRoseManager, _suite_name, _walltime_duration
from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.experiment import (
    RECORD_FILENAME,
    ConfigurationChoice,
    ExperimentPlan,
    ProfilingExperiment,
    ProfilingExperimentStatus,
    component_cores,
)
from access.profiling.manager import STUDY_FILENAME, ProfilingManager
from access.profiling.metrics import tmax
from access.profiling.plotting_utils import RegionGroup
from access.profiling.rose_configuration import RoseProcessGrid, RoseSuiteConfiguration, read_section


def mock_configuration(**kwargs) -> RoseSuiteConfiguration:
    """Returns a rose suite configuration for these tests, overridden by whatever a test needs.

    Args:
        **kwargs: Fields to set, the rest those of a suite running one UM, its process grid over an 8 by 8 grid
            stated in one variable, `um_layout`.

    Returns:
        RoseSuiteConfiguration: The configuration.
    """
    fields = {
        "name": "mock",
        "model_name": "Mock",
        "components": (RoseProcessGrid(name="UM", token="atm", variables=("um_layout",), grid=Domain((8, 8))),),
        "parsers": {"fake-parser": mock.MagicMock()},
    }
    fields.update(kwargs)
    return RoseSuiteConfiguration(**fields)


def mock_manager(work_dir: Path, archive_dir: Path, **kwargs) -> CylcRoseManager:
    """Returns a Cylc Rose manager over mock_configuration(**kwargs)."""
    return CylcRoseManager(work_dir, archive_dir, mock_configuration(**kwargs))


@pytest.fixture()
def manager():
    return mock_manager(Path("/fake/test_path"), Path("/fake/archive_path"))


class TestParseStatus:
    """The state a suite reached, read back from the states and the run statuses Cylc records per task."""

    @staticmethod
    def _suite_db(run_path, tasks, states=None):
        """Writes a cylc-suite.db recording the attempts at each task, `task<i>`, and the state Cylc left it in.

        Args:
            run_path (Path): The run directory.
            tasks (list): The run status of each task's one attempt, or the list of the run statuses of its attempts.
            states (dict[str, str] | None): The state of a task, by name, where it is not the one its latest attempt
                leaves it in: `succeeded` for 0, `running` for None and `failed` for any other run status.
        """
        run_path.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(run_path / "cylc-suite.db")) as connection, connection:
            connection.execute("CREATE TABLE task_jobs (cycle TEXT, name TEXT, submit_num INTEGER, run_status INTEGER)")
            connection.execute("CREATE TABLE task_states (name TEXT, cycle TEXT, status TEXT)")
            for i, attempts in enumerate(tasks):
                name = f"task{i}"
                attempts = attempts if isinstance(attempts, list) else [attempts]
                connection.executemany(
                    "INSERT INTO task_jobs VALUES ('1', ?, ?, ?)",
                    [(name, number, status) for number, status in enumerate(attempts, start=1)],
                )
                state = {0: "succeeded", None: "running"}.get(attempts[-1], "failed")
                connection.execute(
                    "INSERT INTO task_states VALUES (?, '1', ?)", (name, (states or {}).get(name, state))
                )

    def test_every_task_succeeded_is_done(self, manager, tmp_path):
        self._suite_db(tmp_path, [0, 0, 0])
        assert manager.parse_status(Path("/fake/expt"), tmp_path) == ProfilingExperimentStatus.DONE

    def test_any_task_failed_is_failed(self, manager, tmp_path):
        """Even with others still going: one failed task is enough to say the suite did not succeed."""

        self._suite_db(tmp_path, [0, 1, None])
        assert manager.parse_status(Path("/fake/expt"), tmp_path) == ProfilingExperimentStatus.FAILED

    def test_a_task_with_no_status_yet_is_still_running(self, manager, tmp_path):
        self._suite_db(tmp_path, [0, None])
        assert manager.parse_status(Path("/fake/expt"), tmp_path) == ProfilingExperimentStatus.RUNNING

    def test_a_task_cylc_runs_again_is_still_running(self, manager, tmp_path):
        """Its attempt failed, but the suite retries it, as ACCESS-AM3's suites retry atmos_main once."""
        self._suite_db(tmp_path, [0, 1], states={"task1": "retrying"})
        assert manager.parse_status(Path("/fake/expt"), tmp_path) == ProfilingExperimentStatus.RUNNING

    def test_a_task_that_succeeded_when_run_again_is_done(self, manager, tmp_path):
        """Only its latest attempt counts, so the attempt that failed before does not fail the suite for good."""
        self._suite_db(tmp_path, [0, [1, 0]])
        assert manager.parse_status(Path("/fake/expt"), tmp_path) == ProfilingExperimentStatus.DONE

    def test_an_attempt_that_never_ran_does_not_keep_the_suite_running(self, manager, tmp_path):
        """Its job could not be submitted, so it has no run status, but the attempt after it succeeded."""
        self._suite_db(tmp_path, [[None, 0]])
        assert manager.parse_status(Path("/fake/expt"), tmp_path) == ProfilingExperimentStatus.DONE

    def test_a_task_whose_job_could_not_be_submitted_is_failed(self, manager, tmp_path):
        """Its attempt has no run status, as one still going has none, but Cylc has given up on it."""
        self._suite_db(tmp_path, [0, None], states={"task1": "submit-failed"})
        assert manager.parse_status(Path("/fake/expt"), tmp_path) == ProfilingExperimentStatus.FAILED

    def test_no_task_recorded_yet_says_nothing(self, manager, tmp_path):
        self._suite_db(tmp_path, [])
        assert manager.parse_status(Path("/fake/expt"), tmp_path) is None

    def test_an_unreadable_database_says_nothing(self, manager, tmp_path):
        """A file that is there but is not a database, as an interrupted suite can leave behind."""

        (tmp_path / "cylc-suite.db").write_text("this is not a database")
        assert manager.parse_status(Path("/fake/expt"), tmp_path) is None

    def test_no_database_says_nothing(self, manager, tmp_path):
        assert manager.parse_status(Path("/fake/expt"), tmp_path) is None

    def test_no_run_directory_says_nothing(self, manager):
        assert manager.parse_status(Path("/fake/expt"), None) is None


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
    assert isinstance(logs["cylc_suite_log"][0].parser, CylcProfilingParser)
    assert "cylc_tasks" in logs
    assert isinstance(logs["cylc_tasks"][0].parser, CylcDBReader)
    assert "task1_cyclecycle1_fake-parser" in logs
    assert isinstance(logs["task1_cyclecycle1_fake-parser"][0].parser, mock.MagicMock)


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

    assert logs["cylc_suite_log"][0].filepath == run_path / "log/suite/log"
    assert logs["cylc_tasks"][0].filepath == run_path / "cylc-suite.db"
    assert logs["task1_cyclecycle1_fake-parser"][0].filepath == job_out

    # Cylc has no concept of repeated runs, so every log is the single run 0
    assert all(set(runs) == {0} for runs in logs.values())


def test_parse_ncpus(tmp_path, manager):
    """The layout's CPUs, read from the variables of the suite's rose-suite.conf."""

    with pytest.raises(FileNotFoundError, match="Could not find suitable config file"):
        manager.parse_ncpus(tmp_path)

    (tmp_path / "rose-suite.conf").write_text("[env]\num_layout = 2,3\n")
    with pytest.raises(ValueError, match=r"has no section \[jinja2:suite.rc\] in use"):
        manager.parse_ncpus(tmp_path)

    write_suite_conf(tmp_path, {"another_var": "another_value"})
    with pytest.raises(ValueError, match="Cannot find the variable um_layout"):
        manager.parse_ncpus(tmp_path)

    write_suite_conf(tmp_path, {"um_layout": "2,3"})
    assert manager.parse_ncpus(tmp_path) == 6


def test_parse_ncpus_uses_run_path(tmp_path, manager):
    """The separate Cylc run directory is the source of truth when both configs exist."""

    exp_path = tmp_path / "experiment"
    run_path = tmp_path / "runs"
    write_suite_conf(exp_path, {"um_layout": "9,9"})
    write_suite_conf(run_path, {"um_layout": "2,3"}, "log/rose-suite-run.conf")

    assert manager.parse_ncpus(exp_path, run_path) == 6


class TestRequestedNcpus:
    """The CPUs a suite occupied are those its job asked PBS for, which may be more than its layout puts to work."""

    @staticmethod
    def job_script(run_path, task, *directives, cycle="19820101T0000Z", attempt="01"):
        """Writes the job script of a task's latest attempt, with the given PBS directives, as Cylc 7 writes it."""
        attempt_dir = run_path / "log/job" / cycle / task / attempt
        attempt_dir.mkdir(parents=True)
        (attempt_dir.parent / "NN").symlink_to(attempt)
        lines = [
            "#!/bin/bash -l",
            "#",
            "# ++++ THIS IS A CYLC TASK JOB SCRIPT ++++",
            f"# Task: {task}.{cycle}",
            "# Job submit method: pbs",
            "",
            "# DIRECTIVES:",
            f"#PBS -N {task}.{cycle}.u-aa123",
            "#PBS -l walltime=3600",
            "#PBS -q normal",
            *(f"#PBS {directive}" for directive in directives),
            "#PBS -P tm70",
            "export CYLC_DIR='/g/data/hr22/apps/cylc7/cylc_7.9.9'",
        ]
        (attempt_dir / "job").write_text("\n".join(lines) + "\n")

    @pytest.fixture()
    def paths(self, tmp_path):
        """An experiment whose layout puts 6 CPUs to work, and its run directory."""
        exp_path = tmp_path / "experiment"
        write_suite_conf(exp_path, {"um_layout": "2,3"})
        run_path = tmp_path / "runs"
        run_path.mkdir()
        return exp_path, run_path

    def test_the_job_of_the_layout_fills_a_node(self, manager, paths):
        exp_path, run_path = paths
        self.job_script(run_path, "atmos_main", "-l ncpus=48", "-l mem=192GB")

        assert manager.parse_ncpus(exp_path, run_path) == 48

    def test_the_smallest_request_covering_the_layout_is_that_of_its_job(self, manager, paths):
        """Smaller jobs are other tasks, and so are larger ones, the layout's own job being what rounds it up."""
        exp_path, run_path = paths
        self.job_script(run_path, "install_cold", "-l ncpus=1")
        self.job_script(run_path, "atmos_main", "-l ncpus=48")
        self.job_script(run_path, "atmos_main", "-l ncpus=48", cycle="19820201T0000Z")
        self.job_script(run_path, "big_postproc", "-l ncpus=96")

        assert manager.parse_ncpus(exp_path, run_path) == 48

    def test_only_the_latest_attempt_counts(self, manager, paths):
        exp_path, run_path = paths
        self.job_script(run_path, "atmos_main", "-l ncpus=48", attempt="02")
        (run_path / "log/job/19820101T0000Z/atmos_main/01").mkdir()
        (run_path / "log/job/19820101T0000Z/atmos_main/01/job").write_text("#PBS -l ncpus=24\n")

        assert manager.parse_ncpus(exp_path, run_path) == 48

    def test_a_request_among_other_resources(self, manager, paths):
        exp_path, run_path = paths
        self.job_script(run_path, "atmos_main", "-l mem=192GB,ncpus=48,jobfs=10GB")

        assert manager.parse_ncpus(exp_path, run_path) == 48

    @pytest.mark.parametrize(
        "directives",
        [(), ("-l ncpus=1",), ("-l select=1:ncpus=48",)],
        ids=["no request", "no request covering the layout", "chunks"],
    )
    def test_without_a_request_covering_the_layout(self, manager, paths, caplog, directives):
        """The layout's own count is reported then, as it is without a run directory."""
        exp_path, run_path = paths
        self.job_script(run_path, "atmos_main", *directives)

        with caplog.at_level(logging.DEBUG, logger="access.profiling.cylc_manager"):
            assert manager.parse_ncpus(exp_path, run_path) == 6
        assert "asked PBS for the 6 CPUs of the layout" in caplog.text

    def test_without_a_run_directory(self, manager, paths):
        exp_path, _ = paths

        assert manager.parse_ncpus(exp_path) == 6


class TestAddRoseExperiment:
    """A suite is added with the status its run directory records, or as NEW where there is no run of it."""

    ROSE = "u-aa123"

    @pytest.fixture()
    def manager(self, tmp_path):
        manager = mock_manager(tmp_path / "roses", tmp_path / "archive")
        (manager.work_dir / self.ROSE).mkdir(parents=True)
        return manager

    @pytest.mark.parametrize(
        ("statuses", "expected"),
        [
            ([0, 0], ProfilingExperimentStatus.DONE),
            ([0, 1], ProfilingExperimentStatus.FAILED),
            ([0, None], ProfilingExperimentStatus.RUNNING),
        ],
        ids=["succeeded", "failed", "running"],
    )
    def test_a_suite_that_has_run(self, manager, tmp_path, statuses, expected):
        run_path = tmp_path / "cylc-run" / self.ROSE
        TestParseStatus._suite_db(run_path, statuses)

        manager.add_rose_experiment(self.ROSE, run_path=run_path)

        experiment = manager.experiments[self.ROSE]
        assert experiment.path == manager.work_dir / self.ROSE
        assert experiment.run_path == run_path
        assert experiment.status == expected

    @mock.patch("access.profiling.cylc_manager.subprocess.run")
    def test_a_suite_still_to_run_is_new_and_runs(self, mock_subprocess, manager, tmp_path):
        """Its run directory is where Cylc will run it, so it is kept, for its status to be read from once it does."""
        run_path = tmp_path / "cylc-run" / self.ROSE

        manager.add_rose_experiment(self.ROSE, run_path=run_path)

        assert manager.experiments[self.ROSE].status == ProfilingExperimentStatus.NEW
        assert manager.experiments[self.ROSE].run_path == run_path

        manager.run_experiments()

        mock_subprocess.assert_called_once_with(
            ["rose", "suite-run"], cwd=manager.work_dir / self.ROSE, check=True, capture_output=True, text=True
        )
        assert manager.experiments[self.ROSE].status == ProfilingExperimentStatus.RUNNING

        TestParseStatus._suite_db(run_path, [0])
        manager.update_statuses()
        assert manager.experiments[self.ROSE].status == ProfilingExperimentStatus.DONE

    def test_a_run_directory_given_as_a_string(self, manager, tmp_path):
        """As a notebook types it, which is read as the path it names."""
        run_path = tmp_path / "cylc-run" / self.ROSE
        TestParseStatus._suite_db(run_path, [0])

        manager.add_rose_experiment(self.ROSE, run_path=str(run_path))

        assert manager.experiments[self.ROSE].run_path == run_path
        assert manager.experiments[self.ROSE].status == ProfilingExperimentStatus.DONE

    def test_without_a_run_directory(self, manager):
        manager.add_rose_experiment(self.ROSE)

        assert manager.experiments[self.ROSE].run_path is None
        assert manager.experiments[self.ROSE].status == ProfilingExperimentStatus.NEW


def test_add_rose_experiment_rejects_missing_experiment_path(manager):
    """The Rose experiment directory must exist before it can be managed."""

    with (
        mock.patch("access.profiling.cylc_manager.Path.is_dir", return_value=False),
        pytest.raises(ValueError, match="does not exist or is not a directory"),
    ):
        manager.add_rose_experiment("u-aa123", run_path=Path("/scratch/proj/fake-user/cylc-run/u-aa123"))


def test_delete_experiments_removes_path_and_run_path(tmp_path):
    """delete_experiments should use ProfilingExperiment.path and run_path."""

    manager = mock_manager(tmp_path / "work", tmp_path / "archive")
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

    with pytest.raises(KeyError, match="not managed by this manager"):
        manager.delete_experiments(experiments=["u-aa123"])


def test_delete_experiments_warns_for_missing_directories(tmp_path, caplog):
    """Missing experiment and run directories should warn but still remove manager state."""

    manager = mock_manager(tmp_path / "work", tmp_path / "archive")
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

    manager = mock_manager(tmp_path / "work", tmp_path / "archive")
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


def test_a_dry_run_of_a_suite_without_a_run_directory(tmp_path, caplog):
    """As a suite added without one has, rather than naming a run directory 'None'."""
    manager = mock_manager(tmp_path / "work", tmp_path / "archive")
    exp_path = tmp_path / "work/u-aa123"
    exp_path.mkdir(parents=True)
    manager.experiments["u-aa123"] = ProfilingExperiment(path=exp_path, run_path=None)

    with caplog.at_level(logging.INFO):
        manager.delete_experiments(experiments=["u-aa123"], dry_run=True)

    assert f"would delete experiment directory '{exp_path}' and no run directory, since it has none." in caplog.text


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

    mock_subprocess.assert_called_once_with(
        ["rose", "suite-run"], cwd=exp_path, check=True, capture_output=True, text=True
    )


@mock.patch("access.profiling.cylc_manager.subprocess.run")
def test_run_experiments_only_runs_new_experiments(mock_subprocess, manager):
    """run_experiments should only submit experiments with NEW status."""

    manager.experiments["new"] = ProfilingExperiment(path=Path("/fake/new"))
    manager.experiments["new"].status = ProfilingExperimentStatus.NEW
    manager.experiments["done"] = ProfilingExperiment(path=Path("/fake/done"))
    manager.experiments["done"].status = ProfilingExperimentStatus.DONE

    manager.run_experiments()

    assert mock_subprocess.call_count == 1
    mock_subprocess.assert_called_once_with(
        ["rose", "suite-run"], cwd=Path("/fake/new"), check=True, capture_output=True, text=True
    )


@mock.patch("access.profiling.cylc_manager.subprocess.run")
def test_run_experiments_retries_failed_when_asked(mock_subprocess, manager):
    """A suite whose run failed is run anew where retry_failed asks for it, and a new one as usual.

    `--new` removes the run directory first: the database of the failed run records the task that failed, and a run
    over it would still read as failed.
    """

    manager.experiments["failed"] = ProfilingExperiment(path=Path("/fake/failed"))
    manager.experiments["failed"].status = ProfilingExperimentStatus.FAILED
    manager.experiments["new"] = ProfilingExperiment(path=Path("/fake/new"))
    manager.experiments["done"] = ProfilingExperiment(path=Path("/fake/done"))
    manager.experiments["done"].status = ProfilingExperimentStatus.DONE

    manager.run_experiments(retry_failed=True)

    assert mock_subprocess.call_args_list == [
        mock.call(["rose", "suite-run", "--new"], cwd=Path("/fake/failed"), check=True, capture_output=True, text=True),
        mock.call(["rose", "suite-run"], cwd=Path("/fake/new"), check=True, capture_output=True, text=True),
    ]
    assert manager.experiments["failed"].status == ProfilingExperimentStatus.RUNNING


@mock.patch("access.profiling.cylc_manager.subprocess.run")
def test_run_experiments_leaves_failed_alone_by_default(mock_subprocess, manager):
    manager.experiments["failed"] = ProfilingExperiment(path=Path("/fake/failed"))
    manager.experiments["failed"].status = ProfilingExperimentStatus.FAILED

    manager.run_experiments()

    mock_subprocess.assert_not_called()
    assert manager.experiments["failed"].status == ProfilingExperimentStatus.FAILED


@mock.patch("access.profiling.cylc_manager.subprocess.run")
def test_run_experiments_forwards_output_with_prefix(mock_subprocess, caplog, manager):
    """stdout and stderr lines should be logged with the experiment name as prefix."""

    stdout = (
        "[INFO] export CYLC_VERSION=7.9.9\n"
        "[INFO] export ROSE_ORIG_HOST=gadi-login-03.gadi.nci.org.au\n"
        "[INFO] export ROSE_SITE=nci\n"
        "[INFO] export ROSE_VERSION=2019.01.8\n"
    )
    mock_subprocess.return_value = mock.MagicMock(stdout=stdout, stderr="warning: low disk\n")
    manager.experiments["u-aa123"] = ProfilingExperiment(path=Path("/fake/path"))
    manager.experiments["u-aa123"].status = ProfilingExperimentStatus.NEW

    with caplog.at_level(logging.INFO):
        manager.run_experiments()

    assert "[u-aa123] [INFO] export CYLC_VERSION=7.9.9" in caplog.text
    assert "[u-aa123] [INFO] export ROSE_ORIG_HOST=gadi-login-03.gadi.nci.org.au" in caplog.text
    assert "[u-aa123] [INFO] export ROSE_SITE=nci" in caplog.text
    assert "[u-aa123] [INFO] export ROSE_VERSION=2019.01.8" in caplog.text
    assert "[u-aa123] warning: low disk" in caplog.text


def test_run_experiments_logs_stderr_on_failure(caplog, manager):
    """stderr should be logged at ERROR level before the CalledProcessError propagates."""

    error = subprocess.CalledProcessError(1, "rose suite-run", output="partial output\n", stderr="fatal: bad config\n")
    with mock.patch("access.profiling.cylc_manager.subprocess.run", side_effect=error):
        manager.experiments["u-aa123"] = ProfilingExperiment(path=Path("/fake/path"))
        manager.experiments["u-aa123"].status = ProfilingExperimentStatus.NEW

        with caplog.at_level(logging.INFO), pytest.raises(subprocess.CalledProcessError):
            manager.run_experiments()

    assert "[u-aa123] partial output" in caplog.text
    assert "[u-aa123] fatal: bad config" in caplog.text


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


CONTROL_SUITE_CONF = """\
# The control suite.

[jinja2:suite.rc]
CLOCK='PT1H'
!!IOS_NPROC=0
PROJECT='${PROJECT}'
um_layout=8,8
"""

CONTROL_UM_APP_CONF = """\
[namelist:prnt_param]
ltimer=.false.

[!namelist:off]
lflag=.true.
"""

CONTROL_SITE_RC = """\
    [[atmos]]
        -l walltime = 3600
{% macro pbs_mem(x,y,per_cpu=2) -%}
"""

# An allocation that gives the UM every core of the budget: at 4 cores, the process grids 1x4, 2x2 and 4x1.
WHOLE_BUDGET = RootAllocation(subcomponents={"UM": FixedAllocation(core_fraction=1.0)})


def write_control(directory: Path) -> Path:
    """Writes a control suite: a rose-suite.conf, a UM app, a site file, a script, links and version control."""
    for subdirectory in ("app/um", "site", "bin", ".git", ".svn"):
        (directory / subdirectory).mkdir(parents=True)
    (directory / "rose-suite.conf").write_text(CONTROL_SUITE_CONF)
    (directory / "app/um/rose-app.conf").write_text(CONTROL_UM_APP_CONF)
    (directory / "site/gadi.rc").write_text(CONTROL_SITE_RC)
    (directory / "bin/run.sh").write_text("#!/bin/bash\n")
    (directory / "bin/run.sh").chmod(0o755)
    (directory / "site.rc").symlink_to("site/gadi.rc")
    (directory / "sites").symlink_to("site")
    (directory / ".git/HEAD").write_text("ref: refs/heads/main\n")
    return directory


@pytest.fixture()
def home(tmp_path):
    """The user's home directory, under which Cylc runs every suite, in `~/cylc-run`."""
    home = tmp_path / "home"
    with mock.patch("access.profiling.cylc_manager.Path.home", return_value=home):
        yield home


def generating_manager(tmp_path, **kwargs) -> CylcRoseManager:
    """Returns a manager whose control suite is checked out in its working directory, as `config`.

    Args:
        tmp_path (Path): Where the study is.
        **kwargs: Fields of the configuration, which states its walltime in `CLOCK` unless told otherwise.

    Returns:
        CylcRoseManager: The manager.
    """
    work_dir = tmp_path / "work"
    write_control(work_dir / "config")
    return CylcRoseManager(
        work_dir,
        tmp_path / "archive",
        mock_configuration(**{"walltime_variable": "CLOCK", **kwargs}),
        control=GitControlSource("git@github.com:ACCESS-NRI/mock-configs.git", "release-1.0"),
    )


def suite_of(manager, name) -> str:
    """Returns the name of the suite an experiment of the manager's study runs as."""
    return _suite_name(manager.application.name, manager.work_dir, name)


class TestSuiteName:
    """A generated suite is named after the application, the study and the experiment."""

    def test_its_parts(self, tmp_path):
        digest = hashlib.sha256(str(tmp_path.resolve()).encode()).hexdigest()[:8]

        assert _suite_name("n96e", tmp_path, "exp-0001") == f"n96e-{digest}-exp-0001"

    def test_another_study_runs_its_suites_under_other_names(self, tmp_path):
        """Every study numbers its experiments from exp-0001, and Cylc runs a user's suites in one directory."""
        assert _suite_name("n96e", tmp_path / "a", "exp-0001") != _suite_name("n96e", tmp_path / "b", "exp-0001")

    def test_the_same_study_reached_through_a_link_names_them_alike(self, tmp_path):
        (tmp_path / "study").mkdir()
        (tmp_path / "link").symlink_to(tmp_path / "study")

        assert _suite_name("n96e", tmp_path / "link", "exp-0001") == _suite_name("n96e", tmp_path / "study", "exp-0001")


@pytest.mark.parametrize(
    ("hours", "expected"),
    [(24, "'PT24H00M'"), (1.5, "'PT1H30M'"), (30, "'PT30H00M'"), (0.12345, "'PT0H07M24S'")],
)
def test_a_walltime_is_a_quoted_iso_8601_duration(hours, expected):
    """A Jinja2 string, as `MAIN_CLOCK='PT1H10M'` is, its seconds rounded and written only where there are some."""
    assert _walltime_duration(hours) == expected


class TestGeneratingSuites:
    """A generated experiment is a copy of the control suite, with its changes written into its files."""

    def test_a_scaling_study_makes_one_copy_per_layout(self, tmp_path, home):
        """Each copy states its layout and the walltime, and runs in `~/cylc-run` under its own name."""
        manager = generating_manager(tmp_path)

        manager.generate_scaling_experiments([1.0], cores_per_node=4, walltime=2.0, allocations=WHOLE_BUDGET)

        assert list(manager.experiments) == ["exp-0001", "exp-0002", "exp-0003"]
        grids = []
        for name, experiment in manager.experiments.items():
            suite = suite_of(manager, name)
            assert (experiment.path, experiment.run_path) == (
                manager.work_dir / name / suite,
                home / "cylc-run" / suite,
            )
            variables = read_section(experiment.path / "rose-suite.conf", "jinja2:suite.rc")
            assert variables["CLOCK"] == "'PT2H00M'"
            grids.append(variables["um_layout"])
            read_back = manager.application.parse_layout(experiment.path)
            assert manager.application.layout_key(read_back) == manager.layout_key(name), "the layout it was given"
        assert grids == ["1,4", "2,2", "4,1"]

    def test_a_copy_has_the_control_s_files_but_its_version_control(self, tmp_path, home):
        """Its scripts keep their permissions and its links stay links, while the control is left as it was."""
        manager = generating_manager(tmp_path)
        control = manager.work_dir / "config"

        manager.generate_scaling_experiments([1.0], cores_per_node=4, walltime=2.0, allocations=WHOLE_BUDGET)

        suite = manager.experiments["exp-0001"].path
        assert sorted(path.name for path in suite.iterdir()) == [
            "app",
            "bin",
            "rose-suite.conf",
            "site",
            "site.rc",
            "sites",
        ]
        assert (suite / "bin/run.sh").stat().st_mode & 0o777 == 0o755
        assert (suite / "site.rc").readlink() == Path("site/gadi.rc")
        assert (suite / "sites").readlink() == Path("site")
        assert (suite / "app/um/rose-app.conf").read_text() == CONTROL_UM_APP_CONF
        assert (control / "rose-suite.conf").read_text() == CONTROL_SUITE_CONF

    def test_the_records_and_the_history(self, tmp_path, home):
        """The record states where the copy and its runs are, and the history the control options of the call."""
        manager = generating_manager(tmp_path)
        options = {"rose-suite.conf": {"jinja2:suite.rc": {"PROJECT": "'tm70'"}}}

        manager.generate_scaling_experiments(
            [1.0], cores_per_node=4, walltime=2.0, allocations=WHOLE_BUDGET, control_options=options
        )

        record = json.loads((manager.work_dir / "exp-0001" / RECORD_FILENAME).read_text())
        suite = suite_of(manager, "exp-0001")
        assert (record["path"], record["run_path"]) == (f"exp-0001/{suite}", str(home / "cylc-run" / suite))
        assert (record["changes"], record["layout"] is not None) == ({}, True)
        (entry,) = json.loads((manager.work_dir / STUDY_FILENAME).read_text())["history"]
        assert entry["engine"] == {"control_options": options}
        assert entry["control"] == {
            "repository": "git@github.com:ACCESS-NRI/mock-configs.git",
            "start_point": "release-1.0",
        }

    def test_a_later_manager_generates_nothing_twice(self, tmp_path, home, caplog):
        manager = generating_manager(tmp_path)
        manager.generate_scaling_experiments([1.0], cores_per_node=4, walltime=2.0, allocations=WHOLE_BUDGET)

        later = CylcRoseManager(manager.work_dir, manager.archive_dir, manager.application, control=manager.control)
        with caplog.at_level(logging.WARNING):
            later.generate_scaling_experiments([1.0], cores_per_node=4, walltime=2.0, allocations=WHOLE_BUDGET)

        assert list(later.experiments) == ["exp-0001", "exp-0002", "exp-0003"]
        assert "No new experiments to generate" in caplog.text

    def test_the_changes_of_each_file_in_layers(self, tmp_path, home):
        """The control options, then the choice's, over its layout, in both shapes of change.

        The choice wins where both set a variable. An ignored setting is enabled, at the end of its section, and a
        setting of no section goes above the first section, below the comment opening the file.
        """
        manager = generating_manager(tmp_path)
        layout = manager.select_layouts(4, allocations=WHOLE_BUDGET)[0]
        options = {
            "rose-suite.conf": {
                "": {"root-dir": "*=/scratch/tm70/model_scaling"},
                "jinja2:suite.rc": {"CLOCK": "'PT24H00M'", "IOS_NPROC": "2", "PROJECT": "'tm70'"},
            },
            "app/um/rose-app.conf": {"namelist:prnt_param": {"ltimer": ".true."}},
            "site/gadi.rc": {"per_cpu=2": "per_cpu=4"},
        }
        choice = ConfigurationChoice(
            {"rose-suite.conf": {"jinja2:suite.rc": {"CLOCK": "'PT2H'"}}, "site/gadi.rc": {"3600": "7200"}},
            layout=layout,
        )

        manager.generate_experiments([choice], control_options=options)

        suite = manager.experiments["exp-0001"].path
        assert (suite / "rose-suite.conf").read_text() == (
            "# The control suite.\n"
            "root-dir=*=/scratch/tm70/model_scaling\n"
            "\n"
            "[jinja2:suite.rc]\n"
            "CLOCK='PT2H'\n"
            "PROJECT='tm70'\n"
            "um_layout=1,4\n"
            "IOS_NPROC=2\n"
        )
        assert (suite / "app/um/rose-app.conf").read_text() == CONTROL_UM_APP_CONF.replace(".false.", ".true.")
        assert (suite / "site/gadi.rc").read_text() == CONTROL_SITE_RC.replace("3600", "7200").replace("=2", "=4")
        assert manager.experiments["exp-0001"].changes == choice.changes, "what the study stated, not the options"

    def test_a_setting_with_no_value_is_given_one(self, tmp_path, home):
        manager = generating_manager(tmp_path)
        conf = manager.work_dir / "config/rose-suite.conf"
        conf.write_text(conf.read_text() + "EMPTY=\n")

        manager.generate_experiments([ConfigurationChoice({"rose-suite.conf": {"jinja2:suite.rc": {"EMPTY": "1"}}})])

        variables = read_section(manager.experiments["exp-0001"].path / "rose-suite.conf", "jinja2:suite.rc")
        assert variables["EMPTY"] == "1"

    def test_a_control_on_disk_is_copied_from_where_it_is(self, tmp_path, home):
        control = write_control(tmp_path / "roses" / "u-by395")
        manager = CylcRoseManager(
            tmp_path / "work",
            tmp_path / "archive",
            mock_configuration(),
            control=ExistingDirectoryControlSource(control),
        )

        manager.generate_experiments([ConfigurationChoice({"site/gadi.rc": {"3600": "7200"}})])

        suite = manager.experiments["exp-0001"].path
        assert suite.parent == manager.work_dir / "exp-0001"
        assert (suite / "rose-suite.conf").read_text() == CONTROL_SUITE_CONF


class TestChangingAFileNamedByALink:
    """A file named by a symbolic link is read through it, and the copy has the changed file in place of the link."""

    def test_a_link_to_a_file_of_the_suite(self, tmp_path, home):
        """As rose-suite.conf is a link to rose-suite.conf_nci_gadi in ACCESS-AM3's suites.

        The file the link leads to is left as it was, in the copy and in the control: another name for the file does
        not see the change.
        """
        manager = generating_manager(tmp_path)
        control = manager.work_dir / "config"
        (control / "rose-suite.conf").rename(control / "rose-suite.conf_nci_gadi")
        (control / "rose-suite.conf").symlink_to("rose-suite.conf_nci_gadi")

        manager.generate_scaling_experiments([1.0], cores_per_node=4, walltime=2.0, allocations=WHOLE_BUDGET)

        for experiment in manager.experiments.values():
            assert not (experiment.path / "rose-suite.conf").is_symlink()
            assert read_section(experiment.path / "rose-suite.conf", "jinja2:suite.rc")["CLOCK"] == "'PT2H00M'"
            assert (experiment.path / "rose-suite.conf_nci_gadi").read_text() == CONTROL_SUITE_CONF
        assert (control / "rose-suite.conf").readlink() == Path("rose-suite.conf_nci_gadi")
        assert (control / "rose-suite.conf_nci_gadi").read_text() == CONTROL_SUITE_CONF

    @pytest.mark.parametrize("relative", [False, True], ids=["an absolute link", "a relative link"])
    def test_a_link_out_of_the_suite_is_not_written_through(self, tmp_path, home, relative):
        """The file it leads to is left as it was, and the copy's file has its permissions.

        A copy of an absolute link leads to the very file the control's does. A relative one leads elsewhere from the
        copy, so the permissions are those of the file the control's link leads to.
        """
        manager = generating_manager(tmp_path)
        shared = tmp_path / "shared/gadi.rc"
        shared.parent.mkdir()
        shared.write_text(CONTROL_SITE_RC)
        shared.chmod(0o755)
        link = manager.work_dir / "config/site.rc"
        link.unlink()
        link.symlink_to(Path("../../shared/gadi.rc") if relative else shared)

        manager.generate_experiments([ConfigurationChoice({"site.rc": {"3600": "7200"}})])

        copy = manager.experiments["exp-0001"].path / "site.rc"
        assert not copy.is_symlink()
        assert copy.read_text() == CONTROL_SITE_RC.replace("3600", "7200")
        assert copy.stat().st_mode & 0o777 == 0o755
        assert shared.read_text() == CONTROL_SITE_RC


class TestTheEditsAreWorkedOutOnce:
    """What `_refuse_plans` works out is what `_create_experiments` writes, since a large Rose configuration takes
    seconds to read, as long as it is of the same call."""

    @staticmethod
    def plans(changes):
        return [ExperimentPlan(name="exp-0001", layout=None, changes=changes, walltime_hours=None)]

    def test_once_for_a_call(self, tmp_path, home):
        manager = generating_manager(tmp_path)

        with mock.patch.object(CylcRoseManager, "_edits", autospec=True, side_effect=CylcRoseManager._edits) as edits:
            manager.generate_scaling_experiments([1.0], cores_per_node=4, walltime=2.0, allocations=WHOLE_BUDGET)

        assert edits.call_count == 1
        for experiment in manager.experiments.values():
            assert read_section(experiment.path / "rose-suite.conf", "jinja2:suite.rc")["CLOCK"] == "'PT2H00M'"

    @pytest.mark.parametrize(
        ("differs", "expected"),
        [
            ("plans", ["walltime = 1800", "per_cpu=4"]),
            ("control options", ["walltime = 7200", "per_cpu=2"]),
            ("control", ["walltime = 7200", "per_cpu=4", "# another control"]),
        ],
    )
    def test_again_for_anything_else(self, tmp_path, home, differs, expected):
        """The hook can be called on its own, with what `_refuse_plans` did not check."""
        manager = generating_manager(tmp_path)
        plans = self.plans({"site/gadi.rc": {"3600": "7200"}})
        options = {"site/gadi.rc": {"per_cpu=2": "per_cpu=4"}}
        manager._refuse_plans(plans, control_options=options)
        if differs == "plans":
            plans = self.plans({"site/gadi.rc": {"3600": "1800"}})
        elif differs == "control options":
            options = None
        else:
            other = write_control(tmp_path / "other")
            (other / "site/gadi.rc").write_text(CONTROL_SITE_RC + "# another control\n")
            manager.control = ExistingDirectoryControlSource(other)

        created = manager._create_experiments(plans, control_options=options)

        site = (created["exp-0001"].path / "site/gadi.rc").read_text()
        assert all(text in site for text in expected), site

    def test_again_for_a_second_call_with_the_same_plans(self, tmp_path, home):
        """From the files as they are by then."""
        manager = generating_manager(tmp_path)
        plans = self.plans({"site/gadi.rc": {"3600": "7200"}})
        manager._refuse_plans(plans)
        suite = manager._create_experiments(plans)["exp-0001"].path
        shutil.rmtree(suite)
        control_site = manager.work_dir / "config/site/gadi.rc"
        control_site.write_text(control_site.read_text() + "# 3600 seconds\n")

        manager._create_experiments(plans)

        assert (suite / "site/gadi.rc").read_text().endswith("# 7200 seconds\n")


class TestRefusedBeforeANumberIsTaken:
    """What cannot be written into a copy of the control is refused before anything is written."""

    @staticmethod
    def assert_nothing_was_written(manager):
        assert not (manager.work_dir / STUDY_FILENAME).exists(), "no number was taken"
        assert not (manager.work_dir / "exp-0001").exists()
        assert manager.experiments == {}

    @pytest.mark.parametrize(
        ("changes", "match"),
        [
            ({"rose-suite.conf": {"env": {"A": "1"}}}, r"rose-suite.conf has no section \[env\] in use"),
            ({"app/um/rose-app.conf": {"namelist:off": {"lflag": ".false."}}}, r"no section \[namelist:off\] in use"),
            ({"rose-suite.conf": {"um_layout": "4,4"}}, r"give '4,4' for 'um_layout'"),
            ({"rose-suite.conf": {"jinja2:suite.rc": {"um_layout": 4}}}, r"to 4, which is not a string"),
            (
                {"rose-suite.conf": {"jinja2:suite.rc": {"PROJECT": "'a'\n'b'"}}},
                r"Cannot set PROJECT in \[jinja2:suite.rc\] of rose-suite.conf",
            ),
            ({"rose-suite.conf": {"": {"jinja2:suite.rc": "x"}}}, r"Cannot set jinja2:suite.rc in rose-suite.conf"),
            ({"site/gadi.rc": {"walltime = 7200": "x"}}, r"replace 'walltime = 7200', which does not occur in it"),
            ({"site/gadi.rc": {"": "x"}}, r"replace an empty text"),
            ({"site/gadi.rc": {"3600": 7200}}, r"by 7200, which is not a string"),
            ({"site/gadi.rc": "walltime = 7200"}, r"The changes to site/gadi.rc are not a mapping"),
            ({"../gadi.rc": {"3600": "7200"}}, r"'../gadi.rc', which is not inside the suite"),
            ({"/etc/gadi.rc": {"3600": "7200"}}, r"'/etc/gadi.rc', which is not inside the suite"),
            (
                {"sites/gadi.rc": {"3600": "7200"}},
                r"'sites/gadi.rc', which is reached through the symbolic link .*/config/sites to a directory\.",
            ),
            ({"suite.rc": {"3600": "7200"}}, r"'suite.rc', which the suite .* does not have"),
        ],
        ids=[
            "a section the file lacks",
            "a section the file ignores",
            "a section given a value",
            "a value that is not a string",
            "a value of two lines",
            "a section given a value as a setting",
            "a text that does not occur",
            "an empty text",
            "a replacement that is not a string",
            "changes that are not a mapping",
            "a file outside the suite",
            "an absolute path",
            "a file reached through a link to a directory",
            "a file the suite lacks",
        ],
    )
    def test_a_change_that_cannot_be_made(self, tmp_path, home, changes, match):
        manager = generating_manager(tmp_path)

        with pytest.raises(ValueError, match=match):
            manager.generate_experiments([ConfigurationChoice(changes)])

        self.assert_nothing_was_written(manager)

    def test_a_control_option_that_cannot_be_made(self, tmp_path, home):
        manager = generating_manager(tmp_path)

        with pytest.raises(ValueError, match="replace 'walltime = 7200', which does not occur"):
            manager.generate_experiments(
                [ConfigurationChoice({})], control_options={"site/gadi.rc": {"walltime = 7200": "x"}}
            )

        self.assert_nothing_was_written(manager)

    def test_a_text_the_control_options_replaced(self, tmp_path, home):
        """The choice's changes are made in the text the control options left, where the text is no longer."""
        manager = generating_manager(tmp_path)

        with pytest.raises(ValueError, match="replace '3600', which does not occur"):
            manager.generate_experiments(
                [ConfigurationChoice({"site/gadi.rc": {"3600": "7200"}})],
                control_options={"site/gadi.rc": {"3600": "1800"}},
            )

        self.assert_nothing_was_written(manager)

    def test_a_control_suite_that_is_not_checked_out(self, tmp_path, home):
        """Nothing fetches it, so the message says where to check it out, and from where."""
        manager = generating_manager(tmp_path)
        (manager.work_dir / "config/rose-suite.conf").unlink()

        with pytest.raises(
            ValueError,
            match=r"The control suite is not in .*/work/config: there is no rose-suite.conf there\. Nothing here "
            r"fetches a suite, so check out git@github.com:ACCESS-NRI/mock-configs.git at release-1.0 into",
        ):
            manager.generate_experiments([ConfigurationChoice({})])

        self.assert_nothing_was_written(manager)

    def test_a_walltime_the_suite_states_in_no_variable(self, tmp_path, home):
        """As rAM3 does not: its walltime is a change of the study's own, given with generate_experiments."""
        manager = generating_manager(tmp_path, walltime_variable=None)

        with pytest.raises(ValueError, match="states its walltime in no variable of rose-suite.conf"):
            manager.generate_scaling_experiments([1.0], cores_per_node=4, walltime=2.0, allocations=WHOLE_BUDGET)

        self.assert_nothing_was_written(manager)

    def test_an_application_name_that_cannot_begin_a_suite_name(self, tmp_path, home):
        manager = generating_manager(tmp_path, name="n96e test")

        with pytest.raises(ValueError, match="The name of the application, 'n96e test', cannot begin the name"):
            manager.generate_experiments([ConfigurationChoice({})])

        self.assert_nothing_was_written(manager)

    def test_creating_without_a_control(self, tmp_path):
        """The hook is callable on its own, and the control is settable, so the hook checks it again."""
        manager = mock_manager(tmp_path / "work", tmp_path / "archive")
        plan = ExperimentPlan(name="exp-0001", layout=None, changes={}, walltime_hours=None)

        with pytest.raises(ValueError, match="Cannot create experiments without a control"):
            manager._create_experiments([plan])


class TestRegeneratingSuites:
    """A failed suite generated again has the correction written into its copy."""

    @staticmethod
    def failed_study(tmp_path):
        """A study of three suites, the first of which failed after its rose-suite.conf was corrected by hand."""
        manager = generating_manager(tmp_path)
        options = {"site/gadi.rc": {"3600": "36000"}}
        manager.generate_scaling_experiments(
            [1.0], cores_per_node=4, walltime=2.0, allocations=WHOLE_BUDGET, control_options=options
        )
        TestParseStatus._suite_db(manager.experiments["exp-0001"].run_path, [0, 1])
        conf = manager.experiments["exp-0001"].path / "rose-suite.conf"
        conf.write_text(conf.read_text().replace("${PROJECT}", "tm70"))
        return manager, options

    def test_the_correction_reaches_the_failed_suite_only(self, tmp_path, home):
        manager, options = self.failed_study(tmp_path)

        manager.generate_scaling_experiments(
            [1.0],
            cores_per_node=4,
            walltime=3.0,
            allocations=WHOLE_BUDGET,
            regenerate_failed=True,
            control_options=options,
        )

        assert list(manager.experiments) == ["exp-0001", "exp-0002", "exp-0003"]
        clocks = [
            read_section(experiment.path / "rose-suite.conf", "jinja2:suite.rc")["CLOCK"]
            for experiment in manager.experiments.values()
        ]
        assert clocks == ["'PT3H00M'", "'PT2H00M'", "'PT2H00M'"]
        assert manager.experiments["exp-0001"].status == ProfilingExperimentStatus.FAILED, "until it runs again"
        history = json.loads((manager.work_dir / STUDY_FILENAME).read_text())["history"]
        assert [entry["experiments"] for entry in history] == [["exp-0001", "exp-0002", "exp-0003"], ["exp-0001"]]

    def test_a_rose_file_is_edited_in_the_copy_and_another_taken_from_the_control(self, tmp_path, home):
        """A correction made by hand in the copy's rose-suite.conf is kept, and a text is not replaced twice."""
        manager, options = self.failed_study(tmp_path)

        manager.generate_scaling_experiments(
            [1.0],
            cores_per_node=4,
            walltime=3.0,
            allocations=WHOLE_BUDGET,
            regenerate_failed=True,
            control_options=options,
        )

        suite = manager.experiments["exp-0001"].path
        assert read_section(suite / "rose-suite.conf", "jinja2:suite.rc")["PROJECT"] == "'tm70'"
        assert "-l walltime = 36000\n" in (suite / "site/gadi.rc").read_text(), "not 360000"


class TestDeletingThroughALink:
    """Where the run directory is a link, as Rose makes it for a `root-dir`, the run and the link both go."""

    def test_the_run_the_link_leads_to_and_the_link(self, tmp_path, home):
        manager = generating_manager(tmp_path)
        manager.generate_experiments([ConfigurationChoice({})])
        run_path = manager.experiments["exp-0001"].run_path
        run = tmp_path / "scratch/cylc-run" / run_path.name
        (run / "log").mkdir(parents=True)
        run_path.parent.mkdir(parents=True)
        run_path.symlink_to(run)

        manager.delete_experiments(experiments=["exp-0001"])

        assert not run.exists()
        assert not run_path.is_symlink()
        assert not (manager.work_dir / "exp-0001").exists(), "the copy, and the record with its directory"
        assert (manager.work_dir / "config/rose-suite.conf").is_file(), "the control is left alone"

    def test_a_link_leading_nowhere(self, tmp_path, home, caplog):
        manager = generating_manager(tmp_path)
        manager.generate_experiments([ConfigurationChoice({})])
        run_path = manager.experiments["exp-0001"].run_path
        run_path.parent.mkdir(parents=True)
        run_path.symlink_to(tmp_path / "scratch/gone")

        with caplog.at_level(logging.INFO):
            manager.delete_experiments(experiments=["exp-0001"])

        assert not run_path.is_symlink()
        assert f"Deleting the link '{run_path}'." in caplog.text
        assert "Deleting run directory" not in caplog.text

    def test_a_dry_run_names_where_the_link_leads(self, tmp_path, home, caplog):
        """What the deletion would remove, on /scratch where Rose put the run, and not only the link in ~/cylc-run."""
        manager = generating_manager(tmp_path)
        manager.generate_experiments([ConfigurationChoice({})])
        run_path = manager.experiments["exp-0001"].run_path
        run = tmp_path / "scratch/cylc-run" / run_path.name
        run.mkdir(parents=True)
        run_path.parent.mkdir(parents=True)
        run_path.symlink_to(run)

        with caplog.at_level(logging.INFO):
            manager.delete_experiments(experiments=["exp-0001"], dry_run=True)

        assert f"and run directory '{run.resolve()}', which '{run_path}' links to, and then the link." in caplog.text
        assert run.is_dir()
        assert run_path.is_symlink()


def test_a_generated_suite_is_archived_and_found_again(tmp_path, home):
    """With the layout it was given, and the CPUs its rose-suite.conf states."""
    manager = generating_manager(tmp_path)
    layout = manager.select_layouts(4, allocations=WHOLE_BUDGET)[1]
    manager.generate_experiments([ConfigurationChoice(layout=layout)])
    TestParseStatus._suite_db(manager.experiments["exp-0001"].run_path, [0])

    manager.archive_experiments()

    later = CylcRoseManager(manager.work_dir, manager.archive_dir, manager.application)
    experiment = later.experiments["exp-0001"]
    assert experiment.status == ProfilingExperimentStatus.ARCHIVED
    assert (later.layout_key("exp-0001"), experiment.ncpus) == ("rose-layout_mock_atm_2x2", 4)


class TestLayoutsOfAddedSuites:
    """A suite added by hand has the layout its variables state, read back as the analyses need it."""

    @staticmethod
    def added(tmp_path, layouts):
        """Adds one suite per process grid, each stating it in `um_layout`, with a time for its `atmos` task."""
        manager = mock_manager(tmp_path / "work", tmp_path / "archive")
        for (nx, ny), seconds in layouts.items():
            name = f"u-{nx}x{ny}"
            write_suite_conf(manager.work_dir / name, {"um_layout": f"{nx},{ny}"})
            manager.add_rose_experiment(name)
            manager.data[name] = {
                "cylc_tasks": xr.Dataset(
                    {tmax: xr.DataArray([seconds], dims=["region"]).pint.quantify(tmax.units)},
                    coords={"region": ["atmos"]},
                )
            }
        return manager

    def test_the_layout_is_read_from_the_suite_s_variables(self, tmp_path):
        """With its process grid, since the configuration states the grid it decomposes."""

        manager = self.added(tmp_path, {(4, 2): 10.0})

        layout = manager._layout("u-4x2")

        assert [(sub.name, sub.n_cores, tuple(sub.decomposition.grid)) for sub in layout.sub_layouts] == [
            ("UM", 8, (4, 2))
        ]
        assert manager.layout_key("u-4x2") is None, "a layout read back identifies nothing"

    def test_the_layout_and_the_cpus_are_read_from_the_same_file(self, tmp_path):
        """That of the run, where it recorded one, since it says what the run used: here a 2 by 2 grid.

        The suite's own file states a 4 by 2 grid, which the run did not use.
        """
        manager = mock_manager(tmp_path / "work", tmp_path / "archive")
        write_suite_conf(manager.work_dir / "u-aa123", {"um_layout": "4,2"})
        run_path = tmp_path / "cylc-run" / "u-aa123"
        write_suite_conf(run_path, {"um_layout": "2,2"}, "log/rose-suite-run.conf")
        manager.add_rose_experiment("u-aa123", run_path=run_path)

        assert manager._layout("u-aa123").n_cores == 4
        assert manager._ncpus("u-aa123") == 4

    def test_its_cores_are_a_coordinate(self, tmp_path):
        manager = self.added(tmp_path, {(4, 2): 10.0, (2, 2): 18.0})

        gathered = manager.gather(RegionGroup("cylc_tasks", ["atmos"]), [component_cores("UM")])

        assert list(gathered["UM cores"].values) == [8, 4]

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_the_component_scaling_plot_reads_each_suite_against_its_cores(self, mock_plot, tmp_path):
        """A group of a Rose suite's regions names its component, since no log of a suite declares one."""

        manager = self.added(tmp_path, {(4, 2): 10.0, (2, 2): 18.0})

        manager.plot_component_scaling_data([RegionGroup("cylc_tasks", ["atmos"], component="UM")], tmax)

        (stat,) = mock_plot.call_args.args[0]
        seconds = stat[tmax].pint.dequantify().squeeze("region")
        assert {int(cores): float(seconds.sel(ncpus=cores)) for cores in stat["ncpus"].values} == {8: 10.0, 4: 18.0}


def test_delete_experiments_without_a_run_directory(tmp_path, caplog):
    """An experiment added without a Cylc run directory still has its own directory deleted.

    That is the state add_rose_experiment leaves behind whenever the run directory was not given, so deletion has
    to cope with it rather than fail on a run path that was never there.
    """

    manager = mock_manager(tmp_path / "work", tmp_path / "archive")
    exp_path = tmp_path / "work/u-aa123"
    write_suite_conf(exp_path, {"um_layout": "2,3"})
    manager.experiments["u-aa123"] = ProfilingExperiment(path=exp_path)

    with caplog.at_level(logging.WARNING):
        manager.delete_experiments(experiments=["u-aa123"])

    assert "u-aa123" not in manager.experiments
    assert not exp_path.exists()
    assert "Run directory" not in caplog.text


class TestSuiteRecords:
    """The record a suite gets when it is added, from which a manager built later finds it again."""

    ROSE = "u-aa123"

    @staticmethod
    def added(tmp_path, **kwargs):
        manager = mock_manager(tmp_path / "work", tmp_path / "archive")
        (manager.work_dir / TestSuiteRecords.ROSE).mkdir(parents=True)
        manager.add_rose_experiment(TestSuiteRecords.ROSE, **kwargs)
        return manager

    def test_it_states_where_the_suite_and_its_runs_are(self, tmp_path):
        run_path = tmp_path / "cylc-run" / self.ROSE
        manager = self.added(tmp_path, run_path=run_path, labels={"resolution": "n96e"})

        record = json.loads((manager.work_dir / self.ROSE / RECORD_FILENAME).read_text())

        assert record == {
            "version": 1,
            "path": self.ROSE,
            "run_path": str(run_path),
            "complete": False,
            "layout": None,
            "changes": None,
            "labels": {"resolution": "n96e"},
            "ncpus": None,
        }

    def test_a_later_manager_finds_the_suite_and_asks_how_it_went(self, tmp_path):
        """A suite is not added as complete, so its status is what its own records say, once they are read."""
        run_path = tmp_path / "cylc-run" / self.ROSE
        manager = self.added(tmp_path, run_path=run_path)
        TestParseStatus._suite_db(run_path, [0])

        later = mock_manager(manager.work_dir, manager.archive_dir)

        experiment = later.experiments[self.ROSE]
        assert (experiment.path, experiment.run_path) == (manager.work_dir / self.ROSE, run_path)
        assert experiment.status == ProfilingExperimentStatus.NEW
        later.update_statuses()
        assert experiment.status == ProfilingExperimentStatus.DONE

    def test_the_call_is_written_into_the_history(self, tmp_path):
        manager = self.added(tmp_path)

        (entry,) = json.loads((manager.work_dir / STUDY_FILENAME).read_text())["history"]

        assert (entry["call"], entry["experiments"]) == ("add_rose_experiment", [self.ROSE])
        assert (entry["manager"], entry["control"], entry["engine"]) == ("CylcRoseManager", None, {})

    def test_a_rose_named_as_generated_experiments_are_is_refused(self, tmp_path):
        manager = mock_manager(tmp_path / "work", tmp_path / "archive")
        (manager.work_dir / "exp-0001").mkdir(parents=True)

        with pytest.raises(ValueError, match="of the form this manager gives"):
            manager.add_rose_experiment("exp-0001")
        assert manager.experiments == {}

    def test_a_rose_that_is_not_one_directory_is_refused(self, tmp_path):
        """Its record would be kept where a later manager does not look for one."""
        manager = mock_manager(tmp_path / "work", tmp_path / "archive")
        (manager.work_dir / "suites" / "u-aa123").mkdir(parents=True)

        with pytest.raises(ValueError, match="Experiment name 'suites/u-aa123' is not the name of a single directory"):
            manager.add_rose_experiment("suites/u-aa123")
        assert manager.experiments == {}

    @pytest.mark.parametrize("labels", [{"grid": (4, 4)}, {"where": Path("/some/input")}])
    def test_labels_the_record_could_not_give_back_are_refused(self, tmp_path, labels):
        manager = mock_manager(tmp_path / "work", tmp_path / "archive")
        (manager.work_dir / self.ROSE).mkdir(parents=True)

        with pytest.raises(ValueError, match="The labels of an experiment must be JSON values"):
            manager.add_rose_experiment(self.ROSE, labels=labels)

        assert manager.experiments == {}, "nothing is held that a later manager would not find"
        assert not (manager.work_dir / self.ROSE / RECORD_FILENAME).exists()
        assert not (manager.work_dir / STUDY_FILENAME).exists()
