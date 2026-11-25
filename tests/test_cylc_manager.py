# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path
from unittest import mock

import pytest
import xarray as xr
from access.config.parallel_domain import Domain
from conftest import write_suite_conf

from access.profiling.control import ExistingDirectoryControlSource
from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.experiment import (
    RECORD_FILENAME,
    ConfigurationChoice,
    ExperimentPlan,
    ProfilingExperiment,
    ProfilingExperimentStatus,
)
from access.profiling.manager import STUDY_FILENAME, ProfilingManager, component_cores
from access.profiling.metrics import tmax
from access.profiling.plotting_utils import RegionGroup
from access.profiling.rose_configuration import RoseProcessGrid, RoseSuiteConfiguration


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
    """The state a suite reached, read back from the run statuses Cylc records per task."""

    @staticmethod
    def _suite_db(run_path: Path, statuses: list[int | None]) -> None:
        """Writes a cylc-suite.db recording one task job per given run status."""

        run_path.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(run_path / "cylc-suite.db")) as connection, connection:
            connection.execute("CREATE TABLE task_jobs (name TEXT, run_status INTEGER)")
            connection.executemany(
                "INSERT INTO task_jobs VALUES (?, ?)",
                [(f"task{i}", status) for i, status in enumerate(statuses)],
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
    """A suite whose run failed is offered again where retry_failed asks for it."""

    manager.experiments["failed"] = ProfilingExperiment(path=Path("/fake/failed"))
    manager.experiments["failed"].status = ProfilingExperimentStatus.FAILED
    manager.experiments["done"] = ProfilingExperiment(path=Path("/fake/done"))
    manager.experiments["done"].status = ProfilingExperimentStatus.DONE

    manager.run_experiments(retry_failed=True)

    mock_subprocess.assert_called_once_with(
        ["rose", "suite-run"], cwd=Path("/fake/failed"), check=True, capture_output=True, text=True
    )
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


class TestLayoutGenerationHook:
    """The creation hook the base class calls once a study has decided what to generate.

    Nothing here yet writes a change into a rose-suite.conf, so the hook declines rather than quietly creating
    experiments that run the control's own layout - a study of identical suites would look like a flat scaling curve
    rather than like the missing feature it is.
    """

    def test_creating_planned_experiments_is_refused(self, manager):
        """The refusal has to survive a well-formed plan: it is the engine that is missing, not the plan."""

        plan = ExperimentPlan(name="rose-layout_8x6", layout=mock.MagicMock(), changes={}, walltime_hours=2.0)

        with pytest.raises(NotImplementedError, match="rose-suite.conf"):
            manager._create_experiments([plan])

    @pytest.mark.parametrize("call", ["scaling", "choices"])
    def test_a_study_is_refused_before_it_takes_a_number(self, tmp_path, caplog, call):
        """The search finds layouts, and a choice without one asks the configuration nothing, so it is the hook that
        says no, through `_refuse_plans`.

        It says so before the study takes a number: nothing was started, so nothing is written and nothing is said to
        exist.
        """

        manager = CylcRoseManager(
            tmp_path / "work",
            tmp_path / "archive",
            mock_configuration(),
            control=ExistingDirectoryControlSource(tmp_path / "control"),
        )

        with pytest.raises(NotImplementedError, match="Generating experiments is not supported for Cylc Rose suites"):
            if call == "scaling":
                manager.generate_scaling_experiments([1.0], cores_per_node=4, walltime=2.0)
            else:
                manager.generate_experiments([ConfigurationChoice({"rose-suite.conf": {"NPROC": 4}})])

        assert not (manager.work_dir / STUDY_FILENAME).exists()
        assert "Some may already exist" not in caplog.text
        assert manager.experiments == {}


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
