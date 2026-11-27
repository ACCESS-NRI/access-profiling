# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import re
import shutil
import tarfile
from datetime import datetime
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from unittest import mock

import pytest
import xarray as xr
from conftest import MockApplication, layout_of, layout_with

from access.profiling.control import GitControlSource
from access.profiling.experiment import RECORD_FILENAME, ConfigurationChoice, component_cores
from access.profiling.manager import (
    STUDY_FILENAME,
    ProfilingExperiment,
    ProfilingExperimentStatus,
    ProfilingLog,
    ProfilingManager,
    _package_version,
)
from access.profiling.metrics import count, tavg
from access.profiling.results import Region


class MockProfilingManager(ProfilingManager[MockApplication]):
    """Test class inheriting from ProfilingManager to test its methods.

    This class will simulate parsing of some profiling data.

    Note that this mock class assumes that experiments are named after the last part of their path and all experiments
    will be marked as DONE.

    Args:
        paths (list[Path]): List of paths to simulate different configurations.
        ncpus (list[int]): List of number of CPUs corresponding to each path. An experiment without one has a
            count that cannot be read, which parse_ncpus raises as a ValueError, as an engine does.
        datasets (list[dict[str, xr.Dataset]]): What each experiment's logs parse to, keyed by log name, for each
            path. A test that needs logs of another kind patches profiling_logs instead.
        work_dir (Path): The working directory. A test that generates, adds or deletes experiments gives a real
            one, since the manager writes their records and the study file into it.
        archive_dir (Path): The archive directory.
        control_options (dict | None): The study's own changes to the control.
    """

    def __init__(
        self,
        paths: list[Path],
        ncpus: list[int] | None = None,
        datasets: list[dict[str, xr.Dataset]] | None = None,
        work_dir: Path = Path("/fake/work_dir"),
        archive_dir: Path = Path("/fake/archive_dir"),
        control_options: dict | None = None,
    ):
        super().__init__(work_dir, archive_dir, MockApplication(), control_options=control_options)

        # Pre-generate experiments
        for path in paths:
            self.experiments[path.name] = ProfilingExperiment(path=path)
            self.experiments[path.name].status = ProfilingExperimentStatus.DONE

        if ncpus is not None:
            self._mock_ncpus = dict(zip([path.name for path in paths], ncpus, strict=True))
        else:
            self._mock_ncpus = {}
        self._parse_ncpus_calls = []
        self._deleted_experiments = []
        self._created_plans = []
        # What parse_status reports, keyed by experiment name; anything absent reports None, meaning the state
        # could not be determined. Only consulted for experiments this manager thinks are running.
        self._mock_status: dict[str, ProfilingExperimentStatus] = {}
        self._parse_status_calls: list[Path] = []
        self._mock_datasets = dict(zip([path.name for path in paths], datasets, strict=True)) if datasets else {}

    def parse_status(self, path, run_path=None):
        self._parse_status_calls.append(path)
        return self._mock_status.get(path.name)

    def parse_ncpus(self, path, run_path=None):
        """Simulate parsing number of CPUs for a given path."""
        self._parse_ncpus_calls.append((path, run_path))
        if path.name not in self._mock_ncpus:
            raise ValueError(f"No job file says how many CPUs {path.name} occupied.")
        return self._mock_ncpus[path.name]

    def profiling_logs(self, path, run_path=None):
        """One run of each log the test gave the experiment, parsing to the dataset it gave."""
        logs = {}
        for name, dataset in self._mock_datasets.get(path.name, {}).items():
            log = mock.MagicMock(spec=ProfilingLog)
            log.filepath = path / f"{name}.log"
            log.optional = False
            log.parse.return_value = dataset
            logs[name] = {0: log}
        return logs

    def _delete_experiments(self, names, dry_run, **kwargs):
        """Record requested deletions instead of touching the filesystem."""
        self._deleted_experiments.append((names, dry_run))

    def run_experiments(self, retry_failed=False):
        """Nothing to run: these tests drive the experiments' statuses themselves."""

    def _create_experiments(self, plans):
        """Record what generation decided, and register everything new."""
        self._created_plans.append(plans)
        return {
            plan.name: ProfilingExperiment(path=self.work_dir / plan.name, layout=plan.layout)
            for plan in plans
            if not plan.regenerating
        }


def make_component_dataset(tavg_values: list[float]) -> dict[str, xr.Dataset]:
    """Builds mock parsed profiling data for a single component with two regions.

    Args:
        tavg_values (list[float]): Values of the tavg metric for "Region 1" and "Region 2".

    Returns:
        dict[str, xr.Dataset]: Mock data for a single component, named "component".
    """
    regions = ["Region 1", "Region 2"]
    count_array = xr.DataArray([1, 2], dims=["region"]).pint.quantify(count.units)
    tavg_array = xr.DataArray(list(tavg_values), dims=["region"]).pint.quantify(tavg.units)
    return {"component": xr.Dataset(data_vars={count: count_array, tavg: tavg_array}, coords={"region": regions})}


def make_profiling_log(filepath, optional, error=None):
    """Helper function creating a mock profiling log with an explicitly configured boolean 'optional' attribute.

    Args:
        filepath (str): Path reported by the log, as it appears in the log messages.
        optional (bool): Whether the log is optional. Set explicitly, as a bare mock attribute would be truthy.
        error (BaseException | None): Exception raised by parse(). If None, parse() returns an empty dataset.

    Returns:
        mock.MagicMock: Mock log built with a ProfilingLog spec, so that reads of attributes the real class does
        not define are caught.
    """

    log = mock.MagicMock(spec=ProfilingLog)
    log.filepath = Path(filepath)
    log.optional = optional
    if error is not None:
        log.parse.side_effect = error
    else:
        log.parse.return_value = xr.Dataset()
    return log


@pytest.fixture()
def single_exp_manager():
    """Fixture instantiating a manager with a single completed experiment named 'exp1'."""

    manager = MockProfilingManager(paths=[Path("/fake/work_dir/exp1")])
    manager.experiments["exp1"].run_path = Path("/fake/runs/exp1")
    return manager


def test_repr():
    """Test the __repr__ method of ProfilingManager.

    It holds no parsed data, which the results of parse_profiling_data hold. Parsing keeps only the count of CPUs on
    each experiment, which this manager cannot read, so its repr is the same before and after.
    """

    manager = MockProfilingManager(paths=[Path("/fake/work_dir")], datasets=[make_component_dataset([1.0, 2.0])])
    expected = """<MockProfilingManager>
    Application: mock
    Working directory: PosixPath('/fake/work_dir')
    Archive directory: PosixPath('/fake/archive_dir')
    Experiments:
        'work_dir': ProfilingExperiment(path=PosixPath('/fake/work_dir'), status=DONE)
"""
    assert repr(manager) == expected

    manager.parse_profiling_data()

    assert repr(manager) == expected


def test_repr_names_the_control_the_study_perturbs():
    """A manager profiling the same application against two releases in turn must be able to say which one.

    The control line is printed only when there is one - test_repr covers the manager that has none - so a repr
    that dropped it would still look entirely plausible, while two studies of 'mock' became indistinguishable.
    The label is what is shown, not the repository, because that is the short identifier a control exists to give.
    """

    manager = MockProfilingManager(paths=[Path("/fake/work_dir")])
    manager.control = GitControlSource("https://example.com/configurations.git", "release-2025.01.000")

    expected = """<MockProfilingManager>
    Application: mock
    Control: release-2025.01.000
    Working directory: PosixPath('/fake/work_dir')
    Archive directory: PosixPath('/fake/archive_dir')
    Experiments:
        'work_dir': ProfilingExperiment(path=PosixPath('/fake/work_dir'), status=DONE)
"""
    assert repr(manager) == expected


class TestControlChanges:
    """What the application needs changed in the control, with the study's own options merged over it."""

    NEEDED = {"config.yaml": {"env": {"PROFILE": "on", "OUTPUT": "SUMMARY"}, "queue": "normal"}}

    @pytest.fixture
    def needing(self):
        with mock.patch.object(
            MockApplication, "control_changes", new_callable=mock.PropertyMock, return_value=self.NEEDED
        ):
            yield

    def test_the_study_wins_where_both_change_a_setting(self, needing, tmp_path):
        options = {"config.yaml": {"env": {"OUTPUT": "BINARY"}, "umask": 18}}
        manager = MockProfilingManager([], work_dir=tmp_path, control_options=options)

        assert manager._control_options() == {
            "config.yaml": {"env": {"PROFILE": "on", "OUTPUT": "BINARY"}, "queue": "normal", "umask": 18}
        }
        assert manager._engine_record() == {"control_options": manager._control_options()}
        assert options == {"config.yaml": {"env": {"OUTPUT": "BINARY"}, "umask": 18}}, "and changes neither"
        assert self.NEEDED["config.yaml"]["env"] == {"PROFILE": "on", "OUTPUT": "SUMMARY"}

    def test_two_lists_for_one_setting_are_refused(self, tmp_path):
        manager = MockProfilingManager([], work_dir=tmp_path, control_options={"config.yaml": {"modules": [["load"]]}})

        with (
            mock.patch.object(
                MockApplication,
                "control_changes",
                new_callable=mock.PropertyMock,
                return_value={"config.yaml": {"modules": [["use"]]}},
            ),
            pytest.raises(ValueError, match="the control changes of the application and the control options"),
        ):
            manager._control_options()

    def test_the_manager_keeps_a_copy_of_the_options_it_is_given(self, tmp_path):
        """A notebook may go on editing the dictionary, which then does not change what the study generates."""
        options = {"config.yaml": {"umask": 18}}
        manager = MockProfilingManager([], work_dir=tmp_path, control_options=options)

        options["config.yaml"]["umask"] = 0

        assert manager.control_options == {"config.yaml": {"umask": 18}}

    def test_the_manager_says_both(self, needing):
        """What the application adds is not in the notebook that made the manager, so the manager says it."""
        manager = MockProfilingManager([], control_options={"config.yaml": {"umask": 18}})

        lines = repr(manager).splitlines()

        assert f"    Control changes of the application: {self.NEEDED}" in lines
        assert "    Control options: {'config.yaml': {'umask': 18}}" in lines


def test_status_asks_the_engine_and_shows_the_labels():
    """A status does not outlive the session, so the table of them is read afresh, and says what the names do not."""
    manager = MockProfilingManager([Path("/fake/work_dir/a"), Path("/fake/work_dir/b")], ncpus=[4, 8])
    manager.experiments["a"].status = ProfilingExperimentStatus.RUNNING
    manager.experiments["b"].labels = {"dt": 900}
    manager._mock_status = {"a": ProfilingExperimentStatus.DONE}

    status = manager.status()

    assert list(status.index) == ["a", "b"]
    assert list(status["status"]) == ["DONE", "DONE"]
    assert status.loc["b", "dt"] == 900
    assert list(status.columns) == ["status", "ncpus", "layout", "dt"]
    assert MockProfilingManager([]).status().empty


def test_archive_discovery(tmp_path):
    """Test that ProfilingManager discovers archived experiments correctly."""

    archive_dir = tmp_path / "archive_dir"

    # Test when archive directory does not exist
    manager = MockProfilingManager(paths=[], archive_dir=archive_dir)
    assert manager.experiments == {}, "No experiments should be discovered if archive dir does not exist."

    # Test when archive directory exists, but what is named as an archive is not a file
    (archive_dir / "exp0.tar.gz").mkdir(parents=True)
    manager = MockProfilingManager(paths=[], archive_dir=archive_dir)
    assert manager.experiments == {}, "No experiments should be discovered if no archive files are present."

    # Test when archive directory exists and files are present. These archives hold no record, as those made
    # before experiments had one do not, so nothing is known of them but where they are.
    for name in ("exp1", "exp2"):
        tarfile.open(archive_dir / f"{name}.tar.gz", "w:gz").close()
    manager = MockProfilingManager(paths=[], archive_dir=archive_dir)
    assert set(manager.experiments.keys()) == {"exp1", "exp2"}
    for name, experiment in manager.experiments.items():
        assert experiment.path == archive_dir / f"{name}.tar.gz"
        assert experiment.status == ProfilingExperimentStatus.ARCHIVED
        assert experiment.changes is None and experiment.labels == {} and experiment.layout is None


class TestUnreadableArchives:
    """An archive that cannot be read is skipped with a warning, rather than stopping the manager being built."""

    @staticmethod
    def truncated(archive):
        """Cuts a tarball short, as an archive_experiments that was stopped part way leaves it."""
        archive.write_bytes(archive.read_bytes()[:40])

    @pytest.mark.parametrize("damage", ["empty", "truncated", "not gzip"])
    def test_it_is_skipped_and_the_experiment_of_the_working_directory_kept(self, tmp_path, caplog, damage):
        """The experiment found from its record is kept, so that it can be archived again over the broken file."""
        manager = added(tmp_path)
        manager.archive_experiments()
        archive = manager.archive_dir / "release.tar.gz"
        if damage == "empty":
            archive.write_bytes(b"")
        elif damage == "truncated":
            self.truncated(archive)
        else:
            archive.write_text("not an archive")

        with caplog.at_level(logging.WARNING):
            found = later(manager)

        assert f"Skipping the archive '{archive}', which cannot be read" in caplog.text
        assert "overwrite=True" in caplog.text
        assert found.experiments["release"].path == manager.work_dir / "release"
        assert found.experiments["release"].status == ProfilingExperimentStatus.DONE

    def test_the_others_are_still_found(self, tmp_path):
        archive_dir = tmp_path / "archive_dir"
        archive_dir.mkdir()
        tarfile.open(archive_dir / "exp-0001.tar.gz", "w:gz").close()
        (archive_dir / "exp-0002.tar.gz").write_bytes(b"")

        manager = MockProfilingManager(paths=[], archive_dir=archive_dir)

        assert list(manager.experiments) == ["exp-0001"]

    def test_a_record_in_another_format_is_still_refused(self, tmp_path):
        """A record that can be read and is not in this version's format is not damage, and is not skipped."""
        manager = added(tmp_path)
        manager.archive_experiments()
        shutil.rmtree(manager.work_dir)

        with (
            mock.patch("access.profiling.experiment.RECORD_VERSION", 2),
            pytest.raises(ValueError, match="The record in the archive .* states the format version 1"),
        ):
            later(manager)


def test_archived_experiments_are_found_in_the_order_of_their_names(tmp_path):
    """Whatever order the file system lists the archives in, as the records of the working directory are found."""

    archive_dir = tmp_path / "archive_dir"
    archive_dir.mkdir()
    for name in ("exp-0002", "exp-0010", "exp-0001"):
        tarfile.open(archive_dir / f"{name}.tar.gz", "w:gz").close()

    listed = Path.glob
    with mock.patch.object(
        Path, "glob", autospec=True, side_effect=lambda path, pattern: sorted(listed(path, pattern), reverse=True)
    ):
        manager = MockProfilingManager(paths=[], archive_dir=archive_dir)

    assert list(manager.experiments) == ["exp-0001", "exp-0002", "exp-0010"]


class TestUpdateStatuses:
    """Bringing statuses up to date with the state the runs actually reached."""

    def test_only_archived_experiments_are_left_alone(self):
        """Every other state can still move, so every other state is asked about."""

        paths = [Path("/a/new"), Path("/a/running"), Path("/a/done"), Path("/a/failed"), Path("/a/archived")]
        manager = MockProfilingManager(paths)
        for name in manager.experiments:
            manager.experiments[name].status = ProfilingExperimentStatus[name.upper()]
        manager._mock_status = dict.fromkeys(manager.experiments, ProfilingExperimentStatus.RUNNING)

        manager.update_statuses()

        assert sorted(path.name for path in manager._parse_status_calls) == ["done", "failed", "new", "running"]
        assert manager.experiments["archived"].status == ProfilingExperimentStatus.ARCHIVED, (
            "an archive does not change, and asking would mean extracting it to be told so"
        )

    def test_a_new_experiment_run_in_an_earlier_session_is_recognised(self):
        """The case this is all for: a status does not outlive the session, but the run's record does."""

        manager = MockProfilingManager([Path("/a/expt")])
        manager.experiments["expt"].status = ProfilingExperimentStatus.NEW
        manager._mock_status = {"expt": ProfilingExperimentStatus.DONE}

        manager.update_statuses()

        assert manager.experiments["expt"].status == ProfilingExperimentStatus.DONE

    def test_a_failed_experiment_rerun_by_hand_is_recognised(self):
        manager = MockProfilingManager([Path("/a/expt")])
        manager.experiments["expt"].status = ProfilingExperimentStatus.FAILED
        manager._mock_status = {"expt": ProfilingExperimentStatus.DONE}

        manager.update_statuses()

        assert manager.experiments["expt"].status == ProfilingExperimentStatus.DONE

    def test_a_done_experiment_being_rerun_goes_back_to_running(self):
        """Which is what keeps its half-written output from being parsed or archived mid-run."""

        manager = MockProfilingManager([Path("/a/expt")])
        manager.experiments["expt"].status = ProfilingExperimentStatus.DONE
        manager._mock_status = {"expt": ProfilingExperimentStatus.RUNNING}

        manager.update_statuses()

        assert manager.experiments["expt"].status == ProfilingExperimentStatus.RUNNING

    def test_an_undetermined_state_leaves_the_status_alone(self):
        manager = MockProfilingManager([Path("/a/expt")])
        manager.experiments["expt"].status = ProfilingExperimentStatus.RUNNING
        manager._mock_status = {}  # parse_status returns None

        manager.update_statuses()

        assert manager.experiments["expt"].status == ProfilingExperimentStatus.RUNNING

    def test_it_is_asked_again_each_time(self):
        """A status changes while a run is queued, so unlike the CPU count it must not be cached."""

        manager = MockProfilingManager([Path("/a/expt")])
        manager.experiments["expt"].status = ProfilingExperimentStatus.RUNNING

        manager.update_statuses()
        manager.update_statuses()

        assert len(manager._parse_status_calls) == 2


def test_archive_experiments_picks_up_a_run_that_has_since_finished(tmp_path):
    """Test that archiving asks the run how it went rather than trusting a stale status.

    Without this the experiment is still RUNNING as far as the manager knows, and archive() skips it.
    """

    manager = MockProfilingManager([tmp_path / "expt"], ncpus=[4])
    manager.archive_dir = tmp_path / "archive_dir"
    manager.experiments["expt"].status = ProfilingExperimentStatus.RUNNING
    manager._mock_status = {"expt": ProfilingExperimentStatus.DONE}

    with mock.patch.object(ProfilingExperiment, "archive") as mock_archive:
        manager.archive_experiments()

    assert manager.experiments["expt"].status == ProfilingExperimentStatus.DONE
    mock_archive.assert_called_once()


def test_parse_profiling_data_picks_up_a_run_that_has_since_finished():
    """Test that parsing asks the run how it went, so freshly finished data is not skipped."""

    manager = MockProfilingManager([Path("/a/expt")])
    manager.experiments["expt"].status = ProfilingExperimentStatus.RUNNING
    manager._mock_status = {"expt": ProfilingExperimentStatus.DONE}

    with mock.patch.object(MockProfilingManager, "profiling_logs", return_value={}):
        results = manager.parse_profiling_data()

    assert manager.experiments["expt"].status == ProfilingExperimentStatus.DONE
    assert "expt" in results.data, "so its data is parsed rather than passed over as still running"


def test_parse_profiling_data_skips_a_run_that_failed(caplog):
    """Test that a crashed run is reported as FAILED rather than left looking like it is still going."""

    manager = MockProfilingManager([Path("/a/expt")])
    manager.experiments["expt"].status = ProfilingExperimentStatus.RUNNING
    manager._mock_status = {"expt": ProfilingExperimentStatus.FAILED}

    with caplog.at_level(logging.WARNING):
        results = manager.parse_profiling_data()

    assert manager.experiments["expt"].status == ProfilingExperimentStatus.FAILED
    assert "expt" not in results.data
    assert results.skipped == {"expt": "its status is FAILED"}, "the results say why it is not among them"
    assert "FAILED" in caplog.text


@mock.patch("access.profiling.manager.Path.mkdir")
@mock.patch("access.profiling.manager.ProfilingExperiment.archive")
def test_archive_experiments(mock_archive, mock_mkdir):
    """Test the archive_experiments method of ProfilingManager."""

    # Setup mock experiments
    exp_paths = [Path("/fake/work_dir/exp1"), Path("/fake/work_dir/exp2"), Path("/fake/work_dir/exp3")]
    manager = MockProfilingManager(exp_paths, ncpus=[4, 8, 16])
    manager.experiments["exp2"].status = ProfilingExperimentStatus.RUNNING
    manager.experiments["exp3"].status = ProfilingExperimentStatus.NEW

    # Archive experiments
    manager.archive_experiments()

    # Check calls
    mock_mkdir.assert_called_with(parents=True, exist_ok=True)  # Check archive directory creation
    assert mock_archive.call_count == 3, "Should attempt to archive all experiments."
    for name in ("exp1", "exp2", "exp3"):
        mock_archive.assert_any_call(
            Path("/fake/archive_dir") / name,
            exclude_files=None,
            exclude_dirs=None,
            follow_symlinks=False,
            overwrite=False,
            record=manager.experiments[name].record(manager.work_dir),
        )

    # Only the finished experiment has a count worth keeping: its run is over, so the count cannot change.
    assert manager._parse_ncpus_calls == [(Path("/fake/work_dir/exp1"), None)]
    assert manager.experiments["exp1"].record(manager.work_dir)["ncpus"] == 4


def test_add_experiment_from_directory(tmp_path):
    """Test the add_experiment_from_directory method of ProfilingManager."""

    work_dir = tmp_path / "work_dir"
    for directory in ("existing_experiment", "relative_experiment"):
        (work_dir / directory).mkdir(parents=True)
    (tmp_path / "outside_work_dir" / "outside_experiment").mkdir(parents=True)
    manager = MockProfilingManager(paths=[], work_dir=work_dir)

    # Test adding a valid experiment
    manager.add_experiment_from_directory("existing_experiment", work_dir / "existing_experiment")
    assert "existing_experiment" in manager.experiments, "Experiment should be added."
    assert manager.experiments["existing_experiment"].status == ProfilingExperimentStatus.DONE, (
        "Experiment status should be set to DONE."
    )

    # Test adding a valid experiment with relative path
    manager.add_experiment_from_directory("relative_experiment", Path("relative_experiment"))
    assert "relative_experiment" in manager.experiments, "Experiment with relative path should be added."
    assert manager.experiments["relative_experiment"].status == ProfilingExperimentStatus.DONE, (
        "Experiment status should be set to DONE."
    )
    assert manager.experiments["relative_experiment"].path == work_dir / "relative_experiment", (
        "Experiment path should be correctly resolved to absolute path."
    )

    # Test adding an experiment with a non-existing path
    with pytest.raises(ValueError, match="does not exist or is not a directory"):
        manager.add_experiment_from_directory("non_existing_experiment", work_dir / "non_existing_experiment")

    # Test adding an experiment outside the working directory
    with pytest.raises(ValueError, match="is not inside the working directory"):
        manager.add_experiment_from_directory("outside_experiment", tmp_path / "outside_work_dir/outside_experiment")


class TestDirectoriesGivenAsStrings:
    """A notebook types a directory as a string, which is taken as the path it names."""

    def test_the_directories_of_a_manager(self, tmp_path):
        manager = MockProfilingManager(paths=[], work_dir=str(tmp_path / "work"), archive_dir=str(tmp_path / "archive"))

        assert manager.work_dir == tmp_path / "work"
        assert manager.archive_dir == tmp_path / "archive"

    def test_the_directory_of_an_added_experiment(self, tmp_path):
        """Relative to the working directory, as a path is, and found again by a manager built from strings."""

        (tmp_path / "work" / "added").mkdir(parents=True)
        manager = MockProfilingManager(paths=[], work_dir=str(tmp_path / "work"))

        manager.add_experiment_from_directory("added", "added")

        assert manager.experiments["added"].path == tmp_path / "work" / "added"
        found = MockProfilingManager(paths=[], work_dir=str(tmp_path / "work"))
        assert found.experiments["added"].path == tmp_path / "work" / "added"


def test_forget_experiment(caplog, tmp_path):
    """Test the forget_experiment method of ProfilingManager."""

    # Setup mock experiments
    exp_paths = [tmp_path / "exp1", tmp_path / "exp2"]
    manager = MockProfilingManager(exp_paths, work_dir=tmp_path)

    # Delete an existing experiment
    manager.forget_experiment("exp1")
    assert "exp1" not in manager.experiments, "Experiment 'exp1' should be deleted."

    # Attempt to delete a non-existing experiment
    with caplog.at_level(logging.WARNING):
        manager.forget_experiment("non_existing_exp")
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert len(manager.experiments) == 1 and "exp2" in manager.experiments, (
        "Only 'exp2' should remain after attempting to delete a non-existing experiment."
    )


def test_parse_profiling_data(caplog):
    """Test the parse_profiling_data method of ProfilingManager, which returns the logs each experiment holds."""

    exp_name = "exp1"
    manager = MockProfilingManager(paths=[Path("/fake/work_dir/" + exp_name)])
    manager.experiments[exp_name].run_path = Path("/fake/runs/exp1")

    with mock.patch.object(manager, "profiling_logs") as mock_profiling_logs:
        # Setup mock profiling logs
        mock_log = mock.MagicMock()
        type(mock_log).optional = mock.PropertyMock(side_effect=[False, False, True])
        mock_log.parse.side_effect = (xr.Dataset(), xr.Dataset(), FileNotFoundError("Mocked missing file."))
        mock_profiling_logs.return_value = {
            "log": {0: mock_log},
            "optional_log": {0: mock_log},
            "missing_log": {0: mock_log},
        }

        # Parse profiling data for each experiment
        results = manager.parse_profiling_data()
        assert "log" in results.data[exp_name], "Parsed datasets should contain 'log' key."
        assert "optional_log" in results.data[exp_name], "Parsed datasets should contain 'optional_log' key."
        assert "missing_log" not in results.data[exp_name], (
            "Parsed datasets should not contain 'missing_log' key as the file is missing."
        )
        assert mock_log.parse.call_count == 3, "Parse method should be called three times."
        assert "run" not in results.data[exp_name]["log"].dims, "A single run should not add a 'run' dimension."
        mock_profiling_logs.assert_called_once_with(Path("/fake/work_dir/exp1"), Path("/fake/runs/exp1"))
        assert manager._parse_ncpus_calls == [(Path("/fake/work_dir/exp1"), Path("/fake/runs/exp1"))], (
            "the count is read from the same directories as the logs"
        )

    manager.experiments[exp_name].status = ProfilingExperimentStatus.RUNNING
    with caplog.at_level(logging.WARNING):
        manager.parse_profiling_data()
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert "is not completed" in caplog.records[0].message


def test_parse_profiling_data_optional_log_failures_are_skipped_and_logged(single_exp_manager, caplog):
    """Test that optional logs failing to parse are skipped and logged at INFO level, regardless of exception type.

    Missing logs are handled by a dedicated FileNotFoundError branch preceding the general one, so this also pins the
    order of the two except clauses and the fact that other OSError failures are not treated as missing files. The
    healthy log is deliberately placed last, so that parsing is checked to resume with the logs left to handle.
    """

    class MissingLogError(FileNotFoundError):
        """Subclass of FileNotFoundError, as raised by some of the standard library helpers."""

    class CustomParsingError(Exception):
        """Parser failure outside the standard exception hierarchy.

        Optional logs are generated by cross-producting task logs with all known parsers, so parsers are routinely
        handed logs they do not understand and may fail with arbitrary exception types.
        """

    errors = {
        "bad_log": ValueError("boom"),
        "denied_log": PermissionError("denied"),
        "custom_log": CustomParsingError("nope"),
        "missing_log": FileNotFoundError("Mocked missing file."),
        "subclass_missing_log": MissingLogError("Mocked missing file."),
    }
    manager = single_exp_manager
    with mock.patch.object(manager, "profiling_logs") as mock_profiling_logs:
        mock_profiling_logs.return_value = {
            name: {0: make_profiling_log(f"/fake/{name}.out", optional=True, error=error)}
            for name, error in errors.items()
        } | {"good_log": {0: make_profiling_log("/fake/good_log.out", optional=True)}}

        # Parsing should return normally, without propagating any of the parsing errors. Capturing at INFO keeps
        # the records of a missing log as well as those of a log that cannot be read.
        with caplog.at_level(logging.INFO):
            results = manager.parse_profiling_data()

    failed = [record for record in caplog.records if "Failed to parse optional profiling log" in record.message]
    not_found = [record for record in caplog.records if "not found. Skipping." in record.message]
    other_format = [record for record in caplog.records if "is not in the format of its parser" in record.message]
    assert (len(failed), len(not_found), len(other_format)) == (2, 2, 1), (
        "The FileNotFoundError branch should catch exactly the missing-log errors, including subclasses, and the "
        "ValueError branch a log in another format; every other exception type, including other OSErrors, should "
        "fall through to the general branch."
    )
    assert {record.levelname for record in failed} == {"WARNING"}, (
        "A parser that fails in a way it does not mean to is reported as a warning."
    )
    assert {record.levelname for record in not_found + other_format} == {"INFO"}, (
        "A run need not write an optional log, and a Rose suite hands every parser the log of every task."
    )

    by_name = {"bad_log": other_format[0], "denied_log": failed[0], "custom_log": failed[1]}
    by_name |= {"missing_log": not_found[0], "subclass_missing_log": not_found[1]}
    for name, record in by_name.items():
        assert f"/fake/{name}.out" in record.message, f"The record reported for '{name}' should carry its own path."
    assert failed[0].message == (
        "Failed to parse optional profiling log '/fake/denied_log.out' with exception:\n    denied\nSkipping."
    ), "The warning should report the log path and the exception message, each on its own line."
    assert other_format[0].message == (
        "Optional profiling log '/fake/bad_log.out' is not in the format of its parser: boom Skipping."
    )
    assert [record.message for record in caplog.records].count(" Done.") == 1, (
        "' Done.' should only be logged for the log which was parsed successfully."
    )
    assert list(results.data["exp1"]) == ["good_log"], (
        "Only the healthy log, handled after the failing ones, should be stored."
    )


@pytest.mark.parametrize(
    ("optional", "error"),
    [(False, ValueError("boom")), (True, KeyboardInterrupt()), (True, SystemExit())],
    ids=["non_optional_log", "keyboard_interrupt", "system_exit"],
)
def test_parse_profiling_data_errors_outside_the_swallowed_set_propagate(single_exp_manager, caplog, optional, error):
    """Test that the failures the optional log handler is not meant to swallow still propagate.

    Namely failures of non-optional logs, which never reach the handler, and BaseExceptions such as an interrupt,
    which the 'except Exception' clause is not supposed to catch.
    """

    manager = single_exp_manager
    with mock.patch.object(manager, "profiling_logs") as mock_profiling_logs:
        mock_profiling_logs.return_value = {
            "log": {0: make_profiling_log("/fake/log.out", optional=optional, error=error)}
        }

        with caplog.at_level(logging.INFO), pytest.raises(type(error)):
            manager.parse_profiling_data()

    failed = [record for record in caplog.records if "Failed to parse optional profiling log" in record.message]
    assert failed == [], f"{type(error).__name__} should not be caught and logged as a swallowed failure."


class GrammarError(Exception):
    """An error of a parser's own, as lark raises on a Rose configuration it cannot read."""


class TestParsingReadsWhatTellsTheExperimentsApart:
    """Parsing reads the CPUs and the layout of each experiment while its directory is open, once."""

    @staticmethod
    def three_experiments(**kwargs):
        paths = [Path("2cpu_slow"), Path("1cpu"), Path("2cpu_fast")]
        datasets = [make_component_dataset([400.0, 4.0]), make_component_dataset([800.0, 8.0])] + [
            make_component_dataset([300.0, 3.0])
        ]
        return MockProfilingManager(paths, datasets=datasets, **kwargs)

    def test_the_cpus_are_read_once_and_kept_on_the_experiment(self):
        manager = self.three_experiments(ncpus=[2, 1, 2])
        assert manager.experiments["1cpu"].ncpus is None  # Nothing has needed it yet

        manager.parse_profiling_data()
        results = manager.parse_profiling_data()

        assert len(manager._parse_ncpus_calls) == 3  # One per experiment, not one per parse
        assert manager.experiments["1cpu"].ncpus == 1
        assert [experiment.ncpus for experiment in results.experiments.values()] == [2, 1, 2]

    def test_a_count_that_cannot_be_read_stops_nothing(self):
        """It is kept with its reason, for a selection or a plot that needs it to raise."""

        manager = self.three_experiments(ncpus=None)

        results = manager.parse_profiling_data()

        assert results.experiments["1cpu"].ncpus is None
        assert results.experiments["1cpu"].ncpus_error == "No job file says how many CPUs 1cpu occupied."
        with pytest.raises(ValueError, match="The number of CPUs of experiment '2cpu_slow' is not known: No job"):
            results.select_best_experiments(Region("component", "Region 1"), metric=tavg)

    def test_the_layout_is_read_back_once_and_kept(self):
        """An experiment this manager did not generate has its layout read off its configuration."""

        manager = MockProfilingManager([Path("expt")], ncpus=[8], datasets=[make_component_dataset([1.0, 2.0])])
        manager.application.layouts["expt"] = layout_with(component=4)

        manager.parse_profiling_data()
        results = manager.parse_profiling_data()

        assert layout_of(results.experiments["expt"].layout).n_cores == 4
        assert len(manager.application.parse_layout_calls) == 1
        assert manager.experiments["expt"].layout is not None

    def test_a_layout_the_files_do_not_state_stays_unknown(self):
        manager = MockProfilingManager([Path("expt")], ncpus=[8], datasets=[make_component_dataset([1.0, 2.0])])

        results = manager.parse_profiling_data()

        assert results.experiments["expt"].layout is None
        assert results.experiments["expt"].layout_error is None
        assert manager.experiments["expt"].layout is None

    @pytest.mark.parametrize("error", [FileNotFoundError("no config.yaml"), GrammarError("unexpected character")])
    def test_whatever_the_readers_raise_stops_nothing(self, error):
        """A missing configuration file, or one a parser of its own cannot read, is kept as the reason."""
        manager = MockProfilingManager([Path("expt")], ncpus=[8], datasets=[make_component_dataset([1.0, 2.0])])

        with (
            mock.patch.object(manager, "parse_ncpus", side_effect=error),
            mock.patch.object(manager.application, "parse_layout", side_effect=error),
        ):
            results = manager.parse_profiling_data()

        assert (results.experiments["expt"].ncpus_error, results.experiments["expt"].layout_error) == (
            str(error),
            str(error),
        )

    def test_a_layout_that_cannot_be_read_stops_nothing(self):
        manager = MockProfilingManager([Path("expt")], ncpus=[8], datasets=[make_component_dataset([1.0, 2.0])])

        with mock.patch.object(manager.application, "parse_layout", side_effect=ValueError("no variables")):
            results = manager.parse_profiling_data()

        assert results.experiments["expt"].layout_error == "no variables"
        with pytest.raises(ValueError, match="The layout of experiment 'expt' could not be read: no variables"):
            results.gather(Region("component", "Region 1"), [component_cores("component")])

    def test_a_layout_given_when_it_was_generated_is_not_read_again(self):
        manager = MockProfilingManager([Path("expt")], ncpus=[8], datasets=[make_component_dataset([1.0, 2.0])])
        manager.experiments["expt"].layout = layout_with(component=4)

        results = manager.parse_profiling_data()

        assert results.experiments["expt"].layout == layout_with(component=4)
        assert manager.application.parse_layout_calls == []

    def test_parsing_again_leaves_earlier_results_as_they_were(self):
        """Each call reads the logs afresh, so a notebook can compare what two parses returned."""

        manager = MockProfilingManager([Path("expt")], ncpus=[8], datasets=[make_component_dataset([1.0, 2.0])])
        before = manager.parse_profiling_data()

        manager._mock_datasets["expt"] = make_component_dataset([3.0, 4.0])
        after = manager.parse_profiling_data()

        assert before.data["expt"]["component"][tavg].pint.dequantify().values.tolist() == [1.0, 2.0]
        assert after.data["expt"]["component"][tavg].pint.dequantify().values.tolist() == [3.0, 4.0]

    def test_the_labels_go_with_the_data(self):
        manager = MockProfilingManager([Path("expt")], ncpus=[8], datasets=[make_component_dataset([1.0, 2.0])])
        manager.experiments["expt"].labels = {"blocks": 4}

        assert manager.parse_profiling_data().experiments["expt"].labels == {"blocks": 4}


def test_a_replacement_experiment_does_not_inherit_a_deleted_count(tmp_path):
    """Test that an experiment added under the name of a deleted one is parsed afresh.

    The count used to be held by the manager, keyed by name, and nothing removed it when an experiment was
    deleted, so a later experiment reusing that name was handed the deleted one's number of CPUs.
    """

    manager = MockProfilingManager([Path("2cpu"), Path("1cpu")], ncpus=[2, 1], work_dir=tmp_path)
    assert manager._ncpus("1cpu") == 1

    manager.forget_experiment("1cpu")

    # A different experiment that happens to reuse the name, and that ran on more CPUs.
    manager._mock_ncpus["1cpu"] = 8
    manager.experiments["1cpu"] = ProfilingExperiment(path=Path("1cpu"))
    manager.experiments["1cpu"].status = ProfilingExperimentStatus.DONE

    assert manager._ncpus("1cpu") == 8


def make_run_log(datasets: list[xr.Dataset]) -> dict[int, mock.MagicMock]:
    """Builds mock profiling logs for several runs of the same component.

    Args:
        datasets (list[xr.Dataset]): Dataset each run parses to.

    Returns:
        dict[int, mock.MagicMock]: Mock logs keyed by run number. Run numbers are 0, 3, 7, ... so that tests
            distinguish a run number from its position along the run dimension.
    """
    run_logs = {}
    for run, dataset in zip([0, 3, 7, 11], datasets, strict=False):
        log = mock.MagicMock()
        log.optional = False
        log.parse.return_value = dataset
        run_logs[run] = log
    return run_logs


@pytest.fixture()
def run_data():
    """Fixture parsing the logs of an experiment that was run three times.

    Both components hold the same three runs, numbered 0, 3 and 7. Run 7 is the fastest on "Region 1" while run 0
    is the fastest on "Region 2", so tests can tell a whole-experiment selection from a per-region reduction.
    """
    manager = MockProfilingManager(paths=[Path("/fake/work_dir/exp1")])
    logs = {
        "component": make_run_log(
            [
                make_component_dataset([400.0, 1.0])["component"],
                make_component_dataset([600.0, 5.0])["component"],
                make_component_dataset([200.0, 9.0])["component"],
            ]
        ),
        "other": make_run_log(
            [
                make_component_dataset([40.0, 10.0])["component"],
                make_component_dataset([60.0, 50.0])["component"],
                make_component_dataset([20.0, 90.0])["component"],
            ]
        ),
    }
    with mock.patch.object(manager, "profiling_logs", return_value=logs):
        return manager.parse_profiling_data()


def test_parse_profiling_data_multiple_runs(run_data):
    """Several runs of the same log are concatenated along a 'run' dimension."""

    ds = run_data.data["exp1"]["component"]
    assert ds.sizes["run"] == 3
    assert ds.run.values.tolist() == [0, 3, 7]  # Run numbers, not positions
    assert ds[tavg].sel(region="Region 1").pint.dequantify().values.tolist() == [400.0, 600.0, 200.0]


def test_parse_profiling_data_runs_sorted():
    """Runs are ordered by run number, whatever order the logs are reported in."""

    manager = MockProfilingManager(paths=[Path("/fake/work_dir/exp1")])
    logs = {"component": {}}
    for run, tavg_value in ((3, 600.0), (0, 100.0)):  # Deliberately out of order
        log = mock.MagicMock()
        log.optional = False
        log.parse.return_value = make_component_dataset([tavg_value, 1.0])["component"]
        logs["component"][run] = log

    with mock.patch.object(manager, "profiling_logs", return_value=logs):
        results = manager.parse_profiling_data()

    ds = results.data["exp1"]["component"]
    assert ds.run.values.tolist() == [0, 3]
    assert ds[tavg].sel(region="Region 1").pint.dequantify().values.tolist() == [100.0, 600.0]


def test_parse_profiling_data_missing_run_log():
    """If all but one run fail to produce a log, the result has no 'run' dimension."""

    manager = MockProfilingManager(paths=[Path("/fake/work_dir/exp1")])
    present = mock.MagicMock()
    present.optional = True
    present.parse.return_value = make_component_dataset([400.0, 4.0])["component"]
    missing = mock.MagicMock()
    missing.optional = True
    missing.parse.side_effect = FileNotFoundError("Mocked missing file.")

    with mock.patch.object(manager, "profiling_logs", return_value={"component": {0: present, 1: missing}}):
        results = manager.parse_profiling_data()

    ds = results.data["exp1"]["component"]
    assert "run" not in ds.dims
    assert ds[tavg].sel(region="Region 1").pint.dequantify().item() == pytest.approx(400.0)


def test_a_manager_states_how_it_runs_experiments():
    """Every engine runs the experiments its own way, so a manager that does not say how cannot be created."""

    class WithoutRunExperiments(MockProfilingManager):
        run_experiments = ProfilingManager.run_experiments

    with pytest.raises(TypeError, match="run_experiments"):
        WithoutRunExperiments(paths=[Path("/fake/work_dir")])


def added(tmp_path, name="release", **kwargs):
    """A manager on a working directory of its own, holding one experiment added from a directory in it."""

    work_dir = tmp_path / "work"
    (work_dir / name).mkdir(parents=True)
    (work_dir / name / "config.yaml").write_text("ncpus: 4\n")
    manager = MockProfilingManager([], work_dir=work_dir, archive_dir=tmp_path / "archive")
    manager._mock_ncpus = {name: 4}
    manager.add_experiment_from_directory(name, Path(name), **kwargs)
    return manager


def later(manager):
    """Another manager on the same directories, as a later session builds."""

    return MockProfilingManager([], work_dir=manager.work_dir, archive_dir=manager.archive_dir)


class TestRecords:
    """Each experiment's record in its directory, from which a manager built later finds the experiment again."""

    def test_an_added_experiment_gets_one(self, tmp_path):
        manager = added(tmp_path, labels={"version": "1.0"})

        record = json.loads((manager.work_dir / "release" / RECORD_FILENAME).read_text())
        assert record == {
            "version": 1,
            "path": "release",
            "run_path": None,
            "complete": True,
            "layout": None,
            "changes": None,
            "labels": {"version": "1.0"},
            "ncpus": None,
        }

    def test_a_later_manager_finds_the_experiment_again(self, tmp_path):
        manager = added(tmp_path, labels={"version": "1.0"})

        (found,) = later(manager).experiments.items()

        assert found[0] == "release"
        assert found[1].path == manager.work_dir / "release"
        assert found[1].status == ProfilingExperimentStatus.DONE, "it was added as complete, which its record says"
        assert found[1].labels == {"version": "1.0"}
        assert found[1].changes is None, "nothing says what an added experiment changes"

    def test_a_record_in_another_format_is_refused_with_its_name(self, tmp_path):
        """A later version of the package may have written it, and the keys alone would not say so."""
        manager = added(tmp_path)
        record_file = manager.work_dir / "release" / RECORD_FILENAME
        record_file.write_text(json.dumps({**json.loads(record_file.read_text()), "version": 2}))

        with pytest.raises(ValueError, match=rf"The record '{record_file}' states the format version 2"):
            later(manager)

    def test_a_record_cut_short_is_refused_with_its_name(self, tmp_path):
        manager = added(tmp_path)
        record_file = manager.work_dir / "release" / RECORD_FILENAME
        record_file.write_text(record_file.read_text()[:20])

        with pytest.raises(ValueError, match=rf"The record '{record_file}' cannot be read, since it is not JSON"):
            later(manager)

    def test_the_format_a_record_is_held_to_is_the_records(self, tmp_path):
        manager = added(tmp_path)

        with (
            mock.patch("access.profiling.experiment.RECORD_VERSION", 2),
            pytest.raises(ValueError, match="The record .* states the format version 1, and .* reads version 2"),
        ):
            later(manager)

    def test_the_record_is_under_the_name_wherever_the_experiment_is(self, tmp_path):
        work_dir = tmp_path / "work"
        (work_dir / "runs" / "release-1.0").mkdir(parents=True)
        manager = MockProfilingManager([], work_dir=work_dir)

        manager.add_experiment_from_directory("release", Path("runs/release-1.0"))

        assert json.loads((work_dir / "release" / RECORD_FILENAME).read_text())["path"] == "runs/release-1.0"
        assert later(manager).experiments["release"].path == work_dir / "runs" / "release-1.0"

    def test_paths_outside_the_working_directory_are_read_as_they_are(self, tmp_path):
        """As a suite's run directory is, and the path of an experiment a relative working directory was given."""
        work_dir = tmp_path / "work"
        (work_dir / "suite").mkdir(parents=True)
        record = ProfilingExperiment(tmp_path / "elsewhere", run_path=tmp_path / "cylc-run").record(work_dir)
        (work_dir / "suite" / RECORD_FILENAME).write_text(json.dumps(record))

        experiment = MockProfilingManager([], work_dir=work_dir).experiments["suite"]

        assert (experiment.path, experiment.run_path) == (tmp_path / "elsewhere", tmp_path / "cylc-run")
        assert experiment.status == ProfilingExperimentStatus.NEW

    def test_a_name_generated_experiments_are_given_is_refused(self, tmp_path):
        manager = MockProfilingManager([], work_dir=tmp_path)
        (tmp_path / "exp-0003").mkdir()

        with pytest.raises(ValueError, match="of the form this manager gives"):
            manager.add_experiment_from_directory("exp-0003", Path("exp-0003"))
        assert manager.experiments == {}
        assert not (tmp_path / "exp-0003" / RECORD_FILENAME).exists()

    @pytest.mark.parametrize("name", ["by-hand/run1", "/abs", "release/", "..", ".", ""])
    def test_a_name_that_is_not_one_directory_is_refused(self, tmp_path, name):
        """The record is kept in `<work_dir>/<name>`, and only the directories directly in the working directory are
        searched for records, so a later manager would not find it under any other name."""
        manager = MockProfilingManager([], work_dir=tmp_path)
        (tmp_path / "by-hand" / "run1").mkdir(parents=True)

        with pytest.raises(ValueError, match=rf"Experiment name '{re.escape(name)}' is not the name of a single"):
            manager.add_experiment_from_directory(name, Path("by-hand/run1"))
        assert manager.experiments == {}
        assert not list(tmp_path.rglob(RECORD_FILENAME))

    def test_a_record_lacking_an_entry_stops_the_manager_with_its_name(self, tmp_path):
        """Rather than with a bare KeyError from whatever reads the entry first."""
        manager = added(tmp_path)
        record_file = manager.work_dir / "release" / RECORD_FILENAME
        record = json.loads(record_file.read_text())
        del record["labels"]
        record_file.write_text(json.dumps(record))

        with pytest.raises(ValueError, match=rf"The record '{record_file}' cannot be read, since it lacks .*'labels'"):
            later(manager)

    @pytest.mark.parametrize("labels", [{"grid": (4, 4)}, {"where": Path("/some/input")}])
    def test_labels_the_record_could_not_give_back_are_refused(self, tmp_path, labels):
        """A tuple would come back as a list in a later session, and a path could not be written at all."""
        manager = MockProfilingManager([], work_dir=tmp_path)
        (tmp_path / "release").mkdir()

        with pytest.raises(ValueError, match="The labels of an experiment must be JSON values"):
            manager.add_experiment_from_directory("release", Path("release"), labels=labels)

        assert manager.experiments == {}, "nothing is held that a later manager would not find"
        assert not (tmp_path / "release" / RECORD_FILENAME).exists()
        assert not (tmp_path / STUDY_FILENAME).exists()

    def test_forgetting_an_experiment_removes_its_record_and_nothing_else(self, tmp_path):
        manager = added(tmp_path)

        manager.forget_experiment("release")

        assert not (manager.work_dir / "release" / RECORD_FILENAME).exists()
        assert (manager.work_dir / "release" / "config.yaml").exists(), "the experiment's own files are not touched"
        assert later(manager).experiments == {}

    def test_forgetting_on_a_study_that_cannot_be_written_holds_for_the_session(self, tmp_path, caplog):
        """As on a colleague's study: the experiment is forgotten here, and a later manager finds it again."""
        manager = added(tmp_path)

        with (
            mock.patch.object(Path, "unlink", side_effect=PermissionError("read-only")),
            caplog.at_level(logging.WARNING),
        ):
            manager.forget_experiment("release")

        assert "release" not in manager.experiments
        assert f"Could not update the working directory '{manager.work_dir}' (read-only)" in caplog.text
        assert "forgotten in this session only" in caplog.text
        assert list(later(manager).experiments) == ["release"]
        assert [entry["call"] for entry in manager._read_study()["history"]] == ["add_experiment_from_directory"]

    def test_forgetting_when_the_history_cannot_be_written_holds_for_the_session(self, tmp_path, caplog):
        manager = added(tmp_path)

        with (
            mock.patch("access.profiling.manager._write_whole", side_effect=OSError("disk full")),
            caplog.at_level(logging.WARNING),
        ):
            manager.forget_experiment("release")

        assert "release" not in manager.experiments
        assert "(disk full), so experiment 'release' is forgotten in this session only" in caplog.text

    def test_a_directory_that_held_only_the_record_goes_with_it(self, tmp_path):
        work_dir = tmp_path / "work"
        (work_dir / "runs" / "release-1.0").mkdir(parents=True)
        manager = MockProfilingManager([], work_dir=work_dir)
        manager.add_experiment_from_directory("release", Path("runs/release-1.0"))

        manager.forget_experiment("release")

        assert not (work_dir / "release").exists()
        assert (work_dir / "runs" / "release-1.0").is_dir()

    def test_deleting_experiments_removes_their_records(self, tmp_path):
        manager = added(tmp_path)

        manager.delete_experiments(experiments=["release"], dry_run=True)
        assert (manager.work_dir / "release" / RECORD_FILENAME).exists(), "a dry run changes nothing"

        manager.delete_experiments(experiments=["release"])
        assert not (manager.work_dir / "release" / RECORD_FILENAME).exists()
        assert later(manager).experiments == {}

    def test_an_experiment_whose_record_cannot_be_removed_is_still_held(self, tmp_path):
        """The manager goes on holding what a later manager would find again."""
        manager = added(tmp_path)

        with (
            mock.patch.object(Path, "unlink", side_effect=PermissionError("read-only")),
            pytest.raises(PermissionError, match="read-only"),
        ):
            manager.delete_experiments(experiments=["release"])

        assert list(manager.experiments) == ["release"]


class TestWholeWrites:
    """A record or the study file is written whole or not at all, so that a write stopped part way leaves it intact."""

    def test_a_record_whose_write_fails_is_left_as_it_was(self, tmp_path):
        manager = added(tmp_path, labels={"version": "1.0"})
        record_file = manager.work_dir / "release" / RECORD_FILENAME
        before = record_file.read_text()
        manager.experiments["release"].labels = {"version": "2.0"}

        with (
            mock.patch.object(Path, "replace", side_effect=OSError("disk full")),
            pytest.raises(OSError, match="disk full"),
        ):
            manager._write_record("release")

        assert record_file.read_text() == before
        assert sorted(path.name for path in record_file.parent.iterdir()) == [RECORD_FILENAME, "config.yaml"]

    def test_a_study_file_whose_write_fails_is_left_as_it_was(self, tmp_path):
        manager = added(tmp_path)
        study_file = manager.work_dir / STUDY_FILENAME
        before = study_file.read_text()

        with (
            mock.patch("access.profiling.manager.os.fsync", side_effect=OSError("disk full")),
            pytest.raises(OSError, match="disk full"),
        ):
            manager._write_study({**manager._read_study(), "last_number": 7})

        assert study_file.read_text() == before
        assert not list(manager.work_dir.glob(".*.tmp"))

    def test_what_is_written_replaces_the_file_whole(self, tmp_path):
        manager = added(tmp_path)

        manager._write_study({**manager._read_study(), "last_number": 7})

        assert manager._read_study()["last_number"] == 7
        assert not list(manager.work_dir.glob(".*.tmp"))


class TestArchivedRecords:
    """The record an archive carries, and what a manager built later takes from it."""

    def test_an_archive_replaces_the_experiment_of_the_working_directory(self, tmp_path):
        manager = added(tmp_path, labels={"version": "1.0"})
        manager.archive_experiments()

        experiment = later(manager).experiments["release"]

        assert experiment.status == ProfilingExperimentStatus.ARCHIVED
        assert experiment.path == manager.archive_dir / "release.tar.gz"
        assert experiment.labels == {"version": "1.0"}

    def test_the_count_is_kept_so_the_archive_is_not_extracted_for_it(self, tmp_path):
        manager = added(tmp_path)
        manager.archive_experiments()
        found = later(manager)

        assert found._ncpus("release") == 4
        assert found._parse_ncpus_calls == [], "the record states it"

    @pytest.mark.parametrize(
        "error", [FileNotFoundError("config.yaml"), ValueError("NPROC is not set"), GrammarError("unexpected")]
    )
    def test_a_count_that_cannot_be_read_stops_nothing(self, tmp_path, caplog, error):
        """The other experiments are archived, the study file is copied, and the record leaves the count null. Payu's
        parse_ncpus raises the first without a config.yaml, Cylc's the second for a variable it cannot find, and the
        Rose parser an error of its own for a line it cannot read."""
        manager = added(tmp_path)
        (manager.work_dir / "second").mkdir()
        manager.add_experiment_from_directory("second", Path("second"))

        def parse_ncpus(path, run_path=None):
            if path.name == "release":
                raise error
            return 8

        with mock.patch.object(manager, "parse_ncpus", side_effect=parse_ncpus), caplog.at_level(logging.WARNING):
            manager.archive_experiments()

        assert f"The number of CPUs of experiment 'release' could not be read ({error})" in caplog.text
        found = later(manager)
        assert found.experiments["release"].status == ProfilingExperimentStatus.ARCHIVED
        assert found.experiments["release"].ncpus is None
        assert found.experiments["second"].ncpus == 8
        assert (manager.archive_dir / STUDY_FILENAME).is_file()

    def test_the_record_in_the_working_directory_is_left_as_it_was(self, tmp_path):
        """The count belongs to the archive, where nothing can change it any more."""
        manager = added(tmp_path)
        manager.archive_experiments()

        assert json.loads((manager.work_dir / "release" / RECORD_FILENAME).read_text())["ncpus"] is None


class TestStudyFile:
    """What is true of the whole study: the calls that made it, and the highest number given."""

    def test_a_study_with_no_file_starts_empty(self, tmp_path):
        assert MockProfilingManager([], work_dir=tmp_path)._read_study() == {
            "version": 1,
            "last_number": 0,
            "history": [],
        }

    def test_a_study_file_in_another_format_is_refused_with_its_name(self, tmp_path):
        """Before a number is read out of it, so that no experiment is numbered from a file that cannot be read."""
        manager = added(tmp_path)
        study_file = manager.work_dir / STUDY_FILENAME
        study_file.write_text(json.dumps({"version": 2, "last": 3}))

        with pytest.raises(ValueError, match=rf"The study file '{study_file}' states the format version 2"):
            manager._last_number()

    def test_the_format_the_study_file_is_held_to_is_its_own(self, tmp_path):
        """The records stay in theirs. The study file is read when a manager is built, to check its application."""
        manager = added(tmp_path)

        with (
            mock.patch("access.profiling.manager.STUDY_VERSION", 2),
            pytest.raises(ValueError, match="The study file .* states the format version 1, and .* reads version 2"),
        ):
            later(manager)

    def test_a_study_file_lacking_an_entry_is_refused_with_its_name(self, tmp_path):
        manager = added(tmp_path)
        study_file = manager.work_dir / STUDY_FILENAME
        study_file.write_text(json.dumps({"version": 1, "history": []}))

        with pytest.raises(ValueError, match=rf"The study file '{study_file}' cannot be read, since it lacks .*'last_"):
            manager._last_number()

    def test_a_study_of_another_application_is_refused(self, tmp_path):
        """Its layouts could be keyed as this application's without error, and its runs read as this one's."""
        manager = added(tmp_path)
        study_file = manager.work_dir / STUDY_FILENAME
        study = json.loads(study_file.read_text())
        study["history"][0]["application"] = {"class": "OtherApplication", "name": "other"}
        study_file.write_text(json.dumps(study))

        message = (
            rf"The study of the working directory '{manager.work_dir}' and the archive directory "
            rf"'{manager.archive_dir}' was made with OtherApplication 'other', as its study file records, but this "
            r"manager's application is MockApplication 'mock'\. A study has one application"
        )
        with pytest.raises(ValueError, match=message):
            later(manager)

    def test_each_other_application_is_named_once(self, tmp_path):
        manager = added(tmp_path)
        study_file = manager.work_dir / STUDY_FILENAME
        study = json.loads(study_file.read_text())
        entry = study["history"][0]
        others = [{"class": "MockApplication", "name": "renamed"}, {"class": "Other", "name": "mock"}]
        study["history"] = [
            {**entry, "application": application} for application in [*others, *others, entry["application"]]
        ]
        study_file.write_text(json.dumps(study))

        with pytest.raises(ValueError, match=r"made with MockApplication 'renamed' and Other 'mock', as its study"):
            later(manager)

    def test_the_study_file_of_the_archive_directory_names_the_application_too(self, tmp_path):
        """A study whose working directory is gone is checked against the copy beside its archives."""
        manager = added(tmp_path)
        manager.archive_experiments()
        shutil.rmtree(manager.work_dir)
        study_file = manager.archive_dir / STUDY_FILENAME
        study = json.loads(study_file.read_text())
        study["history"][0]["application"]["name"] = "other"
        study_file.write_text(json.dumps(study))

        with pytest.raises(ValueError, match="was made with MockApplication 'other'"):
            later(manager)

    def test_the_study_file_of_the_archive_directory_is_held_to_the_same(self, tmp_path):
        manager = added(tmp_path)
        manager.archive_experiments()
        (manager.work_dir / STUDY_FILENAME).unlink()
        archived = manager.archive_dir / STUDY_FILENAME
        archived.write_text(json.dumps({**json.loads(archived.read_text()), "version": 2}))

        with pytest.raises(ValueError, match=rf"The study file '{archived}' states the format version 2"):
            manager._read_study()

    def test_each_call_is_written_into_the_history(self, tmp_path):
        manager = added(tmp_path)
        manager.forget_experiment("release")
        added(tmp_path / "again").delete_experiments(experiments=["release"])

        history = json.loads((manager.work_dir / STUDY_FILENAME).read_text())["history"]
        again = json.loads((tmp_path / "again" / "work" / STUDY_FILENAME).read_text())["history"]

        assert [entry["call"] for entry in history] == ["add_experiment_from_directory", "forget_experiment"]
        assert [entry["call"] for entry in again] == ["add_experiment_from_directory", "delete_experiments"]
        assert all(entry["experiments"] == ["release"] for entry in history + again)

    def test_an_entry_says_what_made_the_call(self, tmp_path):
        manager = added(tmp_path)

        (entry,) = manager._read_study()["history"]

        assert datetime.fromisoformat(entry.pop("when")).utcoffset().total_seconds() == 0, "in UTC"
        assert entry == {
            "call": "add_experiment_from_directory",
            "access_profiling_version": _package_version(),
            "manager": "MockProfilingManager",
            "application": {"class": "MockApplication", "name": "mock"},
            # An added experiment was not made from this manager's control, so nothing is said of one.
            "control": None,
            "engine": {},
            "experiments": ["release"],
        }

    def test_a_dry_run_and_a_name_not_held_write_nothing(self, tmp_path):
        manager = added(tmp_path)

        manager.delete_experiments(experiments=["release"], dry_run=True)
        manager.forget_experiment("not-held")

        assert len(manager._read_study()["history"]) == 1

    def test_the_version_is_unknown_where_the_package_is_not_installed(self):
        with mock.patch("access.profiling.manager.version", side_effect=PackageNotFoundError):
            assert _package_version() == "unknown"

    def test_archiving_copies_it_beside_the_archives(self, tmp_path):
        manager = added(tmp_path)
        manager.archive_experiments()

        assert (manager.archive_dir / STUDY_FILENAME).read_text() == (manager.work_dir / STUDY_FILENAME).read_text()

    def test_it_is_read_from_the_archive_directory_once_the_working_directory_is_gone(self, tmp_path):
        manager = added(tmp_path)
        manager.archive_experiments()
        shutil.rmtree(manager.work_dir)

        study = later(manager)._read_study()

        assert [entry["call"] for entry in study["history"]] == ["add_experiment_from_directory"]

    def test_a_study_never_written_has_nothing_to_copy(self, tmp_path):
        """As one whose experiments were all put into the manager by hand."""
        path = tmp_path / "work" / "expt"
        path.mkdir(parents=True)
        manager = MockProfilingManager([path], ncpus=[4], work_dir=tmp_path / "work", archive_dir=tmp_path / "archive")

        manager.archive_experiments()

        assert (tmp_path / "archive" / "expt.tar.gz").is_file()
        assert not (tmp_path / "archive" / STUDY_FILENAME).exists()


CARTESIAN = {"ice_in": {"domain_nml": {"distribution_type": "'cartesian'"}}}
ROUNDROBIN = {"ice_in": {"domain_nml": {"distribution_type": "'roundrobin'"}}}


def choosing(tmp_path):
    """A manager with a control and a working directory of its own, ready to generate experiments."""

    manager = MockProfilingManager([], work_dir=tmp_path / "work", archive_dir=tmp_path / "archive")
    manager.control = GitControlSource("https://example.com/repo", "commit")
    return manager


class TestPlanExperiments:
    """One experiment for each configuration choice, recognised afterwards by what the choice states.

    The choices here have no layout, so the application, which raises when it is asked anything, is never
    consulted. Choices with a layout are tested with the Payu manager, whose mock application has layouts.
    """

    def test_each_choice_becomes_an_experiment_numbered_in_order(self, tmp_path):
        manager = choosing(tmp_path)

        manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN), ConfigurationChoice(ROUNDROBIN)]))

        (plans,) = manager._created_plans
        assert [plan.name for plan in plans] == ["exp-0001", "exp-0002"]
        assert list(manager.experiments) == ["exp-0001", "exp-0002"]
        assert all(experiment.status == ProfilingExperimentStatus.NEW for experiment in manager.experiments.values())

    def test_a_choice_without_a_layout_changes_what_it_states_and_nothing_else(self, tmp_path):
        manager = choosing(tmp_path)

        manager.generate(manager.plan_experiments([ConfigurationChoice(ROUNDROBIN)]))

        ((plan,),) = manager._created_plans
        assert plan.changes == ROUNDROBIN
        assert plan.layout is None
        # The walltime is one of the changes where the study wants one, not something the plan states.
        assert plan.walltime_hours is None
        assert not plan.regenerating

    def test_the_record_keeps_the_changes_and_the_labels(self, tmp_path):
        manager = choosing(tmp_path)

        manager.generate(
            manager.plan_experiments([ConfigurationChoice(ROUNDROBIN, labels={"distribution": "roundrobin"})])
        )

        record = json.loads((manager.work_dir / "exp-0001" / RECORD_FILENAME).read_text())
        assert record["changes"] == ROUNDROBIN
        assert record["labels"] == {"distribution": "roundrobin"}
        assert record["layout"] is None
        assert manager.experiments["exp-0001"].labels == {"distribution": "roundrobin"}

    def test_the_changes_and_the_labels_of_an_experiment_are_its_own(self, tmp_path):
        """Changing a choice in place after generation reaches neither the experiment nor what identifies it.

        The experiment's are what later calls are compared with, and what its archive's record is written from.
        """
        manager = choosing(tmp_path)
        choice = ConfigurationChoice({"config.yaml": {"walltime": "2:00:00"}}, labels={"grid": [12, 16]})
        manager.generate(manager.plan_experiments([choice]))

        choice.changes["config.yaml"]["walltime"] = "4:00:00"
        choice.labels["grid"].append(1)

        experiment = manager.experiments["exp-0001"]
        assert experiment.changes == {"config.yaml": {"walltime": "2:00:00"}}
        assert experiment.labels == {"grid": [12, 16]}
        manager.generate(manager.plan_experiments([choice]))
        assert list(manager.experiments) == ["exp-0001", "exp-0002"], "four hours is another configuration"

    def test_a_choice_the_study_has_is_skipped(self, tmp_path, caplog):
        manager = choosing(tmp_path)
        manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))

        with caplog.at_level(logging.INFO):
            manager.generate(
                manager.plan_experiments([ConfigurationChoice(CARTESIAN), ConfigurationChoice(ROUNDROBIN)])
            )

        assert "Experiment exp-0001 already exists for this configuration choice. Skipping addition." in caplog.text
        assert [[plan.name for plan in plans] for plans in manager._created_plans] == [["exp-0001"], ["exp-0002"]]
        assert manager._created_plans[1][0].changes == ROUNDROBIN

    def test_nothing_new_generates_nothing(self, tmp_path, caplog):
        manager = choosing(tmp_path)
        manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))

        manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))

        assert "No new experiments to generate. Will skip generation." in caplog.text
        assert len(manager._created_plans) == 1
        assert len(manager._read_study()["history"]) == 1

    def test_a_later_manager_recognises_the_choice_from_the_record(self, tmp_path):
        manager = choosing(tmp_path)
        manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))

        resumed = later(manager)
        resumed.control = manager.control
        resumed.generate(resumed.plan_experiments([ConfigurationChoice(CARTESIAN), ConfigurationChoice(ROUNDROBIN)]))

        ((plan,),) = resumed._created_plans
        assert (plan.name, plan.changes) == ("exp-0002", ROUNDROBIN)

    def test_changes_are_the_same_whatever_the_order_of_their_keys(self, tmp_path):
        manager = choosing(tmp_path)
        manager.generate(manager.plan_experiments([ConfigurationChoice({"a": {"x": 1, "y": 2}, "b": {"z": 3}})]))

        manager.generate(manager.plan_experiments([ConfigurationChoice({"b": {"z": 3}, "a": {"y": 2, "x": 1}})]))

        assert list(manager.experiments) == ["exp-0001"]

    def test_the_same_choice_twice_in_one_call_is_refused(self, tmp_path):
        manager = choosing(tmp_path)

        with pytest.raises(ValueError, match="Two of the configuration choices are the same configuration"):
            manager.generate(
                manager.plan_experiments(
                    [
                        ConfigurationChoice(CARTESIAN, labels={"attempt": 1}),
                        ConfigurationChoice(CARTESIAN, labels={"attempt": 2}),
                    ]
                )
            )

        assert manager._created_plans == [] and manager.experiments == {}
        assert not (manager.work_dir / STUDY_FILENAME).exists(), "so no number was taken"

    def test_it_is_refused_even_where_the_study_already_has_the_experiment(self, tmp_path):
        manager = choosing(tmp_path)
        manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))

        with pytest.raises(ValueError, match="Two of the configuration choices are the same configuration"):
            manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN), ConfigurationChoice(CARTESIAN)]))

    def test_a_held_experiment_keeps_its_labels(self, tmp_path, caplog):
        manager = choosing(tmp_path)
        manager.generate(
            manager.plan_experiments([ConfigurationChoice(CARTESIAN, labels={"distribution": "cartesian"})])
        )

        manager.generate(
            manager.plan_experiments([ConfigurationChoice(CARTESIAN, labels={"distribution": "the default"})])
        )

        assert manager.experiments["exp-0001"].labels == {"distribution": "cartesian"}
        assert (
            "Experiment exp-0001 keeps the labels it has, {'distribution': 'cartesian'}, and not those of the "
            "configuration choice it was found for, {'distribution': 'the default'}." in caplog.text
        )

    def test_the_same_labels_again_say_nothing(self, tmp_path, caplog):
        manager = choosing(tmp_path)
        manager.generate(
            manager.plan_experiments([ConfigurationChoice(CARTESIAN, labels={"distribution": "cartesian"})])
        )

        manager.generate(
            manager.plan_experiments([ConfigurationChoice(CARTESIAN, labels={"distribution": "cartesian"})])
        )

        assert "keeps the labels" not in caplog.text

    def test_an_added_experiment_is_not_taken_for_a_choice(self, tmp_path):
        """What an added experiment changes is unknown, so it is not the control with nothing changed."""
        manager = added(tmp_path)
        manager.control = GitControlSource("https://example.com/repo", "commit")

        manager.generate(manager.plan_experiments([ConfigurationChoice()]))

        assert list(manager.experiments) == ["release", "exp-0001"]
        assert manager.experiments["exp-0001"].changes == {}

    def test_the_history_says_which_method_was_called(self, tmp_path):
        manager = MockProfilingManager(
            [], work_dir=tmp_path / "work", archive_dir=tmp_path / "archive", control_options={"q": {"queue": "x"}}
        )
        manager.control = GitControlSource("https://example.com/repo", "commit")

        manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))

        (entry,) = manager._read_study()["history"]
        assert entry["call"] == "generate(plan_experiments)"
        assert entry["control"] == manager.control.provenance()
        assert entry["engine"] == {"control_options": {"q": {"queue": "x"}}}
        assert entry["experiments"] == ["exp-0001"]
        assert manager._read_study()["last_number"] == 1

    def test_it_is_refused_without_a_control(self, tmp_path):
        manager = MockProfilingManager([], work_dir=tmp_path / "work", archive_dir=tmp_path / "archive")

        with pytest.raises(ValueError, match="Cannot generate experiments without a control"):
            manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))

        assert manager._created_plans == []

    def test_a_failed_generation_registers_nothing_and_keeps_its_numbers(self, tmp_path, caplog):
        manager = choosing(tmp_path)

        with (
            mock.patch.object(MockProfilingManager, "_create_experiments", side_effect=RuntimeError("engine")),
            pytest.raises(RuntimeError, match="engine"),
        ):
            manager.generate(
                manager.plan_experiments([ConfigurationChoice(CARTESIAN), ConfigurationChoice(ROUNDROBIN)])
            )

        assert manager.experiments == {}
        assert "none of ['exp-0001', 'exp-0002'] were added to this manager" in caplog.text
        assert not (manager.work_dir / "exp-0001").exists()

        manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))
        assert list(manager.experiments) == ["exp-0003"]

    def test_what_the_engine_refuses_beforehand_takes_no_number(self, tmp_path, caplog):
        """Nothing is written before the engine has had its say, so the corrected call starts where this one did."""
        manager = choosing(tmp_path)

        with (
            mock.patch.object(MockProfilingManager, "_refuse_plans", side_effect=ValueError("refused")) as refuse,
            pytest.raises(ValueError, match="refused"),
        ):
            manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))

        (plans,) = refuse.call_args.args
        assert [plan.name for plan in plans] == ["exp-0001"], "it is asked about what is about to be created"
        assert manager._created_plans == [], "the engine is not asked to create what it refused"
        assert not (manager.work_dir / STUDY_FILENAME).exists()
        assert "were added to this manager" not in caplog.text, "nothing may exist that was never started"

        manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))
        assert list(manager.experiments) == ["exp-0001"]

    def test_the_engine_is_not_asked_about_nothing(self, tmp_path):
        """A call that has nothing left to create is not refused for what creating would have needed."""
        manager = choosing(tmp_path)
        manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))

        with mock.patch.object(MockProfilingManager, "_refuse_plans", side_effect=ValueError("refused")) as refuse:
            manager.generate(manager.plan_experiments([ConfigurationChoice(CARTESIAN)]))

        refuse.assert_not_called()
