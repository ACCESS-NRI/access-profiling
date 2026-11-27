# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import math
import re
import shutil
import tarfile
from datetime import datetime
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from unittest import mock

import pytest
import xarray as xr
from access.config.parallel_component import ComponentLayout
from conftest import layout_of, layout_with

from access.profiling.application import Application, LogSpec
from access.profiling.control import GitControlSource
from access.profiling.experiment import RECORD_FILENAME, ConfigurationChoice, ExperimentCoordinate, component_cores
from access.profiling.manager import (
    STUDY_FILENAME,
    ProfilingExperiment,
    ProfilingExperimentStatus,
    ProfilingLog,
    ProfilingManager,
    _package_version,
)
from access.profiling.metrics import count, tavg
from access.profiling.plotting_utils import RegionGroup


class MockApplication(Application):
    """The application axis these tests do not exercise, with the one member they do.

    What the manager's own tests cover - parsing, archiving, bookkeeping, plotting - takes an application
    only to read a layout back and to say which component a log belongs to. The rest raises, so a test that
    reached the layout search by accident would say so.

    Args:
        layouts (dict[str, ComponentLayout]): What parse_layout reports, keyed by the name of the directory
            it is asked about. Anything absent reports None, meaning the layout could not be told.
        log_components (dict[str, str]): The component each log belongs to, keyed by log name.
    """

    def __init__(self, layouts: dict[str, ComponentLayout] | None = None, log_components: dict[str, str] | None = None):
        self.layouts = layouts if layouts is not None else {}
        self.log_components = log_components if log_components is not None else {}
        self.parse_layout_calls: list[Path] = []

    name = "mock"
    experiment_prefix = "mock-layout"

    @property
    def parallel_component(self):
        raise NotImplementedError

    @property
    def logs(self):
        return tuple(
            LogSpec(name, lambda _: None, mock.Mock(), component=component)
            for name, component in self.log_components.items()
        )

    def layout_key(self, layout):
        raise NotImplementedError

    def config_changes(self, layout):
        raise NotImplementedError

    def parse_layout(self, output_dir):
        self.parse_layout_calls.append(output_dir)
        return self.layouts.get(output_dir.name)


class MockProfilingManager(ProfilingManager[MockApplication]):
    """Test class inheriting from ProfilingManager to test its methods.

    This class will simulate parsing of some profiling data.

    Note that this mock class assumes that experiments are named after the last part of their path and all experiments
    will be marked as DONE.

    Args:
        paths (list[Path]): List of paths to simulate different configurations.
        ncpus (list[int]): List of number of CPUs corresponding to each path.
        datasets (list[xr.Dataset]): List of datasets to return for each path.
        work_dir (Path): The working directory. A test that generates, adds or deletes experiments gives a real
            one, since the manager writes their records and the study file into it.
        archive_dir (Path): The archive directory.
    """

    def __init__(
        self,
        paths: list[Path],
        ncpus: list[int] | None = None,
        datasets: list[dict[str, xr.Dataset]] | None = None,
        work_dir: Path = Path("/fake/work_dir"),
        archive_dir: Path = Path("/fake/archive_dir"),
    ):
        super().__init__(work_dir, archive_dir, MockApplication())

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

        if datasets is not None:
            self.data = dict(zip([path.name for path in paths], datasets, strict=True))

    def parse_status(self, path, run_path=None):
        self._parse_status_calls.append(path)
        return self._mock_status.get(path.name)

    def parse_ncpus(self, path, run_path=None):
        """Simulate parsing number of CPUs for a given path."""
        self._parse_ncpus_calls.append((path, run_path))
        return self._mock_ncpus[path.name]

    def profiling_logs(self, path, run_path=None):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Simulate parsing profiling data for a given path."""
        pass

    def _delete_experiments(self, names, dry_run, **kwargs):
        """Record requested deletions instead of touching the filesystem."""
        self._deleted_experiments.append((names, dry_run))

    def run_experiments(self, retry_failed=False):
        """Nothing to run: these tests drive the experiments' statuses themselves."""

    def _create_experiments(self, plans, **runner_options):
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


@pytest.fixture()
def scaling_data():
    """Fixture instantiating fake parsed profiling data for different CPU configurations, as one would get from
    a scaling study.

    The mock data contains two regions, "Region 1" and "Region 2", and two metrics, count and tavg.
    Counts are always [1, 2] while tavg depends on the number of CPUs:
    - For 1 CPU: [600365 s, 2.345388 s]
    - For 2 CPUs: [300182.5 s, 1.172694 s]
    - For 4 CPUs: [300182.5 s, 1.172694 s]
    """
    paths = [Path("1cpu"), Path("4cpu"), Path("2cpu")]
    ncpus = [1, 4, 2]  # Intentionally unordered to test sorting in the manager
    datasets = []
    for n in ncpus:
        regions = ["Region 1", "Region 2"]
        count_array = xr.DataArray([1, 2], dims=["region"]).pint.quantify(count.units)
        tavg_array = xr.DataArray([value / min(n, 2) for value in [600365, 2.345388]], dims=["region"]).pint.quantify(
            tavg.units
        )
        datasets.append(
            {"component": xr.Dataset(data_vars={count: count_array, tavg: tavg_array}, coords={"region": regions})}
        )

    return paths, ncpus, datasets


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


def test_repr(scaling_data):
    """Test the __repr__ method of ProfilingManager."""

    # Test with no data
    manager = MockProfilingManager(paths=[Path("/fake/work_dir")])
    expected = """<MockProfilingManager>
    Application: mock
    Working directory: PosixPath('/fake/work_dir')
    Archive directory: PosixPath('/fake/archive_dir')
    Experiments:
        'work_dir': ProfilingExperiment(path=PosixPath('/fake/work_dir'), status=DONE)
    Data:
        No parsed data.
"""
    assert repr(manager) == expected

    # Test with data
    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    result = repr(manager)
    assert "Data:\n        '1cpu':" in result
    assert "<xarray.Dataset>" in result
    assert "Dimensions:" in result
    assert "Coordinates:" in result
    assert "Data variables:" in result


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
    Data:
        No parsed data.
"""
    assert repr(manager) == expected


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
        manager.parse_profiling_data()

    assert manager.experiments["expt"].status == ProfilingExperimentStatus.DONE
    assert "expt" in manager.data, "so its data is parsed rather than passed over as still running"


def test_parse_profiling_data_skips_a_run_that_failed(caplog):
    """Test that a crashed run is reported as FAILED rather than left looking like it is still going."""

    manager = MockProfilingManager([Path("/a/expt")])
    manager.experiments["expt"].status = ProfilingExperimentStatus.RUNNING
    manager._mock_status = {"expt": ProfilingExperimentStatus.FAILED}

    with caplog.at_level(logging.WARNING):
        manager.parse_profiling_data()

    assert manager.experiments["expt"].status == ProfilingExperimentStatus.FAILED
    assert "expt" not in manager.data
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


def test_delete_experiment(caplog, tmp_path):
    """Test the delete_experiment method of ProfilingManager."""

    # Setup mock experiments
    exp_paths = [tmp_path / "exp1", tmp_path / "exp2"]
    manager = MockProfilingManager(exp_paths, work_dir=tmp_path)

    # Delete an existing experiment
    manager.delete_experiment("exp1")
    assert "exp1" not in manager.experiments, "Experiment 'exp1' should be deleted."

    # Attempt to delete a non-existing experiment
    with caplog.at_level(logging.WARNING):
        manager.delete_experiment("non_existing_exp")
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert len(manager.experiments) == 1 and "exp2" in manager.experiments, (
        "Only 'exp2' should remain after attempting to delete a non-existing experiment."
    )


class TestDeletingForgetsTheParsedData:
    """A table or a plot reads every experiment with parsed data by default, and a deleted one must not be one."""

    @staticmethod
    def parsed(tmp_path):
        paths = [tmp_path / "exp1", tmp_path / "exp2"]
        return MockProfilingManager(paths, datasets=[make_component_dataset([1.0, 2.0])] * 2, work_dir=tmp_path)

    def test_delete_experiment(self, tmp_path):
        manager = self.parsed(tmp_path)

        manager.delete_experiment("exp1")

        assert list(manager.data) == ["exp2"]

    def test_delete_experiments(self, tmp_path):
        manager = self.parsed(tmp_path)

        manager.delete_experiments(["exp1"])

        assert list(manager.data) == ["exp2"]


def test_parse_profiling_data(caplog):
    """Test the _parse_profiling_data_directory method of ProfilingManager."""

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
        manager.parse_profiling_data()
        assert "log" in manager.data[exp_name], "Parsed datasets should contain 'log' key."
        assert "optional_log" in manager.data[exp_name], "Parsed datasets should contain 'optional_log' key."
        assert "missing_log" not in manager.data[exp_name], (
            "Parsed datasets should not contain 'missing_log' key as the file is missing."
        )
        assert mock_log.parse.call_count == 3, "Parse method should be called three times."
        assert "run" not in manager.data[exp_name]["log"].dims, "A single run should not add a 'run' dimension."
        mock_profiling_logs.assert_called_once_with(Path("/fake/work_dir/exp1"), Path("/fake/runs/exp1"))

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
            manager.parse_profiling_data()

    failed = [record for record in caplog.records if "Failed to parse optional profiling log" in record.message]
    not_found = [record for record in caplog.records if "not found. Skipping." in record.message]
    assert len(failed) == 3 and len(not_found) == 2, (
        "The dedicated FileNotFoundError branch should catch exactly the missing-log errors, including subclasses; "
        "every other exception type, including other OSErrors, should fall through to the general branch."
    )
    assert {record.levelname for record in failed} == {"WARNING"}, (
        "An optional log the run wrote and the parser cannot read is reported as a warning, whatever the exception."
    )
    assert {record.levelname for record in not_found} == {"INFO"}, "A run need not write an optional log."

    for name, record in zip(errors, failed + not_found, strict=True):
        assert f"/fake/{name}.out" in record.message, f"The record reported for '{name}' should carry its own path."
    assert failed[0].message == (
        "Failed to parse optional profiling log '/fake/bad_log.out' with exception:\n    boom\nSkipping."
    ), "The warning should report the log path and the exception message, each on its own line."
    assert [record.message for record in caplog.records].count(" Done.") == 1, (
        "' Done.' should only be logged for the log which was parsed successfully."
    )
    assert list(manager.data["exp1"]) == ["good_log"], (
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


@mock.patch("access.profiling.manager.plot_scaling_metrics")
def test_scaling_data(mock_plot, scaling_data):
    """Test the parse_scaling_data and plot_scaling_data methods of ProfilingManager.

    This test will check that datasets are correctly concatenated across different numbers of CPUs
    and that the plotting function is called correctly.
    """
    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)
    manager.experiments["4cpu"].run_path = Path("/fake/runs/4cpu")

    # Test that __repr__ returns info about the data
    result = repr(manager)
    assert "Data:\n        '1cpu':" in result
    assert "<xarray.Dataset>" in result
    assert "Dimensions:" in result
    assert "Coordinates:" in result
    assert "Data variables:" in result

    # Test plotting scaling data for non-existing component
    with pytest.raises(ValueError):
        manager.plot_scaling_data(
            [RegionGroup("non_existing_component", ["Region 1"])],
            metric=tavg,
        )

    # Test plotting scaling data
    manager.plot_scaling_data(
        [RegionGroup("component", ["Region 1"], relabel={"Region 1": "Total"})],
        metric=tavg,
        experiments=["1cpu", "4cpu"],
        xlabel="Number of CPUs",
    )
    assert mock_plot.call_count == 1
    assert mock_plot.call_args.kwargs["xlabel"] == "Number of CPUs"
    scaling_data = mock_plot.call_args.args[0]
    assert isinstance(scaling_data, list)
    assert len(scaling_data) == 1  # One component
    component_data = scaling_data[0]
    assert isinstance(component_data, xr.Dataset)
    assert set(component_data.coords["ncpus"].values) == {1, 4}  # Only 1cpu and 4cpu experiments included
    assert set(component_data.coords["region"].values) == {"Total"}  # Region selection and relabelling
    assert set(component_data.data_vars.keys()) == {count, tavg}
    assert component_data[count].sel(region="Total").values.tolist() == [1, 1]
    assert component_data[tavg].sel(region="Total").values.tolist() == [600365.0, 300182.5]
    assert mock_plot.call_args.args[1] == tavg
    assert (Path("4cpu"), Path("/fake/runs/4cpu")) in manager._parse_ncpus_calls


def test_scaling_data_missing_region_raises_value_error(scaling_data):
    """Test plot_scaling_data raises a clear error when a requested region is missing."""

    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    with pytest.raises(ValueError, match="not found in") as exc_info:
        manager.plot_scaling_data(
            [RegionGroup("component", ["Missing Region"])],
            metric=tavg,
            experiments=["1cpu"],
        )

    message = str(exc_info.value)
    assert "Missing Region" in message
    assert "'component'" in message
    assert "experiment '1cpu'" in message


def test_scaling_data_no_selected_experiments_raises_value_error(scaling_data):
    """Test plot_scaling_data raises when no experiments are selected."""

    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    with pytest.raises(ValueError, match="No experiments selected"):
        manager.plot_scaling_data(
            [RegionGroup("component", ["Region 1"])],
            metric=tavg,
            experiments=[],
        )


def test_scaling_data_missing_experiment_data_raises_value_error(scaling_data):
    """Test plot_scaling_data raises when selected experiments have no parsed data."""

    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    with pytest.raises(ValueError, match=r"No parsed profiling data found for experiment\(s\)") as exc_info:
        manager.plot_scaling_data(
            [RegionGroup("component", ["Region 1"])],
            metric=tavg,
            experiments=["missing_exp"],
        )

    message = str(exc_info.value)
    assert "missing_exp" in message
    assert "Available experiments" in message


@mock.patch("access.profiling.manager.plot_bar_metrics")
def test_bar_chart_data(mock_plot, scaling_data):
    """Test the plot_bar_chart method of ProfilingManager.

    This test checks that bar chart data is correctly extracted from the datasets and that the
    plotting function is called with the right arguments.
    """
    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    # Test plotting bar chart for non-existing component
    with pytest.raises(ValueError):
        manager.plot_bar_chart(
            [RegionGroup("non_existing_component", ["Region 1"])],
            metric=tavg,
        )

    # Test plotting bar chart with region selection, relabelling, and experiment filtering
    manager.plot_bar_chart(
        [RegionGroup("component", ["Region 1", "Region 2"], relabel={"Region 1": "Total"})],
        metric=tavg,
        experiments=["1cpu", "4cpu"],
    )
    assert mock_plot.call_count == 1

    # Verify the data dict passed to plot_bar_metrics
    bar_data = mock_plot.call_args.args[0]
    assert isinstance(bar_data, dict)
    assert set(bar_data.keys()) == {"1cpu", "4cpu"}
    assert bar_data["1cpu"] == pytest.approx([600365.0, 2.345388])
    assert bar_data["4cpu"] == pytest.approx([300182.5, 1.172694])

    # Verify region labels
    region_labels = mock_plot.call_args.args[1]
    assert region_labels == ["Total", "component: Region 2"]

    # Verify metric
    assert mock_plot.call_args.args[2] == tavg

    # Verify show kwarg is passed through
    assert mock_plot.call_args.kwargs["show"] is True

    # Test experiment relabelling
    mock_plot.reset_mock()
    manager.plot_bar_chart(
        [RegionGroup("component", ["Region 1"])],
        metric=tavg,
        experiment_relabel_map={"1cpu": "1 CPU", "4cpu": "4 CPUs"},
        experiments=["1cpu", "4cpu"],
    )
    bar_data = mock_plot.call_args.args[0]
    assert set(bar_data.keys()) == {"1 CPU", "4 CPUs"}
    assert bar_data["1 CPU"] == pytest.approx([600365.0])
    assert bar_data["4 CPUs"] == pytest.approx([300182.5])


@mock.patch("access.profiling.manager.plot_bar_metrics")
def test_bar_chart_all_experiments(mock_plot, scaling_data):
    """Test plot_bar_chart includes all experiments when none are specified."""
    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    manager.plot_bar_chart(
        [RegionGroup("component", ["Region 1"])],
        metric=tavg,
        show=False,
    )
    bar_data = mock_plot.call_args.args[0]
    assert set(bar_data.keys()) == {"1cpu", "4cpu", "2cpu"}
    assert mock_plot.call_args.kwargs["show"] is False


@pytest.fixture()
def layout_scaling_data():
    """Fixture instantiating fake parsed profiling data where two experiments use the same number of CPUs.

    This is what a layout study looks like: '2cpu_slow' and '2cpu_fast' both use 2 CPUs, but '2cpu_fast' is the
    better performing decomposition. The tavg values on "Region 1" are 400 s, 800 s and 300 s respectively.
    """
    paths = [Path("2cpu_slow"), Path("1cpu"), Path("2cpu_fast")]
    ncpus = [2, 1, 2]  # Intentionally unordered, with a repeated number of CPUs
    datasets = [
        make_component_dataset([400.0, 4.0]),
        make_component_dataset([800.0, 8.0]),
        make_component_dataset([300.0, 3.0]),
    ]

    return paths, ncpus, datasets


def test_select_best_experiments(layout_scaling_data):
    """Test select_best_experiments keeps the fastest experiment for each number of CPUs."""

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    # Only one experiment per CPU count, ordered by increasing number of CPUs
    assert manager.select_best_experiments("component", "Region 1", tavg) == ["1cpu", "2cpu_fast"]


def test_select_best_experiments_subset(layout_scaling_data):
    """Test select_best_experiments only selects among the requested experiments."""

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    # The faster 2 CPU experiment is not a candidate, so the slower one survives
    selected = manager.select_best_experiments("component", "Region 1", tavg, experiments=["2cpu_slow", "1cpu"])
    assert selected == ["1cpu", "2cpu_slow"]


def test_select_best_experiments_unique_ncpus(scaling_data):
    """Test select_best_experiments is a no-op, apart from sorting, when all CPU counts are distinct."""

    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    assert manager.select_best_experiments("component", "Region 1", tavg) == ["1cpu", "2cpu", "4cpu"]


def test_select_best_experiments_tie_keeps_first(caplog):
    """Test select_best_experiments keeps the first experiment and warns when two experiments tie."""

    paths = [Path("2cpu_a"), Path("2cpu_b")]
    datasets = [make_component_dataset([400.0, 4.0]), make_component_dataset([400.0, 4.0])]
    manager = MockProfilingManager(paths, ncpus=[2, 2], datasets=datasets)

    with caplog.at_level(logging.WARNING):
        selected = manager.select_best_experiments("component", "Region 1", tavg)

    assert selected == ["2cpu_a"]
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert "2cpu_a" in caplog.records[0].message and "2cpu_b" in caplog.records[0].message


def test_select_best_experiments_keeps_the_incumbent_when_a_later_experiment_is_slower(caplog):
    """A layout that is merely slower than the one already held must lose quietly, neither winning nor warning.

    The warning exists to flag two layouts that cannot be told apart, which is a result worth a second look. A
    simply slower layout is the ordinary outcome of a layout study, and reporting it would bury the real ties in
    noise - while promoting it would hand the scaling plot the wrong point for that CPU count.
    """

    paths = [Path("2cpu_fast"), Path("2cpu_slow")]
    datasets = [make_component_dataset([300.0, 3.0]), make_component_dataset([400.0, 4.0])]
    manager = MockProfilingManager(paths, ncpus=[2, 2], datasets=datasets)

    with caplog.at_level(logging.WARNING):
        selected = manager.select_best_experiments("component", "Region 1", tavg)

    assert selected == ["2cpu_fast"]
    assert caplog.records == [], "A slower layout is not a tie, so nothing should be reported about it."


def test_experiment_ncpus_parsed_once(layout_scaling_data):
    """Test that the number of CPUs of an experiment is only parsed once, however often it is needed."""

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    manager.select_best_experiments("component", "Region 1", tavg)
    manager.select_best_experiments("component", "Region 1", tavg)

    assert len(manager._parse_ncpus_calls) == 3  # One per experiment, not one per call


def test_experiment_ncpus_is_stored_on_the_experiment(layout_scaling_data):
    """Test that the parsed number of CPUs is kept on the experiment rather than beside it."""

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    assert manager.experiments["1cpu"].ncpus is None  # Nothing has needed it yet
    assert manager._ncpus("1cpu") == 1
    assert manager.experiments["1cpu"].ncpus == 1


def test_a_replacement_experiment_does_not_inherit_a_deleted_count(layout_scaling_data, tmp_path):
    """Test that an experiment added under the name of a deleted one is parsed afresh.

    The count used to be held by the manager, keyed by name, and nothing removed it when an experiment was
    deleted, so a later experiment reusing that name was handed the deleted one's number of CPUs.
    """

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets, work_dir=tmp_path)
    assert manager._ncpus("1cpu") == 1

    manager.delete_experiment("1cpu")

    # A different experiment that happens to reuse the name, and that ran on more CPUs.
    manager._mock_ncpus["1cpu"] = 8
    manager.experiments["1cpu"] = ProfilingExperiment(path=Path("1cpu"))
    manager.experiments["1cpu"].status = ProfilingExperimentStatus.DONE

    assert manager._ncpus("1cpu") == 8


@mock.patch("access.profiling.manager.plot_scaling_metrics")
def test_scaling_data_with_best_experiments(mock_plot, layout_scaling_data):
    """Test that feeding select_best_experiments into plot_scaling_data drops the slower layouts."""

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    manager.plot_scaling_data(
        [RegionGroup("component", ["Region 1"])],
        metric=tavg,
        experiments=manager.select_best_experiments("component", "Region 1", tavg),
    )

    component_data = mock_plot.call_args.args[0][0]
    assert component_data.coords["ncpus"].values.tolist() == [1, 2]  # No duplicated CPU counts
    assert component_data[tavg].sel(region="component: Region 1").values.tolist() == [
        800.0,
        300.0,
    ]  # Fastest 2 CPU layout

    assert len(manager._parse_ncpus_calls) == 3  # Selecting and plotting does not parse an experiment twice


def test_scaling_data_duplicate_ncpus_raises_value_error(layout_scaling_data):
    """Test plot_scaling_data refuses to plot when several experiments share a number of CPUs."""

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    with pytest.raises(ValueError, match="same number of CPUs") as exc_info:
        manager.plot_scaling_data(
            [RegionGroup("component", ["Region 1"])],
            metric=tavg,
        )

    message = str(exc_info.value)
    assert "[2]" in message
    assert "select_best_experiments" in message


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
    """Fixture instantiating a manager whose experiment was run three times.

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
        manager.parse_profiling_data()
    return manager


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
        manager.parse_profiling_data()

    ds = manager.data["exp1"]["component"]
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
        manager.parse_profiling_data()

    ds = manager.data["exp1"]["component"]
    assert "run" not in ds.dims
    assert ds[tavg].sel(region="Region 1").pint.dequantify().item() == pytest.approx(400.0)


def test_select_best_run(run_data):
    """select_best_run keeps the fastest run, in every component of the experiment."""

    run_data.select_best_run("component", "Region 1", tavg)

    # Run 7 is the fastest on the ranking region, and is selected in both components
    for ds in run_data.data["exp1"].values():
        assert "run" not in ds.dims
        # The run coordinate is dropped too, so a reduced dataset looks exactly like a single-run one
        assert "run" not in ds.coords
    assert run_data.data["exp1"]["component"][tavg].sel(region="Region 1").pint.dequantify().item() == 200.0
    assert run_data.data["exp1"]["other"][tavg].sel(region="Region 1").pint.dequantify().item() == 20.0


def test_select_best_run_ignores_other_regions(run_data):
    """The chosen run is a whole-experiment choice, not the best value of every region."""

    run_data.select_best_run("component", "Region 1", tavg)

    # Run 0 is faster on "Region 2" (1.0 s), but run 7 won on the ranking region so its value is kept
    assert run_data.data["exp1"]["component"][tavg].sel(region="Region 2").pint.dequantify().item() == 9.0


def test_select_best_run_no_run_dim_is_noop(layout_scaling_data):
    """Data without a 'run' dimension is left untouched."""

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)
    before = manager.data["1cpu"]["component"].copy(deep=True)

    manager.select_best_run("component", "Region 1", tavg)

    xr.testing.assert_identical(manager.data["1cpu"]["component"], before)


def test_aggregate_runs_min(run_data):
    """aggregate_runs reduces each region independently, so regions may come from different runs."""

    run_data.aggregate_runs(how="min")

    ds = run_data.data["exp1"]["component"]
    assert "run" not in ds.dims
    assert ds[tavg].sel(region="Region 1").pint.dequantify().item() == 200.0  # From run 7
    assert ds[tavg].sel(region="Region 2").pint.dequantify().item() == 1.0  # From run 0


def test_aggregate_runs_mean(run_data):
    """aggregate_runs computes the mean over the runs, preserving metric keys and units."""

    run_data.aggregate_runs(how="mean")

    ds = run_data.data["exp1"]["component"]
    assert tavg in ds.data_vars  # Metric objects survive the reduction, unlike aggregate_pe_data
    assert ds[tavg].pint.units == tavg.units
    assert ds[tavg].sel(region="Region 1").pint.dequantify().item() == pytest.approx(400.0)


def test_aggregate_runs_median(run_data):
    """aggregate_runs computes the median over the runs."""

    run_data.aggregate_runs(how="median")

    assert run_data.data["exp1"]["component"][tavg].sel(region="Region 1").pint.dequantify().item() == 400.0


def test_aggregate_runs_max(run_data):
    """aggregate_runs computes the maximum over the runs, region by region."""

    run_data.aggregate_runs(how="max")

    ds = run_data.data["exp1"]["component"]
    assert ds[tavg].sel(region="Region 1").pint.dequantify().item() == 600.0  # From run 3
    assert ds[tavg].sel(region="Region 2").pint.dequantify().item() == 9.0  # From run 7


def test_aggregate_runs_invalid_how(run_data):
    """An unsupported statistic is rejected."""

    with pytest.raises(ValueError, match="Unknown reduction"):
        run_data.aggregate_runs(how="bogus")


@mock.patch("access.profiling.manager.plot_bar_metrics")
def test_select_best_run_then_plot(mock_plot, run_data):
    """Once the runs are reduced, the plotting methods work unchanged."""

    run_data.select_best_run("component", "Region 1", tavg)
    run_data.plot_bar_chart([RegionGroup("component", ["Region 1"])], metric=tavg, show=False)

    assert mock_plot.call_args.args[0] == {"exp1": pytest.approx([200.0])}


def test_select_best_experiments_run_dim_raises_value_error(run_data):
    """select_best_experiments refuses unreduced data instead of failing obscurely."""

    with pytest.raises(ValueError, match="'run' dimension"):
        run_data.select_best_experiments("component", "Region 1", tavg)


def test_scaling_data_run_dim_raises_value_error(run_data):
    """plot_scaling_data refuses unreduced data instead of silently plotting it."""

    with pytest.raises(ValueError, match="'run' dimension"):
        run_data.plot_scaling_data([RegionGroup("component", ["Region 1"])], metric=tavg)


def test_bar_chart_run_dim_raises_value_error(run_data):
    """plot_bar_chart refuses unreduced data instead of failing obscurely."""

    with pytest.raises(ValueError, match="'run' dimension"):
        run_data.plot_bar_chart([RegionGroup("component", ["Region 1"])], metric=tavg)


def test_bar_chart_of_a_metric_with_more_than_one_value_for_each_region(block_study):
    """As data for each PE has, which a bar cannot show, rather than a TypeError from turning it into one number."""

    add_per_pe_log(block_study)

    with pytest.raises(ValueError, match="has the dimensions \\['pe'\\] .* more values than a chart has bars"):
        block_study.plot_bar_chart([RegionGroup("per_pe", ["Region 1"])], tavg, show=False)


@pytest.fixture()
def component_scaling_data():
    """Parsed data for three experiments whose components were given different numbers of cores.

    The log is named "component", as make_component_dataset has it, and is looked for in the layout under
    that name since no mapping says otherwise. The layout holds a second component, "other", given half as
    many cores, so that a group read against one is telling apart from a group read against the other.
    """
    paths = [Path("small"), Path("large"), Path("medium")]
    cores = [60, 240, 120]  # Unordered, so that the plot has to sort them
    datasets = [make_component_dataset([100.0 * 60 / n, 10.0]) for n in cores]
    manager = MockProfilingManager(paths, ncpus=[n * 2 for n in cores], datasets=datasets)
    for path, n in zip(paths, cores, strict=True):
        manager.experiments[path.name].layout = layout_with(component=n, other=n // 2)
    return manager, cores


class TestPlotComponentScalingData:
    """Plotting a metric against the cores the component itself was given."""

    @staticmethod
    def _plotted(mock_plot) -> list[list]:
        """The x values of each group handed to the plot, in the order the groups were given."""

        return [list(stat["ncpus"].values) for stat in mock_plot.call_args.args[0]]

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_the_x_axis_is_the_components_cores(self, mock_plot, component_scaling_data):
        """Not the whole job's: the manager is holding twice as many CPUs for every experiment."""

        manager, cores = component_scaling_data
        manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)

        assert self._plotted(mock_plot) == [sorted(cores)]

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_every_region_asked_for_is_plotted(self, mock_plot, component_scaling_data):
        manager, _ = component_scaling_data
        manager.plot_component_scaling_data([RegionGroup("component", ["Region 1", "Region 2"])], tavg)

        (stat,) = mock_plot.call_args.args[0]
        assert list(stat["region"].values) == ["component: Region 1", "component: Region 2"]

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_a_region_is_labelled_with_the_log_it_came_from(self, mock_plot, component_scaling_data):
        """Region names are only unique within one log, so two components may each have a "Total"."""

        manager, _ = component_scaling_data
        manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)

        (stat,) = mock_plot.call_args.args[0]
        assert list(stat["region"].values) == ["component: Region 1"]

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_a_relabelled_region_stands_on_its_own_name(self, mock_plot, component_scaling_data):
        """Naming one is the caller saying what it should be called, so the log is not put in front of it."""

        manager, _ = component_scaling_data
        manager.plot_component_scaling_data(
            [RegionGroup("component", ["Region 1", "Region 2"], relabel={"Region 1": "Dynamics"})],
            tavg,
        )

        (stat,) = mock_plot.call_args.args[0]
        assert list(stat["region"].values) == ["Dynamics", "component: Region 2"], "one named, one not"

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_one_log_holding_several_components(self, mock_plot, component_scaling_data):
        """The ACCESS-OM3 ESMF summary, in miniature: one log, and each group read against its own cores."""

        manager, _ = component_scaling_data
        manager.plot_component_scaling_data(
            [
                RegionGroup("component", ["Region 1"], component="component"),
                RegionGroup("component", ["Region 2"], component="other"),
            ],
            tavg,
        )

        groups = mock_plot.call_args.args[0]
        assert len(groups) == 2, "both survive, one log or two"
        assert list(groups[0]["ncpus"].values) == [60, 120, 240]
        assert list(groups[1]["ncpus"].values) == [30, 60, 120], "the other component's cores"

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_one_component_spread_over_several_logs(self, mock_plot, component_scaling_data):
        """As the ACCESS-ESM1.6 atmosphere is over its two logs: same cores, different regions."""

        manager, cores = component_scaling_data
        for exp_name in manager.data:
            manager.data[exp_name]["another_log"] = manager.data[exp_name]["component"]

        manager.plot_component_scaling_data(
            [
                RegionGroup("component", ["Region 1"], component="component"),
                RegionGroup("another_log", ["Region 2"], component="component"),
            ],
            tavg,
        )

        groups = mock_plot.call_args.args[0]
        assert len(groups) == 2
        assert all(list(stat["ncpus"].values) == sorted(cores) for stat in groups), "one component, one axis"

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_a_group_naming_no_component_falls_back(self, mock_plot, component_scaling_data):
        """First to what the manager associates with the log, and failing that to the log's own name."""

        manager, cores = component_scaling_data
        manager.application.log_components = {"a_log": "other"}
        for exp_name in manager.data:
            manager.data[exp_name]["a_log"] = manager.data[exp_name]["component"]

        manager.plot_component_scaling_data(
            [RegionGroup("a_log", ["Region 1"]), RegionGroup("component", ["Region 1"])], tavg
        )

        groups = mock_plot.call_args.args[0]
        assert list(groups[0]["ncpus"].values) == [30, 60, 120], "the mapping named 'other'"
        assert list(groups[1]["ncpus"].values) == sorted(cores), "nothing named it, so the log's own name"

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_the_axis_labels_are_passed_on(self, mock_plot, component_scaling_data):
        """Both of them, so a caller can name the axes of a plot this method builds for them."""

        manager, _ = component_scaling_data
        manager.plot_component_scaling_data(
            [RegionGroup("component", ["Region 1"])], tavg, xlabel="Ocean cores", ylabel="Ocean walltime"
        )

        assert mock_plot.call_args.kwargs["xlabel"] == "Ocean cores"
        assert mock_plot.call_args.kwargs["ylabel"] == "Ocean walltime"

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_no_axis_labels_are_given_by_default(self, mock_plot, component_scaling_data):
        manager, _ = component_scaling_data
        manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)

        assert mock_plot.call_args.kwargs["xlabel"] is None
        assert mock_plot.call_args.kwargs["ylabel"] is None

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_the_layout_a_generated_experiment_carries_is_used_as_it_stands(self, mock_plot, component_scaling_data):
        """It is the whole of it, so there is nothing on disk worth reading back."""

        manager, _ = component_scaling_data
        manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)
        assert manager.application.parse_layout_calls == []

    def test_an_experiment_whose_layout_cannot_be_told(self, component_scaling_data):
        manager, _ = component_scaling_data
        manager.experiments["small"].layout = None

        with pytest.raises(ValueError, match="layout of experiment 'small' could not be read"):
            manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)

    def test_a_layout_naming_no_such_component(self, component_scaling_data):
        manager, _ = component_scaling_data

        with pytest.raises(ValueError, match="holds no component 'absent'"):
            manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"], component="absent")], tavg)

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_two_experiments_giving_the_component_the_same_cores(self, mock_plot, component_scaling_data):
        """A layout says what share a component had, not how big the job around it was, so this happens."""

        manager, _ = component_scaling_data
        manager.experiments["large"].layout = layout_with(component=60, other=240)

        manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)

        (stat,) = mock_plot.call_args.args[0]
        assert list(stat["ncpus"].values) == [60, 120], "the two at 60 cores become one point"
        assert stat[tavg].pint.dequantify().values.tolist() == [[25.0], [50.0]], "and it is the faster of them"

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_the_fastest_is_taken_region_by_region(self, mock_plot, component_scaling_data):
        """So a point can take its value for one region from one run and for another from another."""

        manager, _ = component_scaling_data
        manager.experiments["large"].layout = layout_with(component=60, other=240)
        # 'large' is the faster of the two on Region 1 and the slower on Region 2.
        manager.data["large"] = make_component_dataset([25.0, 99.0])

        manager.plot_component_scaling_data([RegionGroup("component", ["Region 1", "Region 2"])], tavg)

        (stat,) = mock_plot.call_args.args[0]
        at_sixty = stat[tavg].sel(ncpus=60).pint.dequantify().values.tolist()
        assert at_sixty == [25.0, 10.0], "Region 1 from 'large', Region 2 from 'small'"

    def test_the_experiments_sharing_a_core_count_are_reported(self, component_scaling_data, caplog):
        """A point that is the best of several is not the only measurement there was, and says so."""

        manager, _ = component_scaling_data
        manager.experiments["large"].layout = layout_with(component=60, other=240)

        with caplog.at_level(logging.INFO):
            manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg, show=False)

        assert "['large', 'small']" in caplog.text
        assert "60 cores" in caplog.text

    @mock.patch("access.profiling.manager.plot_component_scaling")
    def test_a_colliding_experiment_can_be_left_out(self, mock_plot, component_scaling_data):
        manager, _ = component_scaling_data
        manager.experiments["large"].layout = layout_with(component=60, other=240)

        manager.plot_component_scaling_data(
            [RegionGroup("component", ["Region 1"])], tavg, experiments=["small", "medium"]
        )
        assert self._plotted(mock_plot) == [[60, 120]]

    def test_no_experiments_selected(self, component_scaling_data):
        manager, _ = component_scaling_data
        with pytest.raises(ValueError, match="No experiments selected"):
            manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg, experiments=[])

    def test_an_experiment_with_no_parsed_data(self, component_scaling_data):
        manager, _ = component_scaling_data
        with pytest.raises(ValueError, match="No parsed profiling data"):
            manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg, experiments=["nothing"])

    def test_a_log_with_no_parsed_data(self, component_scaling_data):
        manager, _ = component_scaling_data
        with pytest.raises(ValueError, match="No profiling data found for 'absent'"):
            manager.plot_component_scaling_data([RegionGroup("absent", ["Region 1"])], tavg)

    def test_a_region_that_is_not_there(self, component_scaling_data):
        manager, _ = component_scaling_data
        with pytest.raises(ValueError, match="Regions \\['Region 3'\\] not found"):
            manager.plot_component_scaling_data([RegionGroup("component", ["Region 3"])], tavg)

    def test_data_still_holding_several_runs(self, component_scaling_data):
        manager, _ = component_scaling_data
        manager.data["small"]["component"] = manager.data["small"]["component"].expand_dims({"run": [1]})

        with pytest.raises(ValueError, match="'run' dimension"):
            manager.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)


class TestOneListDrivesEveryPlot:
    """The point of the type: a study is described once and handed to all four plotting methods."""

    @staticmethod
    def _groups() -> list[RegionGroup]:
        return [RegionGroup("component", ["Region 1", "Region 2"], component="component", relabel={"Region 1": "Dyn"})]

    @mock.patch("access.profiling.manager.plot_line_metrics")
    @mock.patch("access.profiling.manager.plot_bar_metrics")
    @mock.patch("access.profiling.manager.plot_component_scaling")
    @mock.patch("access.profiling.manager.plot_scaling_metrics")
    def test_the_same_groups_label_every_figure_alike(
        self, mock_scaling, mock_component, mock_bar, mock_line, scaling_data
    ):
        """A reader moving between the four figures reads one set of names."""

        paths, ncpus, datasets = scaling_data
        manager = MockProfilingManager(paths, ncpus, datasets)
        for path, n in zip(paths, ncpus, strict=True):
            manager.experiments[path.name].layout = layout_with(component=n)
        groups = self._groups()

        manager.plot_scaling_data(groups, tavg)
        manager.plot_component_scaling_data(groups, tavg)
        manager.plot_bar_chart(groups, tavg)
        manager.plot_metric(groups, tavg, component_cores("component"))

        expected = ["Dyn", "component: Region 2"]
        assert list(mock_scaling.call_args.args[0][0]["region"].values) == expected
        assert list(mock_component.call_args.args[0][0]["region"].values) == expected
        assert mock_bar.call_args.args[1] == expected
        assert [label for label, _, _ in mock_line.call_args.args[0]] == expected

    @mock.patch("access.profiling.manager.plot_scaling_metrics")
    def test_the_component_is_ignored_where_it_cannot_be_honoured(self, mock_plot, scaling_data):
        """plot_scaling_data reads the whole job, so a group naming a component it could not find still plots."""

        paths, ncpus, datasets = scaling_data
        manager = MockProfilingManager(paths, ncpus, datasets)

        manager.plot_scaling_data([RegionGroup("component", ["Region 1"], component="no such component")], tavg)

        assert list(mock_plot.call_args.args[0][0]["ncpus"].values) == sorted(ncpus)


BLOCK_STUDY_GROUP = RegionGroup("component", ["Region 1", "Region 2"], relabel={"Region 1": "Dynamics"})


@pytest.fixture()
def block_study():
    """Parsed data for four experiments that differ in the cores of a component and in a label.

    | Experiment | `component` cores | `other` cores | `blocks` | Region 1 | Region 2 |
    |---|---|---|---|---|---|
    | exp-0001 | 120 | 60 | 4 | 50 s | 9 s |
    | exp-0002 | 60 | 30 | 4 | 100 s | 10 s |
    | exp-0003 | 120 | 60 | 8 | 40 s | 11 s |
    | exp-0004 | 60 | 30 | 8 | 80 s | 12 s |

    The cores are out of order, so that a plot has to sort them. Of two experiments on the same cores, neither is
    the faster on both regions.
    """
    cores = [120, 60, 120, 60]
    blocks = [4, 4, 8, 8]
    times = [[50.0, 9.0], [100.0, 10.0], [40.0, 11.0], [80.0, 12.0]]
    paths = [Path(f"exp-000{number}") for number in (1, 2, 3, 4)]
    manager = MockProfilingManager(
        paths, ncpus=[2 * n for n in cores], datasets=[make_component_dataset(values) for values in times]
    )
    for path, n, size in zip(paths, cores, blocks, strict=True):
        manager.experiments[path.name].layout = layout_with(component=n, other=n // 2)
        manager.experiments[path.name].labels = {"blocks": size}
    return manager


def add_per_pe_log(manager):
    """Gives every experiment a log with data for each of three PEs, which has more than one value for each region."""

    per_pe = xr.DataArray([[1.0, 2.0, 3.0]], dims=["region", "pe"]).pint.quantify(tavg.units)
    for data in manager.data.values():
        data["per_pe"] = xr.Dataset(data_vars={tavg: per_pe}, coords={"region": ["Region 1"], "pe": [0, 1, 2]})


class TestGather:
    """One dataset for several experiments, with what tells them apart as coordinates."""

    def test_the_experiments_are_put_along_a_dimension_of_their_names(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP)

        assert dict(gathered.sizes) == {"experiment": 4, "region": 2}
        assert list(gathered["experiment"].values) == ["exp-0001", "exp-0002", "exp-0003", "exp-0004"]
        assert set(gathered.coords) == {"experiment", "region"}, "nothing more unless it is asked for"

    def test_the_regions_carry_the_labels_of_the_group(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP)

        assert list(gathered["region"].values) == ["Dynamics", "component: Region 2"]

    def test_the_metrics_keep_their_values_and_their_units(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP)

        times = gathered[tavg].sel(region="Dynamics")
        assert times.pint.units == tavg.units
        assert times.pint.dequantify().values.tolist() == [50.0, 100.0, 40.0, 80.0]
        assert gathered[count].sel(experiment="exp-0003").pint.dequantify().values.tolist() == [1, 2]

    def test_a_label_is_a_coordinate_named_by_its_key(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP, ["blocks"])

        assert gathered["blocks"].dims == ("experiment",)
        assert gathered["blocks"].values.tolist() == [4, 4, 8, 8]

    def test_a_quantity_of_the_layout_is_a_coordinate_with_its_own_name(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP, [component_cores("other"), "blocks"])

        assert gathered["other cores"].dims == ("experiment",)
        assert gathered["other cores"].values.tolist() == [60, 30, 60, 30]
        assert gathered["blocks"].values.tolist() == [4, 4, 8, 8]

    def test_the_experiments_can_be_looked_up_by_a_coordinate(self, block_study):
        """Which is what the coordinates are for: the names say nothing."""

        gathered = block_study.gather(BLOCK_STUDY_GROUP, [component_cores("component"), "blocks"])

        wide = gathered.where((gathered["component cores"] == 120) & (gathered["blocks"] == 8), drop=True)
        assert list(wide["experiment"].values) == ["exp-0003"]

    def test_only_the_experiments_asked_for_in_the_order_they_are_given(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP, ["blocks"], experiments=["exp-0004", "exp-0001"])

        assert list(gathered["experiment"].values) == ["exp-0004", "exp-0001"]
        assert gathered["blocks"].values.tolist() == [8, 4]
        assert gathered[tavg].sel(region="Dynamics").pint.dequantify().values.tolist() == [80.0, 50.0]

    def test_a_name_given_twice_is_gathered_once(self, block_study):
        """As two selections put together give it: an experiment is one entry along the dimension."""

        gathered = block_study.gather(BLOCK_STUDY_GROUP, ["blocks"], experiments=["exp-0003", "exp-0001", "exp-0003"])

        assert list(gathered["experiment"].values) == ["exp-0003", "exp-0001"]
        assert gathered["blocks"].values.tolist() == [8, 4]

    def test_a_value_that_is_a_sequence_stays_one_value(self, block_study):
        """A process grid is one value of an experiment, not a row of a table of its own."""

        shape = ExperimentCoordinate("shape", lambda layout: (layout.n_cores, 1))
        gathered = block_study.gather(BLOCK_STUDY_GROUP, [shape])

        assert gathered["shape"].dims == ("experiment",)
        assert gathered["shape"].values.tolist() == [(180, 1), (90, 1), (180, 1), (90, 1)]

    def test_values_of_different_kinds_are_kept_as_they_are(self, block_study):
        """Not all turned into strings, which is what putting them in one array does by default."""

        block_study.experiments["exp-0001"].labels = {"blocks": "5-8"}
        gathered = block_study.gather(BLOCK_STUDY_GROUP, ["blocks"])

        assert gathered["blocks"].values.tolist() == ["5-8", 4, 8, 8]

    def test_the_component_of_the_group_is_ignored(self, block_study):
        group = RegionGroup("component", ["Region 1"], component="no such component")

        assert dict(block_study.gather(group).sizes) == {"experiment": 4, "region": 1}

    def test_experiments_of_different_sizes_keep_all_their_pes(self, block_study):
        """Data for each PE has as many PEs as the experiment has ranks, and none of them is dropped."""

        for name, n_pes in (("exp-0001", 3), ("exp-0002", 2)):
            per_pe = xr.DataArray([[1.0] * n_pes], dims=["region", "pe"]).pint.quantify(tavg.units)
            block_study.data[name]["per_pe"] = xr.Dataset(
                data_vars={tavg: per_pe}, coords={"region": ["Region 1"], "pe": list(range(n_pes))}
            )

        gathered = block_study.gather(RegionGroup("per_pe", ["Region 1"]), experiments=["exp-0001", "exp-0002"])

        assert dict(gathered.sizes) == {"experiment": 2, "region": 1, "pe": 3}
        missing = gathered[tavg].pint.dequantify().isnull()
        assert missing.sel(region="per_pe: Region 1").values.tolist() == [[False, False, False], [False, False, True]]

    def test_an_experiment_without_the_label(self, block_study):
        block_study.experiments["exp-0002"].labels = {"distribution": "roundrobin"}

        with pytest.raises(
            ValueError, match="Experiment 'exp-0002' has no label 'blocks'. Its labels are \\['distribution'\\]"
        ):
            block_study.gather(BLOCK_STUDY_GROUP, ["blocks"])

    @pytest.mark.parametrize("value", [[5, 8], {"from": 5, "to": 8}])
    def test_a_label_that_is_a_list_or_a_mapping(self, block_study, value):
        """Experiments are grouped by the values of a coordinate, and neither of these can be grouped by."""

        block_study.experiments["exp-0003"].labels = {"blocks": value}

        with pytest.raises(ValueError, match="The label 'blocks' of experiment 'exp-0003' is .* cannot be a coord"):
            block_study.gather(BLOCK_STUDY_GROUP, ["blocks"])

    def test_an_experiment_whose_layout_cannot_be_told(self, block_study):
        block_study.experiments["exp-0003"].layout = None

        with pytest.raises(
            ValueError, match="layout of experiment 'exp-0003' could not be read, .* read 'component cores' from"
        ):
            block_study.gather(BLOCK_STUDY_GROUP, [component_cores("component")])

    def test_a_layout_without_the_quantity_names_the_experiment(self, block_study):
        """The coordinate is handed a layout and nothing else, so the manager says whose layout it was."""

        with pytest.raises(
            ValueError, match="Experiment 'exp-0001' has no 'absent cores'. The layout holds no component 'absent'"
        ):
            block_study.gather(BLOCK_STUDY_GROUP, [component_cores("absent")])

    def test_two_coordinates_with_the_same_name(self, block_study):
        again = ExperimentCoordinate("blocks", lambda layout: layout.n_cores)

        with pytest.raises(ValueError, match="The coordinates \\['blocks'\\] are each named more than once"):
            block_study.gather(BLOCK_STUDY_GROUP, ["blocks", again])

    @pytest.mark.parametrize("name", ["experiment", "region"])
    def test_a_coordinate_named_as_one_the_data_has_of_its_own(self, block_study, name):
        with pytest.raises(ValueError, match=f"The coordinates \\['{name}'\\] are each named more than once"):
            block_study.gather(BLOCK_STUDY_GROUP, [ExperimentCoordinate(name, lambda layout: layout.n_cores)])

    def test_no_experiments_selected(self, block_study):
        with pytest.raises(ValueError, match="No experiments selected"):
            block_study.gather(BLOCK_STUDY_GROUP, experiments=[])

    def test_a_region_that_is_not_there(self, block_study):
        with pytest.raises(ValueError, match="Regions \\['Region 3'\\] not found"):
            block_study.gather(RegionGroup("component", ["Region 3"]))

    def test_data_still_holding_several_runs(self, block_study):
        """The message offers runs first, which leaves the parsed data as it is."""

        data = block_study.data["exp-0001"]
        data["component"] = data["component"].expand_dims({"run": [1]})

        with pytest.raises(ValueError, match="'run' dimension. Pass runs='min', 'max', 'mean' or 'median' to reduce"):
            block_study.gather(BLOCK_STUDY_GROUP)


class TestGatherReducingRuns:
    """The runs of an experiment reduced for one dataset, with the parsed data left holding all of them."""

    def test_each_region_is_reduced_on_its_own(self, run_data):
        gathered = run_data.gather(RegionGroup("component", ["Region 1", "Region 2"]), runs="max")

        assert "run" not in gathered.dims
        # The slowest run differs between the regions: run 3 for the first, run 7 for the second.
        assert gathered[tavg].sel(experiment="exp1").pint.dequantify().values.tolist() == [600.0, 9.0]

    def test_the_parsed_data_keeps_its_runs(self, run_data):
        """So the fastest and the slowest of the runs can both be had, which is what a spread is measured from."""

        group = RegionGroup("component", ["Region 1"])
        fastest = run_data.gather(group, runs="min")[tavg].pint.dequantify()
        slowest = run_data.gather(group, runs="max")[tavg].pint.dequantify()

        assert run_data.data["exp1"]["component"].sizes["run"] == 3
        assert (slowest / fastest).values.tolist() == [[3.0]]

    def test_an_experiment_that_ran_once_is_taken_as_it_is(self, block_study):
        reduced = block_study.gather(BLOCK_STUDY_GROUP, ["blocks"], runs="mean")

        xr.testing.assert_identical(reduced, block_study.gather(BLOCK_STUDY_GROUP, ["blocks"]))

    def test_an_unknown_statistic(self, run_data):
        with pytest.raises(ValueError, match="Unknown reduction 'sum'. Use 'min', 'max', 'mean' or 'median'."):
            run_data.gather(RegionGroup("component", ["Region 1"]), runs="sum")


class TestTable:
    """A metric of the experiments as a table, with a row for each value of one coordinate and a column for each
    value of another."""

    # The regions of BLOCK_STUDY_GROUP the other way round, so that their labels are not in sorted order.
    REVERSED_GROUP = RegionGroup("component", ["Region 2", "Region 1"], relabel={"Region 1": "Dynamics"})

    def test_the_rows_are_sorted_and_the_columns_follow_the_regions(self, block_study):
        table = block_study.table(self.REVERSED_GROUP, tavg, component_cores("component"), "blocks")

        # The experiments give 120 cores first, but the rows are sorted, as plot_metric sorts its x-axis.
        assert table.index.tolist() == [60, 120]
        assert table.index.name == "component cores"
        # The regions are in the order of the group, which is not the order of their labels.
        assert table.columns.tolist() == [
            ("component: Region 2", 4),
            ("component: Region 2", 8),
            ("Dynamics", 4),
            ("Dynamics", 8),
        ]
        assert table.columns.names == ["region", "blocks"]
        assert table["Dynamics"].values.tolist() == [[100.0, 80.0], [50.0, 40.0]]

    def test_the_values_of_the_columns_are_in_the_order_the_experiments_give_them(self, block_study):
        """As plot_metric orders its series: sorting would put a label such as "17" before "9-16"."""

        for name, label in (("exp-0001", "9-16"), ("exp-0002", "9-16"), ("exp-0003", "17"), ("exp-0004", "17")):
            block_study.experiments[name].labels = {"blocks": label}

        table = block_study.table(BLOCK_STUDY_GROUP, tavg, component_cores("component"), "blocks")

        assert table["Dynamics"].columns.tolist() == ["9-16", "17"]

    def test_without_a_second_coordinate_the_columns_are_the_regions(self, block_study):
        table = block_study.table(
            self.REVERSED_GROUP, tavg, component_cores("component"), experiments=["exp-0001", "exp-0002"]
        )

        assert table.columns.tolist() == ["component: Region 2", "Dynamics"]
        assert table.columns.name == "region"
        assert table.values.tolist() == [[10.0, 100.0], [9.0, 50.0]]

    def test_the_values_are_in_the_units_of_the_metric(self, block_study):
        """A table holds numbers, not quantities, so it says what the numbers are in."""

        table = block_study.table(BLOCK_STUDY_GROUP, tavg, component_cores("component"), "blocks")

        assert table.attrs["units"] == str(tavg.units)

    def test_a_pair_of_values_no_experiment_has_is_left_empty(self, block_study):
        table = block_study.table(
            BLOCK_STUDY_GROUP,
            tavg,
            component_cores("component"),
            "blocks",
            experiments=["exp-0001", "exp-0002", "exp-0003"],
        )

        assert math.isnan(table.loc[60, ("Dynamics", 8)])
        assert table.loc[120, ("Dynamics", 8)] == 40.0

    def test_the_runs_are_reduced_as_gather_reduces_them(self, run_data):
        run_data.experiments["exp1"].labels = {"blocks": 4}

        table = run_data.table(RegionGroup("component", ["Region 1"]), tavg, "blocks", runs="max")

        assert table.values.tolist() == [[600.0]]
        assert run_data.data["exp1"]["component"].sizes["run"] == 3

    def test_experiments_sharing_the_values_of_both_coordinates(self, block_study):
        """Two experiments in one cell of the table: rather than pick one, the table names them."""

        with pytest.raises(
            ValueError, match="The experiments \\['exp-0001', 'exp-0002'\\] and \\['exp-0003', 'exp-0004'\\] share"
        ):
            block_study.table(BLOCK_STUDY_GROUP, tavg, "blocks")

    def test_experiments_sharing_a_null_value(self, block_study):
        """A null label is a value like any other, which pandas leaves out of its groups by default."""

        for name in ("exp-0001", "exp-0003"):
            block_study.experiments[name].labels = {"blocks": None}

        with pytest.raises(ValueError, match="The experiments \\['exp-0001', 'exp-0003'\\] share their values"):
            block_study.table(BLOCK_STUDY_GROUP, tavg, "blocks")

    def test_two_regions_labelled_alike(self, block_study):
        """The plots draw them as two lines, but a table would give them one column, and blame the experiments."""

        group = RegionGroup("component", ["Region 1", "Region 2"], relabel={"Region 1": "X", "Region 2": "X"})

        with pytest.raises(ValueError, match="The group gives the same label to several of its regions: \\['X'\\]"):
            block_study.table(group, tavg, "blocks", component_cores("component"))

    def test_a_coordinate_named_value(self, block_study):
        """The name a table would give its column of values, were that column not keyed by the metric."""

        for experiment in block_study.experiments.values():
            experiment.labels = {"value": experiment.labels["blocks"]}

        table = block_study.table(BLOCK_STUDY_GROUP, tavg, "value", component_cores("component"))

        assert table.index.name == "value"
        assert table["Dynamics"].values.tolist() == [[50.0, 100.0], [40.0, 80.0]]

    def test_a_metric_with_more_than_one_value_for_each_region(self, block_study):
        for name, n_pes in (("exp-0001", 3), ("exp-0002", 2)):
            per_pe = xr.DataArray([[1.0] * n_pes], dims=["region", "pe"]).pint.quantify(tavg.units)
            block_study.data[name]["per_pe"] = xr.Dataset(
                data_vars={tavg: per_pe}, coords={"region": ["Region 1"], "pe": list(range(n_pes))}
            )

        with pytest.raises(ValueError, match="has the dimensions \\['pe'\\] besides the experiments and the regions"):
            block_study.table(
                RegionGroup("per_pe", ["Region 1"]),
                tavg,
                component_cores("component"),
                experiments=["exp-0001", "exp-0002"],
            )


class TestSelectExperiments:
    """The experiments with one value of a coordinate, picked out by it rather than by their names."""

    def test_by_a_label(self, block_study):
        assert block_study.select_experiments("blocks", 8) == ["exp-0003", "exp-0004"]

    def test_by_a_quantity_of_the_layout(self, block_study):
        assert block_study.select_experiments(component_cores("component"), 60) == ["exp-0002", "exp-0004"]

    def test_among_the_experiments_given_in_their_order(self, block_study):
        selected = block_study.select_experiments("blocks", 4, experiments=["exp-0004", "exp-0002", "exp-0001"])

        assert selected == ["exp-0002", "exp-0001"]

    def test_only_the_experiments_a_plot_can_read_by_default(self, block_study):
        """An experiment with no parsed data has its labels all the same, but nothing a plot or a table can read."""

        del block_study.data["exp-0001"]

        assert block_study.select_experiments("blocks", 4) == ["exp-0002"]

    def test_a_value_no_experiment_has(self, block_study):
        assert block_study.select_experiments("blocks", 16) == []

    def test_an_experiment_without_the_label_is_not_one_of_them(self, block_study):
        """As in a study of two kinds of choice, each labelled by keys of its own."""

        block_study.experiments["exp-0002"].labels = {"distribution": "roundrobin"}

        assert block_study.select_experiments("blocks", 4) == ["exp-0001"]

    def test_a_label_none_of_them_has(self, block_study):
        """Which is a misspelt key, rather than a value none of them has."""

        with pytest.raises(
            ValueError, match="None of the experiments \\[.*'exp-0004'\\] has a label 'block'. Their labels are"
        ):
            block_study.select_experiments("block", 4)

    def test_nothing_to_select_from(self, block_study):
        assert block_study.select_experiments("blocks", 4, experiments=[]) == []

    def test_an_experiment_this_manager_does_not_hold(self, block_study):
        with pytest.raises(ValueError, match="Experiments \\['nope'\\] are not held by this manager. Available"):
            block_study.select_experiments("blocks", 4, experiments=["exp-0001", "nope"])

    def test_experiments_given_need_no_parsed_data(self, block_study):
        """Labels and layouts are what is selected by, and neither is parsed."""

        del block_study.data["exp-0001"]

        assert block_study.select_experiments("blocks", 4, experiments=["exp-0001", "exp-0002"]) == [
            "exp-0001",
            "exp-0002",
        ]


class TestBarChartNamedByACoordinate:
    """The bars of each experiment named by a coordinate, as plot_metric names its series."""

    @mock.patch("access.profiling.manager.plot_bar_metrics")
    def test_each_experiment_is_named_by_its_value(self, mock_plot, block_study):
        experiments = block_study.select_experiments(component_cores("component"), 120)

        block_study.plot_bar_chart([BLOCK_STUDY_GROUP], tavg, experiments=experiments, label="blocks", show=False)

        assert mock_plot.call_args.args[0] == {"blocks 4": [50.0, 9.0], "blocks 8": [40.0, 11.0]}

    def test_two_experiments_with_the_same_value(self, block_study):
        """Their bars would have one name, and one experiment would replace the other in the chart."""

        with pytest.raises(
            ValueError,
            match="the experiments \\['exp-0001', 'exp-0003'\\] would all be 'component cores 120'; "
            "\\['exp-0002', 'exp-0004'\\] would all be 'component cores 60'",
        ):
            block_study.plot_bar_chart([BLOCK_STUDY_GROUP], tavg, label=component_cores("component"), show=False)

    def test_a_relabel_map_as_well(self, block_study):
        with pytest.raises(ValueError, match="Give experiment_relabel_map or label, not both"):
            block_study.plot_bar_chart(
                [BLOCK_STUDY_GROUP], tavg, experiment_relabel_map={"exp-0001": "one"}, label="blocks", show=False
            )


class TestPlotMetric:
    """Plotting a metric against whatever the study varied, one line for each region and each series."""

    @staticmethod
    def _lines(mock_plot):
        """The lines handed to the plot: the label, the values along x and the values of the metric of each."""

        return [tuple(line) for line in mock_plot.call_args.args[0]]

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_each_region_is_a_line_against_the_coordinate(self, mock_plot, block_study):
        """In the order of the coordinate, whatever the order of the experiments."""

        block_study.plot_metric(
            [BLOCK_STUDY_GROUP], tavg, component_cores("component"), experiments=["exp-0001", "exp-0002"]
        )

        assert self._lines(mock_plot) == [
            ("Dynamics", [60, 120], [100.0, 50.0]),
            ("component: Region 2", [60, 120], [10.0, 9.0]),
        ]

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_a_label_can_go_along_the_x_axis(self, mock_plot, block_study):
        block_study.plot_metric(
            [RegionGroup("component", ["Region 1"])], tavg, "blocks", experiments=["exp-0004", "exp-0002"]
        )

        assert self._lines(mock_plot) == [("component: Region 1", [4, 8], [100.0, 80.0])]

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_every_group_is_plotted(self, mock_plot, block_study):
        groups = [RegionGroup("component", ["Region 2"]), RegionGroup("component", ["Region 1"])]
        block_study.plot_metric(groups, tavg, "blocks", experiments=["exp-0002", "exp-0004"])

        assert self._lines(mock_plot) == [
            ("component: Region 2", [4, 8], [10.0, 12.0]),
            ("component: Region 1", [4, 8], [100.0, 80.0]),
        ]

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_a_series_divides_each_region_into_lines(self, mock_plot, block_study):
        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks")

        assert self._lines(mock_plot) == [
            ("Dynamics, blocks 4", [60, 120], [100.0, 50.0]),
            ("Dynamics, blocks 8", [60, 120], [80.0, 40.0]),
            ("component: Region 2, blocks 4", [60, 120], [10.0, 9.0]),
            ("component: Region 2, blocks 8", [60, 120], [12.0, 11.0]),
        ]

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_the_series_come_in_the_order_the_experiments_give_them(self, mock_plot, block_study):
        block_study.plot_metric(
            [RegionGroup("component", ["Region 1"], relabel={"Region 1": "Dynamics"})],
            tavg,
            component_cores("component"),
            series="blocks",
            experiments=["exp-0003", "exp-0001", "exp-0002", "exp-0004"],
        )

        assert [label for label, _, _ in self._lines(mock_plot)] == ["Dynamics, blocks 8", "Dynamics, blocks 4"]

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_a_quantity_of_the_layout_can_be_the_series(self, mock_plot, block_study):
        block_study.plot_metric(
            [RegionGroup("component", ["Region 1"])], tavg, "blocks", series=component_cores("component")
        )

        assert self._lines(mock_plot) == [
            ("component: Region 1, component cores 120", [4, 8], [50.0, 40.0]),
            ("component: Region 1, component cores 60", [4, 8], [100.0, 80.0]),
        ]

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_experiments_sharing_a_point_become_the_fastest_of_them_region_by_region(self, mock_plot, block_study):
        """Without a series, two experiments are on each number of cores, and neither wins both regions."""

        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"))

        assert self._lines(mock_plot) == [
            ("Dynamics", [60, 120], [80.0, 40.0]),
            ("component: Region 2", [60, 120], [10.0, 9.0]),
        ]

    def test_the_experiments_sharing_a_point_are_reported(self, block_study, caplog):
        """A point that is the best of several is not the only measurement there was, and says so."""

        with caplog.at_level(logging.INFO):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"), show=False)

        assert "Experiments ['exp-0002', 'exp-0004'] all have 'component cores' 60. Plotting the fastest" in caplog.text
        assert "Experiments ['exp-0001', 'exp-0003'] all have 'component cores' 120. Plotting" in caplog.text

    def test_with_a_series_the_report_says_which_line_the_point_is_on(self, block_study, caplog):
        block_study.experiments["exp-0003"].labels = {"blocks": 4}

        with caplog.at_level(logging.INFO):
            block_study.plot_metric(
                [BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks", show=False
            )

        assert "Experiments ['exp-0001', 'exp-0003'] all have 'component cores' 120 and 'blocks' 4." in caplog.text
        assert caplog.text.count("Plotting the fastest of them") == 1, "no other point is shared"

    @pytest.mark.parametrize("order", [["exp-0001", "exp-0003"], ["exp-0003", "exp-0001"]])
    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_an_experiment_with_no_value_for_a_region_is_passed_over(self, mock_plot, block_study, order):
        """Whichever experiment comes first, as plot_component_scaling_data passes over it.

        A region that the run select_best_run kept does not have is NaN, the runs being put together with an outer
        join.
        """
        block_study.data["exp-0001"] = make_component_dataset([math.nan, 9.0])

        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"), experiments=order)

        assert self._lines(mock_plot) == [("Dynamics", [120], [40.0]), ("component: Region 2", [120], [9.0])]

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_a_point_where_no_experiment_has_a_value_is_nan(self, mock_plot, block_study):
        for name in ("exp-0001", "exp-0003"):
            block_study.data[name] = make_component_dataset([math.nan, 9.0])

        block_study.plot_metric([RegionGroup("component", ["Region 1"])], tavg, component_cores("component"))

        ((_, x_values, fastest),) = self._lines(mock_plot)
        assert x_values == [60, 120]
        assert fastest[0] == 80.0 and math.isnan(fastest[1])

    def test_a_metric_with_more_than_one_value_for_each_region(self, block_study):
        """As data for each PE has, rather than a TypeError from turning it into one number."""

        add_per_pe_log(block_study)

        with pytest.raises(ValueError, match="has the dimensions \\['pe'\\] .* more values than a line has points"):
            block_study.plot_metric([RegionGroup("per_pe", ["Region 1"])], tavg, "blocks")

    def test_nothing_is_reported_where_no_point_is_shared(self, block_study, caplog):
        with caplog.at_level(logging.INFO):
            block_study.plot_metric(
                [BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks", show=False
            )

        assert "Plotting the fastest of them" not in caplog.text

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_the_x_axis_is_labelled_with_the_name_of_the_coordinate(self, mock_plot, block_study):
        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks")
        assert mock_plot.call_args.kwargs["xlabel"] == "component cores"
        assert mock_plot.call_args.kwargs["ylabel"] is None

        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks", series=component_cores("component"))
        assert mock_plot.call_args.kwargs["xlabel"] == "blocks"

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_the_axis_labels_the_metric_and_show_are_passed_on(self, mock_plot, block_study):
        block_study.plot_metric(
            [BLOCK_STUDY_GROUP], tavg, "blocks", xlabel="Blocks per rank", ylabel="Walltime (s)", show=False
        )

        assert mock_plot.call_args.args[1] is tavg
        assert mock_plot.call_args.kwargs == {"xlabel": "Blocks per rank", "ylabel": "Walltime (s)", "show": False}

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_it_is_shown_by_default(self, mock_plot, block_study):
        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks")

        assert mock_plot.call_args.kwargs["show"] is True

    def test_it_returns_the_figure_it_drew(self, block_study):
        fig = block_study.plot_metric(
            [BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks", show=False
        )

        (ax,) = fig.axes
        assert len(ax.lines) == 4
        assert ax.get_xlabel() == "component cores"

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_the_component_of_a_group_is_ignored(self, mock_plot, block_study):
        group = RegionGroup("component", ["Region 1"], component="no such component")
        block_study.plot_metric([group], tavg, "blocks", experiments=["exp-0001"])

        assert self._lines(mock_plot) == [("component: Region 1", [4], [50.0])]

    def test_a_value_of_x_that_is_not_a_number(self, block_study):
        """Lines join points in the order of their numbers, and a string has no place among them."""

        block_study.experiments["exp-0002"].labels = {"blocks": "5-8"}

        with pytest.raises(
            ValueError, match="Experiment 'exp-0002' has the value '5-8' for 'blocks', which is not a number"
        ):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks")

    def test_a_boolean_is_not_a_number_along_the_x_axis(self, block_study):
        """Python counts it as one, but true and false have no place along an axis."""

        block_study.experiments["exp-0002"].labels = {"blocks": True}

        with pytest.raises(ValueError, match="Experiment 'exp-0002' has the value True for 'blocks', which is not a"):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks")

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_the_series_are_told_apart_as_they_are_shown(self, mock_plot, block_study):
        """1, True and 1.0 are equal in Python, so keyed by their values they would be one line named after one."""

        for name, value in (("exp-0001", 1), ("exp-0002", True), ("exp-0003", 1.0)):
            block_study.experiments[name].labels = {"threads": value}

        block_study.plot_metric(
            [RegionGroup("component", ["Region 1"])],
            tavg,
            component_cores("component"),
            series="threads",
            experiments=["exp-0001", "exp-0002", "exp-0003"],
        )

        assert self._lines(mock_plot) == [
            ("component: Region 1, threads 1", [120], [50.0]),
            ("component: Region 1, threads True", [60], [100.0]),
            ("component: Region 1, threads 1.0", [120], [40.0]),
        ]

    def test_two_values_of_the_series_shown_alike(self, block_study):
        """Their lines would carry the same label."""

        block_study.experiments["exp-0001"].labels = {"threads": 1}
        block_study.experiments["exp-0002"].labels = {"threads": "1"}

        with pytest.raises(ValueError, match="The values 1 and '1' of 'threads' are both shown as '1'"):
            block_study.plot_metric(
                [BLOCK_STUDY_GROUP],
                tavg,
                component_cores("component"),
                series="threads",
                experiments=["exp-0001", "exp-0002"],
            )

    @mock.patch("access.profiling.manager.plot_line_metrics")
    def test_a_series_need_not_be_a_number(self, mock_plot, block_study):
        for experiment in block_study.experiments.values():
            experiment.labels = {"distribution": "roundrobin"}
        block_study.plot_metric(
            [RegionGroup("component", ["Region 1"])],
            tavg,
            component_cores("component"),
            series="distribution",
            experiments=["exp-0001", "exp-0002"],
        )

        assert self._lines(mock_plot) == [("component: Region 1, distribution roundrobin", [60, 120], [100.0, 50.0])]

    def test_an_experiment_without_the_label_of_the_series(self, block_study):
        block_study.experiments["exp-0004"].labels = {}

        with pytest.raises(ValueError, match="Experiment 'exp-0004' has no label 'blocks'"):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks")

    def test_a_log_with_no_parsed_data_is_said_before_anything_about_the_coordinates(self, block_study):
        with pytest.raises(ValueError, match="No profiling data found for 'absent'"):
            block_study.plot_metric([RegionGroup("absent", ["Region 1"])], tavg, "no such label")

    def test_no_experiments_selected(self, block_study):
        with pytest.raises(ValueError, match="No experiments selected"):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks", experiments=[])

    def test_data_still_holding_several_runs(self, block_study):
        """A plot takes no runs, so the message offers the methods that reduce them."""

        data = block_study.data["exp-0001"]
        data["component"] = data["component"].expand_dims({"run": [1]})

        with pytest.raises(ValueError, match="'run' dimension. Use select_best_run\\(\\)"):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks")


class TestLayoutIsReadBackOnce:
    """An experiment this manager did not generate has its layout read off its configuration."""

    def test_it_is_parsed_and_kept(self):
        manager = MockProfilingManager([Path("expt")], ncpus=[8], datasets=[make_component_dataset([1.0, 2.0])])
        manager.application.layouts["expt"] = layout_with(component=4)

        assert layout_of(manager._layout("expt")).n_cores == 4
        assert layout_of(manager._layout("expt")).n_cores == 4
        assert len(manager.application.parse_layout_calls) == 1
        assert manager.experiments["expt"].layout is not None

    def test_a_layout_that_cannot_be_told_stays_unknown(self):
        manager = MockProfilingManager([Path("expt")], ncpus=[8], datasets=[make_component_dataset([1.0, 2.0])])

        assert manager._layout("expt") is None
        assert manager.experiments["expt"].layout is None


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

        manager.delete_experiment("release")

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
            manager.delete_experiment("release")

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
            manager.delete_experiment("release")

        assert "release" not in manager.experiments
        assert "(disk full), so experiment 'release' is forgotten in this session only" in caplog.text

    def test_a_directory_that_held_only_the_record_goes_with_it(self, tmp_path):
        work_dir = tmp_path / "work"
        (work_dir / "runs" / "release-1.0").mkdir(parents=True)
        manager = MockProfilingManager([], work_dir=work_dir)
        manager.add_experiment_from_directory("release", Path("runs/release-1.0"))

        manager.delete_experiment("release")

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

    @pytest.mark.parametrize("error", [FileNotFoundError("config.yaml"), ValueError("NPROC is not set")])
    def test_a_count_that_cannot_be_read_stops_nothing(self, tmp_path, caplog, error):
        """The other experiments are archived, the study file is copied, and the record leaves the count null. Payu's
        parse_ncpus raises the first without a config.yaml, and Cylc's the second for a variable it cannot find."""
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
        manager.delete_experiment("release")
        added(tmp_path / "again").delete_experiments(experiments=["release"])

        history = json.loads((manager.work_dir / STUDY_FILENAME).read_text())["history"]
        again = json.loads((tmp_path / "again" / "work" / STUDY_FILENAME).read_text())["history"]

        assert [entry["call"] for entry in history] == ["add_experiment_from_directory", "delete_experiment"]
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
        manager.delete_experiment("not-held")

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


class TestGenerateExperiments:
    """One experiment for each configuration choice, recognised afterwards by what the choice states.

    The choices here have no layout, so the application, which raises when it is asked anything, is never
    consulted. Choices with a layout are tested with the Payu manager, whose mock application has layouts.
    """

    def test_each_choice_becomes_an_experiment_numbered_in_order(self, tmp_path):
        manager = choosing(tmp_path)

        manager.generate_experiments([ConfigurationChoice(CARTESIAN), ConfigurationChoice(ROUNDROBIN)])

        (plans,) = manager._created_plans
        assert [plan.name for plan in plans] == ["exp-0001", "exp-0002"]
        assert list(manager.experiments) == ["exp-0001", "exp-0002"]
        assert all(experiment.status == ProfilingExperimentStatus.NEW for experiment in manager.experiments.values())

    def test_a_choice_without_a_layout_changes_what_it_states_and_nothing_else(self, tmp_path):
        manager = choosing(tmp_path)

        manager.generate_experiments([ConfigurationChoice(ROUNDROBIN)])

        ((plan,),) = manager._created_plans
        assert plan.changes == ROUNDROBIN
        assert plan.layout is None
        # The walltime is one of the changes where the study wants one, not something the plan states.
        assert plan.walltime_hours is None
        assert not plan.regenerating

    def test_the_record_keeps_the_changes_and_the_labels(self, tmp_path):
        manager = choosing(tmp_path)

        manager.generate_experiments([ConfigurationChoice(ROUNDROBIN, labels={"distribution": "roundrobin"})])

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
        manager.generate_experiments([choice])

        choice.changes["config.yaml"]["walltime"] = "4:00:00"
        choice.labels["grid"].append(1)

        experiment = manager.experiments["exp-0001"]
        assert experiment.changes == {"config.yaml": {"walltime": "2:00:00"}}
        assert experiment.labels == {"grid": [12, 16]}
        manager.generate_experiments([choice])
        assert list(manager.experiments) == ["exp-0001", "exp-0002"], "four hours is another configuration"

    def test_a_choice_the_study_has_is_skipped(self, tmp_path, caplog):
        manager = choosing(tmp_path)
        manager.generate_experiments([ConfigurationChoice(CARTESIAN)])

        with caplog.at_level(logging.INFO):
            manager.generate_experiments([ConfigurationChoice(CARTESIAN), ConfigurationChoice(ROUNDROBIN)])

        assert "Experiment exp-0001 already exists for this configuration choice. Skipping addition." in caplog.text
        assert [[plan.name for plan in plans] for plans in manager._created_plans] == [["exp-0001"], ["exp-0002"]]
        assert manager._created_plans[1][0].changes == ROUNDROBIN

    def test_nothing_new_generates_nothing(self, tmp_path, caplog):
        manager = choosing(tmp_path)
        manager.generate_experiments([ConfigurationChoice(CARTESIAN)])

        manager.generate_experiments([ConfigurationChoice(CARTESIAN)])

        assert "No new experiments to generate. Will skip generation." in caplog.text
        assert len(manager._created_plans) == 1
        assert len(manager._read_study()["history"]) == 1

    def test_a_later_manager_recognises_the_choice_from_the_record(self, tmp_path):
        manager = choosing(tmp_path)
        manager.generate_experiments([ConfigurationChoice(CARTESIAN)])

        resumed = later(manager)
        resumed.control = manager.control
        resumed.generate_experiments([ConfigurationChoice(CARTESIAN), ConfigurationChoice(ROUNDROBIN)])

        ((plan,),) = resumed._created_plans
        assert (plan.name, plan.changes) == ("exp-0002", ROUNDROBIN)

    def test_changes_are_the_same_whatever_the_order_of_their_keys(self, tmp_path):
        manager = choosing(tmp_path)
        manager.generate_experiments([ConfigurationChoice({"a": {"x": 1, "y": 2}, "b": {"z": 3}})])

        manager.generate_experiments([ConfigurationChoice({"b": {"z": 3}, "a": {"y": 2, "x": 1}})])

        assert list(manager.experiments) == ["exp-0001"]

    def test_the_same_choice_twice_in_one_call_is_refused(self, tmp_path):
        manager = choosing(tmp_path)

        with pytest.raises(ValueError, match="Two of the configuration choices are the same configuration"):
            manager.generate_experiments(
                [ConfigurationChoice(CARTESIAN, labels={"run": 1}), ConfigurationChoice(CARTESIAN, labels={"run": 2})]
            )

        assert manager._created_plans == [] and manager.experiments == {}
        assert not (manager.work_dir / STUDY_FILENAME).exists(), "so no number was taken"

    def test_it_is_refused_even_where_the_study_already_has_the_experiment(self, tmp_path):
        manager = choosing(tmp_path)
        manager.generate_experiments([ConfigurationChoice(CARTESIAN)])

        with pytest.raises(ValueError, match="Two of the configuration choices are the same configuration"):
            manager.generate_experiments([ConfigurationChoice(CARTESIAN), ConfigurationChoice(CARTESIAN)])

    def test_a_held_experiment_keeps_its_labels(self, tmp_path, caplog):
        manager = choosing(tmp_path)
        manager.generate_experiments([ConfigurationChoice(CARTESIAN, labels={"distribution": "cartesian"})])

        manager.generate_experiments([ConfigurationChoice(CARTESIAN, labels={"distribution": "the default"})])

        assert manager.experiments["exp-0001"].labels == {"distribution": "cartesian"}
        assert (
            "Experiment exp-0001 keeps the labels it has, {'distribution': 'cartesian'}, and not those of the "
            "configuration choice it was found for, {'distribution': 'the default'}." in caplog.text
        )

    def test_the_same_labels_again_say_nothing(self, tmp_path, caplog):
        manager = choosing(tmp_path)
        manager.generate_experiments([ConfigurationChoice(CARTESIAN, labels={"distribution": "cartesian"})])

        manager.generate_experiments([ConfigurationChoice(CARTESIAN, labels={"distribution": "cartesian"})])

        assert "keeps the labels" not in caplog.text

    def test_an_added_experiment_is_not_taken_for_a_choice(self, tmp_path):
        """What an added experiment changes is unknown, so it is not the control with nothing changed."""
        manager = added(tmp_path)
        manager.control = GitControlSource("https://example.com/repo", "commit")

        manager.generate_experiments([ConfigurationChoice()])

        assert list(manager.experiments) == ["release", "exp-0001"]
        assert manager.experiments["exp-0001"].changes == {}

    def test_the_history_says_which_method_was_called(self, tmp_path):
        manager = choosing(tmp_path)

        manager.generate_experiments([ConfigurationChoice(CARTESIAN)], some_option="value")

        (entry,) = manager._read_study()["history"]
        assert entry["call"] == "generate_experiments"
        assert entry["control"] == manager.control.provenance()
        assert entry["engine"] == {"some_option": "value"}
        assert entry["experiments"] == ["exp-0001"]
        assert manager._read_study()["last_number"] == 1

    def test_it_is_refused_without_a_control(self, tmp_path):
        manager = MockProfilingManager([], work_dir=tmp_path / "work", archive_dir=tmp_path / "archive")

        with pytest.raises(ValueError, match="Cannot generate experiments without a control"):
            manager.generate_experiments([ConfigurationChoice(CARTESIAN)])

        assert manager._created_plans == []

    def test_a_failed_generation_registers_nothing_and_keeps_its_numbers(self, tmp_path, caplog):
        manager = choosing(tmp_path)

        with (
            mock.patch.object(MockProfilingManager, "_create_experiments", side_effect=RuntimeError("engine")),
            pytest.raises(RuntimeError, match="engine"),
        ):
            manager.generate_experiments([ConfigurationChoice(CARTESIAN), ConfigurationChoice(ROUNDROBIN)])

        assert manager.experiments == {}
        assert "none of ['exp-0001', 'exp-0002'] were added to this manager" in caplog.text
        assert not (manager.work_dir / "exp-0001").exists()

        manager.generate_experiments([ConfigurationChoice(CARTESIAN)])
        assert list(manager.experiments) == ["exp-0003"]

    def test_what_the_engine_refuses_beforehand_takes_no_number(self, tmp_path, caplog):
        """Nothing is written before the engine has had its say, so the corrected call starts where this one did."""
        manager = choosing(tmp_path)

        with (
            mock.patch.object(MockProfilingManager, "_refuse_plans", side_effect=ValueError("refused")) as refuse,
            pytest.raises(ValueError, match="refused"),
        ):
            manager.generate_experiments([ConfigurationChoice(CARTESIAN)], some_option="value")

        (plans,) = refuse.call_args.args
        assert [plan.name for plan in plans] == ["exp-0001"], "it is asked about what is about to be created"
        assert refuse.call_args.kwargs == {"some_option": "value"}
        assert manager._created_plans == [], "the engine is not asked to create what it refused"
        assert not (manager.work_dir / STUDY_FILENAME).exists()
        assert "were added to this manager" not in caplog.text, "nothing may exist that was never started"

        manager.generate_experiments([ConfigurationChoice(CARTESIAN)])
        assert list(manager.experiments) == ["exp-0001"]

    def test_the_engine_is_not_asked_about_nothing(self, tmp_path):
        """A call that has nothing left to create is not refused for what creating would have needed."""
        manager = choosing(tmp_path)
        manager.generate_experiments([ConfigurationChoice(CARTESIAN)])

        with mock.patch.object(MockProfilingManager, "_refuse_plans", side_effect=ValueError("refused")) as refuse:
            manager.generate_experiments([ConfigurationChoice(CARTESIAN)])

        refuse.assert_not_called()
