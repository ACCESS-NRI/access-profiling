# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path
from unittest import mock

import pytest
import xarray as xr

from access.profiling.manager import ProfilingExperiment, ProfilingExperimentStatus, ProfilingLog, ProfilingManager
from access.profiling.metrics import count, tavg


class MockProfilingManager(ProfilingManager):
    """Test class inheriting from ProfilingManager to test its methods.

    This class will simulate parsing of some profiling data.

    Note that this mock class assumes that experiments are named after the last part of their path and all experiments
    will be marked as DONE.

    Args:
        paths (list[Path]): List of paths to simulate different configurations.
        ncpus (list[int]): List of number of CPUs corresponding to each path.
        datasets (list[xr.Dataset]): List of datasets to return for each path.
    """

    def __init__(
        self,
        paths: list[Path],
        ncpus: list[int] | None = None,
        datasets: list[dict[str, xr.Dataset]] | None = None,
    ):
        super().__init__(Path("/fake/work_dir"), Path("/fake/archive_dir"))

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

        if datasets is not None:
            self.data = dict(zip([path.name for path in paths], datasets, strict=True))

    # The layout API is abstract on ProfilingManager but plays no part in these tests, which cover the parsing,
    # archiving and bookkeeping side of the manager.
    @property
    def parallel_component(self):
        raise NotImplementedError

    def layout_branch_name(self, layout):
        raise NotImplementedError

    def layout_config_changes(self, layout):
        raise NotImplementedError

    def parse_ncpus(self, path, run_path=None):
        """Simulate parsing number of CPUs for a given path."""
        self._last_parse_ncpus_args = (path, run_path)
        self._parse_ncpus_calls.append((path, run_path))
        return self._mock_ncpus[path.name]

    def profiling_logs(self, path, run_path=None):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Simulate parsing profiling data for a given path."""
        pass

    def _delete_experiment(self, name, dry_run):
        """Record requested deletions instead of touching the filesystem."""
        self._deleted_experiments.append((name, dry_run))


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


@mock.patch("access.profiling.manager.Path.is_dir")
@mock.patch("access.profiling.manager.Path.glob")
@mock.patch("access.profiling.manager.Path.is_file")
@mock.patch("access.profiling.manager.ProfilingExperiment")
def test_archive_discovery(mock_experiment, mock_is_file, mock_glob, mock_is_dir):
    """Test that ProfilingManager discovers archived experiments correctly."""

    mock_glob.return_value = [Path("/fake/archive_dir/exp1.tar.gz"), Path("/fake/archive_dir/exp2.tar.gz")]

    # Test when archive directory does not exist
    mock_is_dir.return_value = False
    manager = MockProfilingManager(paths=[])
    assert manager.experiments == {}, "No experiments should be discovered if archive dir does not exist."

    # Test when archive directory exists, but there are no files (treat all paths as non-files)
    mock_is_dir.return_value = True
    mock_is_file.return_value = False
    manager = MockProfilingManager(paths=[])
    assert manager.experiments == {}, "No experiments should be discovered if no archive files are present."

    # Test when archive directory exists and files are present
    mock_is_dir.return_value = True
    mock_is_file.return_value = True
    manager = MockProfilingManager(paths=[])
    assert set(manager.experiments.keys()) == {"exp1", "exp2"}
    assert mock_experiment.call_count == 2
    mock_experiment.assert_any_call(path=Path("/fake/archive_dir/exp1.tar.gz"))
    mock_experiment.assert_any_call(path=Path("/fake/archive_dir/exp2.tar.gz"))


@mock.patch("access.profiling.manager.Path.mkdir")
@mock.patch("access.profiling.manager.ProfilingExperiment.archive")
def test_archive_experiments(mock_archive, mock_mkdir):
    """Test the archive_experiments method of ProfilingManager."""

    # Setup mock experiments
    exp_paths = [Path("/fake/work_dir/exp1"), Path("/fake/work_dir/exp2"), Path("/fake/work_dir/exp3")]
    manager = MockProfilingManager(exp_paths)
    manager.experiments["exp2"].status = ProfilingExperimentStatus.RUNNING
    manager.experiments["exp3"].status = ProfilingExperimentStatus.NEW

    # Archive experiments
    manager.archive_experiments()

    # Check calls
    mock_mkdir.assert_called_with(parents=True, exist_ok=True)  # Check archive directory creation
    assert mock_archive.call_count == 3, "Should attempt to archive all experiments."
    mock_archive.assert_any_call(
        Path("/fake/archive_dir/exp1"), exclude_files=None, exclude_dirs=None, follow_symlinks=False, overwrite=False
    )
    mock_archive.assert_any_call(
        Path("/fake/archive_dir/exp2"), exclude_files=None, exclude_dirs=None, follow_symlinks=False, overwrite=False
    )
    mock_archive.assert_any_call(
        Path("/fake/archive_dir/exp3"), exclude_files=None, exclude_dirs=None, follow_symlinks=False, overwrite=False
    )
    assert mock_archive.call_count == 3


@mock.patch("access.profiling.manager.Path.is_dir")
def test_add_experiment_from_directory(mock_is_dir):
    """Test the add_experiment_from_directory method of ProfilingManager."""

    mock_is_dir.return_value = False
    manager = MockProfilingManager(paths=[])

    # Test adding a valid experiment
    mock_is_dir.return_value = True
    manager.add_experiment_from_directory("existing_experiment", Path("/fake/work_dir/existing_experiment"))
    assert "existing_experiment" in manager.experiments, "Experiment should be added."
    assert manager.experiments["existing_experiment"].status == ProfilingExperimentStatus.DONE, (
        "Experiment status should be set to DONE."
    )

    # Test adding a valid experiment with relative path
    mock_is_dir.return_value = True
    manager.add_experiment_from_directory("relative_experiment", Path("relative_experiment"))
    assert "relative_experiment" in manager.experiments, "Experiment with relative path should be added."
    assert manager.experiments["relative_experiment"].status == ProfilingExperimentStatus.DONE, (
        "Experiment status should be set to DONE."
    )
    assert manager.experiments["relative_experiment"].path == Path("/fake/work_dir/relative_experiment"), (
        "Experiment path should be correctly resolved to absolute path."
    )

    # Test adding an experiment with a non-existing path
    mock_is_dir.return_value = False
    with pytest.raises(ValueError, match="does not exist or is not a directory"):
        manager.add_experiment_from_directory("non_existing_experiment", Path("/fake/work_dir/non_existing_experiment"))

    # Test adding an experiment outside the working directory
    mock_is_dir.return_value = True
    with pytest.raises(ValueError, match="is not inside the working directory"):
        manager.add_experiment_from_directory("outside_experiment", Path("/fake/outside_work_dir/outside_experiment"))


def test_delete_experiment(caplog):
    """Test the delete_experiment method of ProfilingManager."""

    # Setup mock experiments
    exp_paths = [Path("/fake/work_dir/exp1"), Path("/fake/work_dir/exp2")]
    manager = MockProfilingManager(exp_paths)

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
        # records of both levels, so that the two except clauses can be told apart.
        with caplog.at_level(logging.INFO):
            manager.parse_profiling_data()

    failed = [record for record in caplog.records if "Failed to parse optional profiling log" in record.message]
    not_found = [record for record in caplog.records if "not found. Skipping." in record.message]
    assert len(failed) == 3 and len(not_found) == 2, (
        "The dedicated FileNotFoundError branch should catch exactly the missing-log errors, including subclasses; "
        "every other exception type, including other OSErrors, should fall through to the general branch."
    )
    assert {record.levelname for record in failed + not_found} == {"INFO"}, (
        "Optional log parse failures should be reported at INFO level, regardless of exception type."
    )
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
            components=["non_existing_component"],
            regions=[["Region 1"]],
            metric=tavg,
        )

    # Test plotting scaling data
    manager.plot_scaling_data(
        components=["component"],
        regions=[["Region 1"]],
        metric=tavg,
        region_relabel_map={"Region 1": "Total"},
        experiments=["1cpu", "4cpu"],
    )
    assert mock_plot.call_count == 1
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

    with pytest.raises(ValueError, match="Requested region\\(s\\)") as exc_info:
        manager.plot_scaling_data(
            components=["component"],
            regions=[["Missing Region"]],
            metric=tavg,
            experiments=["1cpu"],
        )

    message = str(exc_info.value)
    assert "Missing Region" in message
    assert "component 'component'" in message
    assert "experiment '1cpu'" in message


def test_scaling_data_no_selected_experiments_raises_value_error(scaling_data):
    """Test plot_scaling_data raises when no experiments are selected."""

    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    with pytest.raises(ValueError, match="No experiments selected for scaling plot"):
        manager.plot_scaling_data(
            components=["component"],
            regions=[["Region 1"]],
            metric=tavg,
            experiments=[],
        )


def test_scaling_data_missing_experiment_data_raises_value_error(scaling_data):
    """Test plot_scaling_data raises when selected experiments have no parsed data."""

    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    with pytest.raises(ValueError, match="No parsed profiling data found for experiment\(s\)") as exc_info:
        manager.plot_scaling_data(
            components=["component"],
            regions=[["Region 1"]],
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
            components=["non_existing_component"],
            regions=[["Region 1"]],
            metric=tavg,
        )

    # Test plotting bar chart with region selection, relabelling, and experiment filtering
    manager.plot_bar_chart(
        components=["component"],
        regions=[["Region 1", "Region 2"]],
        metric=tavg,
        region_relabel_map={"Region 1": "Total"},
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
    assert region_labels == ["Total", "Region 2"]

    # Verify metric
    assert mock_plot.call_args.args[2] == tavg

    # Verify show kwarg is passed through
    assert mock_plot.call_args.kwargs["show"] is True

    # Test experiment relabelling
    mock_plot.reset_mock()
    manager.plot_bar_chart(
        components=["component"],
        regions=[["Region 1"]],
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
        components=["component"],
        regions=[["Region 1"]],
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


def test_experiment_ncpus_parsed_once(layout_scaling_data):
    """Test that the number of CPUs of an experiment is only parsed once, however often it is needed."""

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    manager.select_best_experiments("component", "Region 1", tavg)
    manager.select_best_experiments("component", "Region 1", tavg)

    assert len(manager._parse_ncpus_calls) == 3  # One per experiment, not one per call


@mock.patch("access.profiling.manager.plot_scaling_metrics")
def test_scaling_data_with_best_experiments(mock_plot, layout_scaling_data):
    """Test that feeding select_best_experiments into plot_scaling_data drops the slower layouts."""

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    manager.plot_scaling_data(
        components=["component"],
        regions=[["Region 1"]],
        metric=tavg,
        experiments=manager.select_best_experiments("component", "Region 1", tavg),
    )

    component_data = mock_plot.call_args.args[0][0]
    assert component_data.coords["ncpus"].values.tolist() == [1, 2]  # No duplicated CPU counts
    assert component_data[tavg].sel(region="Region 1").values.tolist() == [800.0, 300.0]  # Fastest 2 CPU layout

    assert len(manager._parse_ncpus_calls) == 3  # Selecting and plotting does not parse an experiment twice


def test_scaling_data_duplicate_ncpus_raises_value_error(layout_scaling_data):
    """Test plot_scaling_data refuses to plot when several experiments share a number of CPUs."""

    paths, ncpus, datasets = layout_scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    with pytest.raises(ValueError, match="same number of CPUs") as exc_info:
        manager.plot_scaling_data(
            components=["component"],
            regions=[["Region 1"]],
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


def test_aggregate_runs_invalid_how(run_data):
    """An unsupported statistic is rejected."""

    with pytest.raises(ValueError, match="Unknown reduction"):
        run_data.aggregate_runs(how="bogus")


@mock.patch("access.profiling.manager.plot_bar_metrics")
def test_select_best_run_then_plot(mock_plot, run_data):
    """Once the runs are reduced, the plotting methods work unchanged."""

    run_data.select_best_run("component", "Region 1", tavg)
    run_data.plot_bar_chart(components=["component"], regions=[["Region 1"]], metric=tavg, show=False)

    assert mock_plot.call_args.args[0] == {"exp1": pytest.approx([200.0])}


def test_select_best_experiments_run_dim_raises_value_error(run_data):
    """select_best_experiments refuses unreduced data instead of failing obscurely."""

    with pytest.raises(ValueError, match="'run' dimension"):
        run_data.select_best_experiments("component", "Region 1", tavg)


def test_scaling_data_run_dim_raises_value_error(run_data):
    """plot_scaling_data refuses unreduced data instead of silently plotting it."""

    with pytest.raises(ValueError, match="'run' dimension"):
        run_data.plot_scaling_data(components=["component"], regions=[["Region 1"]], metric=tavg)


def test_bar_chart_run_dim_raises_value_error(run_data):
    """plot_bar_chart refuses unreduced data instead of failing obscurely."""

    with pytest.raises(ValueError, match="'run' dimension"):
        run_data.plot_bar_chart(components=["component"], regions=[["Region 1"]], metric=tavg)
