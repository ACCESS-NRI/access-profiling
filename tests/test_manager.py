# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path
from unittest import mock

import pytest
import xarray as xr

from access.profiling.manager import ProfilingExperiment, ProfilingExperimentStatus, ProfilingManager
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
            self.experiments[path.name] = ProfilingExperiment(path)
            self.experiments[path.name].status = ProfilingExperimentStatus.DONE

        if ncpus is not None:
            self._mock_ncpus = dict(zip([path.name for path in paths], ncpus, strict=True))
        else:
            self._mock_ncpus = {}

        if datasets is not None:
            self.data = dict(zip([path.name for path in paths], datasets, strict=True))

    def parse_ncpus(self, path):
        """Simulate parsing number of CPUs for a given path."""
        return self._mock_ncpus[path.name]

    def profiling_logs(self, path):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Simulate parsing profiling data for a given path."""
        pass


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
    mock_experiment.assert_any_call(Path("/fake/archive_dir/exp1.tar.gz"))
    mock_experiment.assert_any_call(Path("/fake/archive_dir/exp2.tar.gz"))


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

    with mock.patch.object(manager, "profiling_logs") as mock_profiling_logs:
        # Setup mock profiling logs
        mock_log = mock.MagicMock()
        type(mock_log).optional = mock.PropertyMock(side_effect=[False, False, True])
        mock_log.parse.side_effect = (xr.Dataset(), xr.Dataset(), FileNotFoundError("Mocked missing file."))
        mock_profiling_logs.return_value = {
            "log": mock_log,
            "optional_log": mock_log,
            "missing_log": mock_log,
        }

        # Parse profiling data for each experiment
        manager.parse_profiling_data()
        assert "log" in manager.data[exp_name], "Parsed datasets should contain 'log' key."
        assert "optional_log" in manager.data[exp_name], "Parsed datasets should contain 'optional_log' key."
        assert "missing_log" not in manager.data[exp_name], (
            "Parsed datasets should not contain 'missing_log' key as the file is missing."
        )
        assert mock_log.parse.call_count == 3, "Parse method should be called three times."

    manager.experiments[exp_name].status = ProfilingExperimentStatus.RUNNING
    with caplog.at_level(logging.WARNING):
        manager.parse_profiling_data()
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert "is not completed" in caplog.records[0].message


@mock.patch("access.profiling.manager.plot_scaling_metrics")
def test_scaling_data(mock_plot, scaling_data):
    """Test the parse_scaling_data and plot_scaling_data methods of ProfilingManager.

    This test will check that datasets are correctly concatenated across different numbers of CPUs
    and that the plotting function is called correctly.
    """
    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

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
