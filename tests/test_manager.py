# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

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
        datasets: list[xr.Dataset] | None = None,
    ):
        super().__init__(Path("/fake/work_dir"), Path("/fake/archive_dir"))

        if ncpus is not None:
            self._mock_ncpus = dict(zip([path.name for path in paths], ncpus, strict=True))
        else:
            self._mock_ncpus = {}
        if datasets is not None:
            self._mock_datasets = dict(zip([path.name for path in paths], datasets, strict=True))
        else:
            self._mock_datasets = {}

        # Pre-generate experiments
        for path in paths:
            self.experiments[path.name] = ProfilingExperiment(path)
            self.experiments[path.name].status = ProfilingExperimentStatus.DONE

    def parse_ncpus(self, path):
        """Simulate parsing number of CPUs for a given path."""
        return self._mock_ncpus[path.name]

    def parse_profiling_data(self, path):
        """Simulate parsing profiling data for a given path."""
        return {"component": self._mock_datasets[path.name]}


def test_repr():
    """Test the __repr__ method of ProfilingManager."""

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
    paths = [Path("1cpu"), Path("2cpu"), Path("4cpu")]
    ncpus = [1, 4, 2]  # Intentionally unordered to test sorting in the manager
    datasets = []
    for n in ncpus:
        regions = ["Region 1", "Region 2"]
        count_array = xr.DataArray([1, 2], dims=["region"]).pint.quantify(count.units)
        tavg_array = xr.DataArray([value / min(n, 2) for value in [600365, 2.345388]], dims=["region"]).pint.quantify(
            tavg.units
        )
        datasets.append(xr.Dataset(data_vars={count: count_array, tavg: tavg_array}, coords={"region": regions}))

    return paths, ncpus, datasets


@mock.patch("access.profiling.manager.plot_scaling_metrics")
def test_scaling_data(mock_plot, scaling_data):
    """Test the parse_scaling_data and plot_scaling_data methods of ProfilingManager.

    This test will check that datasets are correctly concatenated across different numbers of CPUs
    and that the plotting function is called correctly.
    """
    paths, ncpus, datasets = scaling_data
    manager = MockProfilingManager(paths, ncpus, datasets)

    manager.parse_scaling_data()

    assert set(manager.data.keys()) == {"component"}
    assert set(manager.data["component"].dims) == {"ncpus", "region"}, (
        "Dataset should have dimensions 'ncpus' and 'region'!"
    )
    assert manager.data["component"].sizes["ncpus"] == 3, "Dataset should have 2 values for 'ncpus'!"
    assert manager.data["component"].sizes["region"] == 2, "Dataset should have 3 values for 'region'!"

    assert manager.data["component"]["ncpus"].values.tolist() == sorted(ncpus), (
        "Dataset should have correct 'ncpus' coordinate values (in sorted order)!"
    )
    assert manager.data["component"]["region"].values.tolist() == ["Region 1", "Region 2"], (
        "Dataset should have correct 'region' coordinate values!"
    )

    assert set(manager.data["component"].data_vars) == {count, tavg}, "Dataset should have data_vars for each metric!"
    assert all(manager.data["component"][metric].shape == (3, 2) for metric in (count, tavg)), (
        "Dataset data vars should have shape (3, 2)!"
    )
    assert all(manager.data["component"][metric].data.units == metric.units for metric in (count, tavg)), (
        "Dataset data_vars should have correct units!"
    )
    assert all(manager.data["component"][count].sel(ncpus=1) == datasets[0][count]), (
        "Dataset data_vars should have correct values for ncpus=1!"
    )
    for i, n in enumerate(ncpus):
        for metric in (count, tavg):
            assert all(manager.data["component"][metric].sel(ncpus=n) == datasets[i][metric]), (
                f"Dataset data_vars for {metric} should have correct values for ncpus={n}!"
            )

    manager.plot_scaling_data(components=["component"], regions=[["Region 1", "Region 2"]], metric=tavg)
    mock_plot.assert_called_once_with(
        [manager.data["component"]],
        [["Region 1", "Region 2"]],
        tavg,
        region_relabel_map=None,
    )

    # Also test that __repr__ returns info about the dataset
    result = repr(manager)
    assert "Data:\n        'component':" in result
    assert "<xarray.Dataset>" in result
    assert "Dimensions:" in result
    assert "Coordinates:" in result
    assert "Data variables:" in result
