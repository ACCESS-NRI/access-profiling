# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest
import xarray as xr

from access.profiling.manager import ProfilingExperiment, ProfilingExperimentStatus, ProfilingLog, ProfilingManager
from access.profiling.metrics import count, tavg, tmax


def test_profiling_log():
    """Test the ProfilingLog class."""

    # Mock parser and path
    mock_parser = mock.MagicMock(autospec=True)
    mock_parser.metrics = [tavg, tmax]
    mock_parser.parse.return_value = {
        "region": ["Region 1", "Region 2"],
        tavg: [1.0, 2.0],
        tmax: [3.0, 4.0],
    }

    mock_path = mock.MagicMock()

    # Instantiate ProfilingLog and parse
    profiling_log = ProfilingLog(filepath=mock_path, parser=mock_parser)
    dataset = profiling_log.parse()

    # Check dataset contents
    assert set(dataset.dims) == {"region"}
    assert set(dataset.data_vars) == {tavg, tmax}
    assert list(dataset["region"].values) == ["Region 1", "Region 2"]
    assert list(dataset[tavg].values) == [1.0, 2.0]
    assert list(dataset[tmax].values) == [3.0, 4.0]

    # Check parser and path calls
    mock_parser.parse.assert_called_once_with(mock_path)


def test_profiling_experiment():
    """Test the ProfilingExperiment class."""

    experiment = ProfilingExperiment(path=Path("/fake/path"))

    assert experiment.path == Path("/fake/path")
    assert experiment.status == ProfilingExperimentStatus.NEW

    experiment.status = ProfilingExperimentStatus.RUNNING
    assert experiment.status == ProfilingExperimentStatus.RUNNING


class MockProfilingManager(ProfilingManager):
    """Test class inheriting from ProfilingManager to test its methods.

    This class will simulate parsing of some profiling data.

    Args:
        paths (list[Path]): List of paths to simulate different configurations.
        ncpus (list[int]): List of number of CPUs corresponding to each path.
        datasets (list[xr.Dataset]): List of datasets to return for each path.
    """

    def __init__(self, paths, ncpus, datasets) -> None:
        super().__init__(Path("/fake/work_dir"))

        self._mock_ncpus = dict(zip([path.name for path in paths], ncpus, strict=True))
        self._mock_datasets = dict(zip([path.name for path in paths], datasets, strict=True))

        # Pre-generate experiments
        for path in paths:
            self.experiments[path] = ProfilingExperiment(path)
            self.experiments[path].status = ProfilingExperimentStatus.DONE

    def parse_ncpus(self, path):
        """Simulate parsing number of CPUs for a given path."""
        return self._mock_ncpus[path.name]

    def parse_profiling_data(self, path):
        """Simulate parsing profiling data for a given path."""
        return {"component": self._mock_datasets[path.name]}


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
