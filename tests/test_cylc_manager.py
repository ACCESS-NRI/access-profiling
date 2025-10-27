# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest
import xarray as xr

from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.metrics import tmax
from access.profiling.parser import ProfilingParser


class MockCylcManager(CylcRoseManager):
    """Test class inheriting from CylcRoseManager to test its methods."""

    def parse_ncpus(self, path):
        return 4

    @property
    def known_parsers(self) -> dict[str, ProfilingParser]:
        return {"fake-parser": mock.MagicMock()}


@pytest.fixture()
def manager():
    return MockCylcManager(Path("/fake/test_path"))


@mock.patch("access.profiling.cylc_manager.ProfilingLog.parse")
@mock.patch("access.profiling.cylc_manager.Path.glob")
@mock.patch("access.profiling.cylc_manager.Path.is_file")
def test_parse_profiling_data_missing_files(mock_is_file, mock_path_glob, mock_parse, manager):
    """Test the parse_profiling_data method of PayuManager with missing directories."""

    # no log files
    mock_path_glob.return_value = []
    with pytest.raises(RuntimeError):
        manager.parse_profiling_data(Path("/fake/path"))
    assert mock_parse.call_count == 2
    mock_path_glob.assert_called_once()

    # a log is present, but with no data
    mock_path_glob.return_value = [Path("/fake/path/cycle1/task1/NN/job.out")]
    # return something "valid" for the cylc loc and db, but fail to read the component log.
    mock_parse.side_effect = [xr.Dataset(), xr.Dataset(), ValueError()]
    with pytest.raises(RuntimeError):
        manager.parse_profiling_data(Path("/fake/path"))
    assert mock_parse.call_count == 5
    assert mock_path_glob.call_count == 2


@mock.patch("access.profiling.cylc_manager.Path.glob", return_value=[Path("/fake/path/cycle1/task1/NN/job.out")])
@mock.patch("access.profiling.cylc_manager.ProfilingLog.parse", return_value=xr.Dataset())
def test_parse_profiling_data(mock_parse, mock_glob, manager):
    """Test the parse_profiling_data method of PayuManager."""

    manager.known_parsers["fake-parser"].read.return_value = {"regions": ["aregion"], tmax: 1}
    datasets = manager.parse_profiling_data(Path("/fake/path"))

    # Check correct path access
    assert mock_parse.call_count == 3  # Called for each log file
    mock_glob.assert_called_once()

    # Check returned datasets
    assert "cylc_suite_log" in datasets
    assert isinstance(datasets["cylc_suite_log"], xr.Dataset)
    assert "cylc_tasks" in datasets
    assert isinstance(datasets["cylc_tasks"], xr.Dataset)
    assert "task1_cyclecycle1_fake-parser" in datasets
    assert isinstance(datasets["task1_cyclecycle1_fake-parser"], xr.Dataset)
