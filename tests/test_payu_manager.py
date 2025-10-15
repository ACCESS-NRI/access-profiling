# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest
import xarray as xr

from access.profiling.manager import ProfilingLog
from access.profiling.payu_manager import PayuManager


class MockPayuManager(PayuManager):
    """Test class inheriting from PayuConfigProfiling to test its methods."""

    def get_component_logs(self, path):
        return {"component": ProfilingLog(path, mock.MagicMock())}


@mock.patch("access.profiling.payu_manager.YAMLParser")
@mock.patch("access.profiling.payu_manager.Path.read_text", return_value="mock config content")
def test_ncpus(mock_read_text, mock_yaml_parser):
    """Test the parse_ncpus method of PayuManager."""
    config_profiling = MockPayuManager()

    # Mock the YAMLParser to return the number of cpus
    mock_yaml_parser().parse.return_value = {"ncpus": 4}
    ncpus = config_profiling.parse_ncpus(Path("/fake/path"))
    assert mock_read_text.call_count == 1
    assert ncpus == 4

    # Mock the YAMLParser to return dictionary of submodels
    mock_yaml_parser().parse.return_value = {"submodels": [{"ncpus": 2}, {"ncpus": 3}]}
    ncpus = config_profiling.parse_ncpus(Path("/fake/path"))
    assert mock_read_text.call_count == 2
    assert ncpus == 5


@mock.patch("access.profiling.payu_manager.Path.is_dir")
@mock.patch("access.profiling.payu_manager.Path.glob")
def test_parse_profiling_data_missing_directories(mock_glob, mock_is_dir):
    """Test the parse_profiling_data method of PayuManager with missing directories."""

    config_profiling = MockPayuManager()

    # Missing archive directory
    mock_is_dir.return_value = False
    with pytest.raises(FileNotFoundError):
        config_profiling.parse_profiling_data(Path("/fake/path"))
    mock_is_dir.assert_called_once()

    # Missing output directories
    mock_is_dir.return_value = True
    mock_glob.return_value = []
    with pytest.raises(FileNotFoundError):
        config_profiling.parse_profiling_data(Path("/fake/path"))
    mock_glob.assert_called_with("output*")


def path_glob_side_effect(pattern):
    """Side effect function for Path.glob to simulate different directory contents."""

    if pattern == "payu_jobs/*/run/*.json":
        return [Path("payu_jobs/job1/run/log1.json"), Path("payu_jobs/job2/run/log2.json")]
    elif pattern == "output*":
        return [Path("output1"), Path("output2")]
    else:
        return []


@mock.patch.object(Path, "is_dir", return_value=True)
@mock.patch.object(Path, "glob", side_effect=path_glob_side_effect)
@mock.patch("access.profiling.payu_manager.ProfilingLog.parse", return_value=xr.Dataset())
def test_parse_profiling_data(mock_parse, mock_glob, mock_is_dir):
    """Test the parse_profiling_data method of PayuManager."""

    config_profiling = MockPayuManager()
    datasets = config_profiling.parse_profiling_data(Path("/fake/path"))

    # Check correct path access
    assert mock_is_dir.call_count == 1  # Called to check archive directory
    assert mock_glob.call_count == 2  # Called for payu_jobs and output directories
    assert mock_parse.call_count == 2  # Called for each log file

    # Check returned datasets
    assert "payu" in datasets
    assert isinstance(datasets["payu"], xr.Dataset)
    assert "component" in datasets
    assert isinstance(datasets["component"], xr.Dataset)
