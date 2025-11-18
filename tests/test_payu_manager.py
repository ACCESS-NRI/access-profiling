# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest
import xarray as xr

from access.profiling.experiment import ProfilingLog
from access.profiling.manager import ProfilingManager
from access.profiling.payu_manager import PayuManager, ProfilingExperiment, ProfilingExperimentStatus


class MockPayuManager(PayuManager):
    """Test class inheriting from PayuConfigProfiling to test its methods."""

    def get_component_logs(self, path):
        return {"component": ProfilingLog(path, mock.MagicMock())}


@pytest.fixture(scope="function")
def manager():
    return MockPayuManager(Path("/fake/test_path"), Path("/fake/archive_path"))


def test_nruns(manager):
    """Test the nruns property of PayuManager."""
    # Default value
    assert manager.nruns == 1

    # Set valid value
    manager.nruns = 5
    assert manager.nruns == 5

    # Set invalid value
    with pytest.raises(ValueError):
        manager.nruns = 0


def test_startfrom_restart(manager):
    """Test the startfrom_restart property of PayuManager."""
    # Default value
    assert manager.startfrom_restart == "cold"

    # Set value
    manager.startfrom_restart = "restart000"
    assert manager.startfrom_restart == "restart000"


@mock.patch("access.profiling.payu_manager.YAMLParser")
@mock.patch("access.profiling.payu_manager.Path.read_text", return_value="mock config content")
def test_ncpus(mock_read_text, mock_yaml_parser, manager):
    """Test the parse_ncpus method of PayuManager."""

    # Mock the YAMLParser to return the number of cpus
    mock_yaml_parser().parse.return_value = {"ncpus": 4}
    ncpus = manager.parse_ncpus(Path("/fake/path"))
    assert mock_read_text.call_count == 1
    assert ncpus == 4

    # Mock the YAMLParser to return dictionary of submodels
    mock_yaml_parser().parse.return_value = {"submodels": [{"ncpus": 2}, {"ncpus": 3}]}
    ncpus = manager.parse_ncpus(Path("/fake/path"))
    assert mock_read_text.call_count == 2
    assert ncpus == 5


def test_generate_experiments(manager):
    """Test the generate_experiments method of PayuManager."""
    branches = ["branch1", "branch2"]

    manager.generate_experiments(branches)
    for branch in branches:
        assert branch in manager.experiments
        assert isinstance(manager.experiments[branch], ProfilingExperiment)

    manager.generate_experiments([branches[0]])
    for branch in branches:
        assert branch in manager.experiments
        assert isinstance(manager.experiments[branch], ProfilingExperiment)


@mock.patch("access.profiling.payu_manager.ExperimentRunner")
def test_run_experiments(mock_experiment_runner, manager):
    """Test the run_experiments method of PayuManager."""

    with mock.patch.dict(
        manager.experiments,
        {
            "branch1": mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch1")),
            "branch2": mock.MagicMock(status=ProfilingExperimentStatus.NEW, path=Path("branch2")),
            "branch3": mock.MagicMock(status=ProfilingExperimentStatus.DONE, path=Path("branch3")),
        },
    ):
        manager.run_experiments()
        expected_call = {
            "test_path": Path("/fake/test_path"),
            "repository_directory": "config",
            "running_branches": ["branch1", "branch2"],
            "keep_uuid": True,
            "nruns": [1, 1],
            "startfrom_restart": ["cold", "cold"],
        }
        mock_experiment_runner.assert_called_once_with(expected_call)

    # Rerun again with no NEW experiments
    with mock.patch.dict(
        manager.experiments,
        {
            "branch1": mock.MagicMock(status=ProfilingExperimentStatus.DONE, path=Path("branch1")),
            "branch2": mock.MagicMock(status=ProfilingExperimentStatus.DONE, path=Path("branch2")),
            "branch3": mock.MagicMock(status=ProfilingExperimentStatus.RUNNING, path=Path("branch3")),
        },
    ):
        mock_experiment_runner.reset_mock()
        manager.run_experiments()
        mock_experiment_runner.assert_not_called()


@mock.patch.object(ProfilingManager, "archive_experiments")
def test_archive_experiments(mock_archive, manager):
    """Test the archive_experiments method of PayuManager.

    The only thing to test here is that the correct exclude files and dirs are passed to the parent method.
    """

    # No arguments passed
    manager.archive_experiments()
    assert mock_archive.call_count == 1
    mock_archive.assert_called_with(
        exclude_dirs=[".git", "restart*"], exclude_files=["*.nc"], follow_symlinks=True, overwrite=False
    )
    mock_archive.reset_mock()

    # Custom arguments passed
    manager.archive_experiments(exclude_dirs=["dir1"], exclude_files=["file1"])
    assert mock_archive.call_count == 1
    mock_archive.assert_called_with(
        exclude_dirs=["dir1"], exclude_files=["file1"], follow_symlinks=True, overwrite=False
    )


@mock.patch("access.profiling.payu_manager.Path.is_dir")
@mock.patch("access.profiling.payu_manager.Path.glob")
def test_parse_profiling_data_missing_directories(mock_glob, mock_is_dir, manager):
    """Test the parse_profiling_data method of PayuManager with missing directories."""

    # Missing archive directory
    mock_is_dir.return_value = False
    with pytest.raises(FileNotFoundError):
        manager.parse_profiling_data(Path("/fake/path"))
    mock_is_dir.assert_called_once()

    # Missing output directories
    mock_is_dir.return_value = True
    mock_glob.return_value = []
    with pytest.raises(FileNotFoundError):
        manager.parse_profiling_data(Path("/fake/path"))
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
def test_parse_profiling_data(mock_parse, mock_glob, mock_is_dir, manager):
    """Test the parse_profiling_data method of PayuManager."""

    with mock.patch.object(manager, "get_component_logs", wraps=manager.get_component_logs) as mock_get_logs:
        datasets = manager.parse_profiling_data(Path("/fake/path"))

        # Check correct path access
        assert mock_is_dir.call_count == 1  # Called to check archive directory
        assert mock_glob.call_count == 2  # Called for payu_jobs and output directories
        assert mock_parse.call_count == 2  # Called for each log file
        assert mock_get_logs.call_count == 1
        mock_get_logs.assert_called_with(Path("output1"))

        # Check returned datasets
        assert "payu" in datasets
        assert isinstance(datasets["payu"], xr.Dataset)
        assert "component" in datasets
        assert isinstance(datasets["component"], xr.Dataset)
