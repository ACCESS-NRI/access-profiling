# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest

from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.parser import ProfilingParser


class MockCylcManager(CylcRoseManager):
    """Test class inheriting from CylcRoseManager to test its methods."""

    @property
    def known_parsers(self) -> dict[str, ProfilingParser]:
        return {"fake-parser": mock.MagicMock()}


@pytest.fixture()
def manager():
    return MockCylcManager(Path("/fake/test_path"), Path("/fake/archive_path"), layout_variable="um_layout")


@mock.patch("access.profiling.cylc_manager.Path.glob")
def test_parse_profiling_logs(mock_path_glob, manager):
    """Test the parse_profiling_logs method of CylcRoseManager with missing directories."""

    # no component log files
    mock_path_glob.return_value = []
    with pytest.raises(RuntimeError):
        manager.profiling_logs(Path("/fake/path"))
    mock_path_glob.assert_called_once()

    # component log files are present
    mock_path_glob.reset_mock()
    mock_path_glob.return_value = [Path("/fake/path/cycle1/task1/NN/job.out")]
    # return something "valid" for the cylc loc and db, but fail to read the component log.
    logs = manager.profiling_logs(Path("/fake/path"))
    mock_path_glob.assert_called_once()
    assert "cylc_suite_log" in logs
    assert isinstance(logs["cylc_suite_log"].parser, CylcProfilingParser)
    assert "cylc_tasks" in logs
    assert isinstance(logs["cylc_tasks"].parser, CylcDBReader)
    assert "task1_cyclecycle1_fake-parser" in logs
    assert isinstance(logs["task1_cyclecycle1_fake-parser"].parser, mock.MagicMock)


@mock.patch("access.profiling.access_models.Path.is_file")
@mock.patch("access.profiling.access_models.Path.read_text")
def test_parse_ncpus(mock_read_text, mock_is_file, manager):
    """Test the parse_ncpus method of CylcRoseManager."""

    # mock absence of rose-conf file
    mock_is_file.return_value = False
    with pytest.raises(FileNotFoundError):
        manager.parse_ncpus(Path("/fake/path"))

    # mock absence of layout variable
    mock_is_file.return_value = True
    mock_read_text.return_value = "another_var=another_value"
    with pytest.raises(ValueError):
        manager.parse_ncpus(Path("/fake/path"))

    # mock presence of layout variable
    mock_read_text.return_value += "\n um_layout = 2,3"
    manager.parse_ncpus(Path("/fake/path"))
