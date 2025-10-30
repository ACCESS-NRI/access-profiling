# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

from access.profiling.experiment import ProfilingExperiment, ProfilingExperimentStatus, ProfilingLog
from access.profiling.metrics import tavg, tmax


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
