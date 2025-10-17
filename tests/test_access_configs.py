# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

from access.profiling.access_configs import ESM16ConfigProfiling
from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.um_parser import UMProfilingParser


@mock.patch.object(Path, "is_file")
def test_esm16_config_profiling(mock_is_file):
    """Test the ESM16ConfigProfiling class."""

    # Instantiate ESM16ConfigProfiling
    config_profiling = ESM16ConfigProfiling()

    # Mock the presence of all log files
    mock_is_file.side_effect = [True, True, True]
    logs = config_profiling.get_component_logs(Path("/fake/path"))
    assert "UM" in logs
    assert "MOM5" in logs
    assert "CICE5" in logs
    assert isinstance(logs["UM"].parser, UMProfilingParser)
    assert isinstance(logs["MOM5"].parser, FMSProfilingParser)
    assert isinstance(logs["CICE5"].parser, CICE5ProfilingParser)

    # Mock the absence of UM log file
    mock_is_file.side_effect = [False, True, True]
    logs = config_profiling.get_component_logs(Path("/fake/path"))
    assert "UM" not in logs
    assert "MOM5" in logs
    assert "CICE5" in logs

    # Mock the absence of MOM5 log file
    mock_is_file.side_effect = [True, False, True]
    logs = config_profiling.get_component_logs(Path("/fake/path"))
    assert "UM" in logs
    assert "MOM5" not in logs
    assert "CICE5" in logs

    # Mock the absence of CICE5 log file
    mock_is_file.side_effect = [True, True, False]
    logs = config_profiling.get_component_logs(Path("/fake/path"))
    assert "UM" in logs
    assert "MOM5" in logs
    assert "CICE5" not in logs
