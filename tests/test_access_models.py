# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import mock

import pytest
from access.config import YAMLParser

from access.profiling.access_models import ESM16Profiling, RAM3Profiling
from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser


@mock.patch.object(YAMLParser, "parse", return_value={"UM_STDOUT_FILE": "file", "model": "file"})
@mock.patch.object(Path, "read_text", return_value="some text")
@mock.patch.object(Path, "is_file")
def test_esm16_config_profiling(mock_is_file, mock_yaml_parse, mock_path_read_text):
    """Test the ESM16ConfigProfiling class."""

    # Instantiate ESM16ConfigProfiling
    config_profiling = ESM16Profiling(Path("/fake/test_path"))

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


@mock.patch("access.profiling.access_models.Path.is_file")
@mock.patch("access.profiling.access_models.Path.read_text")
def test_ram3_config_profiling(mock_read_text, mock_is_file):
    """Test the rAM3Profiling class."""

    # Instantiate rAM3Profiling
    config_profiling = RAM3Profiling(Path("/fake/path"), layout_variable="um_layout")
    assert "UM_regions" in config_profiling.known_parsers, '"UM_regions" key not in known_parsers.'
    assert isinstance(config_profiling.known_parsers["UM_regions"], UMProfilingParser), (
        "UM_regions known_parser not UMProfilingParser type."
    )
    assert "UM_total" in config_profiling.known_parsers, '"UM_total" key not in known_parsers.'
    assert isinstance(config_profiling.known_parsers["UM_total"], UMTotalRuntimeParser), (
        "UM_total known parser not UMTotalRuntimeParser type."
    )

    # mock absence of rose-conf file
    mock_is_file.return_value = False
    with pytest.raises(FileNotFoundError):
        config_profiling.parse_ncpus(Path("/fake/path"))

    # mock absence of layout variable
    mock_is_file.return_value = True
    mock_read_text.return_value = "another_var=another_value"
    with pytest.raises(ValueError):
        config_profiling.parse_ncpus(Path("/fake/path"))

    # mock presence of layout variable
    mock_read_text.return_value += "\num_layout=2,3"
    config_profiling.parse_ncpus(Path("/fake/path"))
