# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import pytest

from access.profiling.metrics import count, tmax, tmin
from access.profiling.parser import ProfilingParser, _convert_from_string, _read_text_file


class MockProfilingParser(ProfilingParser):
    """A Mock concrete Profiling Parser."""

    def __init__(self, data: dict):
        self._data = data

    @property
    def metrics(self) -> list:
        return [count, tmin, tmax]

    def parse(self, file_path: str | Path | os.PathLike) -> dict:
        return self._data[_read_text_file(file_path)]


@pytest.fixture(scope="module")
def profiling_data():
    """Fixture instantiating fake parsed profiling data."""
    return {
        "1cpu_stream": {
            "region": ["Region 1", "Region 2", "Region 3"],
            count: [1, 2, 3],
            tmin: [1.0, 2.0, 3.0],
            tmax: [4.0, 5.0, 6.0],
        },
        "2cpu_stream": {
            "region": ["Region 1", "Region 2", "Region 3"],
            count: [3, 4, 5],
            tmin: [2.0, 3.0, 4.0],
            tmax: [5.0, 6.0, 7.0],
        },
    }


@pytest.fixture(scope="module")
def parser(profiling_data):
    """Fixture instantiating the Mock parser."""
    return MockProfilingParser(profiling_data)


def test_base_parser(tmp_path, profiling_data, parser):
    """Tests methods and properties of abstract base class, ProfilingParser."""
    assert parser.metrics == [count, tmin, tmax], "Incorrect metrics returned in MockProfilingParser!"
    for stream in ("1cpu_stream", "2cpu_stream"):
        log_file = tmp_path / "1cpu.log"
        log_file.write_text(stream)
        assert parser.parse(log_file) == profiling_data[stream], f'Incorrect profiling stats returned for "{stream}"'
        log_file.unlink()


def test_str2num():
    """Tests conversion of numbers to most appropriate type."""
    str2int = _convert_from_string("42")
    assert type(str2int) is int
    assert str2int == 42
    str2float = _convert_from_string("-1.23")
    assert type(str2float) is float
    assert str2float == -1.23
    str2float = _convert_from_string("0.00000")
    assert str2float == 0.0
    str2str = _convert_from_string("somestr")
    assert type(str2str) is str
    assert str2str == "somestr"


def test_read_text_file(tmp_path):
    """Tests _read_text_file exceptions."""
    with pytest.raises(TypeError):
        _read_text_file(1)
    bytes_file = tmp_path / "bytes"
    bytes_file.write_bytes(bytes(range(256)))
    with pytest.raises(ValueError):
        _read_text_file(bytes_file)
    with pytest.raises(FileNotFoundError):
        _read_text_file(tmp_path / "nonexistent.log")
