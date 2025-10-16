# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access.profiling.metrics import count, tmax, tmin
from access.profiling.parser import ProfilingParser, _convert_from_string


class MockProfilingParser(ProfilingParser):
    """A Mock concrete Profiling Parser."""

    def __init__(self, data: dict):
        self._data = data

    @property
    def metrics(self) -> list:
        return [count, tmin, tmax]

    def read(self, stream: str) -> dict:
        return self._data[stream]


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


def test_base_parser(profiling_data, parser):
    """Tests methods and properties of abstract base class, ProfilingParser."""
    assert parser.metrics == [count, tmin, tmax], "Incorrect metrics returned in MockProfilingParser!"
    for stream in ("1cpu_stream", "2cpu_stream"):
        assert parser.read(stream) == profiling_data[stream], f'Incorrect profiling stats returned for "{stream}"'


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
