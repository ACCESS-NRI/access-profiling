# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import pytest
import xarray as xr

from access.profiling.metrics import count, tmax, tmin
from access.profiling.parser import ProfilingParser, _convert_from_string, _read_text_file, aggregate_pe_data


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


@pytest.fixture(scope="module")
def per_pe_dataset():
    """Dataset with a 'pe' dimension for testing aggregate_pe_data."""
    # 2 regions, 4 PEs; tmin values deliberately unequal to test imbalance
    return xr.Dataset(
        {
            tmin: xr.DataArray([[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]], dims=["region", "pe"]),
            tmax: xr.DataArray([[2.0, 4.0, 6.0, 8.0], [1.0, 3.0, 5.0, 7.0]], dims=["region", "pe"]),
        },
        coords={"region": ["Region 1", "Region 2"], "pe": [0, 1, 2, 3]},
    )


def test_aggregate_pe_data_values(per_pe_dataset):
    """Tests that all nine derived variables are computed correctly."""
    result = aggregate_pe_data(per_pe_dataset)

    base = str(tmin).replace(" ", "_")

    # Region 1 tmin values: [1, 2, 3, 4]
    assert result[f"{base}_min_pe"].sel(region="Region 1").item() == pytest.approx(1.0)
    assert result[f"{base}_max_pe"].sel(region="Region 1").item() == pytest.approx(4.0)
    assert result[f"{base}_mean_pe"].sel(region="Region 1").item() == pytest.approx(2.5)
    assert result[f"{base}_median_pe"].sel(region="Region 1").item() == pytest.approx(2.5)
    assert result[f"{base}_std_pe"].sel(region="Region 1").item() == pytest.approx(
        pytest.approx((sum((x - 2.5) ** 2 for x in [1, 2, 3, 4]) / 4) ** 0.5, rel=1e-5)
    )
    assert result[f"{base}_total_pe"].sel(region="Region 1").item() == pytest.approx(10.0)
    assert result[f"{base}_argmin_pe"].sel(region="Region 1").item() == 0  # PE 0 has min
    assert result[f"{base}_argmax_pe"].sel(region="Region 1").item() == 3  # PE 3 has max
    # imbalance = (4 - 1) / 2.5 = 1.2
    assert result[f"{base}_imbalance_pe"].sel(region="Region 1").item() == pytest.approx(1.2)

    # 'pe' dimension should be gone; 'region' coordinate preserved
    assert "pe" not in result.dims
    assert "region" in result.coords


def test_aggregate_pe_data_no_pe_dim():
    """Tests that ValueError is raised when the dataset has no 'pe' dimension."""
    ds = xr.Dataset({tmin: xr.DataArray([1.0, 2.0], dims=["region"])}, coords={"region": ["r1", "r2"]})
    with pytest.raises(ValueError, match="'pe' dimension"):
        aggregate_pe_data(ds)


def test_aggregate_pe_data_perfect_balance():
    """Tests that imbalance is 0 when all PEs have equal values."""
    ds = xr.Dataset(
        {tmin: xr.DataArray([[5.0, 5.0, 5.0], [3.0, 3.0, 3.0]], dims=["region", "pe"])},
        coords={"region": ["r1", "r2"], "pe": [0, 1, 2]},
    )
    result = aggregate_pe_data(ds)
    base = str(tmin).replace(" ", "_")
    assert result[f"{base}_imbalance_pe"].sel(region="r1").item() == pytest.approx(0.0)
    assert result[f"{base}_imbalance_pe"].sel(region="r2").item() == pytest.approx(0.0)
