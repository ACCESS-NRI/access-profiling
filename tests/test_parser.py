# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from pint import Unit

from access.profiling.metrics import count, tavg, tmax, tmin
from access.profiling.parser import (
    ProfilingParser,
    _convert_from_string,
    _read_text_file,
    aggregate_pe_data,
    flatten_hierarchical,
    pe_statistic,
)


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
        # Passing something that is not a path is the point of the test.
        _read_text_file(1)  # pyright: ignore[reportArgumentType]
    bytes_file = tmp_path / "bytes"
    bytes_file.write_bytes(bytes(range(256)))
    with pytest.raises(ValueError):
        _read_text_file(bytes_file)
    with pytest.raises(FileNotFoundError):
        _read_text_file(tmp_path / "nonexistent.log")


@pytest.fixture(scope="module")
def per_pe_dataset():
    """Dataset with a 'pe' dimension for testing aggregate_pe_data, with units as ProfilingLog.parse gives it.

    Its PEs are numbered from 4, as those of a component that does not run on the first PEs of a job are, so that
    the PE a value was recorded on cannot be mistaken for its position.
    """
    # 2 regions, 4 PEs; tmin values deliberately unequal to test imbalance
    return xr.Dataset(
        {
            tmin: xr.DataArray([[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]], dims=["region", "pe"]).pint.quantify(
                tmin.units
            ),
            tmax: xr.DataArray([[2.0, 4.0, 6.0, 8.0], [1.0, 3.0, 5.0, 7.0]], dims=["region", "pe"]).pint.quantify(
                tmax.units
            ),
        },
        coords={"region": ["Region 1", "Region 2"], "pe": [4, 5, 6, 7]},
    )


def magnitude(result, metric, stat, region):
    """The value of a statistic of a metric across PEs for one region, without its units."""
    return result[pe_statistic(metric, stat)].sel(region=region).pint.magnitude.item()


def test_aggregate_pe_data_values(per_pe_dataset):
    """Tests that all nine derived variables are computed correctly."""
    result = aggregate_pe_data(per_pe_dataset)

    expected_std = (sum((x - 2.5) ** 2 for x in [1, 2, 3, 4]) / 4) ** 0.5

    # Region 1 tmin values: [1, 2, 3, 4], on PEs 4 to 7
    assert magnitude(result, tmin, "min", "Region 1") == pytest.approx(1.0)
    assert magnitude(result, tmin, "max", "Region 1") == pytest.approx(4.0)
    assert magnitude(result, tmin, "mean", "Region 1") == pytest.approx(2.5)
    assert magnitude(result, tmin, "median", "Region 1") == pytest.approx(2.5)
    assert magnitude(result, tmin, "std", "Region 1") == pytest.approx(expected_std, rel=1e-5)
    assert magnitude(result, tmin, "total", "Region 1") == pytest.approx(10.0)
    assert magnitude(result, tmin, "argmin", "Region 1") == 4  # PE 4 has min
    assert magnitude(result, tmin, "argmax", "Region 1") == 7  # PE 7 has max
    # imbalance = (4 - 1) / 2.5 = 1.2
    assert magnitude(result, tmin, "imbalance", "Region 1") == pytest.approx(1.2)
    # Region 2 tmax values: [1, 3, 5, 7]
    assert magnitude(result, tmax, "argmax", "Region 2") == 7

    # 'pe' dimension should be gone; 'region' coordinate preserved
    assert "pe" not in result.dims
    assert "region" in result.coords


def test_aggregate_pe_data_is_keyed_by_metrics_with_units(per_pe_dataset):
    """The results are metrics like any others, so that they can be plotted, with the units of what they are."""
    result = aggregate_pe_data(per_pe_dataset)

    assert len(result.data_vars) == 2 * 9
    for metric in result.data_vars:
        assert result[metric].pint.units == metric.units
    assert result[pe_statistic(tmin, "max")].pint.units == tmin.units
    assert result[pe_statistic(tmin, "argmax")].pint.units == Unit("dimensionless")
    assert result[pe_statistic(tmin, "imbalance")].pint.units == Unit("dimensionless")


def test_aggregate_pe_data_without_units():
    """Values without units are taken to be in the units of their metrics, as ProfilingLog.parse takes them."""
    ds = xr.Dataset({tmin: xr.DataArray([[1.0, 3.0]], dims=["region", "pe"])}, coords={"region": ["r1"], "pe": [0, 1]})
    result = aggregate_pe_data(ds)

    assert result[pe_statistic(tmin, "max")].pint.units == tmin.units
    assert magnitude(result, tmin, "max", "r1") == pytest.approx(3.0)


def test_pe_statistic_is_one_metric_per_statistic():
    """Metrics are compared by identity, so asking twice has to give the same metric to look the result up with."""
    tmin_max = pe_statistic(tmin, "max")

    assert pe_statistic(tmin, "max") is tmin_max
    assert pe_statistic(tmin, stat="max") is tmin_max
    assert pe_statistic(tmax, "max") is not tmin_max
    assert pe_statistic(tmin, "min") is not tmin_max
    assert str(tmin_max) == "minimum_time_max_pe"
    assert tmin_max.description == "Maximum across PEs of minimum time"


def test_pe_statistic_unknown():
    with pytest.raises(ValueError, match="Unknown statistic across PEs 'p90'"):
        pe_statistic(tmin, "p90")


def test_aggregate_pe_data_no_pe_dim():
    """Tests that ValueError is raised when the dataset has no 'pe' dimension."""
    ds = xr.Dataset({tmin: xr.DataArray([1.0, 2.0], dims=["region"])}, coords={"region": ["r1", "r2"]})
    with pytest.raises(ValueError, match="'pe' dimension"):
        aggregate_pe_data(ds)


def test_aggregate_pe_data_a_variable_that_is_not_a_metric():
    ds = xr.Dataset({"tmin": xr.DataArray([[1.0, 2.0]], dims=["region", "pe"])}, coords={"pe": [0, 1]})
    with pytest.raises(TypeError, match="not keyed by a ProfilingMetric"):
        aggregate_pe_data(ds)


def test_aggregate_pe_data_perfect_balance():
    """Tests that imbalance is 0 when all PEs have equal values, and where they are all 0."""
    ds = xr.Dataset(
        {tmin: xr.DataArray([[5.0, 5.0, 5.0], [0.0, 0.0, 0.0]], dims=["region", "pe"])},
        coords={"region": ["r1", "r2"], "pe": [0, 1, 2]},
    )
    result = aggregate_pe_data(ds)
    assert magnitude(result, tmin, "imbalance", "r1") == pytest.approx(0.0)
    assert magnitude(result, tmin, "imbalance", "r2") == pytest.approx(0.0)


def test_aggregate_pe_data_leaves_out_pes_without_a_value():
    ds = xr.Dataset(
        {tmin: xr.DataArray([[1.0, np.nan, 3.0]], dims=["region", "pe"])},
        coords={"region": ["r1"], "pe": [10, 11, 12]},
    )
    result = aggregate_pe_data(ds)

    assert magnitude(result, tmin, "min", "r1") == pytest.approx(1.0)
    assert magnitude(result, tmin, "mean", "r1") == pytest.approx(2.0)
    assert magnitude(result, tmin, "argmax", "r1") == 12


def test_aggregate_pe_data_keeps_the_coordinates_not_along_pe():
    ds = xr.Dataset(
        {tmin: xr.DataArray([[1.0, 2.0]], dims=["region", "pe"])},
        coords={"region": ["r1"], "pe": [0, 1], "host": ("pe", ["gadi-cpu-1", "gadi-cpu-2"]), "ncpus": 96},
    )
    result = aggregate_pe_data(ds)

    assert "host" not in result.coords
    assert result.coords["ncpus"].item() == 96


def test_flatten_hierarchical_skips_a_top_level_measurement():
    """Top-level keys are region names; a metric there would be a measurement belonging to no region.

    The hierarchical format has no way to express that, so it is skipped rather than flattened into a
    region with no name.
    """
    data = {
        tavg: 1.0,  # not a region, so not a subtree
        "[ESMF]": {tavg: 2.0},
    }

    flattened = flatten_hierarchical(data, [tavg])

    assert flattened["region"] == ["[ESMF]"]
    assert flattened[tavg] == [2.0]
