# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from unittest import mock

import pint
import pytest
import xarray as xr

from access.profiling.metrics import count, tavg
from access.profiling.scaling import parallel_efficiency, parallel_speedup, plot_scaling_metrics


@pytest.fixture()
def simple_scaling_data():
    """Fixture instantiating a dataset containing scaling data.

    The mock data contains two regions, "Region 1" and "Region 2", and two metrics, hits and tavg.
    Hits are always [1, 2] while tavg depends on the number of CPUs:
        - For 1 CPU: [600365 s, 2.345388 s]
        - For 2 CPUs: [300182.5 s, 1.172694 s]
        - For 4 CPUs: [300182.5 s, 1.172694 s]
    """
    ncpus = [1, 2, 4]
    datasets = []
    for n in ncpus:
        datasets.append(
            xr.Dataset(
                data_vars={
                    count: xr.DataArray([[1, 2]], dims=["ncpus", "region"]),
                    tavg: xr.DataArray(
                        [[value / min(n, 2) for value in [600365, 2.345388]]], dims=["ncpus", "region"]
                    ).pint.quantify("seconds"),
                },
                coords={"region": ["Region 1", "Region 2"], "ncpus": [n]},
            )
        )
    return xr.concat(datasets, dim="ncpus")


def test_parallel_speedup(simple_scaling_data):
    """Test parallel speedup calculation."""
    speedup = parallel_speedup(simple_scaling_data, tavg)

    assert speedup.shape == (2, 3)
    assert speedup.name == "speedup"
    assert str(speedup.pint.units) == "dimensionless"
    assert speedup.attrs == {}
    assert list(speedup.coords) == ["region", "ncpus"]
    assert list(speedup["ncpus"].values) == [1, 2, 4]
    assert list(speedup["region"].values) == ["Region 1", "Region 2"]
    speedup = speedup.pint.dequantify()  # Dequantify to remove warnings when getting values
    assert speedup.sel(ncpus=1, region="Region 1").values == pytest.approx(1.0)
    assert speedup.sel(ncpus=2, region="Region 1").values == pytest.approx(2.0)
    assert speedup.sel(ncpus=4, region="Region 1").values == pytest.approx(2.0)
    assert speedup.sel(ncpus=1, region="Region 2").values == pytest.approx(1.0)
    assert speedup.sel(ncpus=2, region="Region 2").values == pytest.approx(2.0)
    assert speedup.sel(ncpus=4, region="Region 2").values == pytest.approx(2.0)


def test_parallel_efficiency(simple_scaling_data):
    """Test parallel efficiency calculation."""
    ureg = pint.UnitRegistry()

    eff = parallel_efficiency(simple_scaling_data, tavg)

    assert eff.shape == (2, 3)
    assert eff.name == "parallel efficiency"
    assert str(eff.pint.units) == "percent"
    assert eff.attrs == {}
    assert list(eff.coords) == ["region", "ncpus"]
    assert list(eff["ncpus"].values) == [1, 2, 4]
    assert list(eff["region"].values) == ["Region 1", "Region 2"]
    eff = eff.pint.dequantify()  # Dequantify to remove warnings when getting values
    assert eff.sel(ncpus=1, region="Region 1").values == pytest.approx(100 * ureg.percent)
    assert eff.sel(ncpus=2, region="Region 1").values == pytest.approx(100 * ureg.percent)
    assert eff.sel(ncpus=4, region="Region 1").values == pytest.approx(50 * ureg.percent)
    assert eff.sel(ncpus=1, region="Region 2").values == pytest.approx(100 * ureg.percent)
    assert eff.sel(ncpus=2, region="Region 2").values == pytest.approx(100 * ureg.percent)
    assert eff.sel(ncpus=4, region="Region 2").values == pytest.approx(50 * ureg.percent)


def test_incorrect_units(simple_scaling_data):
    """Test calculation with incorrect units."""
    with pytest.raises(ValueError):
        parallel_speedup(simple_scaling_data, count)


@mock.patch("matplotlib.pyplot.show", autospec=True)
def test_plot_scaling_metrics(mock_plt, simple_scaling_data):
    """Test plotting scaling metrics. Currently only checks that the function runs without errors."""

    plot_scaling_metrics(
        stats=[simple_scaling_data],
        metric=tavg,
        xcoordinate="ncpus",
    )
    mock_plt.assert_called_once()
