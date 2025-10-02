# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Functions to calculate metrics related to parallel scaling of applications."""

import xarray as xr

from access.profiling.metrics import ProfilingMetric


def parallel_speedup(stats: xr.Dataset, metric: ProfilingMetric) -> xr.DataArray:
    """Calculates the parallel speedup from scaling data.

    Args:
        stats (Dataset): Scaling data, stored as a xarray dataset.
        metric (ProfilingMetric): Metric to use for the speedup calculation.

    Returns:
        DataArray: Parallel speedup.
    Raises:
        ValueError: If metric units are not time (e.g., seconds).
    """
    if stats[metric].pint.dimensionality != "[time]":
        raise ValueError("Metric units must be time (e.g., seconds)!")
    speedup = stats[metric].sel(ncpus=stats["ncpus"].min()) / stats[metric]
    speedup.name = "speedup"
    return speedup


def parallel_efficiency(stats: xr.Dataset, metric: ProfilingMetric) -> xr.DataArray:
    """Calculates the parallel efficiency from scaling data.

    Args:
        stats (Dataset): Scaling data, stored as a xarray dataset.
        metric (ProfilingMetric): Metric to use for the efficiency calculation.

    Returns:
        DataArray: Parallel efficiency.
    """
    speedup = parallel_speedup(stats, metric)
    eff = speedup * (speedup.ncpus.min() / speedup.ncpus)
    eff = eff.pint.to("percent")
    eff.name = "parallel efficiency"
    return eff
