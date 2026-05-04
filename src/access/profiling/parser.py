# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Classes and utilities for reading and transforming profiling data.

Data formats
------------
Parsers return a plain dict. Three shapes are supported:

Flat (standard)
    One list per metric, all the same length as 'region':

        {'region': [...], metric_a: [...], metric_b: [...]}

Hierarchical (nested dict)
    Used when regions form a call-stack tree (e.g. ESMF). String keys are child
    region names; ProfilingMetric keys are metric values. The two key types never
    collide, so no separator is needed:

        {'[ESMF]': {tavg: 2558.6, '[ICE] RunPhase1': {tavg: 155.8, ...}}}

    Use flatten_hierarchical() to convert to the flat format.

Per-PE
    Each region has one measurement per processing element (MPI process). Metric
    values are 2D lists of shape (n_regions, n_pes); a 'pe' key holds the PE IDs:

        {'region': [...], 'pe': [0, 1, ..., N-1], metric_a: [[...], [...]]}

    Use aggregate_pe_data() on the resulting xarray Dataset to reduce over PEs
    and compute load-imbalance statistics (see metrics.py for the naming convention).

Utilities
---------
flatten_hierarchical(data, metrics)
    Converts a hierarchical nested dict to the standard flat dict (DFS pre-order).
aggregate_pe_data(ds)
    Reduces a per-PE Dataset along 'pe', producing variables named
    {var}_{stat}_pe for each input variable and statistic.
"""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

# Next import is required to register pint with xarray
import pint_xarray  # noqa: F401
import xarray as xr

from access.profiling.metrics import ProfilingMetric


class ProfilingParser(ABC):
    """Abstract parser of profiling data.

    The main purpose of a parser is to read profiling data from a file and return it
    as a dict. Three output shapes are supported (see module docstring for full details):

    Flat (standard)::

        {
            'region': ['region1', 'region2', ...],
            metric_a: [val1a, val2a, ...],
            metric_b: [val1b, val2b, ...],
        }

    Hierarchical (nested dict, no 'region' key)::

        {
            'root_region': {
                metric_a: val,
                'child_region': {metric_a: val, ...},
            }
        }

    Per-PE (flat with an additional 'pe' key; metric values are 2D lists)::

        {
            'region': ['region1', 'region2', ...],
            'pe': [0, 1, ..., N-1],
            metric_a: [[pe0_val1, pe1_val1, ...], [pe0_val2, pe1_val2, ...]],
        }
    """

    _metrics: list[ProfilingMetric]

    @property
    def metrics(self) -> list[ProfilingMetric]:
        """list: Metrics available when using this parser."""
        return self._metrics

    @abstractmethod
    def parse(self, file_path: str | Path | os.PathLike) -> dict:
        """Parse the given file.

        Args:
            file_path (str | Path | os.PathLike): file to parse.

        Returns:
            dict: profiling data in one of the three formats described in the class
                docstring (flat, hierarchical, or per-PE).

        Raises:
            ValueError: If no parsable text is found in file_path.
            TypeError: If file_path cannot be converted to a valid Path object.
            FileNotFoundError: If file_path doesn't exist or isn't a file.
        """


def flatten_hierarchical(data: dict, metrics: list[ProfilingMetric]) -> dict:
    """Converts a hierarchical (nested dict) parser output into the standard flat format.

    Traverses the nested dict depth-first (pre-order: parent before children).
    At each node, string keys are treated as child region names and ProfilingMetric
    keys as metric values — these types never collide so no separator is needed.

    Args:
        data (dict): Nested dict as returned by a hierarchical parser. At each level,
            string keys are child region names and ProfilingMetric keys are metric values.
        metrics (list[ProfilingMetric]): Metrics to extract from each node.

    Returns:
        dict: Standard flat profiling dict with a 'region' key and one list per metric.
    """
    result: dict = {"region": []}
    for m in metrics:
        result[m] = []

    def _visit(node: dict, name: str) -> None:
        result["region"].append(name)
        for m in metrics:
            result[m].append(node.get(m))
        for key, value in node.items():
            if isinstance(key, str):  # child region — ProfilingMetric keys are not str
                _visit(value, key)

    for region_name, region_data in data.items():
        _visit(region_data, region_name)

    return result


def aggregate_pe_data(ds: xr.Dataset) -> xr.Dataset:
    """Aggregates a per-PE profiling Dataset into summary statistics over the pe dimension.

    For each data variable, computes the following reductions over the 'pe' dimension
    and returns them as new string-named variables following the {var}_{stat}_pe
    convention defined in metrics.py:

    ========================  =============================================
    Variable                  Description
    ========================  =============================================
    ``{var}_min_pe``          minimum across PEs
    ``{var}_max_pe``          maximum across PEs
    ``{var}_mean_pe``         mean across PEs
    ``{var}_median_pe``       median across PEs
    ``{var}_std_pe``          standard deviation across PEs
    ``{var}_total_pe``        sum across PEs (total work done by all PEs)
    ``{var}_argmin_pe``       PE index of the minimum value (dimensionless)
    ``{var}_argmax_pe``       PE index of the maximum value (dimensionless)
    ``{var}_imbalance_pe``    (max - min) / mean; 0 = perfectly balanced
    ========================  =============================================

    Args:
        ds (xr.Dataset): Dataset with a 'pe' dimension, as returned by
            ProfilingLog.parse() for per-PE parser output.

    Returns:
        xr.Dataset: New Dataset with 'pe' reduced, containing the derived variables
            described above. All non-pe coordinates are preserved.

    Raises:
        ValueError: If ds does not have a 'pe' dimension.
    """
    if "pe" not in ds.dims:
        raise ValueError("Dataset does not have a 'pe' dimension.")

    result_vars = {}
    for var in ds.data_vars:
        da = ds[var]
        base = str(var).replace(" ", "_")
        vmin = da.min("pe")
        vmax = da.max("pe")
        vmean = da.mean("pe")
        result_vars[f"{base}_min_pe"] = vmin
        result_vars[f"{base}_max_pe"] = vmax
        result_vars[f"{base}_mean_pe"] = vmean
        result_vars[f"{base}_median_pe"] = da.median("pe")
        result_vars[f"{base}_std_pe"] = da.std("pe")
        result_vars[f"{base}_total_pe"] = da.sum("pe")
        result_vars[f"{base}_argmin_pe"] = da.argmin("pe")
        result_vars[f"{base}_argmax_pe"] = da.argmax("pe")
        result_vars[f"{base}_imbalance_pe"] = xr.where(vmean != 0, (vmax - vmin) / vmean, 0)

    coords = {k: v for k, v in ds.coords.items() if k != "pe"}
    return xr.Dataset(result_vars, coords=coords)


def _convert_from_string(value: str) -> Any:
    """Tries to convert a string to the most appropriate numeric type. Leaves it unchanged if conversion does not
    succeed.

    Args:
        value (str): string to convert.

    Returns:
        Any: the converted string or the original string.
    """
    for type_conversion in (int, float):
        try:
            return type_conversion(value)
        except Exception:
            continue
    return value


def _test_file(file_path: str | Path | os.PathLike) -> Path:
    """Checks whether file_path is a valid path.

    Args:
        file_path (str | Path | os.PathLike): the path to check.

    Returns
        Path: file_path as a Path object.

    Raises:
        TypeError: if file_path cannot be turned into a Path object.
        FileNotFoundError: if file_path can be converted into a Path, but the file dosen't exist.
    """

    try:
        path = Path(file_path)
    except TypeError as e:
        raise TypeError(f"{file_path} is not a valid path.") from e

    if not path.is_file():
        raise FileNotFoundError(f"{file_path} is not a file or doesn't exist.")

    return path


def _read_text_file(file_path: str | Path | os.PathLike) -> str:
    """Checks whether file_path is a valid path to a text file and tries to read it.

    Args:
        file_path (str | Path | os.PathLike): the path to check/read

    Returns:
        str: The text within the file.

    Raises:
        TypeError: if file_path is not a valid path
        FileNotFoundError: if file_path is a path, but is not a file or doesn't exist.
        ValueError: if file_path is a file, but cannot be read as a text file.
    """

    path = _test_file(file_path)

    try:
        return path.read_text()
    except UnicodeDecodeError as e:
        raise ValueError(f"{file_path} is not a text file.") from e
