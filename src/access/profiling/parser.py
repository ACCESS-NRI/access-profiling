# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Classes and utilities for reading and transforming profiling data.

**Data formats**

Parsers return a plain dict. Three shapes are supported:

- **Flat** (standard): one list per metric, all the same length as 'region':

    ```python
    {'region': [...], metric_a: [...], metric_b: [...]}
    ```

- **Hierarchical** (nested dict): used when regions form a call-stack tree (e.g. ESMF). String keys are child
  region names; ProfilingMetric keys are metric values. The two key types never collide, so no separator is
  needed:

    ```python
    {'[ESMF]': {tavg: 2558.6, '[ICE] RunPhase1': {tavg: 155.8, ...}}}
    ```

    Use `flatten_hierarchical()` to convert to the flat format.

- **Per-PE**: each region has one measurement per processing element (MPI process). Metric values are 2D lists
  of shape (n_regions, n_pes); a 'pe' key holds the PE IDs:

    ```python
    {'region': [...], 'pe': [0, 1, ..., N-1], metric_a: [[...], [...]]}
    ```

    Use `aggregate_pe_data()` on the resulting xarray Dataset to reduce over PEs and compute load-imbalance
    statistics.

**Utilities**

- `flatten_hierarchical(data, metrics)` converts a hierarchical nested dict to the standard flat dict (DFS
  pre-order).
- `aggregate_pe_data(ds)` reduces a per-PE Dataset along 'pe', producing one variable for each metric and
  statistic, keyed by the metric `pe_statistic(metric, stat)` returns and named `<base>_<stat>_pe`, following
  the naming convention of metrics.py.
"""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

# Next import is required to register pint with xarray
import pint_xarray  # noqa: F401
import xarray as xr
from pint import Unit

from access.profiling.metrics import ProfilingMetric

ProfilingData = dict[str | ProfilingMetric, Any]
"""What a parser returns, and what the helpers below pass around.

One dict is keyed by both kinds of key, as the data formats above describe: str for 'region', 'pe' and child
region names, ProfilingMetric for the measurements. The two never collide, which is what makes a single dict
workable - but it also means neither dict[str, ...] nor dict[ProfilingMetric, ...] describes it.
"""


class ProfilingParser(ABC):
    """Abstract parser of profiling data.

    The main purpose of a parser is to read profiling data from a file and return it
    as a dict. Three output shapes are supported (see module docstring for full details):

    Flat (standard):

    ```python
    {
        'region': ['region1', 'region2', ...],
        metric_a: [val1a, val2a, ...],
        metric_b: [val1b, val2b, ...],
    }
    ```

    Hierarchical (nested dict, no 'region' key):

    ```python
    {
        'root_region': {
            metric_a: val,
            'child_region': {metric_a: val, ...},
        }
    }
    ```

    Per-PE (flat with an additional 'pe' key; metric values are 2D lists):

    ```python
    {
        'region': ['region1', 'region2', ...],
        'pe': [0, 1, ..., N-1],
        metric_a: [[pe0_val1, pe1_val1, ...], [pe0_val2, pe1_val2, ...]],
    }
    ```
    """

    _metrics: list[ProfilingMetric]

    @property
    def metrics(self) -> list[ProfilingMetric]:
        """Returns the metrics available when using this parser.

        Returns:
            list[ProfilingMetric]: The metrics.
        """
        return self._metrics

    @abstractmethod
    def parse(self, file_path: str | Path | os.PathLike) -> ProfilingData:
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


def flatten_hierarchical(data: ProfilingData, metrics: list[ProfilingMetric]) -> ProfilingData:
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
        # Top-level keys are region names. A ProfilingMetric here would mean a measurement with no region,
        # which the hierarchical format has no way to express.
        if isinstance(region_name, str):
            _visit(region_data, region_name)

    return result


# What each statistic across PEs is, as the start of the description of its metric.
_PE_STATISTICS = {
    "min": "Minimum across PEs of",
    "max": "Maximum across PEs of",
    "mean": "Mean across PEs of",
    "median": "Median across PEs of",
    "std": "Standard deviation across PEs of",
    "total": "Sum across PEs of",
    "argmin": "PE with the smallest",
    "argmax": "PE with the largest",
    "imbalance": "Imbalance across PEs, (max - min) / mean, of",
}
_pe_statistic_metrics: dict[tuple[ProfilingMetric, str], ProfilingMetric] = {}


def pe_statistic(metric: ProfilingMetric, stat: str) -> ProfilingMetric:
    """Returns the metric of a statistic across PEs of another metric, as aggregate_pe_data reports it.

    The metric is made once for each metric and statistic, and the same object is returned every time after,
    since metrics are compared by identity: `aggregate_pe_data(ds)[pe_statistic(tavg, "max")]` is the maximum
    across PEs of `tavg`.

    Its name follows the `<base>_<stat>_pe` convention of `access.profiling.metrics`, the base being the name of
    the metric with its spaces replaced by underscores, and its description is the title of the bar charts that
    plot it. A statistic of the values keeps their units, while the PEs of the minimum and the maximum, and the
    imbalance, are dimensionless.

    Args:
        metric (ProfilingMetric): The metric the statistic is taken of.
        stat (str): The statistic: "min", "max", "mean", "median", "std", "total", "argmin", "argmax" or
            "imbalance".

    Returns:
        ProfilingMetric: The metric of the statistic.

    Raises:
        ValueError: If stat is not one of the statistics above.
    """
    if stat not in _PE_STATISTICS:
        raise ValueError(f"Unknown statistic across PEs {stat!r}. Choose one of: {', '.join(_PE_STATISTICS)}.")
    key = (metric, stat)
    if key not in _pe_statistic_metrics:
        units = Unit("dimensionless") if stat in ("argmin", "argmax", "imbalance") else metric.units
        name = f"{str(metric).replace(' ', '_')}_{stat}_pe"
        _pe_statistic_metrics[key] = ProfilingMetric(name, units, f"{_PE_STATISTICS[stat]} {metric}")
    return _pe_statistic_metrics[key]


def aggregate_pe_data(ds: xr.Dataset) -> xr.Dataset:
    """Aggregates a per-PE profiling Dataset into summary statistics over the pe dimension.

    For each metric of the dataset, computes the following reductions over the 'pe' dimension, and returns each
    as a variable keyed by the metric `pe_statistic(metric, stat)` returns, with that metric's units:

    | stat | Description |
    |---|---|
    | `min` | minimum across PEs |
    | `max` | maximum across PEs |
    | `mean` | mean across PEs |
    | `median` | median across PEs |
    | `std` | standard deviation across PEs |
    | `total` | sum across PEs (total work done by all PEs) |
    | `argmin` | PE on which the minimum was recorded, as the 'pe' coordinate labels it |
    | `argmax` | PE on which the maximum was recorded, as the 'pe' coordinate labels it |
    | `imbalance` | (max - min) / mean; 0 = perfectly balanced, and 0 where the mean is 0 |

    PEs without a value, NaN, are left out of every statistic.

    Args:
        ds (xr.Dataset): Dataset with a 'pe' dimension, as returned by
            ProfilingLog.parse() for per-PE parser output.

    Returns:
        xr.Dataset: New Dataset with 'pe' reduced, containing the derived variables
            described above. All coordinates not along 'pe' are preserved.

    Raises:
        ValueError: If ds does not have a 'pe' dimension.
        TypeError: If a variable of ds is not keyed by a ProfilingMetric.
    """
    if "pe" not in ds.dims:
        raise ValueError("Dataset does not have a 'pe' dimension.")

    # The statistics are computed on the bare values, and each is given the units of its own metric afterwards,
    # which is what makes the PEs of the minimum and the maximum dimensionless.
    values = ds.pint.dequantify()
    result_vars = {}
    for metric in values.data_vars:
        if not isinstance(metric, ProfilingMetric):
            raise TypeError(f"Variable {metric!r} is not keyed by a ProfilingMetric, so it has no statistics to name.")
        da = values[metric]
        vmin = da.min("pe")
        vmax = da.max("pe")
        vmean = da.mean("pe")
        statistics = {
            "min": vmin,
            "max": vmax,
            "mean": vmean,
            "median": da.median("pe"),
            "std": da.std("pe"),
            "total": da.sum("pe"),
            "argmin": da.idxmin("pe"),
            "argmax": da.idxmax("pe"),
            # The mean is masked where it is 0 so that nothing is divided by it there.
            "imbalance": xr.where(vmean != 0, (vmax - vmin) / vmean.where(vmean != 0), 0),
        }
        for stat, value in statistics.items():
            derived = pe_statistic(metric, stat)
            value.attrs = {}  # the units of the values, which the reductions keep, are replaced by the metric's
            result_vars[derived] = value.pint.quantify(derived.units)

    coords = {name: coord for name, coord in ds.coords.items() if "pe" not in coord.dims}
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
