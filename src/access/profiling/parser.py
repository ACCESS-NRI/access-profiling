# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Classes and utilities to build profiling parsers for reading profiling data."""

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any

# Next import is required to register pint with xarray
import pint_xarray  # noqa: F401
import xarray as xr

from access.profiling.metrics import ProfilingMetric


class ProfilingParser(ABC):
    """Abstract parser of profiling data.

    The main purpose of a parser of profiling data is to read said data from a file or directory and return it in a
    standard format.

    Once parsed, the profiling data should be stored in a dict in the following way:

    {
        'region': ['region1', 'region2', ...],
        'metric a': [val1a, val2a, ...],
        'metric b': [val1b, val2b, ...],
        ...
    }

    The 'region' values correspond to the labels of the profile regions. Then, for each metric, there is a list of
    values, one for each profiling region. Therefore, 'val1a', is the value for metric a of region 1.
    """

    _metrics: list[ProfilingMetric]

    @property
    def metrics(self) -> list[ProfilingMetric]:
        """list: Metrics available when using this parser."""
        return self._metrics

    @abstractmethod
    def read(self, stream: str) -> dict:
        """Parse the given text.

        Args:
            stream (str): text to parse.

        Returns:
            dict: profiling data.
        """

    def parse_data_series(self, logs: list[str], varname: str, vars: Iterable) -> xr.Dataset:
        """Given a list of logs containing profiling data, parse the data and return it as a xarray dataset.

        For example, if the logs correspond to different runs of the same application with different number of CPUs,
        then varname should be "ncpus" and vars could be a list with core counts:

            log_1cpu = open("log_1cpu.txt").read()
            log_2cpu = open("log_2cpu.txt").read()
            log_4cpu = open("log_4cpu.txt").read()
            scaling_data = parser.parse_data_series(
                logs= [log_1cpu, log_2cpu, log_4cpu],
                varname="ncpus",
                vars=[1, 2, 4]
            )

        Args:
            Logs (list[str]): Logs to parse.
            varname (str): Name of the variable that changes between logs.
            vars (Iterable): An iterable returning the value of the variable that changes between logs.

        Returns:
            Dataset: Series profiling data.
        """
        datasets = []
        for var, log in zip(vars, logs, strict=True):
            data = self.read(log)
            datasets.append(
                xr.Dataset(
                    data_vars=dict(
                        zip(
                            self.metrics,
                            [
                                xr.DataArray([data[metric]], dims=[varname, "region"]).pint.quantify(metric.units)
                                for metric in self.metrics
                            ],
                            strict=True,
                        )
                    ),
                    coords={varname: [var], "region": data["region"]},
                )
            )

        # Create dataset with all the data
        return xr.concat(datasets, dim=varname)


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
