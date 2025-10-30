# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from pathlib import Path

import xarray as xr

from access.profiling.parser import ProfilingParser


class ProfilingLog:
    """Represents a profiling log file.

    Args:
        filepath (Path): Path to the log file.
        parser (ProfilingParser): Parser to use for this log file.
    """

    filepath: Path  # Path to the log file
    parser: ProfilingParser  # Parser to use for this log file

    def __init__(self, filepath: Path, parser: ProfilingParser):
        self.filepath = filepath
        self.parser = parser

    def parse(self) -> xr.Dataset:
        """Parses the log file and returns the profiling data as an xarray Dataset.

        Returns:
           xr.Dataset: Parsed profiling data."""
        path = self.filepath
        data = self.parser.parse(path)
        return xr.Dataset(
            data_vars=dict(
                zip(
                    self.parser.metrics,
                    [
                        xr.DataArray(data[metric], dims=["region"]).pint.quantify(metric.units)
                        for metric in self.parser.metrics
                    ],
                    strict=True,
                )
            ),
            coords={"region": data["region"]},
        )


class ProfilingExperimentStatus(Enum):
    """Enumeration representing the status of a profiling experiment."""

    NEW = 1  # Experiment has been created but not started
    RUNNING = 2  # Experiment is running or is queued
    DONE = 3  # Experiment has finished


class ProfilingExperiment:
    """Represents a profiling experiment.

    Args:
        path (Path): Path to the experiment directory.
    """

    path: Path  # Path to the experiment directory
    status: ProfilingExperimentStatus = ProfilingExperimentStatus.NEW  # Status of the experiment

    def __init__(self, path: Path) -> None:
        self.path = path
