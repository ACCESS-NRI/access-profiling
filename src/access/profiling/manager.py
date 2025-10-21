# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path

import xarray as xr

from access.profiling.metrics import ProfilingMetric
from access.profiling.parser import ProfilingParser
from access.profiling.scaling import plot_scaling_metrics


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
        log = path.read_text()
        data = self.parser.read(log)
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

    NEW = 1
    RUNNING = 2
    DONE = 3


class ProfilingExperiment:
    """Represents a profiling experiment.

    Args:
        path (Path): Path to the experiment directory.
    """

    path: Path  # Path to the experiment directory
    status: ProfilingExperimentStatus = ProfilingExperimentStatus.NEW  # Status of the experiment

    def __init__(self, path: Path) -> None:
        self.path = path


class ProfilingManager(ABC):
    """Abstract base class to handle profiling data and workflows.

    This high-level class defines methods to parse different types of profiling data. Currently,
    it supports parsing and plotting scaling data.

    Args:
        work_dir (Path): Path to directory used to generate and run profiling experiments.
    """

    work_dir: Path  # Path to directory used to generate and run profiling experiments.
    experiments: dict[str, ProfilingExperiment] = {}  # Dictionary storing ProfilingExperiment instances.
    data: dict[str, xr.Dataset] = {}  # Dictionary mapping component names to their profiling datasets.

    def __init__(self, work_dir: Path) -> None:
        super().__init__()
        self.work_dir = work_dir

    @abstractmethod
    def parse_profiling_data(self, path: Path) -> dict[str, xr.Dataset]:
        """Parses profiling data from the specified path.

        Args:
            path (Path): Path to the experiment directory.

        Returns:
            dict[str, xr.Dataset]: Dictionary mapping component names to their profiling datasets.
        """

    @abstractmethod
    def parse_ncpus(self, path: Path) -> int:
        """Parses the number of CPUs used in a given experiment in the specified path.

        Args:
            path (Path): Path to the experiment directory.

        Returns:
            int: Number of CPUs used in the experiment.
        """

    def parse_scaling_data(self):
        """Parses profiling data from the experiments."""
        self.data = {}
        for exp in self.experiments.values():
            if exp.status == ProfilingExperimentStatus.DONE:
                datasets = self.parse_profiling_data(exp.path)

                # Find number of cpus used
                ncpus = self.parse_ncpus(exp.path)

                # Add ncpus dimension and concatenate with existing data
                for name, ds in datasets.items():
                    ds = ds.expand_dims({"ncpus": 1}).assign_coords({"ncpus": [ncpus]})
                    if name in self.data:
                        self.data[name] = xr.concat([self.data[name], ds], dim="ncpus", join="outer")
                    else:
                        self.data[name] = ds

    def plot_scaling_data(
        self,
        components: list[str],
        regions: list[list[str]],
        metric: ProfilingMetric,
        region_relabel_map: dict | None = None,
    ):
        """Plots scaling data for the specified components, regions and metric.

        Args:
            components (list[str]): List of component names to plot.
            regions (list[list[str]]): List of regions to plot for each component.
            metric (ProfilingMetric): Metric to use for the scaling plots.
            region_relabel_map (dict | None): Optional mapping to relabel regions in the plots.
        """
        plot_scaling_metrics([self.data[c] for c in components], regions, metric, region_relabel_map=region_relabel_map)
