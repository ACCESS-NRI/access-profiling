# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from pathlib import Path

import xarray as xr
from matplotlib.figure import Figure

from access.profiling.experiment import ProfilingExperiment, ProfilingExperimentStatus
from access.profiling.metrics import ProfilingMetric
from access.profiling.scaling import plot_scaling_metrics


class ProfilingManager(ABC):
    """Abstract base class to handle profiling data and workflows.

    This high-level class defines methods to parse different types of profiling data. Currently,
    it supports parsing and plotting scaling data.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
    """

    work_dir: Path  # Working directory where profiling experiments will be generated and run.
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
                        self.data[name] = xr.concat([self.data[name], ds], dim="ncpus", join="outer").sortby("ncpus")
                    else:
                        self.data[name] = ds

    def plot_scaling_data(
        self,
        components: list[str],
        regions: list[list[str]],
        metric: ProfilingMetric,
        region_relabel_map: dict | None = None,
    ) -> Figure:
        """Plots scaling data for the specified components, regions and metric.

        Args:
            components (list[str]): List of component names to plot.
            regions (list[list[str]]): List of regions to plot for each component.
            metric (ProfilingMetric): Metric to use for the scaling plots.
            region_relabel_map (dict | None): Optional mapping to relabel regions in the plots.
        """
        return plot_scaling_metrics(
            [self.data[c] for c in components], regions, metric, region_relabel_map=region_relabel_map
        )
