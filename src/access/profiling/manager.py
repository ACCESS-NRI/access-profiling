# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from pathlib import Path

import xarray as xr
from matplotlib.figure import Figure

from access.profiling.experiment import ProfilingExperiment, ProfilingExperimentStatus
from access.profiling.metrics import ProfilingMetric
from access.profiling.scaling import plot_scaling_metrics

logger = logging.getLogger(__name__)


class ProfilingManager(ABC):
    """Abstract base class to handle profiling data and workflows.

    This high-level class defines methods to parse different types of profiling data. Currently,
    it supports parsing and plotting scaling data.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        archive_exclude_patterns (list[str] | None): File patterns to exclude when archiving experiments.
    """

    work_dir: Path  # Working directory where profiling experiments will be generated and run.
    archive_dir: Path  # Directory where completed experiments will be archived.
    experiments: dict[str, ProfilingExperiment]  # Dictionary storing ProfilingExperiment instances.
    data: dict[str, xr.Dataset]  # Dictionary mapping component names to their profiling datasets.

    def __init__(self, work_dir: Path, archive_dir: Path):
        super().__init__()
        self.work_dir = work_dir
        self.archive_dir = archive_dir
        self.experiments = {}
        self.data = {}

        # Discover experiments in the archive directory
        if self.archive_dir.is_dir():
            for branch_path in self.archive_dir.glob("*.tar.gz"):
                if branch_path.is_file():
                    branch_name = branch_path.name[: -len(".tar.gz")]
                    logger.info(f"Found archived experiment: {branch_name}")
                    self.experiments[branch_name] = ProfilingExperiment(branch_path)

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

    def archive_experiments(
        self,
        exclude_dirs: list[str] | None = None,
        exclude_files: list[str] | None = None,
        follow_symlinks: bool = False,
        overwrite: bool = False,
    ) -> None:
        """Archives completed experiments to the specified archive path.

        This method will create a tar.gz archive containing relevant data from an experiment. No data will be deleted
        once an experiment is archived, but data will be parsed directly from the archive instead of the original
        experiment directory.

        Args:
            exclude_dirs (list[str] | None): Directory patterns to exclude when archiving experiments.
            exclude_files (list[str] | None): File patterns to exclude when archiving experiments.
            follow_symlinks (bool): Whether to follow symlinks when archiving experiments. Defaults to False.
            overwrite (bool): Whether to overwrite existing archives. Defaults to False.
        """
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        for branch, exp in self.experiments.items():
            exp.archive(
                self.archive_dir / branch,
                exclude_dirs=exclude_dirs,
                exclude_files=exclude_files,
                follow_symlinks=follow_symlinks,
                overwrite=overwrite,
            )

    def parse_scaling_data(self):
        """Parses profiling data from the experiments."""
        self.data = {}
        for exp in self.experiments.values():
            if exp.status == ProfilingExperimentStatus.DONE or exp.status == ProfilingExperimentStatus.ARCHIVED:
                with exp.directory() as exp_path:
                    datasets = self.parse_profiling_data(exp_path)

                    # Find number of cpus used
                    ncpus = self.parse_ncpus(exp_path)

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
