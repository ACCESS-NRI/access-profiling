# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import textwrap
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

    def __repr__(self) -> str:
        """Returns a string representation of the ProfilingManager."""

        indent = "    "
        summary = f"<{type(self).__name__}>\n"
        summary += indent + f"Working directory: {self.work_dir!r}\n"
        summary += indent + f"Archive directory: {self.archive_dir!r}\n"
        summary += indent + "Experiments:\n"
        for name, exp in self.experiments.items():
            summary += indent * 2 + f"'{name}': {exp!r}\n"
        summary += indent + "Data:\n"
        if self.data == {}:
            summary += indent * 2 + "No parsed data.\n"
        else:
            for name, ds in self.data.items():
                summary += indent * 2 + f"'{name}':\n"
                summary += textwrap.indent(f"{ds}\n", indent * 3)
        return summary

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

    def add_experiment_from_directory(self, name: str, path: Path) -> None:
        """Adds an existing experiment from the specified directory.

        Note that the directory must already exist on disk and be inside the working directory. Also, the experiment
        will be marked as DONE, so any runs associated with the experiment must already be completed.

        Args:
            name (str): Name of the experiment.
            path (Path): Path to the experiment directory.
        Raises:
            ValueError: If the specified path does not exist, is not a directory, or is not inside the working
            directory.
        """
        if not path.is_absolute():
            path = self.work_dir / path
        if not path.is_dir():
            raise ValueError(f"Experiment path '{path}' does not exist or is not a directory.")
        if not path.resolve().is_relative_to(self.work_dir.resolve()):
            raise ValueError(f"Experiment path '{path}' is not inside the working directory '{self.work_dir}'.")
        self.experiments[name] = ProfilingExperiment(path)
        self.experiments[name].status = ProfilingExperimentStatus.DONE

    def delete_experiment(self, name: str) -> None:
        """Deletes the specified experiment.

        Note that this only removes the experiment from the manager's tracking; it does not delete any files on disk.

        Args:
            name (str): Name of the experiment to delete.
        """
        if name in self.experiments:
            del self.experiments[name]
        else:
            logger.warning(f"Experiment '{name}' not found; cannot delete.")

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
