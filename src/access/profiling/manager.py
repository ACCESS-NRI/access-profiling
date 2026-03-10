# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import textwrap
from abc import ABC, abstractmethod
from pathlib import Path

import xarray as xr
from matplotlib.figure import Figure

from access.profiling.experiment import ProfilingExperiment, ProfilingExperimentStatus, ProfilingLog
from access.profiling.metrics import ProfilingMetric
from access.profiling.plotting_utils import plot_bar_metrics
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
    data: dict[
        str, dict[str, xr.Dataset]
    ]  # Dictionary mapping experiments to component names and their profiling datasets.

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
    def profiling_logs(self, path: Path) -> dict[str, ProfilingLog]:
        """Returns all profiling logs from the specified path.

        Args:
            path (Path): Path to the experiment directory.

        Returns:
            dict[str, ProfilingLog]: Dictionary of profiling logs.
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

    def parse_profiling_data(self):
        """Parses profiling data from the experiments."""
        self.data = {}
        for exp_name, exp in self.experiments.items():
            if exp.status == ProfilingExperimentStatus.DONE or exp.status == ProfilingExperimentStatus.ARCHIVED:
                logger.info(f"Parsing profiling data for experiment '{exp_name}'.")
                self.data[exp_name] = {}
                with exp.directory() as exp_path:
                    # Parse all logs
                    logs = self.profiling_logs(exp_path)
                    for log_name, log in logs.items():
                        logger.info(f"Parsing {log_name} profiling log: {log.filepath}. ")
                        if log.optional:
                            try:
                                self.data[exp_name][log_name] = log.parse()
                            except FileNotFoundError:
                                logger.info(f"Optional profiling log '{log.filepath}' not found. Skipping.")
                                continue
                        else:
                            self.data[exp_name][log_name] = log.parse()
                        logger.info(" Done.")
            else:
                logger.warning(
                    f"Experiment '{exp_name}' is not completed (status: {exp.status.name}). Skipping parsing profiling "
                    "data."
                )

    def plot_scaling_data(
        self,
        components: list[str],
        regions: list[list[str]],
        metric: ProfilingMetric,
        region_relabel_map: dict | None = None,
        experiments: list[str] | None = None,
    ) -> Figure:
        """Plots scaling data for the specified components, regions and metric.

        Args:
            components (list[str]): List of component names to plot.
            regions (list[list[str]]): List of regions to plot for each component.
            metric (ProfilingMetric): Metric to use for the scaling plots.
            region_relabel_map (dict | None): Optional mapping to relabel regions in the plots.
        """

        # Find number of cpus used for each experiment
        ncpus = {}
        for exp_name in self.data:
            with self.experiments[exp_name].directory() as exp_path:
                # Find number of cpus used
                ncpus[exp_name] = self.parse_ncpus(exp_path)

        # Gather scaling data for each component
        scaling_data = []
        for component, component_regions in zip(components, regions, strict=True):
            component_data = None
            for exp_name in self.data:
                # Skip experiments not in the specified list
                if experiments is not None and exp_name not in experiments:
                    continue

                ds = self.data[exp_name].get(component)
                if ds is None:
                    raise ValueError(f"No profiling data found for component '{component}' in experiment '{exp_name}'.")

                # Select only the desired regions
                ds = ds.sel(region=component_regions)

                # Relabel regions if a relabel map is provided
                if region_relabel_map is not None:
                    ds = ds.assign_coords(region=[region_relabel_map.get(n, n) for n in ds.region.values])

                # Add ncpus dimension
                ds = ds.expand_dims({"ncpus": 1}).assign_coords({"ncpus": [ncpus[exp_name]]})

                # Concatenate data along ncpus dimension
                if component_data is None:
                    component_data = ds
                else:
                    component_data = xr.concat([component_data, ds], dim="ncpus", join="outer").sortby("ncpus")

            scaling_data.append(component_data)

        return plot_scaling_metrics(scaling_data, metric)

    def plot_bar_chart(
        self,
        components: list[str],
        regions: list[list[str]],
        metric: ProfilingMetric,
        region_relabel_map: dict | None = None,
        experiments: list[str] | None = None,
        show: bool = True,
    ) -> Figure:
        """Plots a bar chart of a profiling metric over regions, grouped by experiment.

        Regions are placed along the x-axis. Within each region group, there is one bar per
        experiment, coloured by experiment name.

        Args:
            components (list[str]): List of component names to include.
            regions (list[list[str]]): List of regions to include for each component.
            metric (ProfilingMetric): Metric to plot.
            region_relabel_map (dict | None): Optional mapping to relabel regions in the plot.
            experiments (list[str] | None): Optional list of experiment names to include. If None, all experiments
                are included.
            show (bool): Whether to show the generated plot. Default: True.

        Returns:
            Figure: The Matplotlib figure containing the bar chart.

        Raises:
            ValueError: If no profiling data is found for a specified component in any experiment.
        """
        exp_names = experiments if experiments is not None else list(self.data.keys())
        relabel = region_relabel_map or {}

        # Build a lookup from display label to (component, original_region) and preserve input order.
        region_info: list[tuple[str, str, str]] = []  # (component, original_region, display_label)
        for component, component_regions in zip(components, regions, strict=True):
            for region in component_regions:
                region_info.append((component, region, relabel.get(region, region)))
        region_labels = [label for _, _, label in region_info]

        # Extract metric values per experiment, reading directly from the datasets
        bar_data: dict[str, list[float]] = {}
        for exp_name in exp_names:
            values = []
            for component, region, _ in region_info:
                ds = self.data[exp_name].get(component)
                if ds is None:
                    raise ValueError(f"No profiling data found for component '{component}' in experiment '{exp_name}'.")
                values.append(float(ds[metric].sel(region=region).pint.dequantify().values))
            bar_data[exp_name] = values

        return plot_bar_metrics(bar_data, region_labels, metric, show=show)
