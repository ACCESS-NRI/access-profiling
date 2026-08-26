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
    it supports parsing and plotting scaling data, including selecting the best performing experiment
    for each number of CPUs.

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
    _ncpus_cache: dict[str, int]  # Number of CPUs of each experiment, parsed on demand.

    def __init__(self, work_dir: Path, archive_dir: Path):
        super().__init__()
        self.work_dir = work_dir
        self.archive_dir = archive_dir
        self.experiments = {}
        self.data = {}
        self._ncpus_cache = {}

        # Discover experiments in the archive directory
        if self.archive_dir.is_dir():
            for branch_path in self.archive_dir.glob("*.tar.gz"):
                if branch_path.is_file():
                    branch_name = branch_path.name[: -len(".tar.gz")]
                    logger.info(f"Found archived experiment: {branch_name}")
                    self.experiments[branch_name] = ProfilingExperiment(path=branch_path)

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
            for name, exp_data in self.data.items():
                summary += indent * 2 + f"'{name}':\n"
                for comp_name, ds in exp_data.items():
                    summary += indent * 3 + f"'{comp_name}':\n"
                    summary += textwrap.indent(f"{ds}\n", indent * 4)
        return summary

    @abstractmethod
    def profiling_logs(self, path: Path, run_path: Path | None = None) -> dict[str, ProfilingLog]:
        """Returns all profiling logs from the specified path.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            dict[str, ProfilingLog]: Dictionary of profiling logs.
        """

    @abstractmethod
    def parse_ncpus(self, path: Path, run_path: Path | None = None) -> int:
        """Parses the number of CPUs used in a given experiment in the specified path.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

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
        self.experiments[name] = ProfilingExperiment(path=path)
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

    @abstractmethod
    def _delete_experiment(self, name: str, dry_run: bool, **kwargs) -> None:
        """Deletes the on-disk artifacts of a single experiment.

        This is the configuration-specific counterpart to delete_experiments, which handles selection, validation and
        manager-state bookkeeping. Implementations should only remove files and, when dry_run is True, log what would
        be removed without making any changes.

        Args:
            name (str): Name of the experiment to delete. Guaranteed to be managed by this instance.
            dry_run (bool): If True, log what would be deleted without making any changes.
            **kwargs: Configuration-specific options forwarded verbatim from delete_experiments.
        """

    def delete_experiments(
        self,
        experiments: list[str] | None = None,
        all_experiments: bool = False,
        dry_run: bool = False,
        **kwargs,
    ) -> None:
        """Deletes experiments and removes them from the manager.

        The selection, validation and manager-state bookkeeping are handled here, while the actual on-disk deletion is
        delegated to the configuration-specific _delete_experiment method.

        Args:
            experiments (list[str] | None): List of experiment names to delete.
            all_experiments (bool): If True, deletes all experiments managed by this instance.
            dry_run (bool): If True, logs what would be deleted without making any changes. Defaults to False.
            **kwargs: Configuration-specific options forwarded to _delete_experiment.

        Raises:
            ValueError: If both experiments and all_experiments are specified, or neither is.
            KeyError: If any experiment name is not managed by this instance.
        """
        if all_experiments and experiments is not None:
            raise ValueError("Pass either experiments=[...] or all_experiments=True, not both.")
        if not all_experiments and not experiments:
            raise ValueError("No experiments specified. Pass either experiments=[...] or all_experiments=True.")
        existing = set(self.experiments.keys())
        names_to_delete = existing if all_experiments else set(experiments)
        unmanaged = names_to_delete - existing
        if unmanaged:
            raise KeyError(
                f"Experiments {unmanaged} are not managed by this manager "
                f"(existing: {existing}). Please check the names and try again."
            )

        for name in names_to_delete:
            self._delete_experiment(name, dry_run=dry_run, **kwargs)

        if dry_run:
            return

        for name in names_to_delete:
            del self.experiments[name]

    def parse_profiling_data(self):
        """Parses profiling data from the experiments."""
        self.data = {}
        for exp_name, exp in self.experiments.items():
            if exp.status == ProfilingExperimentStatus.DONE or exp.status == ProfilingExperimentStatus.ARCHIVED:
                logger.info(f"Parsing profiling data for experiment '{exp_name}'.")
                self.data[exp_name] = {}
                with exp.directory() as (exp_path, run_path):
                    # Parse all logs
                    logs = self.profiling_logs(exp_path, run_path)
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

    def _ncpus(self, exp_name: str) -> int:
        """Returns the number of CPUs used by an experiment, parsing it at most once.

        Args:
            exp_name (str): Name of the experiment.

        Returns:
            int: Number of CPUs used by the experiment.
        """
        if exp_name not in self._ncpus_cache:
            with self.experiments[exp_name].directory() as (exp_path, run_path):
                self._ncpus_cache[exp_name] = self.parse_ncpus(exp_path, run_path)
        return self._ncpus_cache[exp_name]

    def select_best_experiments(
        self,
        component: str,
        region: str,
        metric: ProfilingMetric,
        experiments: list[str] | None = None,
    ) -> list[str]:
        """Selects the best performing experiment for each number of CPUs.

        Scaling studies often contain several experiments that use the same number of CPUs, for instance different
        domain decomposition layouts of the same total core count. Plotting all of them produces duplicated ncpus
        coordinates and meaningless speedup and efficiency curves. This method keeps a single experiment per CPU
        count: the one with the smallest value of the given metric, measured on the given region of the given
        component. Smaller is always better.

        The returned list is meant to be passed to the experiments argument of the plotting methods. If two
        experiments with the same number of CPUs have exactly the same value, the first one is kept and a warning
        is logged.

        Args:
            component (str): Name of the component holding the region used to rank experiments.
            region (str): Name of the region used to rank experiments.
            metric (ProfilingMetric): Metric used to rank experiments. The smallest value wins.
            experiments (list[str] | None): Optional list of experiment names to select from. If None, all
                experiments with parsed profiling data are considered.

        Returns:
            list[str]: Names of the selected experiments, one per distinct number of CPUs, ordered by increasing
                number of CPUs.

        Raises:
            KeyError: If an experiment has no parsed profiling data, or if the component, region or metric is not
                available in one of them.
        """
        exp_names = experiments if experiments is not None else list(self.data.keys())

        best: dict[int, tuple[str, float]] = {}
        for exp_name in exp_names:
            value = float(self.data[exp_name][component][metric].sel(region=region).pint.dequantify().values)
            ncpus = self._ncpus(exp_name)
            incumbent = best.get(ncpus)
            if incumbent is None or value < incumbent[1]:
                best[ncpus] = (exp_name, value)
            elif value == incumbent[1]:
                logger.warning(
                    f"Experiments '{incumbent[0]}' and '{exp_name}' have the same {metric} ({value} "
                    f"{metric.units}) for region '{region}' of component '{component}' at {ncpus} CPUs. "
                    f"Keeping '{incumbent[0]}'."
                )

        return [name for _, (name, _) in sorted(best.items())]

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
            experiments (list[str] | None): Optional list of experiment names to include. If None, all experiments
                with parsed profiling data are included.

        Returns:
            Figure: The Matplotlib figure containing the scaling plots.

        Raises:
            ValueError: If no experiments are selected, if a selected experiment has no parsed profiling data, if no
                profiling data is found for a specified component, if a requested region is missing, or if several
                of the selected experiments use the same number of CPUs.
        """

        exp_names = experiments if experiments is not None else list(self.data.keys())
        if not exp_names:
            raise ValueError("No experiments selected for scaling plot.")

        missing_experiments = [exp_name for exp_name in exp_names if exp_name not in self.data]
        if missing_experiments:
            raise ValueError(
                f"No parsed profiling data found for experiment(s): {missing_experiments}. "
                f"Available experiments: {list(self.data.keys())}."
            )

        # Find number of cpus used for each experiment
        ncpus = {exp_name: self._ncpus(exp_name) for exp_name in exp_names}

        # Speedup and efficiency are ill-defined if several experiments share the same number of cpus
        cpu_counts = list(ncpus.values())
        duplicated_ncpus = sorted({n for n in cpu_counts if cpu_counts.count(n) > 1})
        if duplicated_ncpus:
            raise ValueError(
                f"Several selected experiments use the same number of CPUs {duplicated_ncpus}, which makes speedup "
                "and efficiency ill-defined. Use select_best_experiments() to keep only the best performing "
                "experiment for each number of CPUs, or restrict the selection with experiments=[...]."
            )

        # Gather scaling data for each component
        scaling_data = []
        for component, component_regions in zip(components, regions, strict=True):
            component_data = None
            for exp_name in exp_names:
                ds = self.data[exp_name].get(component)
                if ds is None:
                    raise ValueError(f"No profiling data found for component '{component}' in experiment '{exp_name}'.")

                available_regions = ds.coords["region"].values.tolist()
                missing_regions = [region for region in component_regions if region not in available_regions]
                if missing_regions:
                    raise ValueError(
                        f"Requested region(s) {missing_regions} not found for component '{component}' "
                        f"in experiment '{exp_name}'. Available regions: {available_regions}."
                    )

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
        experiment_relabel_map: dict | None = None,
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
            experiment_relabel_map (dict | None): Optional mapping to relabel experiments in the plot.
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

        exp_relabel = experiment_relabel_map or {}
        relabelled_bar_data = {exp_relabel.get(k, k): v for k, v in bar_data.items()}

        return plot_bar_metrics(relabelled_bar_data, region_labels, metric, show=show)
