# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import itertools
import logging
import textwrap
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import xarray as xr
from access.config.parallel_allocation_strategies import RootAllocation
from access.config.parallel_component import ComponentLayout, ParallelComponent
from access.config.parallel_layouts import iter_layouts
from matplotlib.figure import Figure

from access.profiling.experiment import ProfilingExperiment, ProfilingExperimentStatus, ProfilingLog
from access.profiling.metrics import ProfilingMetric
from access.profiling.plotting_utils import plot_bar_metrics
from access.profiling.scaling import plot_component_scaling, plot_scaling_metrics

logger = logging.getLogger(__name__)

_RUN_DIM_ERROR = (
    "Profiling data still has a 'run' dimension. Use select_best_run() to keep the best run of each experiment, "
    "or aggregate_runs() to reduce over runs."
)


@dataclass(frozen=True)
class RegionGroup:
    """Regions of one profiling log, to be read against the cores one component was given.

    A log and a component are not the same thing, which is why both are said here. One log can hold the
    regions of several components - the ACCESS-OM3 ESMF summary is the whole model in one file, and its
    mediator, atmosphere, runoff and waves appear nowhere else - and one component can be spread over
    several logs, as the ACCESS-ESM1.6 atmosphere is over the two the UM file is read into, and as the
    ACCESS-OM3 sea ice is over its own log and the ESMF summary. So a group names the log its regions come
    from and, where it is not the obvious one, the component whose cores they are to be read against.

    Which component a region belongs to cannot be worked out from its name. A bracketed realm like
    "[OCN] RunPhase1" says one thing, but "[OCN-TO-MED] RunPhase1" is a coupler between two and looks the
    same; "cice_run_total" says nothing at all; and the call stack that would settle it is not kept. Hence
    this.

    Args:
        log (str): Name of the profiling log the regions come from, as get_component_logs names it.
        regions (Sequence[str]): Regions of that log to plot, each of which becomes a line.
        component (str | None): Component whose cores these regions are read against, named as the layout
            names it. None (the default) takes the component the manager associates with this log, and
            failing that the log's own name - which is enough for a log that is one component's alone.
    """

    log: str
    regions: Sequence[str] = ()
    component: str | None = None

    def __post_init__(self) -> None:
        # Kept as a tuple, so that a group given a list is still the frozen thing it claims to be.
        object.__setattr__(self, "regions", tuple(self.regions))


def find_component(layout: ComponentLayout, name: str) -> ComponentLayout | None:
    """Returns the part of a layout belonging to the named component, wherever it sits in it.

    A component is looked for at any depth, since a tree groups its components as the model runs them
    rather than as a reader asks about them: the ACCESS-OM3 sea ice, for one, sits under the range it
    shares with the mediator and the atmosphere rather than beside the ocean.

    Args:
        layout (ComponentLayout): Layout to look in, itself included.
        name (str): Name of the component to look for.

    Returns:
        ComponentLayout | None: The layout of that component, or None if it holds no such component.
    """
    if layout.name == name:
        return layout
    for sub_layout in layout.sub_layouts:
        found = find_component(sub_layout, name)
        if found is not None:
            return found
    return None


def _reject_shared_core_counts(component: str, cores: dict[str, int]) -> None:
    """Refuses a set of experiments that would put two points at one place on the x-axis.

    Note that this is a different question from the one plot_scaling_data asks. Layouts agreeing on the
    whole job's size may well disagree about any one component, and layouts differing in size may still
    give one component the same share, so a collision here is neither implied by nor implies one there.

    Args:
        component (str): Name of the component being plotted, for the message.
        cores (dict[str, int]): Cores that component was given, keyed by experiment.

    Raises:
        ValueError: If two or more experiments give the component the same number of cores.
    """
    by_count: dict[int, list[str]] = {}
    for exp_name, count in cores.items():
        by_count.setdefault(count, []).append(exp_name)

    collisions = {count: names for count, names in by_count.items() if len(names) > 1}
    if collisions:
        raise ValueError(
            f"Several experiments give component '{component}' the same number of cores, so they would "
            "share a point on the x-axis: "
            + "; ".join(f"{count} cores: {sorted(names)}" for count, names in sorted(collisions.items()))
            + ". Select among them with the experiments argument."
        )


def _region_label(log: str, region: str, region_relabel_map: dict | None) -> str:
    """Returns the label a region is to be plotted under.

    A region is named after the log it came from, since region names are only made unique within one log
    and two components may each have a "Total". A caller who relabels one has said what they want it
    called, though, so that name is left to stand on its own.

    Args:
        log (str): Name of the log the region came from.
        region (str): Name of the region.
        region_relabel_map (dict | None): Optional mapping from region name to the label to plot it under.

    Returns:
        str: The label.
    """
    if region_relabel_map is not None and region in region_relabel_map:
        return region_relabel_map[region]
    return f"{log}: {region}"


def _component_names(layout: ComponentLayout) -> list[str]:
    """Returns the name of every component in a layout, itself included, for saying what it does hold."""

    names = [layout.name]
    for sub_layout in layout.sub_layouts:
        names.extend(_component_names(sub_layout))
    return names


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

    # The component a log belongs to, where the log is one component's alone and is named something else.
    # This is only what a RegionGroup falls back on when it names no component itself: a log holding the
    # regions of several components has no one entry here, and says which is which group by group.
    _layout_component_names: dict[str, str] = {}

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
    def profiling_logs(self, path: Path, run_path: Path | None = None) -> dict[str, dict[int, ProfilingLog]]:
        """Returns all profiling logs from the specified path.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            dict[str, dict[int, ProfilingLog]]: Dictionary mapping log names to their logs, keyed by run number.
                Configurations with no concept of repeated runs should return a single run, numbered 0.
        """

    @abstractmethod
    def parse_ncpus(self, path: Path, run_path: Path | None = None) -> int:
        """Parses the number of CPUs a given experiment occupied.

        This is what the experiment cost, not what it put to work: schedulers hand out whole compute nodes, so
        a run that gives its components 402 cores on 104 core nodes still occupies, and is charged for, all 416.
        Two layouts that fill the same nodes are the same size for the purposes of a scaling study, however
        differently they divide the cores among the components.

        How to find that number is up to each subclass, since it depends on the workflow engine and on what the
        scheduler recorded.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            int: Number of CPUs the experiment occupied.
        """

    @abstractmethod
    def parse_layout(self, path: Path, run_path: Path | None = None) -> ComponentLayout | None:
        """Parses the layout a given experiment runs, from the configuration it runs it with.

        This is what the components were given, which is the counterpart of parse_ncpus rather than a
        breakdown of it: the cores put to work, component by component, where parse_ncpus reports the whole
        nodes the job was charged for. The two differ by whatever was left idle to fill a node.

        What comes back states the cores, ranks and threads of each component, but not necessarily how any
        of them divided its domain: a configuration says how many ranks a component has far more readily
        than it says what grid they are arranged in. So a parsed layout is for reading rather than for
        generating from - a layout an experiment was generated from is the whole of it, and is kept on the
        experiment instead.

        Returning None means the configuration does not say, which is a missing answer rather than an
        error, as it is for parse_status: a subclass should log the reason at DEBUG and return None rather
        than raise, so that an experiment whose layout cannot be read is one that simply cannot be plotted
        against its components.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            ComponentLayout | None: The layout the experiment runs, or None if it cannot be told.
        """

    @abstractmethod
    def parse_status(self, path: Path, run_path: Path | None = None) -> ProfilingExperimentStatus | None:
        """Parses the state a given experiment's run actually reached.

        Submitting a run says nothing about how it ended, so this reads what the workflow engine recorded and
        reports DONE, FAILED or RUNNING accordingly. How to find that out is up to each subclass, since it
        depends on the engine and on what it writes.

        Returning None means the state could not be determined - no record written yet, or one that cannot be
        read - and leaves the experiment's status as it stands. That is a missing answer rather than an error,
        so a subclass should log the reason at DEBUG and return None rather than raise.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            ProfilingExperimentStatus | None: The state the run reached, or None if it cannot be determined.
        """

    @property
    @abstractmethod
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how the model is parallelised.

        Returns:
            ParallelComponent: Root of the component tree, holding the domains and the requirements that every
                valid layout of this model must satisfy. Requirements specific to a particular study belong in the
                allocation strategy passed to the layout search instead.
        """

    @abstractmethod
    def layout_branch_name(self, layout: ComponentLayout) -> str:
        """Returns the name of the branch holding the experiment for a given layout.

        Args:
            layout (ComponentLayout): Layout of the model components, as returned by the layout search.
        Returns:
            str: Branch name. Must be distinct for every distinct layout, as it is what identifies an experiment.
        """

    @abstractmethod
    def layout_config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run the model with a given layout.

        Args:
            layout (ComponentLayout): Layout of the model components, as returned by the layout search.
        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control directory.
        """

    def select_layouts(
        self,
        total_cores: int,
        allocations: RootAllocation | None = None,
        max_layouts: int | None = None,
    ) -> list[ComponentLayout]:
        """Returns the valid layouts of the model for a given number of cores, fewest idle cores first.

        Args:
            total_cores (int): Total number of cores the layouts must distribute among the model components.
            allocations (RootAllocation | None): Allocation strategy deciding how many cores each component may
                receive, and any further constraints the layouts must satisfy. None (the default) leaves every
                component unconstrained, which is rarely what is wanted: the number of valid layouts grows very
                quickly with the number of cores.
            max_layouts (int | None): Maximum number of layouts to enumerate. None (the default) enumerates all of
                them. Note that this bounds the *enumeration*, so the returned layouts are the first ones found and
                not necessarily those with the fewest idle cores. Returning the best ones instead would mean
                ranking the whole result set, which is the cost this argument exists to avoid: the search is lazy
                and layout counts reach millions at production core counts. Sorting is applied to what was
                enumerated, so it orders the returned layouts without deciding which ones they are.
        Returns:
            list[ComponentLayout]: The layouts found, sorted by increasing number of idle cores.
        """
        layouts = iter_layouts(self.parallel_component, total_cores, allocations=allocations)
        if max_layouts is not None:
            layouts = itertools.islice(layouts, max_layouts + 1)
        found = list(layouts)
        if max_layouts is not None and len(found) > max_layouts:
            logger.warning(
                f"More than {max_layouts} layouts found for {total_cores} cores. Only the first {max_layouts} "
                "enumerated will be used, which are not necessarily the ones with the fewest idle cores: "
                "finding those would mean enumerating all of them. Tighten the allocation strategy to choose "
                "which layouts are found rather than how many."
            )
            found = found[:max_layouts]
        return sorted(found, key=lambda layout: layout.idle_cores)

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
        # Only DONE experiments are archived, so a run that finished since the last look should be counted
        # among them rather than skipped as still running.
        self.update_statuses()

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
        """Parses profiling data from the experiments.

        Configurations that can be run several times produce one set of profiling logs per run. In that case the
        parsed datasets are concatenated along a 'run' dimension, coordinated by the run number reported by the
        configuration. Experiments with a single run are stored without a 'run' dimension, so that they can be used
        directly. Use select_best_run() or aggregate_runs() to reduce the 'run' dimension before plotting. Note that
        if all but one run fail to produce a log, the result has no 'run' dimension.
        """
        # A run that finished since the last look is one whose data is wanted now, so ask before deciding
        # which experiments have any.
        self.update_statuses()

        self.data = {}
        for exp_name, exp in self.experiments.items():
            if exp.status == ProfilingExperimentStatus.DONE or exp.status == ProfilingExperimentStatus.ARCHIVED:
                logger.info(f"Parsing profiling data for experiment '{exp_name}'.")
                self.data[exp_name] = {}
                with exp.directory() as (exp_path, run_path):
                    # Parse all logs
                    logs = self.profiling_logs(exp_path, run_path)
                    for log_name, run_logs in logs.items():
                        datasets = {}
                        for run, log in run_logs.items():
                            logger.info(f"Parsing {log_name} profiling log for run {run}: {log.filepath}. ")
                            if log.optional:
                                try:
                                    datasets[run] = log.parse()
                                except FileNotFoundError:
                                    logger.info(f"Optional profiling log '{log.filepath}' not found. Skipping.")
                                    continue
                                except Exception as e:
                                    # might be useful to make this a warning instead of info to help catch parse
                                    # failures for logs that should've succeeded.
                                    logger.info(
                                        f"Failed to parse optional profiling log '{log.filepath}' with exception:\n"
                                        f"    {e}\nSkipping."
                                    )
                                    continue
                            else:
                                datasets[run] = log.parse()
                            logger.info(" Done.")
                        # A single run is stored as is; several runs are concatenated along a new 'run' dimension.
                        if len(datasets) == 1:
                            self.data[exp_name][log_name] = next(iter(datasets.values()))
                        elif datasets:
                            self.data[exp_name][log_name] = xr.concat(
                                [ds.expand_dims({"run": [run]}) for run, ds in sorted(datasets.items())],
                                dim="run",
                                join="outer",
                            )
            else:
                logger.warning(
                    f"Experiment '{exp_name}' is not completed (status: {exp.status.name}). Skipping parsing profiling "
                    "data."
                )

    def _ncpus(self, exp_name: str) -> int:
        """Returns the number of CPUs occupied by an experiment, parsing it at most once.

        The count is kept on the experiment itself, so it lasts exactly as long as the experiment does: an
        experiment added under a name used by a deleted one is parsed afresh rather than inheriting what the
        old one used, and an archived experiment keeps the count parsed before it was archived instead of
        having its tarball extracted again to read the same answer back.

        Args:
            exp_name (str): Name of the experiment.

        Returns:
            int: Number of CPUs the experiment occupied, as reported by parse_ncpus.
        """
        experiment = self.experiments[exp_name]
        if experiment.ncpus is None:
            with experiment.directory() as (exp_path, run_path):
                experiment.ncpus = self.parse_ncpus(exp_path, run_path)
        return experiment.ncpus

    def _layout(self, exp_name: str) -> ComponentLayout | None:
        """Returns the layout an experiment runs, reading it back at most once.

        An experiment generated from a layout search already carries the layout it was generated from, and
        is answered from that: it is the whole of it, and nothing on disk says more. Only an experiment
        this manager did not generate is read back from its configuration, and what that gives is kept on
        the experiment afterwards, for the same reasons the CPU count is.

        Args:
            exp_name (str): Name of the experiment.

        Returns:
            ComponentLayout | None: The layout the experiment runs, or None if it cannot be told.
        """
        experiment = self.experiments[exp_name]
        if experiment.layout is None:
            with experiment.directory() as (exp_path, run_path):
                experiment.layout = self.parse_layout(exp_path, run_path)
        return experiment.layout

    def update_statuses(self) -> None:
        """Brings the status of every experiment up to date with the state its run actually reached.

        Every experiment is consulted but the archived ones, and each of the others for its own reason. A
        status lives in memory and so does not outlive the session, which means NEW says only that this
        manager has not submitted the experiment, not that it has never run. A failed experiment may have been
        fixed and run again by hand. A finished one may have been run again deliberately - parse_status reads
        the newest run, so a run under way reads back as running, which is what keeps parse_profiling_data off
        its half-written output and archive_experiments from tarring it mid-run.

        Only an archived experiment is settled: its tarball does not change, the directory it was made from
        may be gone, and asking would mean extracting the archive to be told what is already known.

        The answer is never cached, unlike the CPU count in _ncpus - a count does not change once a run is
        over, whereas the whole point of a status is that it does.

        An experiment whose state cannot be determined is left as it stands, so an experiment not yet run and
        one whose records cannot be read both simply keep the status they had.
        """
        for name, experiment in self.experiments.items():
            if experiment.status == ProfilingExperimentStatus.ARCHIVED:
                continue
            with experiment.directory() as (exp_path, run_path):
                status = self.parse_status(exp_path, run_path)
            if status is None:
                logger.debug(f"Could not determine the state of the run of experiment '{name}'. Leaving it be.")
                continue
            if status != experiment.status:
                logger.info(f"Experiment '{name}' is now {status.name}.")
                experiment.status = status

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

        Experiments are grouped by the number of CPUs they occupied, which is what parse_ncpus reports, so two
        layouts that fill the same compute nodes compete with each other even when they hand different numbers of
        cores to the model components.

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
            ValueError: If the profiling data still has a 'run' dimension.
        """
        exp_names = experiments if experiments is not None else list(self.data.keys())

        best: dict[int, tuple[str, float]] = {}
        for exp_name in exp_names:
            ds = self.data[exp_name][component]
            if "run" in ds.dims:
                raise ValueError(_RUN_DIM_ERROR)
            value = float(ds[metric].sel(region=region).pint.dequantify().values)
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

    def select_best_run(self, component: str, region: str, metric: ProfilingMetric) -> None:
        """Keeps only the best performing run of each experiment, discarding the others.

        Configurations that can be run several times produce profiling data with a 'run' dimension. This method
        keeps a single run per experiment: the one with the smallest value of the given metric, measured on the
        given region of the given component. Smaller is always better.

        The chosen run is selected in *every* component of the experiment, so the result always describes a single
        run that actually took place, and never mixes measurements taken during different runs. Use
        aggregate_runs() instead to compute statistics over the runs.

        Datasets without a 'run' dimension are left untouched, so this is safe to call on any manager. The reduction
        is destructive: call parse_profiling_data() again to recover the individual runs.

        Args:
            component (str): Name of the component holding the region used to rank runs.
            region (str): Name of the region used to rank runs.
            metric (ProfilingMetric): Metric used to rank runs. The smallest value wins.

        Raises:
            KeyError: If the component, region or metric is not available in one of the experiments, or if the
                chosen run is missing from one of the components of that experiment.
        """
        for exp_name, components in self.data.items():
            ranking = components[component]
            if "run" not in ranking.dims:
                continue
            values = ranking[metric].sel(region=region).pint.dequantify()
            run = int(values.run.values[int(values.argmin("run"))])
            logger.info(f"Keeping run {run} of experiment '{exp_name}'.")
            self.data[exp_name] = {
                name: ds.sel(run=run, drop=True) if "run" in ds.dims else ds for name, ds in components.items()
            }

    def aggregate_runs(self, how: str = "min") -> None:
        """Reduces the 'run' dimension of every experiment with the given statistic.

        Configurations that can be run several times produce profiling data with a 'run' dimension. This method
        collapses it, computing the requested statistic over the runs.

        Note that each region and each metric is reduced independently, so unlike select_best_run() the result does
        not correspond to any run that actually took place: with how="min", different regions can come from
        different runs. Use "min" to estimate the best achievable timings and "mean" or "median" to describe the
        typical ones. Integer metrics, such as call counts, become floats.

        Datasets without a 'run' dimension are left untouched, so this is safe to call on any manager. The reduction
        is destructive: call parse_profiling_data() again to recover the individual runs.

        Args:
            how (str): Statistic to compute over the runs. One of "min", "mean" or "median". Defaults to "min".

        Raises:
            ValueError: If how is not one of the supported statistics.
        """
        if how not in ("min", "mean", "median"):
            raise ValueError(f"Unknown reduction '{how}'. Use 'min', 'mean' or 'median'.")
        for exp_name, components in self.data.items():
            self.data[exp_name] = {
                name: getattr(ds, how)("run") if "run" in ds.dims else ds for name, ds in components.items()
            }

    def plot_scaling_data(
        self,
        components: list[str],
        regions: list[list[str]],
        metric: ProfilingMetric,
        region_relabel_map: dict | None = None,
        experiments: list[str] | None = None,
        xlabel: str | None = None,
    ) -> Figure:
        """Plots scaling data for the specified components, regions and metric.

        Args:
            components (list[str]): List of component names to plot.
            regions (list[list[str]]): List of regions to plot for each component.
            metric (ProfilingMetric): Metric to use for the scaling plots.
            region_relabel_map (dict | None): Optional mapping to relabel regions in the plots.
            experiments (list[str] | None): Optional list of experiment names to include. If None, all experiments
                with parsed profiling data are included.
            xlabel (str | None): Optional custom label for the x-axis of both subplots. If None, the
                default label derived from the x-coordinate is kept.

        Returns:
            Figure: The Matplotlib figure containing the scaling plots.

        Raises:
            ValueError: If no experiments are selected, if a selected experiment has no parsed profiling data, if no
                profiling data is found for a specified component, if a requested region is missing, if the
                profiling data still has a 'run' dimension, or if several of the selected experiments use the same
                number of CPUs.
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

        if any("run" in ds.dims for exp_name in exp_names for ds in self.data[exp_name].values()):
            raise ValueError(_RUN_DIM_ERROR)

        # Find number of cpus used for each experiment
        ncpus = {exp_name: self._ncpus(exp_name) for exp_name in exp_names}

        # Speedup and efficiency are ill-defined if several experiments share the same number of cpus. The counts
        # are the occupied ones, so two layouts filling the same nodes collide here even if their component core
        # counts differ; select_best_experiments() groups them the same way and is the way to resolve this.
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
            component_data = []
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
                component_data.append(ds.expand_dims({"ncpus": [ncpus[exp_name]]}))

            # Concatenate data along ncpus dimension
            scaling_data.append(xr.concat(component_data, dim="ncpus", join="outer").sortby("ncpus"))

        return plot_scaling_metrics(scaling_data, metric, xlabel=xlabel)

    def _component_cores(self, exp_name: str, component: str) -> int:
        """Returns the cores an experiment gave one of its components.

        Args:
            exp_name (str): Name of the experiment.
            component (str): Name of the component, as the layout names it.

        Returns:
            int: Number of cores that component was given.

        Raises:
            ValueError: If the experiment's layout cannot be told, or names no such component.
            NotImplementedError: If this manager's configurations have no layout to read.
        """
        layout = self._layout(exp_name)
        if layout is None:
            raise ValueError(
                f"The layout of experiment '{exp_name}' could not be read, so there is nothing to say how "
                f"many cores it gave '{component}'."
            )

        found = find_component(layout, component)
        if found is None:
            raise ValueError(
                f"The layout of experiment '{exp_name}' holds no component '{component}'. It holds "
                f"{sorted(_component_names(layout))}."
            )
        return found.n_cores

    def _group_component(self, group: RegionGroup) -> str:
        """Returns the component a group of regions is to be read against.

        Args:
            group (RegionGroup): The group.

        Returns:
            str: The component, as the layout names it.
        """
        if group.component is not None:
            return group.component
        return self._layout_component_names.get(group.log, group.log)

    def _group_data(self, group: RegionGroup, exp_names: list[str], region_relabel_map: dict | None) -> dict:
        """Returns the regions of a group for each experiment, keyed by experiment.

        Args:
            group (RegionGroup): Regions to select, and the log to select them from.
            exp_names (list[str]): Experiments to select them for.
            region_relabel_map (dict | None): Optional mapping from region name to the label to plot it under.

        Returns:
            dict: The selected data, keyed by experiment name.

        Raises:
            ValueError: If an experiment has no data for the group's log, or lacks one of its regions.
        """
        selected = {}
        for exp_name in exp_names:
            ds = self.data[exp_name].get(group.log)
            if ds is None:
                raise ValueError(f"No profiling data found for '{group.log}' in experiment '{exp_name}'.")

            available_regions = ds.coords["region"].values.tolist()
            missing_regions = [region for region in group.regions if region not in available_regions]
            if missing_regions:
                raise ValueError(
                    f"Regions {missing_regions} not found in '{group.log}' for experiment '{exp_name}'. "
                    f"Available regions: {available_regions}."
                )

            ds = ds.sel(region=list(group.regions))
            # From here the coordinate is the label the curve will carry rather than the name of a region:
            # the dataset exists only to be plotted, and settling the label here is what lets a relabelled
            # region stand on its own, which the plot could not know to do.
            ds = ds.assign_coords(
                region=[_region_label(group.log, region, region_relabel_map) for region in ds.region.values]
            )
            selected[exp_name] = ds
        return selected

    def plot_component_scaling_data(
        self,
        groups: list[RegionGroup],
        metric: ProfilingMetric,
        region_relabel_map: dict | None = None,
        experiments: list[str] | None = None,
        xlabel: str | None = None,
        ylabel: str | None = None,
        show: bool = True,
    ) -> Figure:
        """Plots a metric for the given regions against the cores their own component was given.

        Where plot_scaling_data reads every component against the whole job, this reads each against its
        own cores. That is the question to ask of a component whose share of the budget is what changed:
        an ocean given half again as many cores either ran faster for it or did not, and the total the job
        occupied says nothing about which.

        Each group names the log its regions come from and the component they belong to, because the two
        are not the same: one log can hold several components' regions and one component can be spread over
        several logs. See RegionGroup.

        The regions of a group are plotted as their own lines rather than added together, since the timers
        most of these models write are inclusive - a region and the region it sits inside both count the
        same seconds, and adding them would count them twice.

        Note that a region can be a wait rather than work, and will scale backwards if it is. MOM5's
        oasis_recv and the ACCESS-OM3 couplers go up as their component is given more cores, because it
        finishes its own work sooner and waits longer on the components that have not.

        Args:
            groups (list[RegionGroup]): Regions to plot, and what to read each of them against.
            metric (ProfilingMetric): The metric to plot.
            region_relabel_map (dict | None): Optional mapping from region name to the label to plot it
                under. A region named here is plotted under that label alone; one left out is plotted as
                "<log>: <region>", since region names are only made unique within one log.
            experiments (list[str] | None): Experiments to plot. None (the default) plots all of those
                with parsed profiling data.
            xlabel (str | None): Optional label for the x-axis.
            ylabel (str | None): Optional label for the y-axis. If None, the metric names it.
            show (bool): Whether to show the generated plot. Default: True.

        Returns:
            Figure: The Matplotlib figure the lines are plotted on.

        Raises:
            ValueError: If no experiments are selected, if a selected experiment has no parsed profiling
                data, if no profiling data is found for a group's log, if a requested region is missing, if
                the profiling data still has a 'run' dimension, if an experiment's layout cannot be told or
                names no such component, or if two experiments give one component the same number of cores.
            NotImplementedError: If this manager's configurations have no layout to read, as Cylc Rose ones
                do not. There is nothing for this plot to put on its x-axis in that case.
        """
        exp_names = experiments if experiments is not None else list(self.data.keys())
        if not exp_names:
            raise ValueError("No experiments selected for scaling plot.")

        missing_data = [exp_name for exp_name in exp_names if exp_name not in self.data]
        if missing_data:
            raise ValueError(f"No parsed profiling data for experiments: {sorted(missing_data)}.")

        if any("run" in ds.dims for exp_name in exp_names for ds in self.data[exp_name].values()):
            raise ValueError(_RUN_DIM_ERROR)

        # A list rather than a mapping: neither the log nor the component is unique across groups, since
        # one of each is exactly what the other kind of group is for.
        scaling_data = []
        for group in groups:
            # What was asked for is settled first, since a log or a region this manager holds nothing for
            # is a plainer thing to be told than anything about cores.
            selected = self._group_data(group, exp_names, region_relabel_map)

            # Then where each of them goes: the cores this group's component was given are its x axis.
            component = self._group_component(group)
            cores = {exp_name: self._component_cores(exp_name, component) for exp_name in exp_names}
            _reject_shared_core_counts(component, cores)

            group_data = [ds.expand_dims({"ncpus": [cores[exp_name]]}) for exp_name, ds in selected.items()]
            scaling_data.append((group.log, xr.concat(group_data, dim="ncpus", join="outer").sortby("ncpus")))

        return plot_component_scaling(scaling_data, metric, xlabel=xlabel, ylabel=ylabel, show=show)

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
            ValueError: If no profiling data is found for a specified component in any experiment, or if the
                profiling data still has a 'run' dimension.
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
                if "run" in ds.dims:
                    raise ValueError(_RUN_DIM_ERROR)
                values.append(float(ds[metric].sel(region=region).pint.dequantify().values))
            bar_data[exp_name] = values

        exp_relabel = experiment_relabel_map or {}
        relabelled_bar_data = {exp_relabel.get(k, k): v for k, v in bar_data.items()}

        return plot_bar_metrics(relabelled_bar_data, region_labels, metric, show=show)
