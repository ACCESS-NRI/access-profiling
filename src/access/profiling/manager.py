# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import itertools
import logging
import textwrap
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, TypeVar

import xarray as xr
from access.config.parallel_allocation_strategies import RootAllocation
from access.config.parallel_component import ComponentLayout, ParallelComponent
from access.config.parallel_layouts import iter_layouts
from matplotlib.figure import Figure

from access.profiling.application import Application
from access.profiling.control import ControlSource
from access.profiling.experiment import (
    ExperimentPlan,
    ProfilingExperiment,
    ProfilingExperimentStatus,
    ProfilingLog,
)
from access.profiling.metrics import ProfilingMetric
from access.profiling.plotting_utils import plot_bar_metrics
from access.profiling.scaling import plot_component_scaling, plot_scaling_metrics

logger = logging.getLogger(__name__)

# What a manager is profiling. A manager is written once per workflow engine, and each engine drives a
# particular kind of application - Payu drives a PayuConfiguration, a Cylc Rose suite a
# RoseSuiteConfiguration - so the engine knows the concrete type and should not have to rediscover it.
AppT = TypeVar("AppT", bound=Application)

_RUN_DIM_ERROR = (
    "Profiling data still has a 'run' dimension. Use select_best_run() to keep the best run of each experiment, "
    "or aggregate_runs() to reduce over runs."
)


@dataclass(frozen=True)
class RegionGroup:
    """Regions of one profiling log, to be read against the cores one component was given.

    A log and a component are not the same thing, which is why both are said here. One log can hold the
    regions of several components - a coupled framework often writes one summary covering the whole run,
    and some components appear nowhere else - and one component can be spread over several logs, either
    because its single file is read by two parsers or because it also appears in such a summary. So a group
    names the log its regions come from and, where it is not the obvious one, the component whose cores
    they are to be read against.

    Which component a region belongs to cannot be worked out from its name. A name prefixed with one
    component says one thing, but a name prefixed with a pair of them is a coupler between two and looks the
    same; many names say nothing at all; and the call stack that would settle it is not kept. Hence this.

    Args:
        log (str): Name of the profiling log the regions come from, as the configuration's LogSpec names it.
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

    A component is looked for at any depth, since a tree groups its components as the application runs them
    rather than as a reader asks about them: a component that takes its turn on a range of cores shared with
    others sits under that range, not beside the components running concurrently with it.

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


def _note_shared_core_counts(component: str, cores: dict[str, int]) -> None:
    """Says which experiments measured a component on the same number of cores as another.

    They are all kept - the fastest of them is what gets plotted - but a reader of the figure should be
    told that a point on it is the best of several rather than the only measurement there was.

    Note that this is a different question from the one plot_scaling_data asks. Layouts agreeing on the
    whole job's size may well disagree about any one component, and layouts differing in size may still
    give one component the same share, so a collision here is neither implied by nor implies one there.
    That is exactly why it happens: a study dividing a fixed budget differently, or varying the budget
    while holding one component's share, measures that component twice on the same cores.

    Args:
        component (str): Name of the component being plotted, for the message.
        cores (dict[str, int]): Cores that component was given, keyed by experiment.
    """
    by_count: dict[int, list[str]] = {}
    for exp_name, count in cores.items():
        by_count.setdefault(count, []).append(exp_name)

    for count, names in sorted(by_count.items()):
        if len(names) > 1:
            logger.info(
                f"Experiments {sorted(names)} all give component '{component}' {count} cores. Plotting the "
                "fastest of them at that point."
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


class ProfilingManager(ABC, Generic[AppT]):
    """Abstract base class to handle profiling data and workflows.

    This high-level class defines methods to parse different types of profiling data. Currently,
    it supports parsing and plotting scaling data, including selecting the best performing experiment
    for each number of CPUs.

    A manager is the workflow engine, and nothing about any one application: what is being profiled arrives
    as an Application and what it is profiled against as a ControlSource. So a subclass is written once
    per engine rather than once per application, and a new application, or a new setup of one, is data rather
    than code.

    Which of the two answers a question is decided by what wrote the thing being read: the manager reads what
    the engine and the scheduler wrote - the logs, the job records, the CPU count the scheduler charged for -
    and the application reads what its own input files say.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        application (AppT): The application being profiled, which says how it is parallelised, what it
            writes and how a layout is realised in its own input files.
        control (ControlSource | None): Where the control every experiment perturbs comes from.
            None (the default) is enough to read and plot experiments that already exist, and is rejected by
            generate_scaling_experiments, which has nothing to perturb without it.
    """

    work_dir: Path  # Working directory where profiling experiments will be generated and run.
    archive_dir: Path  # Directory where completed experiments will be archived.
    experiments: dict[str, ProfilingExperiment]  # Dictionary storing ProfilingExperiment instances.
    data: dict[
        str, dict[str, xr.Dataset]
    ]  # Dictionary mapping experiments to component names and their profiling datasets.

    def __init__(
        self,
        work_dir: Path,
        archive_dir: Path,
        application: AppT,
        control: ControlSource | None = None,
    ):
        super().__init__()
        self.work_dir = work_dir
        self.archive_dir = archive_dir
        self._application = application
        self._control = control
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
        summary += indent + f"Application: {self.application.name}\n"
        if self.control is not None:
            summary += indent + f"Control: {self.control.label}\n"
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

    @property
    def application(self) -> AppT:
        """Returns the application being profiled.

        Returns:
            AppT: The application, as the concrete type this manager's runner drives.
        """
        return self._application

    @property
    def control(self) -> ControlSource | None:
        """Returns where the control configuration every experiment perturbs comes from.

        Returns:
            ControlSource | None: The control, or None if this manager was given none.
        """
        return self._control

    @control.setter
    def control(self, value: ControlSource | None) -> None:
        """Sets where the control configuration comes from.

        Settable so that one manager can profile the same configuration against several controls in turn -
        two releases, or a release and a fork - without being rebuilt and losing the experiments it holds.

        Args:
            value (ControlSource | None): The control.
        """
        self._control = value

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
        a run that gives its components 402 cores on 104 core nodes still occupies, and is charged for, all
        416.
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
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how the model is parallelised.

        Returns:
            ParallelComponent: Root of the component tree, as the configuration being profiled states it. It
                holds the domains and the requirements that every valid layout of that configuration must
                satisfy; requirements specific to a particular study belong in the allocation strategy passed
                to the layout search instead.
        """
        return self.application.parallel_component

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

    @staticmethod
    def _validate_sizing(num_nodes_list: list[float], cores_per_node: int) -> None:
        """Rejects the sizes no layout search could be made for.

        Everything is checked before the first search, so that a bad value late in the list costs nothing and
        generate_scaling_experiments either generates all the experiments it was asked for or none of them.

        Args:
            num_nodes_list (list[float]): Numbers of nodes to generate experiments for.
            cores_per_node (int): Number of cores available on each node.

        Raises:
            ValueError: If cores_per_node is not a positive integer, or if any of the node counts is not
                positive.
        """
        if not isinstance(cores_per_node, int) or cores_per_node <= 0:
            raise ValueError(f"Cores per node must be a positive integer. Got {cores_per_node} instead")

        for num_nodes in num_nodes_list:
            if num_nodes <= 0:
                raise ValueError(f"Number of nodes must be > 0. Got {num_nodes} instead")

    def _plan_experiments(
        self,
        num_nodes_list: list[float],
        cores_per_node: int,
        walltime: float | Callable[[float], float],
        allocations: RootAllocation | Callable[[float], RootAllocation] | None,
        max_layouts: int | None,
        regenerate_failed: bool,
    ) -> list[ExperimentPlan]:
        """Returns the experiments a scaling study is to create, one per layout it has not covered yet.

        What generate_scaling_experiments decides, separated from what it then does with it: nothing here
        touches the working directory, so what a study would generate can be worked out without generating it.

        Args:
            num_nodes_list (list[float]): Numbers of nodes to generate experiments for.
            cores_per_node (int): Number of cores available on each node.
            walltime (float | Callable[[float], float]): Walltime in hours, or a function of the node count.
            allocations (RootAllocation | Callable[[float], RootAllocation] | None): Allocation strategy, or a
                function of the node count, or None to leave every component unconstrained.
            max_layouts (int | None): Maximum number of layouts to enumerate for each number of nodes.
            regenerate_failed (bool): Whether to plan an experiment this manager already holds again when its
                run failed.

        Returns:
            list[ExperimentPlan]: What to create, in the order the sizes were asked for.
        """
        plans: list[ExperimentPlan] = []
        # What this call has already covered. self.experiments on its own does not say, the registration
        # being deferred until the engine has returned, and the same layout is regularly valid at two
        # different node counts.
        planned: set[str] = set()
        for num_nodes in num_nodes_list:
            total_cores = int(num_nodes * cores_per_node)
            layouts = self.select_layouts(
                total_cores,
                allocations=allocations(num_nodes) if callable(allocations) else allocations,
                max_layouts=max_layouts,
            )
            if not layouts:
                logger.warning(
                    f"No layouts found for {num_nodes} nodes ({total_cores} cores). Check the bounds and the "
                    "constraints of the allocation strategy."
                )
                continue
            logger.info(f"Found {len(layouts)} layouts for {num_nodes} nodes ({total_cores} cores).")

            walltime_hrs = walltime(num_nodes) if callable(walltime) else walltime

            for layout in layouts:
                name = self.application.experiment_name(layout)
                if name in planned:
                    continue

                existing = self.experiments.get(name)
                if existing is not None and not (
                    regenerate_failed and existing.status == ProfilingExperimentStatus.FAILED
                ):
                    logger.info(f"Experiment {name} already exists. Skipping addition.")
                    continue
                if existing is not None:
                    logger.info(f"Regenerating experiment {name}, whose run failed.")

                planned.add(name)
                plans.append(
                    ExperimentPlan(
                        name=name,
                        layout=layout,
                        changes=self.application.config_changes(layout),
                        walltime_hours=walltime_hrs,
                        regenerating=existing is not None,
                    )
                )

        return plans

    def generate_scaling_experiments(
        self,
        num_nodes_list: list[float],
        cores_per_node: int,
        walltime: float | Callable[[float], float],
        allocations: RootAllocation | Callable[[float], RootAllocation] | None = None,
        max_layouts: int | None = None,
        regenerate_failed: bool = False,
        **runner_options,
    ) -> None:
        """Generates scaling experiments, one per valid layout of the configuration being profiled.

        For each requested number of nodes, the valid layouts are enumerated and each one becomes an
        experiment. Layouts whose experiment is already known to this manager, or was already planned earlier
        in the same call, are skipped, so the same layout found for two different numbers of nodes only
        generates one experiment.

        That happens whenever a layout fits both budgets, which is a matter of how much waste the component
        tree tolerates: a layout spending 508 cores is valid on 520 and on 546, leaving 2.3% and 7.0% of them
        idle. Note that it does not arise for an allocation strategy written in fractions of the total, since
        its bounds move with the budget; it is strategies stated in cores, and callables, that repeat
        themselves.

        An experiment that failed may be generated again, which is what regenerate_failed asks for. The
        changes each experiment makes are worked out afresh from the arguments of this call rather than
        replayed from what was generated before, so correcting whatever produced the failure - the walltime,
        the allocation, the configuration's own config_changes - and calling this method again is what puts
        the correction on the experiment.

        Regenerating leaves the status alone, so an experiment that failed still reads as failed: what a run
        did is the run's to say, not generation's, and the record of the failure is still there to be read.
        Running it again therefore takes run_experiments(retry_failed=True), which is also what clears that
        record.

        Experiments become known to this manager only once the engine has created them, since an experiment
        it holds is one that can be run, archived and deleted. A generation that fails therefore registers
        nothing, not even what the engine managed to create before failing, and it is the whole call that is
        abandoned: correcting the cause and calling this method again generates the rest. A regenerated
        experiment is already registered and so survives such a failure, whether or not the engine reached it.

        Deciding what to generate is the same question for every workflow engine and is answered here;
        creating it is each engine's own and is left to _create_experiments.

        Args:
            num_nodes_list (list[float]): Numbers of nodes to generate experiments for. Fractional values are
                allowed; the number of cores the layouts are searched for is the product with cores_per_node,
                truncated to an integer.
            cores_per_node (int): Number of cores available on each node. Must be a positive integer.
            walltime (float | Callable[[float], float]): Walltime in hours to request for each experiment,
                either as a fixed value or as a function of the number of nodes.
            allocations (RootAllocation | Callable[[float], RootAllocation] | None): Allocation strategy
                deciding how many cores each component may receive, either as a single strategy or as a
                function of the number of nodes. A single strategy is usually enough, since the bounds of an
                allocation may be written as fractions of the total core count, which resolve to a different
                number of cores at every size in the study. Pass a function only where a bound cannot be
                expressed that way; note that allocation bounds are in cores, so it typically multiplies by
                cores_per_node itself. None (the default) leaves every component unconstrained.
            max_layouts (int | None): Maximum number of layouts to enumerate for each number of nodes. None
                (the default) enumerates all of them.
            regenerate_failed (bool): Whether to generate an experiment this manager already holds again when
                its run failed, so that a correction reaches it. False (the default) skips every experiment
                already held, whatever became of it. Only failed ones are ever regenerated: a running one
                would be rewritten underneath a live job, and one that finished has results that its
                configuration should go on matching.
            **runner_options: Further options for the workflow engine, passed through to _create_experiments.

        Raises:
            ValueError: If this manager has no control, if cores_per_node is not a positive integer, or if any
                of the node counts is not positive.
            Exception: Whatever the workflow engine raises, unchanged. No experiment is registered when it
                does.
        """
        if self.control is None:
            raise ValueError(
                "Cannot generate experiments without a control: every experiment is a perturbation of one, so "
                "there is nothing to generate from. Pass a ControlSource to this manager."
            )
        self._validate_sizing(num_nodes_list, cores_per_node)
        plans = self._plan_experiments(
            num_nodes_list, cores_per_node, walltime, allocations, max_layouts, regenerate_failed
        )

        if not plans:
            logger.warning("No new experiments to generate. Will skip generation.")
            return

        try:
            created = self._create_experiments(plans, **runner_options)
        except Exception:
            # How far the engine got is unknown, so nothing new is registered: an experiment this manager has
            # never heard of is simply generated again on the next call, whereas one registered without
            # anything on disk behind it would be submitted by run_experiments() and would never be
            # generated, the check above having claimed it.
            new_names = sorted(plan.name for plan in plans if not plan.regenerating)
            logger.warning(
                f"Experiment generation failed, so none of {new_names} were added to this manager. Some may "
                "already exist in the working directory; correcting the cause and calling this method again "
                "generates the rest and leaves those alone."
            )
            regenerating = sorted(plan.name for plan in plans if plan.regenerating)
            if regenerating:
                logger.warning(
                    f"The experiments being regenerated, {regenerating}, are still held by this manager, "
                    "since they were already. How far the engine got through them is unknown, so some may "
                    "carry the correction and others not."
                )
            raise

        self.experiments.update(created)

    @abstractmethod
    def _create_experiments(self, plans: list[ExperimentPlan], **runner_options) -> dict[str, ProfilingExperiment]:
        """Creates the planned experiments with this manager's workflow engine.

        Called once with everything generate_scaling_experiments decided to create, so that an engine which
        sets up many experiments in one invocation is invoked once.
        Raising abandons the whole call and registers nothing.

        Args:
            plans (list[ExperimentPlan]): What to create. A plan marked as regenerating corrects an experiment
                this manager already holds, so the engine is to apply its changes to what is already there
                rather than start it over.
            **runner_options: Whatever generate_scaling_experiments was passed for this engine.

        Returns:
            dict[str, ProfilingExperiment]: The experiments this manager is to start holding, keyed by name.
                The ones being regenerated are left out, being held already.
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
        names_to_delete = existing if all_experiments else set(experiments or ())
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
                        datasets: dict[int, xr.Dataset] = {}
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
            with experiment.directory() as (exp_path, _):
                experiment.layout = self.application.parse_layout(exp_path)
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
        # A log the configuration declares a component for is read against that component; one it does not -
        # a coupled profile covering several, or a log of another configuration entirely - is read against
        # its own name, and a caller naming regions says which component each group is.
        return self.application.component_for_log(group.log) or group.log

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
        a component given half again as many cores either ran faster for it or did not, and the total the
        job occupied says nothing about which.

        Each group names the log its regions come from and the component they belong to, because the two
        are not the same: one log can hold several components' regions and one component can be spread over
        several logs. See RegionGroup.

        The regions of a group are plotted as their own lines rather than added together, since timers are
        usually inclusive - a region and the region it sits inside both count the same seconds, and adding
        them would count them twice.

        Note that a region can be a wait rather than work, and will scale backwards if it is. A region that
        receives from a coupled peer goes up as its own component is given more cores, because it finishes
        its own work sooner and waits longer on the components that have not.

        A study readily measures one component twice on the same number of cores, since a layout giving it
        a certain share says nothing about the size of the job around it. Where it has, the fastest of
        those measurements is what is plotted, and the rest are reported at INFO. The fastest is taken
        region by region, so with several regions in a group the point at that core count may take its
        value for one region from one run and for another from another: each curve answers how fast its
        own region went on that many cores, and deciding a single winning run would mean picking a region
        to decide it by.

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
                the profiling data still has a 'run' dimension, or if an experiment's layout cannot be told
                or names no such component.
            NotImplementedError: If the application being profiled states no layout, so that what each
                component was given cannot be told. There is nothing for this plot to put on its x-axis
                in that case.
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
            _note_shared_core_counts(component, cores)

            # Grouping by the cores is what reduces the experiments that share a count to the fastest of
            # them, and it orders the result as it goes, so there is nothing left to sort.
            group_data = [ds.expand_dims({"ncpus": [cores[exp_name]]}) for exp_name, ds in selected.items()]
            scaling_data.append((group.log, xr.concat(group_data, dim="ncpus", join="outer").groupby("ncpus").min()))

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
