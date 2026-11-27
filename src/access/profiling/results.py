# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The profiling data of a study once it is parsed, and what is selected, tabulated and plotted from it.

`ProfilingManager.parse_profiling_data` returns a `ProfilingResults`: what the logs of every finished experiment hold,
with what tells each experiment from the others, its labels, its layout and the CPUs its job occupied. They are read
when the experiment is parsed, while its directory is open, so that nothing is extracted from an archive again, and
the results need no manager to be read.

Nothing changes a `ProfilingResults` once it is made. A selection or a reduction returns new results, and leaves the
ones it was called on as they were. A notebook can therefore keep the runs of a study, their best run and their mean
side by side, and a cell gives the same answer whichever cells ran before it.

A `run` dimension appears only where more than one run produced a log, and the selections, tables and plots refuse
it until `select_best_run` or `aggregate_runs` has removed it. The two answer different questions: the first keeps,
in every log, one run that took place, while the second reduces each region and metric on its own, so that its
minimum can mix runs.
"""

import copy
import logging
import math
from collections.abc import Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from numbers import Real
from types import MappingProxyType
from typing import Any

import pandas as pd
import xarray as xr
from access.config.parallel_component import ComponentLayout
from matplotlib.figure import Figure

from access.profiling.application import Application
from access.profiling.experiment import ExperimentCoordinate, component_cores
from access.profiling.metrics import ProfilingMetric
from access.profiling.plotting_utils import RegionGroup, plot_bar_metrics, plot_line_metrics
from access.profiling.scaling import plot_component_scaling, plot_scaling_metrics

logger = logging.getLogger(__name__)

# The statistics the runs of an experiment can be reduced with, named as the methods of a dataset computing them.
_RUN_STATISTICS = ("min", "max", "mean", "median")


def _check_run_statistic(how: str) -> None:
    """Refuses a statistic the runs of an experiment cannot be reduced with.

    Args:
        how (str): Name of the statistic.

    Raises:
        ValueError: If how is not one of "min", "max", "mean" or "median".
    """
    if how not in _RUN_STATISTICS:
        raise ValueError(f"Unknown reduction '{how}'. Use 'min', 'max', 'mean' or 'median'.")


def _coordinate_name(coordinate: ExperimentCoordinate | str) -> str:
    """Returns what a coordinate is called: its name, or the key of the label it is."""

    return coordinate if isinstance(coordinate, str) else coordinate.name


def _refuse_values_besides_regions(metric: ProfilingMetric, dims: Iterable[Hashable], holder: str) -> None:
    """Refuses a metric with more than one value for each experiment and region, as data for each PE has.

    Args:
        metric (ProfilingMetric): The metric.
        dims (Iterable[Hashable]): The dimensions of its values.
        holder (str): What has too few places for them, such as "a table has cells", for the message.

    Raises:
        ValueError: If the values have dimensions other than the experiments and the regions.
    """
    extra = sorted(set(dims) - {"experiment", "region"})
    if extra:
        raise ValueError(
            f"The metric '{metric.name}' has the dimensions {extra} besides the experiments and the regions, so "
            f"it has more values than {holder}. Choose a metric with one value for each region."
        )


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


@dataclass(frozen=True, eq=False)
class ParsedExperiment:
    """What parsing read of one experiment: its profiling data, and what tells it from the other experiments.

    The data and the labels are copied into read-only mappings, so that the results they are part of do not change
    under a caller who goes on editing what they passed. The labels are copied whole, since a label can be a list.

    Args:
        data (Mapping[str, xr.Dataset]): The parsed profiling data, keyed by the name of the log. A dataset has a
            `run` dimension where several runs of the experiment produced the log.
        labels (Mapping[str, Any]): The study's own keys and values for the experiment. Empty by default.
        layout (ComponentLayout | None): The layout the experiment ran, or None where it cannot be told.
        ncpus (int | None): The number of CPUs its job occupied, or None where it cannot be told.
        layout_error (str | None): Why the layout could not be read, where reading it failed. None (the default)
            where it did not, including for a layout its files do not state.
        ncpus_error (str | None): Why the number of CPUs could not be read, where reading it failed.
    """

    data: Mapping[str, xr.Dataset]
    labels: Mapping[str, Any] = field(default_factory=dict)
    layout: ComponentLayout | None = None
    ncpus: int | None = None
    layout_error: str | None = None
    ncpus_error: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", MappingProxyType(dict(self.data)))
        object.__setattr__(self, "labels", MappingProxyType(copy.deepcopy(dict(self.labels))))


@dataclass(frozen=True, eq=False, repr=False)
class ProfilingResults:
    """The parsed profiling data of a study, which tables and plots are made from.

    `ProfilingManager.parse_profiling_data` returns one, and every selection and reduction returns another: nothing
    changes the results a method is called on. An experiment is read from the results by its name, and told apart
    from the others by coordinates: the key of one of its labels, or an `ExperimentCoordinate` read from its layout.

    Args:
        application (Application): The application the experiments are of, which says which component each of its
            logs belongs to.
        experiments (Mapping[str, ParsedExperiment]): What was parsed of each experiment, keyed by its name, in the
            order the experiments are to be read in.
        skipped (Mapping[str, str]): Why each experiment of the study that was not parsed was not, keyed by its name.
            Empty by default.
    """

    application: Application
    experiments: Mapping[str, ParsedExperiment]
    skipped: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "experiments", MappingProxyType(dict(self.experiments)))
        object.__setattr__(self, "skipped", MappingProxyType(dict(self.skipped)))

    def __repr__(self) -> str:
        """Returns which experiments the results hold, with their logs and labels, and which were not parsed."""
        lines = [f"<ProfilingResults of {self.application.name}: {len(self.experiments)} experiments>"]
        for name, experiment in self.experiments.items():
            logs = ", ".join(experiment.data) if experiment.data else "no logs"
            labels = f", labels {dict(experiment.labels)}" if experiment.labels else ""
            lines.append(f"    '{name}': {logs}{labels}")
        if self.skipped:
            lines.append("    Not parsed:")
            lines.extend(f"        '{name}': {reason}" for name, reason in self.skipped.items())
        return "\n".join(lines)

    @property
    def data(self) -> Mapping[str, Mapping[str, xr.Dataset]]:
        """Returns the parsed profiling data, keyed by experiment and then by log.

        Returns:
            Mapping[str, Mapping[str, xr.Dataset]]: The data, read-only. A dataset's variables are keyed by the
                metric objects, and it has a `run` dimension where several runs of the experiment produced its log.
        """
        return MappingProxyType({name: experiment.data for name, experiment in self.experiments.items()})

    def _with_data(self, data: Mapping[str, Mapping[str, xr.Dataset]]) -> "ProfilingResults":
        """Returns results holding other data for the same experiments, which is what a reduction makes.

        Args:
            data (Mapping[str, Mapping[str, xr.Dataset]]): The data of every experiment, keyed by its name.

        Returns:
            ProfilingResults: The new results.
        """
        return ProfilingResults(
            self.application,
            {
                name: ParsedExperiment(
                    data[name],
                    experiment.labels,
                    experiment.layout,
                    experiment.ncpus,
                    experiment.layout_error,
                    experiment.ncpus_error,
                )
                for name, experiment in self.experiments.items()
            },
            self.skipped,
        )

    def _ncpus(self, exp_name: str) -> int:
        """Returns the number of CPUs the job of an experiment occupied.

        Args:
            exp_name (str): Name of the experiment.

        Returns:
            int: Number of CPUs the experiment occupied, as the manager's parse_ncpus reported it.

        Raises:
            ValueError: If the count is not known, saying why where reading it failed.
        """
        experiment = self.experiments[exp_name]
        if experiment.ncpus is None:
            reason = f": {experiment.ncpus_error}" if experiment.ncpus_error is not None else "."
            raise ValueError(f"The number of CPUs of experiment '{exp_name}' is not known{reason}")
        return experiment.ncpus

    def _layout(self, exp_name: str) -> ComponentLayout | None:
        """Returns the layout an experiment ran.

        Args:
            exp_name (str): Name of the experiment.

        Returns:
            ComponentLayout | None: The layout, or None if its files do not say.

        Raises:
            ValueError: If reading the layout failed, saying why.
        """
        experiment = self.experiments[exp_name]
        if experiment.layout_error is not None:
            raise ValueError(f"The layout of experiment '{exp_name}' could not be read: {experiment.layout_error}")
        return experiment.layout

    def select_best_experiments(
        self,
        log: str,
        region: str,
        metric: ProfilingMetric,
        experiments: list[str] | None = None,
    ) -> list[str]:
        """Selects the best performing experiment for each number of CPUs.

        Scaling studies often contain several experiments that use the same number of CPUs, for instance different
        domain decomposition layouts of the same total core count. Plotting all of them produces duplicated ncpus
        coordinates and meaningless speedup and efficiency curves. This method keeps a single experiment per CPU
        count: the one with the smallest value of the given metric, measured on the given region of the given
        log. Smaller is always better.

        Experiments are grouped by the number of CPUs they occupied, which is what parse_ncpus reports, so two
        layouts that fill the same compute nodes compete with each other even when they hand different numbers of
        cores to the application's components.

        The returned list is meant to be passed to the experiments argument of the plotting methods. If two
        experiments with the same number of CPUs have exactly the same value, the first one is kept and a warning
        is logged.

        Args:
            log (str): Name of the profiling log holding the region used to rank experiments.
            region (str): Name of the region used to rank experiments.
            metric (ProfilingMetric): Metric used to rank experiments. The smallest value wins.
            experiments (list[str] | None): Optional list of experiment names to select from. If None, all
                experiments of these results are considered.

        Returns:
            list[str]: Names of the selected experiments, one per distinct number of CPUs, ordered by increasing
                number of CPUs.

        Raises:
            ValueError: If no experiments are selected, if one of them is not in these results, if one of them has
                no data for the log or the region, if the profiling data still has a 'run' dimension, or if the
                number of CPUs of one of them is not known.
        """
        exp_names = self._selected_experiments(experiments)
        ranking = self._group_data(RegionGroup(log, [region]), exp_names)

        best: dict[int, tuple[str, float]] = {}
        for exp_name, ds in ranking.items():
            value = float(ds[metric].isel(region=0).pint.dequantify().values)
            ncpus = self._ncpus(exp_name)
            incumbent = best.get(ncpus)
            if incumbent is None or value < incumbent[1]:
                best[ncpus] = (exp_name, value)
            elif value == incumbent[1]:
                logger.warning(
                    f"Experiments '{incumbent[0]}' and '{exp_name}' have the same {metric} ({value} "
                    f"{metric.units}) for region '{region}' of log '{log}' at {ncpus} CPUs. "
                    f"Keeping '{incumbent[0]}'."
                )

        return [name for _, (name, _) in sorted(best.items())]

    def select_best_run(self, log: str, region: str, metric: ProfilingMetric) -> "ProfilingResults":
        """Returns results keeping only the best performing run of each experiment.

        Configurations that can be run several times produce profiling data with a 'run' dimension. This method
        keeps a single run per experiment: the one with the smallest value of the given metric, measured on the
        given region of the given log. Smaller is always better.

        The chosen run is selected in *every* log of the experiment, so the result always describes a single
        run that actually took place, and never mixes measurements taken during different runs. Use
        aggregate_runs() instead to compute statistics over the runs.

        An experiment whose log has no 'run' dimension is kept as it is. The results this is called on are left as
        they were, so they still hold every run.

        Args:
            log (str): Name of the profiling log holding the region used to rank runs.
            region (str): Name of the region used to rank runs.
            metric (ProfilingMetric): Metric used to rank runs. The smallest value wins.

        Returns:
            ProfilingResults: The results of the best run of each experiment.

        Raises:
            ValueError: If these results hold no experiments, or if one of them has no data for the log or the
                region.
            KeyError: If the chosen run is missing from one of the logs of that experiment.
        """
        exp_names = self._selected_experiments(None, refuse_runs=False)
        ranking = self._group_data(RegionGroup(log, [region]), exp_names)

        data = {}
        for exp_name, logs in self.data.items():
            values = ranking[exp_name][metric].isel(region=0).pint.dequantify()
            if "run" not in values.dims:
                data[exp_name] = logs
                continue
            run = int(values.run.values[int(values.argmin("run"))])
            logger.info(f"Keeping run {run} of experiment '{exp_name}'.")
            data[exp_name] = {name: ds.sel(run=run, drop=True) if "run" in ds.dims else ds for name, ds in logs.items()}
        return self._with_data(data)

    def aggregate_runs(self, how: str = "min") -> "ProfilingResults":
        """Returns results with the 'run' dimension of every experiment reduced with the given statistic.

        Configurations that can be run several times produce profiling data with a 'run' dimension. This method
        collapses it, computing the requested statistic over the runs.

        Note that each region and each metric is reduced independently, so unlike select_best_run() the result does
        not correspond to any run that actually took place: with how="min", different regions can come from
        different runs. Use "min" to estimate the best achievable timings, "max" the worst, and "mean" or "median"
        to describe the typical ones. Integer metrics, such as call counts, become floats.

        Datasets without a 'run' dimension are kept as they are. The results this is called on are left as they
        were, so the minimum and the maximum of the same results measure the spread of the runs.

        Args:
            how (str): Statistic to compute over the runs. One of "min", "max", "mean" or "median". Defaults to
                "min".

        Returns:
            ProfilingResults: The reduced results.

        Raises:
            ValueError: If how is not one of the supported statistics.
        """
        _check_run_statistic(how)
        return self._with_data(
            {
                exp_name: {name: getattr(ds, how)("run") if "run" in ds.dims else ds for name, ds in logs.items()}
                for exp_name, logs in self.data.items()
            }
        )

    def plot_scaling_data(
        self,
        groups: list[RegionGroup],
        metric: ProfilingMetric,
        experiments: list[str] | None = None,
        xlabel: str | None = None,
        show: bool = True,
    ) -> Figure:
        """Plots speedup and efficiency of the given regions against the cores the whole job occupied.

        This is what a scaling study costs: every region is read against what the scheduler charged for,
        whatever share of it the region's own component was given. Each group's `component` is therefore
        ignored here; plot_component_scaling_data is the one that reads it.

        Args:
            groups (list[RegionGroup]): Regions to plot, and the logs they come from. Each region becomes
                one line, labelled as its group says.
            metric (ProfilingMetric): Metric to use for the scaling plots.
            experiments (list[str] | None): Optional list of experiment names to include. If None, all
                experiments with parsed profiling data are included.
            xlabel (str | None): Optional custom label for the x-axis of both subplots. If None, the
                default label derived from the x-coordinate is kept.
            show (bool): Whether to show the generated plot. Default: True.

        Returns:
            Figure: The Matplotlib figure containing the scaling plots.

        Raises:
            ValueError: If no experiments are selected, if a selected experiment has no parsed profiling
                data, if no profiling data is found for a group's log, if a requested region is missing, if
                the profiling data still has a 'run' dimension, or if several of the selected experiments
                use the same number of CPUs.
        """
        exp_names = self._selected_experiments(experiments)

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

        scaling_data = []
        for group in groups:
            selected = self._group_data(group, exp_names)
            group_data = [ds.expand_dims({"ncpus": [ncpus[exp_name]]}) for exp_name, ds in selected.items()]
            scaling_data.append(xr.concat(group_data, dim="ncpus", join="outer").sortby("ncpus"))

        return plot_scaling_metrics(scaling_data, metric, xlabel=xlabel, show=show)

    def _coordinate_value(self, exp_name: str, coordinate: ExperimentCoordinate | str) -> Hashable:
        """Returns the value one coordinate has for one experiment.

        A string is the key of one of the experiment's labels. Anything else is read from the layout the
        experiment runs.

        Args:
            exp_name (str): Name of the experiment.
            coordinate (ExperimentCoordinate | str): The coordinate, or the key of a label.

        Returns:
            Hashable: The value.

        Raises:
            ValueError: If the experiment has no such label, if the label is a list or a mapping, if the
                experiment's layout cannot be told, or if the layout does not have the quantity.
        """
        if isinstance(coordinate, str):
            labels = self.experiments[exp_name].labels
            if coordinate not in labels:
                raise ValueError(
                    f"Experiment '{exp_name}' has no label '{coordinate}'. Its labels are {sorted(labels)}."
                )
            value = labels[coordinate]
            # Every other JSON value is one experiments can be told apart and grouped by.
            if isinstance(value, list | dict):
                raise ValueError(
                    f"The label '{coordinate}' of experiment '{exp_name}' is {value!r}. A list or a mapping "
                    "cannot be a coordinate, so give the label a string or a number."
                )
            return value

        layout = self._layout(exp_name)
        if layout is None:
            raise ValueError(
                f"The layout of experiment '{exp_name}' could not be read, so there is nothing to read "
                f"'{coordinate.name}' from."
            )
        try:
            return coordinate.value(layout)
        except ValueError as error:
            # The coordinate is handed a layout and nothing else, so only here can the experiment be named.
            raise ValueError(f"Experiment '{exp_name}' has no '{coordinate.name}'. {error}") from error

    def _component_cores(self, exp_name: str, component: str) -> int:
        """Returns the cores an experiment gave one of its components.

        The one method here that genuinely takes a component: everything else that names data takes a log.

        Args:
            exp_name (str): Name of the experiment.
            component (str): Name of the component, as the layout names it - never a log name.

        Returns:
            int: Number of cores that component was given.

        Raises:
            ValueError: If the experiment's layout cannot be told, or names no such component.
        """
        return self._coordinate_value(exp_name, component_cores(component))

    def _group_component(self, group: RegionGroup) -> str:
        """Returns the component a group of regions is to be read against.

        Args:
            group (RegionGroup): The group.

        Returns:
            str: The component, as the layout names it.
        """
        if group.component is not None:
            return group.component
        # A log the application declares a component for is read against that component; one it does not -
        # a coupled profile covering several, or a log of another application entirely - is read against
        # its own name, and a caller naming regions says which component each group is.
        return self.application.component_for_log(group.log) or group.log

    def _selected_experiments(self, experiments: list[str] | None, refuse_runs: bool = True) -> list[str]:
        """Returns the experiments a selection, a plot or a table is to read, having checked there is something to read.

        Held here rather than written out per plot: they all ask the same three questions, and the one
        that did not ask them failed obscurely instead - a bare KeyError for an unknown name, and a
        division by zero inside the bar plotter for an empty selection.

        A name given more than once is read once, where it is first given. Each experiment is one entry of a
        dataset, a table or a chart, and two selections put together readily name one experiment twice.

        Args:
            experiments (list[str] | None): Experiments asked for, or None for every one of these results.
            refuse_runs (bool): Whether to refuse data that still has a 'run' dimension. True (the default) for
                every reader but the reductions, which remove it.

        Returns:
            list[str]: Their names, each once.

        Raises:
            ValueError: If none are selected, saying why where these results hold none, if one of them is not in
                these results, or if their data still has a 'run' dimension and refuse_runs is true.
        """
        if experiments is None and not self.experiments:
            passed_over = f" Parsing passed over {dict(self.skipped)}." if self.skipped else ""
            raise ValueError(
                "These results hold no experiments: parse_profiling_data parses the experiments that have finished, "
                f"DONE or ARCHIVED, and found none.{passed_over}"
            )
        exp_names = list(dict.fromkeys(experiments)) if experiments is not None else list(self.experiments)
        if not exp_names:
            raise ValueError("No experiments selected.")

        missing_experiments = [exp_name for exp_name in exp_names if exp_name not in self.experiments]
        if missing_experiments:
            raise ValueError(
                f"No parsed profiling data found for experiment(s): {missing_experiments}. "
                f"Available experiments: {list(self.experiments)}."
            )

        if refuse_runs:
            with_runs = [
                exp_name for exp_name in exp_names if any("run" in ds.dims for ds in self.data[exp_name].values())
            ]
            if with_runs:
                raise ValueError(
                    f"The profiling data of experiments {with_runs} still has a 'run' dimension, since they ran "
                    "several times. Reduce it first: select_best_run() returns results with the best run of each "
                    "experiment, and aggregate_runs() results with each region reduced over the runs."
                )

        return exp_names

    def _group_data(self, group: RegionGroup, exp_names: list[str]) -> dict:
        """Returns the regions of a group for each experiment, keyed by experiment.

        Args:
            group (RegionGroup): Regions to select, the log to select them from, and what to call them.
            exp_names (list[str]): Experiments to select them for.

        Returns:
            dict: The selected data, keyed by experiment name, its region coordinate being the label each
                region is to be plotted under.

        Raises:
            ValueError: If an experiment has no data for the group's log, or lacks one of its regions.
        """
        selected = {}
        for exp_name in exp_names:
            ds = self.data[exp_name].get(group.log)
            if ds is None:
                raise ValueError(
                    f"No profiling data found for '{group.log}' in experiment '{exp_name}'. Its logs are "
                    f"{list(self.data[exp_name])}."
                )

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
            ds = ds.assign_coords(region=[group.label(region) for region in ds.region.values])
            selected[exp_name] = ds
        return selected

    def gather(
        self,
        group: RegionGroup,
        coords: Sequence[ExperimentCoordinate | str] = (),
        experiments: list[str] | None = None,
    ) -> xr.Dataset:
        """Collects the regions of a group for several experiments into one dataset.

        The experiments are put along a new `experiment` dimension, whose coordinate is their names. A name
        says nothing about its experiment, so what tells the experiments apart is added as further coordinates
        along that dimension, one for each entry of `coords`. A string is the key of a label, and the coordinate
        holds the label of each experiment. An `ExperimentCoordinate` is read from the layout of each experiment,
        and the coordinate has its name.

        The dataset is the one a table is made from. Its `region` coordinate holds the labels of the group, as
        the plots show them, and its variables are the metrics of the log, with their units.

        Experiments that ran several times are refused, as the plots refuse them: select_best_run() or
        aggregate_runs() returns results without their runs. Gathering both the fastest and the slowest of the runs
        is how their spread is measured.

        Args:
            group (RegionGroup): Regions to collect, and the log they come from. Its `component` is ignored.
            coords (Sequence[ExperimentCoordinate | str]): What to tell the experiments apart by: keys of
                labels, and quantities of the layouts. Empty by default.
            experiments (list[str] | None): Optional list of experiment names to include, in the order they are
                first given: a name given twice is included once. If None, all experiments of these results are
                included.

        Returns:
            xr.Dataset: The regions of the group for every experiment, along `experiment` and `region`.

        Raises:
            ValueError: If no experiments are selected, if a selected experiment is not in these results, if no
                profiling data is found for the group's log, if a requested region is missing, if the profiling data
                still has a 'run' dimension, if two coordinates have the same name or one is named as a coordinate
                the data has of its own, if an experiment has no such label or the label is a list or a mapping, or
                if an experiment's layout cannot be told or does not have the quantity.
        """
        exp_names = self._selected_experiments(experiments)
        selected = self._group_data(group, exp_names)
        gathered = xr.concat(
            [ds.expand_dims({"experiment": [exp_name]}) for exp_name, ds in selected.items()],
            dim="experiment",
            join="outer",
        )

        names = [_coordinate_name(coordinate) for coordinate in coords]
        taken = sorted({name for name in names if names.count(name) > 1 or name in gathered.coords})
        if taken:
            raise ValueError(
                f"The coordinates {taken} are each named more than once, counting the coordinates the data has "
                f"of its own, {sorted(gathered.coords)}. Give each coordinate a name of its own."
            )

        # One value at a time, and put together by xarray: a value that is itself a sequence, such as a process
        # grid, stays one value, and values of different kinds are kept as they are instead of becoming strings.
        return gathered.assign_coords(
            {
                name: xr.Variable.concat(
                    [xr.Variable((), self._coordinate_value(exp_name, coordinate)) for exp_name in exp_names],
                    dim="experiment",
                )
                for name, coordinate in zip(names, coords, strict=True)
            }
        )

    def table(
        self,
        group: RegionGroup,
        metric: ProfilingMetric,
        index: ExperimentCoordinate | str,
        columns: ExperimentCoordinate | str | None = None,
        experiments: list[str] | None = None,
    ) -> pd.DataFrame:
        """Returns a metric of the regions of a group as a table, arranged by what tells the experiments apart.

        This is what gather() collects, with the experiments found again by the values of their coordinates
        instead of by their names. There is a row for each value of `index`, sorted, as plot_metric() sorts its
        x-axis. There is a column for each region of the group, in the order of the group and under its labels,
        and with `columns`, one for each value of that coordinate within each region, in the order the
        experiments give the values, as plot_metric() orders its series. A cell no experiment has values for is
        left empty.

        A table holds numbers rather than quantities, so the values are in the units of the metric, which the
        table states in its `attrs`, under "units".

        Args:
            group (RegionGroup): Regions to tabulate, and the log they come from. Its `component` is ignored.
            metric (ProfilingMetric): The metric to tabulate.
            index (ExperimentCoordinate | str): What the rows are: the key of a label, or a quantity of the layout.
            columns (ExperimentCoordinate | str | None): What the columns of each region are, in the same way.
                None (the default) gives each region a single column.
            experiments (list[str] | None): Experiments to tabulate. None (the default) tabulates all of those
                of these results.

        Returns:
            pd.DataFrame: The table.

        Raises:
            ValueError: If the group gives two of its regions the same label, since each region has a column of its
                own, if two experiments have the same values of `index` and `columns`, since a cell holds one
                value, if the metric has more than one value for each experiment and region, or for any of the
                reasons gather() raises.
        """
        # The plots draw two regions labelled alike as two lines or two bars, but a table would give them one column.
        labels = [group.label(region) for region in group.regions]
        alike = sorted({label for label in labels if labels.count(label) > 1})
        if alike:
            raise ValueError(
                f"The group gives the same label to several of its regions: {alike}. A table would give them one "
                "column, so give each region of the group a label of its own."
            )

        coords = [index] if columns is None else [index, columns]
        names = [_coordinate_name(coordinate) for coordinate in coords]
        gathered = self.gather(group, coords, experiments)
        values = gathered[metric].pint.dequantify()
        _refuse_values_besides_regions(metric, values.dims, "a table has cells")

        # The column of the values is keyed by the metric, which is never equal to the name of a coordinate.
        frame = values.to_dataframe(name=metric).reset_index()
        shared = frame[frame.duplicated([*names, "region"], keep=False)]
        if not shared.empty:
            # A null label is a value of its own, and pandas leaves out such groups unless told otherwise.
            by_values = shared.groupby(names, sort=False, dropna=False)["experiment"]
            groups = [sorted(set(experiments)) for _, experiments in by_values]
            raise ValueError(
                f"The experiments {' and '.join(str(group) for group in groups)} share their values of {names}, so a "
                "table cannot tell them apart. Select experiments that differ in them."
            )

        # A pivot sorts its columns, so they are put back in the order of the group and of the experiments.
        regions = list(gathered["region"].values)
        if columns is None:
            table = frame.pivot(index=names[0], columns="region", values=metric)
            table = table.reindex(columns=pd.Index(regions, name="region"))
        else:
            table = frame.pivot(index=names[0], columns=["region", names[1]], values=metric)
            column_values = list(dict.fromkeys(gathered[names[1]].values.tolist()))
            table = table.reindex(
                columns=pd.MultiIndex.from_product([regions, column_values], names=["region", names[1]])
            )
        table.attrs["units"] = str(metric.units)
        return table

    def select_experiments(
        self,
        coordinate: ExperimentCoordinate | str,
        value: Hashable,
        experiments: list[str] | None = None,
    ) -> list[str]:
        """Returns the experiments with a given value of a coordinate.

        The names of the experiments say nothing about them, so this is how the experiments of one setting or of
        one layout are picked out for a plot or a table: by the key of a label, or by a quantity of the layout.

        The experiments of one study need not have the same labels, as those generated from different kinds of
        choice do not, so an experiment without the label is not one of those with the value. A label that none of
        them has is refused instead, as a misspelt key would be. Every layout of a study has the quantities of
        its components, so a layout without the quantity is refused too.

        Args:
            coordinate (ExperimentCoordinate | str): The key of a label, or a quantity of the layout.
            value (Hashable): The value the experiments are to have.
            experiments (list[str] | None): Experiments to select from. None (the default) selects from all of
                those of these results.

        Returns:
            list[str]: The experiments with that value, in the order they were given. Empty if none has it.

        Raises:
            ValueError: If an experiment is not in these results, if none of the experiments has the label or
                the label of one is a list or a mapping, or if an experiment's layout cannot be told or does not
                have the quantity.
        """
        exp_names = experiments if experiments is not None else list(self.experiments)
        unknown = [exp_name for exp_name in exp_names if exp_name not in self.experiments]
        if unknown:
            raise ValueError(
                f"Experiments {unknown} are not in these results. Available experiments: {list(self.experiments)}."
            )

        if isinstance(coordinate, str):
            labelled = [exp_name for exp_name in exp_names if coordinate in self.experiments[exp_name].labels]
            if exp_names and not labelled:
                keys = sorted({key for exp_name in exp_names for key in self.experiments[exp_name].labels})
                raise ValueError(
                    f"None of the experiments {exp_names} has a label '{coordinate}'. Their labels are {keys}."
                )
            exp_names = labelled
        return [exp_name for exp_name in exp_names if self._coordinate_value(exp_name, coordinate) == value]

    def plot_component_scaling_data(
        self,
        groups: list[RegionGroup],
        metric: ProfilingMetric,
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
        """
        exp_names = self._selected_experiments(experiments)

        # A list rather than a mapping: neither the log nor the component is unique across groups, since
        # one of each is exactly what the other kind of group is for.
        scaling_data = []
        for group in groups:
            # What was asked for is settled first, since a log or a region these results hold nothing for
            # is a plainer thing to be told than anything about cores.
            selected = self._group_data(group, exp_names)

            # Then where each of them goes: the cores this group's component was given are its x axis.
            component = self._group_component(group)
            cores = {exp_name: self._component_cores(exp_name, component) for exp_name in exp_names}
            _note_shared_core_counts(component, cores)

            # Grouping by the cores is what reduces the experiments that share a count to the fastest of
            # them, and it orders the result as it goes, so there is nothing left to sort.
            group_data = [ds.expand_dims({"ncpus": [cores[exp_name]]}) for exp_name, ds in selected.items()]
            scaling_data.append(xr.concat(group_data, dim="ncpus", join="outer").groupby("ncpus").min())

        return plot_component_scaling(scaling_data, metric, xlabel=xlabel, ylabel=ylabel, show=show)

    def plot_bar_chart(
        self,
        groups: list[RegionGroup],
        metric: ProfilingMetric,
        experiment_relabel_map: dict | None = None,
        experiments: list[str] | None = None,
        show: bool = True,
        label: ExperimentCoordinate | str | None = None,
    ) -> Figure:
        """Plots a bar chart of a profiling metric over regions, grouped by experiment.

        Regions are placed along the x-axis, in the order the groups give them, with one bar per
        experiment within each. No cores appear on either axis, so each group's `component` is ignored
        here, as it is by plot_scaling_data.

        The bars of an experiment are named after it, unless `experiment_relabel_map` names them otherwise or
        `label` names them by a coordinate. A coordinate names them as `"<name> <value>"`, as plot_metric() names
        its series, and so needs a value of its own for every experiment plotted.

        Args:
            groups (list[RegionGroup]): Regions to plot, and the logs they come from. Each region becomes
                one position on the x-axis, labelled as its group says.
            metric (ProfilingMetric): Metric to plot.
            experiment_relabel_map (dict | None): Optional mapping to relabel experiments in the plot.
                Experiments are named by the manager rather than enumerated by the caller, so unlike the
                regions they have no group to carry their labels.
            experiments (list[str] | None): Optional list of experiment names to include. If None, all
                experiments are included.
            show (bool): Whether to show the generated plot. Default: True.
            label (ExperimentCoordinate | str | None): What to name the bars of each experiment by: the key of a
                label, or a quantity of the layout. None (the default) names them after the experiment, or as
                experiment_relabel_map says.

        Returns:
            Figure: The Matplotlib figure containing the bar chart.

        Raises:
            ValueError: If both experiment_relabel_map and label are given, if no experiments are selected, if a
                selected experiment has no parsed profiling data, if no profiling data is found for a group's
                log, if a requested region is missing, if the profiling data still has a 'run' dimension, if the
                metric has more than one value for each region, if two experiments have the same value of label,
                or if label cannot be read for an experiment.
        """
        if label is not None and experiment_relabel_map is not None:
            raise ValueError("Give experiment_relabel_map or label, not both: both say what the bars are named.")
        exp_names = self._selected_experiments(experiments)

        if label is not None:
            name = _coordinate_name(label)
            named: dict[str, list[str]] = {}
            for exp_name in exp_names:
                named.setdefault(f"{name} {self._coordinate_value(exp_name, label)}", []).append(exp_name)
            shared = {bars: names for bars, names in named.items() if len(names) > 1}
            if shared:
                clashes = "; ".join(f"{names} would all be '{bars}'" for bars, names in shared.items())
                raise ValueError(
                    f"The bars of each experiment need a name of their own, but the experiments {clashes}. Select "
                    f"experiments that differ in '{name}'."
                )
            experiment_relabel_map = {exp_name: bars for bars, names in named.items() for exp_name in names}

        region_labels: list[str] = []
        bar_data: dict[str, list[float]] = {exp_name: [] for exp_name in exp_names}
        for group in groups:
            region_labels.extend(group.label(region) for region in group.regions)
            for exp_name, ds in self._group_data(group, exp_names).items():
                _refuse_values_besides_regions(metric, ds[metric].dims, "a chart has bars")
                # By position rather than by label: the selection is already in the order the group asked
                # for, and two regions a caller chose to label the same way would make a lookup ambiguous.
                bar_data[exp_name].extend(float(value) for value in ds[metric].pint.dequantify().values)

        exp_relabel = experiment_relabel_map or {}
        relabelled_bar_data = {exp_relabel.get(k, k): v for k, v in bar_data.items()}

        return plot_bar_metrics(relabelled_bar_data, region_labels, metric, show=show)

    def _points(
        self, exp_names: list[str], x: ExperimentCoordinate | str, series: ExperimentCoordinate | str | None
    ) -> dict[str | None, dict[Real, list[str]]]:
        """Returns which experiments are at each point of a plot of a metric against a coordinate.

        Says, at INFO, which experiments share a point: the fastest of them is what gets plotted, and a reader
        of the figure should be told that the point is the best of several.

        A line is told apart from the others by its label, so the series are keyed by the text their values are
        shown as. The values 1, 1.0 and True are equal in Python, but are shown as three lines. Two values shown
        alike, such as 1 and "1", are refused, since their lines could not be told apart.

        Args:
            exp_names (list[str]): Experiments to place.
            x (ExperimentCoordinate | str): What the experiments are placed along the x-axis by.
            series (ExperimentCoordinate | str | None): What the experiments are divided into lines by, if
                anything.

        Returns:
            dict[str | None, dict[Real, list[str]]]: The experiments at each point, keyed by the value of the
                series as it is shown, in the order the experiments give them, and then by the value of x. Without
                a series, the one key is None.

        Raises:
            ValueError: If a coordinate cannot be read for an experiment, if a value of x is not a number or is a
                boolean, or if two values of the series are shown alike.
        """
        points: dict[str | None, dict[Real, list[str]]] = {}
        shown: dict[str, Hashable] = {}
        for exp_name in exp_names:
            x_value = self._coordinate_value(exp_name, x)
            # A boolean is a number to Python, but true and false have no place along an axis.
            if isinstance(x_value, bool) or not isinstance(x_value, Real):
                raise ValueError(
                    f"Experiment '{exp_name}' has the value {x_value!r} for '{_coordinate_name(x)}', which is not "
                    "a number, so it has no place along an x-axis. Use the coordinate as the series instead, or "
                    "compare the experiments with plot_bar_chart."
                )
            key = None
            if series is not None:
                series_value = self._coordinate_value(exp_name, series)
                key = str(series_value)
                first = shown.setdefault(key, series_value)
                if (type(first), first) != (type(series_value), series_value):
                    raise ValueError(
                        f"The values {first!r} and {series_value!r} of '{_coordinate_name(series)}' are both shown as "
                        f"'{key}', so their lines could not be told apart. Give every experiment a value of the same "
                        "type."
                    )
            points.setdefault(key, {}).setdefault(x_value, []).append(exp_name)

        for key, along_x in points.items():
            for x_value, names in sorted(along_x.items()):
                if len(names) > 1:
                    where = f"'{_coordinate_name(x)}' {x_value}"
                    if series is not None:
                        where += f" and '{_coordinate_name(series)}' {key}"
                    logger.info(
                        f"Experiments {sorted(names)} all have {where}. Plotting the fastest of them at that point."
                    )
        return points

    def plot_metric(
        self,
        groups: list[RegionGroup],
        metric: ProfilingMetric,
        x: ExperimentCoordinate | str,
        series: ExperimentCoordinate | str | None = None,
        experiments: list[str] | None = None,
        xlabel: str | None = None,
        ylabel: str | None = None,
        show: bool = True,
    ) -> Figure:
        """Plots a metric for the given regions against a coordinate of the experiments.

        Where plot_component_scaling_data puts the cores of a component along the x-axis, this puts whatever
        the study varied: one of the labels of the experiments, named by its key, or a quantity of their
        layouts, given as an `ExperimentCoordinate`. Its values must be numbers, and booleans are not. The axes are
        linear and no ideal line is drawn, since the coordinate need not be a number of cores.

        Each region is one line. With `series`, the experiments are first divided by the value of another
        coordinate, and each region has one line for each value, in the order the experiments give the values.
        A line is labelled with its region, followed by the name and the value of the series. The values are told
        apart as they are shown, so 1 and 1.0 are two lines, and two values shown alike, such as 1 and "1", are
        refused.

        Where several experiments share both the value of `x` and the value of `series`, the fastest of them is
        what is plotted, and they are reported at INFO. The fastest is taken region by region, as
        plot_component_scaling_data takes it, passing over an experiment with no value for the region (NaN). Each
        group's `component` is ignored here.

        Args:
            groups (list[RegionGroup]): Regions to plot, and the logs they come from.
            metric (ProfilingMetric): The metric to plot.
            x (ExperimentCoordinate | str): What goes along the x-axis: the key of a label, or a quantity of
                the layout.
            series (ExperimentCoordinate | str | None): What the experiments are divided into lines by: the key
                of a label, or a quantity of the layout. None (the default) draws one line for each region.
            experiments (list[str] | None): Experiments to plot. None (the default) plots all of those
                with parsed profiling data.
            xlabel (str | None): Optional label for the x-axis. If None, the name of `x` is used.
            ylabel (str | None): Optional label for the y-axis. If None, the metric names it.
            show (bool): Whether to show the generated plot. Default: True.

        Returns:
            Figure: The Matplotlib figure the lines are plotted on.

        Raises:
            ValueError: If no experiments are selected, if a selected experiment has no parsed profiling
                data, if no profiling data is found for a group's log, if a requested region is missing, if
                the profiling data still has a 'run' dimension, if the metric has more than one value for each
                region, if an experiment has no such label or the label is a list or a mapping, if an experiment's
                layout cannot be told or does not have the quantity, if a value of `x` is not a number or is a
                boolean, or if two values of `series` are shown alike.
        """
        exp_names = self._selected_experiments(experiments)
        # What was asked for is settled first, as plot_component_scaling_data settles it.
        selected = [self._group_data(group, exp_names) for group in groups]
        for group_data in selected:
            for ds in group_data.values():
                _refuse_values_besides_regions(metric, ds[metric].dims, "a line has points")
        points = self._points(exp_names, x, series)

        lines = []
        for group, group_data in zip(groups, selected, strict=True):
            # By position rather than by label, as plot_bar_chart reads the regions.
            for index, region in enumerate(group.regions):
                values = {
                    exp_name: float(ds[metric].isel(region=index).pint.dequantify())
                    for exp_name, ds in group_data.items()
                }
                for key, along_x in points.items():
                    label = group.label(region)
                    if series is not None:
                        label += f", {_coordinate_name(series)} {key}"
                    x_values = sorted(along_x)
                    # NaN, a region an experiment has no value for, is passed over as the xarray reduction of
                    # plot_component_scaling_data passes over it. Python's min() keeps it or not by where it is.
                    fastest = []
                    for x_value in x_values:
                        measured = [values[name] for name in along_x[x_value] if not math.isnan(values[name])]
                        fastest.append(min(measured, default=math.nan))
                    lines.append((label, x_values, fastest))

        return plot_line_metrics(
            lines, metric, xlabel=xlabel if xlabel is not None else _coordinate_name(x), ylabel=ylabel, show=show
        )
