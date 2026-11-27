# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The profiling data of a study once it is parsed, and what is selected, tabulated and plotted from it.

`ProfilingManager.parse_profiling_data` returns a `ProfilingResults`: what the logs of every finished experiment hold,
with what tells each experiment from the others, its labels, its layout and the CPUs its job occupied. They are read
when the experiment is parsed, while its directory is open, so that nothing is extracted from an archive again, and
the results need no manager to be read.

The plots return the axes they drew on, and show nothing themselves: a notebook shows the figure at the end of the
cell, and the axes are what a notebook customises, as `ax.set(yscale="log")` does. A plot given axes draws on them.

Nothing changes a `ProfilingResults` once it is made. A selection or a reduction returns new results, with datasets of
their own, and leaves the ones it was called on as they were. A notebook can therefore keep the runs of a study, their
best run and their mean side by side, and a cell gives the same answer whichever cells ran before it. The datasets in
`data` are the results' own, so a dataset to be changed, by adding a variable for example, is copied first.

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
from matplotlib.axes import Axes

from access.profiling.application import Application
from access.profiling.experiment import ExperimentCoordinate, component_cores
from access.profiling.metrics import ProfilingMetric, tmax
from access.profiling.plotting_utils import plot_bar_metrics, plot_line_metrics
from access.profiling.scaling import plot_component_scaling, plot_scaling_metrics

logger = logging.getLogger(__name__)

# The statistics the runs of an experiment can be reduced with, named as the methods of a dataset computing them.
_RUN_STATISTICS = ("min", "max", "mean", "median")

BUILT_IN_COORDINATES: tuple[str, ...] = ("experiment", "ncpus")
"""The coordinates every experiment has: its name, and the CPUs its job occupied, which no label may be named."""


@dataclass(frozen=True)
class Region:
    """One region of one profiling log, and what a table or a plot calls it.

    A region is named as its log names it, which is the name of a timer in the application's own output, and that
    name is only unique within the log: two components may each have a "Total". The label is what a table or a plot
    shows instead, and is the name itself unless one is given. Two regions a table or a plot is given must have
    labels of their own.

    The component is whose cores the region is read against by `ProfilingResults.plot_component_scaling_data`, the
    one reader that puts a component's cores on an axis. A log and a component are not the same thing: one log can
    hold the regions of several components, as the summary of a coupled framework does, and which component a region
    belongs to cannot be worked out from its name.

    For example, the time ACCESS-OM3 spent in its ocean, as the ESMF profiling summary records it:

    ```python
    ocean = Region("ESMF", "[OCN] RunPhase1", label="MOM6", component="ocn")
    ```

    Args:
        log (str): Name of the profiling log the region comes from, as the application names it, such as "ESMF" or
            "payu".
        name (str): Name of the region, as the log names it, such as "[OCN] RunPhase1".
        label (str | None): What a table or a plot calls the region. None (the default) is its name. A copy made
            with `dataclasses.replace` keeps the label of the region it copies, so give it a label of its own when
            it names another region.
        component (str | None): The component whose cores the region is read against, named as the layout names it.
            None (the default) takes the component the application declares for the log, and failing that the name
            of the log, which is enough for a log that is one component's alone.

    Raises:
        ValueError: If the log or the name is empty.
    """

    log: str
    name: str
    label: str | None = None
    component: str | None = None

    def __post_init__(self) -> None:
        if not self.log or not self.name:
            raise ValueError(
                f"A region is named by its log and by its name in the log. Got the log {self.log!r} and the name "
                f"{self.name!r}."
            )
        if self.label is None:
            object.__setattr__(self, "label", self.name)


def _refuse_regions_to_rank_by(by: object) -> None:
    """Refuses anything but one region to rank experiments or runs by, since a ranking reads one value of each.

    Args:
        by (object): What a selection was given to rank by.

    Raises:
        TypeError: If it is not a Region.
    """
    if not isinstance(by, Region):
        raise TypeError(
            f"Experiments and runs are ranked by one region, given as a Region, such as Region('ESMF', "
            f"'[ensemble] RunPhase1'). Got {by!r}."
        )


def _region_list(regions: Region | Sequence[Region]) -> list[Region]:
    """Returns the regions a reader was given as a list, having checked that each can be told from the others.

    Args:
        regions (Region | Sequence[Region]): One region, or several.

    Returns:
        list[Region]: The regions, in the order they were given.

    Raises:
        TypeError: If one of them is not a Region.
        ValueError: If no region is given, or if two of them have the same label.
    """
    listed = [regions] if isinstance(regions, Region) else list(regions)
    if not listed:
        raise ValueError("No regions given. Name at least one, as Region(log, name).")
    others = [region for region in listed if not isinstance(region, Region)]
    if others:
        raise TypeError(
            f"Regions are given as Region objects, such as Region('ESMF', '[ensemble] RunPhase1'). Got {others!r}."
        )
    labels = [region.label for region in listed]
    alike = sorted({label for label in labels if labels.count(label) > 1})
    if alike:
        raise ValueError(
            f"Several regions are labelled {alike}, so a table or a plot could not tell them apart. Give each a label "
            "of its own, as Region(log, name, label=...)."
        )
    return listed


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

    Raises:
        ValueError: If a label is named as one of the `BUILT_IN_COORDINATES`.
    """

    data: Mapping[str, xr.Dataset]
    labels: Mapping[str, Any] = field(default_factory=dict)
    layout: ComponentLayout | None = None
    ncpus: int | None = None
    layout_error: str | None = None
    ncpus_error: str | None = None

    def __post_init__(self) -> None:
        reserved = sorted(set(self.labels) & set(BUILT_IN_COORDINATES))
        if reserved:
            raise ValueError(
                f"The labels {reserved} are named as coordinates every experiment has, {list(BUILT_IN_COORDINATES)}, "
                "which they would hide. Give the labels other keys."
            )
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
            order the experiments are to be read in. No label of an experiment may be named as one of the
            `BUILT_IN_COORDINATES`.
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

    def _names(self, refuse_runs: bool = True) -> list[str]:
        """Returns the experiments a selection, a table or a plot is to read, having checked there is something to read.

        Args:
            refuse_runs (bool): Whether to refuse data that still has a 'run' dimension. True (the default) for
                every reader but the reductions, which remove it.

        Returns:
            list[str]: The names of every experiment of these results, in their order.

        Raises:
            ValueError: If these results hold no experiments, or if their data still has a 'run' dimension and
                refuse_runs is true.
        """
        if not self.experiments:
            passed_over = f" Parsing passed over {dict(self.skipped)}." if self.skipped else ""
            raise ValueError(
                "These results hold no experiments. parse_profiling_data parses the experiments that have finished, "
                f"DONE or ARCHIVED, and a selection keeps those of them it matches.{passed_over}"
            )
        exp_names = list(self.experiments)
        if refuse_runs:
            with_runs = [
                exp_name for exp_name in exp_names if any("run" in ds.dims for ds in self.data[exp_name].values())
            ]
            if with_runs:
                raise ValueError(
                    f"The profiling data of experiments {with_runs} still has a 'run' dimension, since they ran "
                    "several times. Reduce it first: select_best_run() returns results with the best run of each "
                    "experiment, aggregate_runs() results with each region reduced over the runs, and split_runs() "
                    "results with each run an experiment of its own."
                )
        return exp_names

    def _only(self, exp_names: Iterable[str]) -> "ProfilingResults":
        """Returns results holding only some of the experiments of these, in the order these hold them.

        Args:
            exp_names (Iterable[str]): The experiments to keep.

        Returns:
            ProfilingResults: The new results.
        """
        kept = set(exp_names)
        return ProfilingResults(
            self.application, {name: exp for name, exp in self.experiments.items() if name in kept}, self.skipped
        )

    def _region_component(self, region: Region) -> str:
        """Returns the component a region is to be read against.

        Args:
            region (Region): The region.

        Returns:
            str: The component, as the layout names it.
        """
        if region.component is not None:
            return region.component
        # A log the application declares a component for is read against that component; one it does not -
        # a coupled profile covering several, or a log of another application entirely - is read against
        # its own name, and a caller naming regions says which component each is.
        return self.application.component_for_log(region.log) or region.log

    def _region_data(
        self, regions: Sequence[Region], exp_names: list[str], metric: ProfilingMetric | None = None
    ) -> dict[str, xr.Dataset]:
        """Returns the regions asked for of each experiment, as one dataset each, keyed by experiment.

        Args:
            regions (Sequence[Region]): Regions to select, which `_region_list` checked.
            exp_names (list[str]): Experiments to select them for.
            metric (ProfilingMetric | None): The one metric the reader needs, which every log of the regions must
                have and which is all that is kept. None keeps the metrics every log of the regions has.

        Returns:
            dict[str, xr.Dataset]: The selected data, keyed by experiment name, its region coordinate being the label
                of each region, in the order the regions were given.

        Raises:
            ValueError: If an experiment has no data for the log of a region, lacks a region, or has no such metric
                in the log of a region.
        """
        selected = {}
        for exp_name in exp_names:
            logs = self.data[exp_name]
            for log in dict.fromkeys(region.log for region in regions):
                ds = logs.get(log)
                if ds is None:
                    raise ValueError(
                        f"No profiling data found for '{log}' in experiment '{exp_name}'. Its logs are {list(logs)}."
                    )
                available_regions = ds.coords["region"].values.tolist()
                missing_regions = [
                    region.name for region in regions if region.log == log and region.name not in available_regions
                ]
                if missing_regions:
                    raise ValueError(
                        f"Regions {missing_regions} not found in '{log}' for experiment '{exp_name}'. "
                        f"Available regions: {available_regions}."
                    )
                if metric is not None and metric not in ds.data_vars:
                    raise ValueError(
                        f"The log '{log}' of experiment '{exp_name}' has no metric '{metric.name}'. Its metrics are "
                        f"{[str(variable) for variable in ds.data_vars]}."
                    )

            # From here the coordinate is the label the region is shown under rather than its name: the data exists
            # only to be shown, and two regions of two logs may well have the same name.
            parts = [
                logs[region.log].sel(region=[region.name]).assign_coords(region=[region.label]) for region in regions
            ]
            kept = [metric] if metric is not None else [v for v in parts[0].data_vars if all(v in p for p in parts)]
            selected[exp_name] = xr.concat([part[kept] for part in parts], dim="region", join="outer")
        return selected

    def regions(self, log: str | None = None) -> pd.DataFrame:
        """Returns the regions each log holds, which is what a `Region` names, and in how many experiments.

        Args:
            log (str | None): The log to list the regions of. None (the default) lists those of every log.

        Returns:
            pd.DataFrame: One row for each region of each log, in the order the logs and their regions are first
                met, with the columns "log", "region" and "experiments", the number of experiments that have it.

        Raises:
            ValueError: If no experiment has the log asked for.
        """
        counts: dict[tuple[str, Hashable], int] = {}
        for logs in self.data.values():
            for log_name, ds in logs.items():
                if log is None or log_name == log:
                    for region in ds.coords["region"].values.tolist():
                        counts[(log_name, region)] = counts.get((log_name, region), 0) + 1
        held = list(dict.fromkeys(name for logs in self.data.values() for name in logs))
        if log is not None and log not in held:
            raise ValueError(f"None of the experiments has a log '{log}'. Their logs are {held}.")
        return pd.DataFrame(
            [(log_name, region, n) for (log_name, region), n in counts.items()],
            columns=["log", "region", "experiments"],
        )

    def select_experiments(self, coordinate: ExperimentCoordinate | str, *values: Hashable) -> "ProfilingResults":
        """Returns results holding the experiments with one of the given values of a coordinate.

        The names of the experiments say nothing about them, so this is how the experiments of one setting or of
        one layout are picked out for a table or a plot: by the key of a label, by a quantity of the layout, by
        "ncpus", the CPUs the job occupied, or by "experiment", the name.

        The experiments of one study need not have the same labels, as those generated from different kinds of
        choice do not, so an experiment without the label is not one of those with the value. A label that none of
        them has is refused instead, as a misspelt key would be. Every layout of a study has the quantities of
        its components, so a layout without the quantity is refused too. A name that these results do not hold is
        refused as well, saying why where the experiment was not parsed, so that a plot is never drawn without an
        experiment it was asked for.

        Args:
            coordinate (ExperimentCoordinate | str): The key of a label, a quantity of the layout, "ncpus" or
                "experiment".
            *values (Hashable): The values the experiments may have, at least one.

        Returns:
            ProfilingResults: The experiments with one of the values, in the order these results hold them, which
                hold none where no experiment has one of them.

        Raises:
            ValueError: If no value is given, if these results hold no experiments, if one of the names is not that of
                an experiment these results hold, if none of the experiments has the label or the label of one is a
                list or a mapping, or if an experiment's layout cannot be told or does not have the quantity.
        """
        if not values:
            raise ValueError(f"Give at least one value of '{_coordinate_name(coordinate)}' to select experiments by.")
        exp_names = self._names(refuse_runs=False)
        if coordinate == "experiment":
            self._refuse_names_not_held(values)
        if isinstance(coordinate, str) and coordinate not in BUILT_IN_COORDINATES:
            labelled = [exp_name for exp_name in exp_names if coordinate in self.experiments[exp_name].labels]
            if not labelled:
                keys = sorted({key for exp_name in exp_names for key in self.experiments[exp_name].labels})
                raise ValueError(
                    f"None of the experiments {exp_names} has a label '{coordinate}'. Their labels are {keys}."
                )
            exp_names = labelled
        return self._only(exp_name for exp_name in exp_names if self._coordinate_value(exp_name, coordinate) in values)

    def _refuse_names_not_held(self, names: Sequence[Hashable]) -> None:
        """Refuses the names of experiments these results do not hold, saying why for those that were not parsed.

        Args:
            names (Sequence[Hashable]): The names asked for.

        Raises:
            ValueError: If one of the names is not that of an experiment these results hold.
        """
        missing = [name for name in names if not isinstance(name, str) or name not in self.experiments]
        if missing:
            why = "".join(
                f" '{name}' was not parsed: {self.skipped[name]}."
                for name in missing
                if isinstance(name, str) and name in self.skipped
            )
            raise ValueError(
                f"These results hold no experiment named {missing}.{why} They hold {list(self.experiments)}. Give "
                "each name as a value of its own, not a list of them."
            )

    def select_best_experiments(
        self,
        by: Region,
        *,
        per: ExperimentCoordinate | str | Sequence[ExperimentCoordinate | str] = "ncpus",
        metric: ProfilingMetric = tmax,
    ) -> "ProfilingResults":
        """Returns results holding the best performing experiment for each value of a coordinate.

        Scaling studies often contain several experiments that use the same number of CPUs, for instance different
        domain decomposition layouts of the same total core count. Plotting all of them produces duplicated ncpus
        coordinates and meaningless speedup and efficiency curves. This method keeps a single experiment for each
        number of CPUs: the one with the smallest value of the metric in the region. Smaller is always better.

        Experiments are grouped by the number of CPUs they occupied, which is what parse_ncpus reports, so two
        layouts that fill the same compute nodes compete with each other even when they hand different numbers of
        cores to the application's components. A study that also varied a setting keeps the best experiment for
        each number of CPUs and each value of the setting, with `per=["ncpus", "<label>"]`.

        If two experiments of the same group have exactly the same value, the first one is kept and a warning is
        logged.

        Args:
            by (Region): The region the experiments are ranked by. One that covers the whole run gives the layouts
                that make the whole application fastest.
            per (ExperimentCoordinate | str | Sequence[ExperimentCoordinate | str]): What the experiments are
                grouped by: one coordinate or several. "ncpus" (the default) keeps one experiment for each number of
                CPUs.
            metric (ProfilingMetric): Metric used to rank experiments. The smallest value wins. Defaults to tmax.

        Returns:
            ProfilingResults: The best experiment of each group, in the order these results hold them.

        Raises:
            TypeError: If `by` is not one Region.
            ValueError: If these results hold no experiments, if one of them has no data for the log, the region or
                the metric, if the profiling data still has a 'run' dimension, or if a coordinate cannot be read
                for an experiment.
        """
        _refuse_regions_to_rank_by(by)
        coordinates = [per] if isinstance(per, str | ExperimentCoordinate) else list(per)
        exp_names = self._names()
        ranking = self._region_data(_region_list(by), exp_names, metric)

        best: dict[tuple, tuple[str, float]] = {}
        for exp_name, ds in ranking.items():
            _refuse_values_besides_regions(metric, ds[metric].dims, "a ranking has places")
            value = float(ds[metric].isel(region=0).pint.dequantify().values)
            key = tuple(self._coordinate_value(exp_name, coordinate) for coordinate in coordinates)
            incumbent = best.get(key)
            if incumbent is None or value < incumbent[1]:
                best[key] = (exp_name, value)
            elif value == incumbent[1]:
                where = ", ".join(
                    f"'{_coordinate_name(coordinate)}' {item}"
                    for coordinate, item in zip(coordinates, key, strict=True)
                )
                logger.warning(
                    f"Experiments '{incumbent[0]}' and '{exp_name}' have the same {metric} ({value} "
                    f"{metric.units}) for region '{by.name}' of log '{by.log}' at {where}. Keeping '{incumbent[0]}'."
                )

        return self._only(name for name, _ in best.values())

    def select_best_run(self, by: Region, *, metric: ProfilingMetric = tmax) -> "ProfilingResults":
        """Returns results keeping only the best performing run of each experiment.

        Configurations that can be run several times produce profiling data with a 'run' dimension. This method
        keeps a single run per experiment: the one with the smallest value of the metric in the region. Smaller is
        always better.

        The chosen run is selected in *every* log of the experiment, so the result always describes a single
        run that actually took place, and never mixes measurements taken during different runs. Use
        aggregate_runs() instead to compute statistics over the runs.

        An experiment whose log has no 'run' dimension is kept as it is. The results this is called on are left as
        they were, so they still hold every run.

        Args:
            by (Region): The region the runs are ranked by.
            metric (ProfilingMetric): Metric used to rank runs. The smallest value wins. Defaults to tmax.

        Returns:
            ProfilingResults: The results of the best run of each experiment.

        Raises:
            TypeError: If `by` is not one Region.
            ValueError: If these results hold no experiments, or if one of them has no data for the log, the region
                or the metric.
            KeyError: If the chosen run is missing from one of the logs of that experiment.
        """
        _refuse_regions_to_rank_by(by)
        exp_names = self._names(refuse_runs=False)
        ranking = self._region_data(_region_list(by), exp_names, metric)

        data = {}
        for exp_name, logs in self.data.items():
            values = ranking[exp_name][metric].isel(region=0).pint.dequantify()
            # Each log is a dataset of its own in the new results, even one without runs, so that a variable added to
            # one is not added to the other.
            if "run" not in values.dims:
                data[exp_name] = {name: ds.copy() for name, ds in logs.items()}
                continue
            run = int(values.run.values[int(values.argmin("run"))])
            logger.info(f"Keeping run {run} of experiment '{exp_name}'.")
            data[exp_name] = {
                name: ds.sel(run=run, drop=True) if "run" in ds.dims else ds.copy() for name, ds in logs.items()
            }
        return self._with_data(data)

    def aggregate_runs(self, how: str = "min") -> "ProfilingResults":
        """Returns results with the 'run' dimension of every experiment reduced with the given statistic.

        Configurations that can be run several times produce profiling data with a 'run' dimension. This method
        collapses it, computing the requested statistic over the runs.

        Note that each region and each metric is reduced independently, so unlike select_best_run() the result does
        not correspond to any run that actually took place: with how="min", different regions can come from
        different runs. Use "min" to estimate the best achievable timings, "max" the worst, and "mean" or "median"
        to describe the typical ones. Integer metrics, such as call counts, become floats.

        Datasets without a 'run' dimension are kept as they are, as copies of their own. The results this is called on
        are left as they were, so the minimum and the maximum of the same results measure the spread of the runs.

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
                exp_name: {
                    name: getattr(ds, how)("run") if "run" in ds.dims else ds.copy() for name, ds in logs.items()
                }
                for exp_name, logs in self.data.items()
            }
        )

    def split_runs(self) -> "ProfilingResults":
        """Returns results with each run of every experiment as an experiment of its own, labelled with its run.

        A selection, a table or a plot can then be arranged by the label "run" as by any other, which is how the runs
        of one experiment are compared with each other: a table with a row for each run, or a bar chart with a bar
        for each of a few runs picked with select_experiments("run", ...).

        The experiment made of a run is named "<experiment> run <run>", and keeps the labels, the layout and the CPUs
        of the experiment it is a run of. A log that only one of the runs produced has no 'run' dimension, so there
        is no telling which run it is of: it is left out of every one of them. An experiment that ran once is kept
        as it is, without a label "run". The results this is called on are left as they were.

        Returns:
            ProfilingResults: The results of every run.

        Raises:
            ValueError: If the name of a run is already that of another experiment, as one added under the name
                "<experiment> run <run>" would be.
        """
        experiments = {}
        for exp_name, experiment in self.experiments.items():
            runs = sorted({int(run) for ds in experiment.data.values() if "run" in ds.dims for run in ds["run"].values})
            if not runs:
                experiments[exp_name] = experiment
                continue
            for run in runs:
                run_name = f"{exp_name} run {run}"
                if run_name in self.experiments or run_name in experiments:
                    raise ValueError(
                        f"Run {run} of experiment '{exp_name}' would be named '{run_name}', which is already the name "
                        "of another experiment. Select the experiments to split with select_experiments first."
                    )
                data = {
                    log: ds.sel(run=run, drop=True)
                    for log, ds in experiment.data.items()
                    if "run" in ds.dims and run in ds["run"].values
                }
                experiments[run_name] = ParsedExperiment(
                    data,
                    {**experiment.labels, "run": run},
                    experiment.layout,
                    experiment.ncpus,
                    experiment.layout_error,
                    experiment.ncpus_error,
                )
        return ProfilingResults(self.application, experiments, self.skipped)

    def plot_scaling_data(
        self,
        regions: "Region | Sequence[Region]",
        *,
        series: ExperimentCoordinate | str | None = None,
        metric: ProfilingMetric = tmax,
        xlabel: str | None = None,
    ) -> tuple[Axes, Axes]:
        """Plots speedup and efficiency of the given regions against the cores the whole job occupied.

        This is what a scaling study costs: every region is read against what the scheduler charged for,
        whatever share of it the region's own component was given. Each region's `component` is therefore
        ignored here; plot_component_scaling_data is the one that reads it.

        With `series`, the experiments are first divided by the value of a coordinate, such as a setting the study
        varied, and each region has one curve for each value, labelled with its region and the value. The speedup
        and the efficiency of a curve are relative to the smallest number of CPUs of its own experiments.

        The figure holds the speedup, the efficiency, and a table of the metric beneath them, and is shown by
        nothing here: a notebook shows it at the end of the cell.

        Args:
            regions (Region | Sequence[Region]): Regions to plot. Each region becomes one line, under its label.
            series (ExperimentCoordinate | str | None): What the experiments are divided into curves by: the key of
                a label, or a quantity of the layout. None (the default) draws one curve for each region.
            metric (ProfilingMetric): Metric to use for the scaling plots. Defaults to tmax.
            xlabel (str | None): Optional custom label for the x-axis of both subplots. If None, the
                default label derived from the x-coordinate is kept.

        Returns:
            tuple[Axes, Axes]: The axes of the speedup and of the efficiency.

        Raises:
            ValueError: If these results hold no experiments, if one has no data for the log, the region or the
                metric of a region, if two regions have the same label, if the profiling data still has a 'run'
                dimension, if an experiment has no such label as `series` or the label is a list or a mapping, if an
                experiment's layout cannot be told or does not have the quantity, if two values of `series` are shown
                alike, or if several of the experiments of one series use the same number of CPUs.
        """
        listed = _region_list(regions)
        exp_names = self._names()

        # Find number of cpus used for each experiment
        ncpus = {exp_name: self._ncpus(exp_name) for exp_name in exp_names}
        by_series = self._series(exp_names, series)

        # Speedup and efficiency are ill-defined if several experiments of one curve share the same number of cpus.
        # The counts are the occupied ones, so two layouts filling the same nodes collide here even if their
        # component core counts differ; select_best_experiments() groups them the same way and is the way to
        # resolve this.
        for key, names in by_series.items():
            cpu_counts = [ncpus[exp_name] for exp_name in names]
            duplicated_ncpus = sorted({n for n in cpu_counts if cpu_counts.count(n) > 1})
            if duplicated_ncpus:
                within = "" if series is None else f" with '{_coordinate_name(series)}' {key}"
                # A label is named by its key, which per= takes as it is; a quantity of the layout is named by the
                # coordinate itself, since per= reads a string as the key of a label.
                if series is None:
                    per = ""
                elif isinstance(series, str):
                    per = f", with per=['ncpus', {series!r}]"
                else:
                    per = ", with per=['ncpus', series], series being the coordinate given to this plot"
                raise ValueError(
                    f"Several experiments{within} use the same number of CPUs {duplicated_ncpus}, which makes speedup "
                    "and efficiency ill-defined. Use select_best_experiments() to keep only the best performing "
                    f"experiment for each number of CPUs{per}, or select_experiments() to keep some of them."
                )

        selected = self._region_data(listed, exp_names, metric)
        stats = []
        for key, names in by_series.items():
            data = [selected[exp_name].expand_dims({"ncpus": [ncpus[exp_name]]}) for exp_name in names]
            stat = xr.concat(data, dim="ncpus", join="outer").sortby("ncpus")
            if series is not None:
                stat = stat.assign_coords(
                    region=[f"{label}, {_coordinate_name(series)} {key}" for label in stat.region.values]
                )
            stats.append(stat)
        return plot_scaling_metrics(stats, metric, xlabel=xlabel)

    def _coordinate_value(self, exp_name: str, coordinate: ExperimentCoordinate | str) -> Hashable:
        """Returns the value one coordinate has for one experiment.

        "experiment" is the name of the experiment and "ncpus" the CPUs its job occupied. Any other string is the
        key of one of the experiment's labels, and anything else is read from the layout the experiment runs.

        Args:
            exp_name (str): Name of the experiment.
            coordinate (ExperimentCoordinate | str): The coordinate, or the key of a label.

        Returns:
            Hashable: The value.

        Raises:
            ValueError: If the experiment has no such label, if the label is a list or a mapping, if the number of
                CPUs or the layout of the experiment is not known, or if the layout does not have the quantity.
        """
        if coordinate == "experiment":
            return exp_name
        if coordinate == "ncpus":
            return self._ncpus(exp_name)
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

        Args:
            exp_name (str): Name of the experiment.
            component (str): Name of the component, as the layout names it - never a log name.

        Returns:
            int: Number of cores that component was given.

        Raises:
            ValueError: If the experiment's layout cannot be told, or names no such component.
        """
        return self._coordinate_value(exp_name, component_cores(component))

    def gather(
        self, regions: "Region | Sequence[Region]", coords: Sequence[ExperimentCoordinate | str] = ()
    ) -> xr.Dataset:
        """Collects the given regions of every experiment into one dataset.

        The experiments are put along a new `experiment` dimension, whose coordinate is their names. A name
        says nothing about its experiment, so what tells the experiments apart is added as further coordinates
        along that dimension, one for each entry of `coords`. A string is the key of a label, and the coordinate
        holds the label of each experiment, or one of the coordinates every experiment has: "ncpus", the CPUs its
        job occupied, and "experiment", its name, which the dataset has anyway. An `ExperimentCoordinate` is read
        from the layout of each experiment, and the coordinate has its name.

        The dataset is the one a table is made from. Its `region` coordinate holds the labels of the regions, as
        the plots show them, and its variables are the metrics that every log of the regions has, with their units.

        Experiments that ran several times are refused, as the plots refuse them: select_best_run(),
        aggregate_runs() or split_runs() returns results without their runs. Gathering both the fastest and the
        slowest of the runs is how their spread is measured.

        Args:
            regions (Region | Sequence[Region]): Regions to collect. Their `component` is ignored.
            coords (Sequence[ExperimentCoordinate | str]): What to tell the experiments apart by: keys of
                labels, quantities of the layouts, "ncpus" and "experiment". Empty by default.

        Returns:
            xr.Dataset: The regions of every experiment, along `experiment` and `region`.

        Raises:
            ValueError: If these results hold no experiments, if one of them has no data for the log or the region
                of a region, if two regions have the same label, if the profiling data still has a 'run' dimension,
                if two coordinates have the same name or one is named as a coordinate the data has of its own, if
                an experiment has no such label or the label is a list or a mapping, or if an experiment's layout
                cannot be told or does not have the quantity.
        """
        return self._gather(_region_list(regions), coords)

    def _gather(
        self,
        regions: Sequence[Region],
        coords: Sequence[ExperimentCoordinate | str],
        metric: ProfilingMetric | None = None,
    ) -> xr.Dataset:
        """Collects regions of every experiment into one dataset, keeping only one metric if one is given.

        Args:
            regions (Sequence[Region]): Regions to collect, which `_region_list` checked.
            coords (Sequence[ExperimentCoordinate | str]): What to tell the experiments apart by.
            metric (ProfilingMetric | None): The one metric to keep, which every log of the regions must have. None
                keeps those every log of the regions has.

        Returns:
            xr.Dataset: The regions of every experiment, along `experiment` and `region`.

        Raises:
            ValueError: For the reasons gather() raises, or if a log has no such metric.
        """
        exp_names = self._names()
        selected = self._region_data(regions, exp_names, metric)
        gathered = xr.concat(
            [ds.expand_dims({"experiment": [exp_name]}) for exp_name, ds in selected.items()],
            dim="experiment",
            join="outer",
        )

        names = [_coordinate_name(coordinate) for coordinate in coords]
        # The names of the experiments are a coordinate of the data already, and asking for them is asking for that.
        added = [
            (name, coordinate) for name, coordinate in zip(names, coords, strict=True) if coordinate != "experiment"
        ]
        taken = sorted(
            {name for name in names if names.count(name) > 1} | {name for name, _ in added if name in gathered.coords}
        )
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
                for name, coordinate in added
            }
        )

    def table(
        self,
        regions: "Region | Sequence[Region]",
        index: ExperimentCoordinate | str,
        columns: ExperimentCoordinate | str | None = None,
        *,
        metric: ProfilingMetric = tmax,
    ) -> pd.DataFrame:
        """Returns a metric of the given regions as a table, arranged by what tells the experiments apart.

        This is what gather() collects, with the experiments found again by the values of their coordinates
        instead of by their names. There is a row for each value of `index`, sorted, as plot_metric() sorts its
        x-axis. There is a column for each region, in the order they are given and under their labels, and with
        `columns`, one for each value of that coordinate within each region, in the order the experiments give the
        values, as plot_metric() orders its series. A cell no experiment has values for is left empty.

        A table holds numbers rather than quantities, so the values are in the units of the metric, which the
        table states in its `attrs`, under "units".

        Args:
            regions (Region | Sequence[Region]): Regions to tabulate. Their `component` is ignored.
            index (ExperimentCoordinate | str): What the rows are: the key of a label, a quantity of the layout,
                "ncpus" or "experiment".
            columns (ExperimentCoordinate | str | None): What the columns of each region are, in the same way.
                None (the default) gives each region a single column.
            metric (ProfilingMetric): The metric to tabulate. Defaults to tmax.

        Returns:
            pd.DataFrame: The table.

        Raises:
            ValueError: If two regions have the same label, since each region has a column of its own, if two
                experiments have the same values of `index` and `columns`, since a cell holds one value, if the
                metric has more than one value for each experiment and region, or for any of the reasons gather()
                raises.
        """
        listed = _region_list(regions)
        coords = [index] if columns is None else [index, columns]
        names = [_coordinate_name(coordinate) for coordinate in coords]
        gathered = self._gather(listed, coords, metric)
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

        # A pivot sorts its columns, so they are put back in the order of the regions and of the experiments.
        labels = [region.label for region in listed]
        if columns is None:
            table = frame.pivot(index=names[0], columns="region", values=metric)
            table = table.reindex(columns=pd.Index(labels, name="region"))
        else:
            table = frame.pivot(index=names[0], columns=["region", names[1]], values=metric)
            column_values = list(dict.fromkeys(gathered[names[1]].values.tolist()))
            table = table.reindex(
                columns=pd.MultiIndex.from_product([labels, column_values], names=["region", names[1]])
            )
        table.attrs["units"] = str(metric.units)
        return table

    def plot_component_scaling_data(
        self,
        regions: "Region | Sequence[Region]",
        *,
        metric: ProfilingMetric = tmax,
        xlabel: str | None = None,
        ylabel: str | None = None,
        ax: Axes | None = None,
    ) -> Axes:
        """Plots a metric for the given regions against the cores their own component was given.

        Where plot_scaling_data reads every component against the whole job, this reads each against its
        own cores. That is the question to ask of a component whose share of the budget is what changed:
        a component given half again as many cores either ran faster for it or did not, and the total the
        job occupied says nothing about which.

        Each region names the log it comes from and, where the application does not say, the component it belongs
        to, because the two are not the same: one log can hold several components' regions and one component can be
        spread over several logs. See Region.

        The regions are plotted as their own lines rather than added together, since timers are usually inclusive
        - a region and the region it sits inside both count the same seconds, and adding them would count them
        twice.

        Note that a region can be a wait rather than work, and will scale backwards if it is. A region that
        receives from a coupled peer goes up as its own component is given more cores, because it finishes
        its own work sooner and waits longer on the components that have not.

        A study readily measures one component twice on the same number of cores, since a layout giving it
        a certain share says nothing about the size of the job around it. Where it has, the fastest of
        those measurements is what is plotted, and the rest are reported at INFO. The fastest is taken
        region by region, so with several regions of one component the point at that core count may take its
        value for one region from one run and for another from another: each curve answers how fast its
        own region went on that many cores, and deciding a single winning run would mean picking a region
        to decide it by.

        Args:
            regions (Region | Sequence[Region]): Regions to plot, and what to read each of them against.
            metric (ProfilingMetric): The metric to plot. Defaults to tmax.
            xlabel (str | None): Optional label for the x-axis.
            ylabel (str | None): Optional label for the y-axis. If None, the metric names it.
            ax (Axes | None): The axes to draw on. None (the default) draws on a figure of its own, which nothing
                here shows: a notebook shows it at the end of the cell.

        Returns:
            Axes: The axes the lines are drawn on.

        Raises:
            ValueError: If these results hold no experiments, if one has no data for the log, the region or the
                metric of a region, if two regions have the same label, if the profiling data still has a 'run'
                dimension, or if an experiment's layout cannot be told or names no such component.
        """
        listed = _region_list(regions)
        exp_names = self._names()
        # What was asked for is settled first, since a log or a region these results hold nothing for is a plainer
        # thing to be told than anything about cores.
        selected = self._region_data(listed, exp_names, metric)

        # Then where each of them goes: the cores of a region's component are its x axis, so the regions are put
        # together by component, in the order their components are first named.
        by_component: dict[str, list[str]] = {}
        for region in listed:
            by_component.setdefault(self._region_component(region), []).append(region.label)

        scaling_data = []
        for component, labels in by_component.items():
            cores = {exp_name: self._component_cores(exp_name, component) for exp_name in exp_names}
            _note_shared_core_counts(component, cores)

            # Grouping by the cores is what reduces the experiments that share a count to the fastest of
            # them, and it orders the result as it goes, so there is nothing left to sort.
            data = [
                ds.sel(region=labels).expand_dims({"ncpus": [cores[exp_name]]}) for exp_name, ds in selected.items()
            ]
            scaling_data.append(xr.concat(data, dim="ncpus", join="outer").groupby("ncpus").min())

        return plot_component_scaling(scaling_data, metric, xlabel=xlabel, ylabel=ylabel, ax=ax)

    def plot_bar_chart(
        self,
        regions: "Region | Sequence[Region]",
        *,
        metric: ProfilingMetric = tmax,
        label: ExperimentCoordinate | str | None = None,
        experiment_relabel_map: dict | None = None,
        ax: Axes | None = None,
    ) -> Axes:
        """Plots a bar chart of a profiling metric over regions, grouped by experiment.

        Regions are placed along the x-axis, in the order they are given, with one bar per experiment within each.
        No cores appear on either axis, so each region's `component` is ignored here, as it is by
        plot_scaling_data.

        The bars of an experiment are named after it, unless `experiment_relabel_map` names them otherwise or
        `label` names them by a coordinate. A coordinate names them as `"<name> <value>"`, as plot_metric() names
        its series, and so needs a value of its own for every experiment plotted.

        Args:
            regions (Region | Sequence[Region]): Regions to plot. Each region becomes one position on the x-axis,
                under its label.
            metric (ProfilingMetric): Metric to plot. Defaults to tmax.
            label (ExperimentCoordinate | str | None): What to name the bars of each experiment by: the key of a
                label, a quantity of the layout, or "ncpus". None (the default) names them after the experiment, or
                as experiment_relabel_map says.
            experiment_relabel_map (dict | None): Optional mapping to relabel experiments in the plot.
                Experiments are named by the manager rather than enumerated by the caller, so unlike the
                regions they have no label of their own.
            ax (Axes | None): The axes to draw on. None (the default) draws on a figure of its own, sized to the
                bars, which nothing here shows: a notebook shows it at the end of the cell.

        Returns:
            Axes: The axes the bar chart is drawn on.

        Raises:
            ValueError: If both experiment_relabel_map and label are given, if these results hold no experiments, if
                one has no data for the log, the region or the metric of a region, if two regions have the same
                label, if the profiling data still has a 'run' dimension, if the metric has more than one value for
                each region, if two experiments have the same value of label, or if label cannot be read for an
                experiment.
        """
        if label is not None and experiment_relabel_map is not None:
            raise ValueError("Give experiment_relabel_map or label, not both: both say what the bars are named.")
        listed = _region_list(regions)
        exp_names = self._names()

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

        bar_data: dict[str, list[float]] = {}
        for exp_name, ds in self._region_data(listed, exp_names, metric).items():
            _refuse_values_besides_regions(metric, ds[metric].dims, "a chart has bars")
            # By position rather than by label: the selection is already in the order the regions were given.
            bar_data[exp_name] = [float(value) for value in ds[metric].pint.dequantify().values]

        exp_relabel = experiment_relabel_map or {}
        relabelled_bar_data = {exp_relabel.get(k, k): v for k, v in bar_data.items()}

        return plot_bar_metrics(relabelled_bar_data, [region.label for region in listed], metric, ax=ax)

    def _series(self, exp_names: list[str], series: ExperimentCoordinate | str | None) -> dict[str | None, list[str]]:
        """Returns the experiments of each series, the lines a plot divides the experiments into.

        A line is told apart from the others by its label, so the series are keyed by the text their values are
        shown as. The values 1, 1.0 and True are equal in Python, but are shown as three lines. Two values shown
        alike, such as 1 and "1", are refused, since their lines could not be told apart.

        Args:
            exp_names (list[str]): Experiments to divide.
            series (ExperimentCoordinate | str | None): What the experiments are divided by, if anything.

        Returns:
            dict[str | None, list[str]]: The experiments of each series, keyed by the value of the series as it is
                shown, in the order the experiments give the values. Without a series, the one key is None.

        Raises:
            ValueError: If the series cannot be read for an experiment, or if two of its values are shown alike.
        """
        if series is None:
            return {None: list(exp_names)}
        groups: dict[str | None, list[str]] = {}
        shown: dict[str, Hashable] = {}
        for exp_name in exp_names:
            value = self._coordinate_value(exp_name, series)
            key = str(value)
            first = shown.setdefault(key, value)
            if (type(first), first) != (type(value), value):
                raise ValueError(
                    f"The values {first!r} and {value!r} of '{_coordinate_name(series)}' are both shown as '{key}', so "
                    "their lines could not be told apart. Give every experiment a value of the same type."
                )
            groups.setdefault(key, []).append(exp_name)
        return groups

    def _points(
        self, exp_names: list[str], x: ExperimentCoordinate | str, series: ExperimentCoordinate | str | None
    ) -> dict[str | None, dict[Real, list[str]]]:
        """Returns which experiments are at each point of a plot of a metric against a coordinate.

        Says, at INFO, which experiments share a point: the fastest of them is what gets plotted, and a reader
        of the figure should be told that the point is the best of several. The series are divided by `_series`.

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
        for key, names in self._series(exp_names, series).items():
            along_x = points.setdefault(key, {})
            for exp_name in names:
                x_value = self._coordinate_value(exp_name, x)
                # A boolean is a number to Python, but true and false have no place along an axis.
                if isinstance(x_value, bool) or not isinstance(x_value, Real):
                    raise ValueError(
                        f"Experiment '{exp_name}' has the value {x_value!r} for '{_coordinate_name(x)}', which is "
                        "not a number, so it has no place along an x-axis. Use the coordinate as the series instead, "
                        "or compare the experiments with plot_bar_chart."
                    )
                along_x.setdefault(x_value, []).append(exp_name)

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
        regions: "Region | Sequence[Region]",
        x: ExperimentCoordinate | str,
        *,
        series: ExperimentCoordinate | str | None = None,
        metric: ProfilingMetric = tmax,
        xlabel: str | None = None,
        ylabel: str | None = None,
        ax: Axes | None = None,
    ) -> Axes:
        """Plots a metric for the given regions against a coordinate of the experiments.

        Where plot_component_scaling_data puts the cores of a component along the x-axis, this puts whatever
        the study varied: one of the labels of the experiments, named by its key, a quantity of their layouts,
        given as an `ExperimentCoordinate`, or "ncpus", the CPUs their jobs occupied. Its values must be numbers,
        and booleans are not. The axes are linear and no ideal line is drawn, since the coordinate need not be a
        number of cores.

        Each region is one line. With `series`, the experiments are first divided by the value of another
        coordinate, and each region has one line for each value, in the order the experiments give the values.
        A line is labelled with its region, followed by the name and the value of the series. The values are told
        apart as they are shown, so 1 and 1.0 are two lines, and two values shown alike, such as 1 and "1", are
        refused.

        Where several experiments share both the value of `x` and the value of `series`, the fastest of them is
        what is plotted, and they are reported at INFO. The fastest is taken region by region, as
        plot_component_scaling_data takes it, passing over an experiment with no value for the region (NaN). Each
        region's `component` is ignored here.

        Args:
            regions (Region | Sequence[Region]): Regions to plot.
            x (ExperimentCoordinate | str): What goes along the x-axis: the key of a label, a quantity of the
                layout, or "ncpus".
            series (ExperimentCoordinate | str | None): What the experiments are divided into lines by, in the same
                way. None (the default) draws one line for each region.
            metric (ProfilingMetric): The metric to plot. Defaults to tmax.
            xlabel (str | None): Optional label for the x-axis. If None, the name of `x` is used.
            ylabel (str | None): Optional label for the y-axis. If None, the metric names it.
            ax (Axes | None): The axes to draw on. None (the default) draws on a figure of its own, which nothing
                here shows: a notebook shows it at the end of the cell.

        Returns:
            Axes: The axes the lines are drawn on.

        Raises:
            ValueError: If these results hold no experiments, if one has no data for the log, the region or the
                metric of a region, if two regions have the same label, if the profiling data still has a 'run'
                dimension, if the metric has more than one value for each region, if an experiment has no such label
                or the label is a list or a mapping, if an experiment's layout cannot be told or does not have the
                quantity, if a value of `x` is not a number or is a boolean, or if two values of `series` are shown
                alike.
        """
        listed = _region_list(regions)
        exp_names = self._names()
        # What was asked for is settled first, as plot_component_scaling_data settles it.
        selected = self._region_data(listed, exp_names, metric)
        for ds in selected.values():
            _refuse_values_besides_regions(metric, ds[metric].dims, "a line has points")
        points = self._points(exp_names, x, series)

        lines = []
        # By position rather than by label, as plot_bar_chart reads the regions.
        for index, region in enumerate(listed):
            values = {
                exp_name: float(ds[metric].isel(region=index).pint.dequantify()) for exp_name, ds in selected.items()
            }
            for key, along_x in points.items():
                line_label = region.label
                if series is not None:
                    line_label += f", {_coordinate_name(series)} {key}"
                x_values = sorted(along_x)
                # NaN, a region an experiment has no value for, is passed over as the xarray reduction of
                # plot_component_scaling_data passes over it. Python's min() keeps it or not by where it is.
                fastest = []
                for x_value in x_values:
                    measured = [values[name] for name in along_x[x_value] if not math.isnan(values[name])]
                    fastest.append(min(measured, default=math.nan))
                lines.append((line_label, x_values, fastest))

        return plot_line_metrics(
            lines, metric, xlabel=xlabel if xlabel is not None else _coordinate_name(x), ylabel=ylabel, ax=ax
        )
