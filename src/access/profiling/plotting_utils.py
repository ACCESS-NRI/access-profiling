# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import matplotlib.pyplot as plt
from matplotlib.figure import Figure

from access.profiling.metrics import ProfilingMetric


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

    One list of groups describes a study to all three plotting methods. Only plot_component_scaling_data
    reads `component`, being the only one whose x axis is a component's own cores; plot_scaling_data reads
    every region against the whole job and plot_bar_chart puts no cores on either axis, so both ignore it
    rather than refusing a list that carries it. A component named here is still checked by the plot that
    uses it, so a misspelling is not lost as long as the same list reaches that plot.

    Args:
        log (str): Name of the profiling log the regions come from, as the application's LogSpec names it.
            This is the log, never a component: the parsed data is keyed by log throughout.
        regions (Sequence[str]): Regions of that log to plot, each of which becomes a line or a bar.
        component (str | None): Component whose cores these regions are read against, named as the layout
            names it, and read only by plot_component_scaling_data. None (the default) takes the component
            the application declares for this log, and failing that the log's own name - which is enough
            for a log that is one component's alone. Name it for a log that is no single component's, such
            as a coupled summary covering several.
        relabel (Mapping[str, str] | None): What to call the named regions, keyed by region name. A region
            left out is plotted as "<log>: <region>", since region names are only made unique within one
            log and two components may each have a "Total"; a region named here is plotted under its label
            alone, the caller having said what they want it called.

    Raises:
        ValueError: If no regions are given, or if a label names a region the group does not hold.
    """

    log: str
    regions: Sequence[str]
    component: str | None = None
    relabel: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        # Both are copied rather than kept, so that a group given a list or a dict is still the frozen
        # thing it claims to be and cannot change under a caller who goes on editing what they passed.
        object.__setattr__(self, "regions", tuple(self.regions))
        if not self.regions:
            raise ValueError(f"The group of log {self.log!r} names no regions, so there is nothing to plot.")
        if self.relabel is not None:
            object.__setattr__(self, "relabel", dict(self.relabel))
            unknown = sorted(region for region in self.relabel if region not in self.regions)
            if unknown:
                raise ValueError(
                    f"The group of log {self.log!r} labels region(s) {unknown}, which it does not hold. "
                    f"It holds {list(self.regions)}."
                )

    def label(self, region: str) -> str:
        """Returns the label one of this group's regions is to be plotted under.

        A region is named after the log it came from, since region names are only made unique within one
        log. A caller who has relabelled one has said what they want it called, though, so that name is
        left to stand on its own.

        Args:
            region (str): Name of the region, as the log names it.

        Returns:
            str: The label.
        """
        if self.relabel is not None and region in self.relabel:
            return self.relabel[region]
        return f"{self.log}: {region}"


def calculate_column_widths(table_data: list[list], first_col_fraction: float | None = None) -> list:
    """Calculate column widths based on content character length and required width for first column.

    Args:
        table_data (list[list]): Table data including headers. e.g.
                                 [[  "ncpus",  "col1", "col2", "col3"],
                                  ["region1",    0.1,    0.2,    0.3],
                                  ["region2",    1. ,    2. ,    3. ]]
        first_col_fraction (float): If provided, controls the fraction of the table width
                                    assigned to the first column. Default None.
                                    If set to 0.0 or None, all columns have the same width.
                                    Must be between 0.0 (inclusive) and 1.0 (exclusive).

    Returns:
        list : Column width fractions, adding up to 1.

    Raises:
        ValueError: If table_data has fewer than 2 rows or 2 columns.
        ValueError: If table_data shape is not rectangular, i.e., all rows do not have the same number of columns.
    """
    if not table_data:
        return []

    # Check that table has a header row and row-label column, and no missing elements.
    if len(table_data) > 1:
        for row in table_data[1:]:
            if len(row) != len(table_data[0]):
                raise ValueError("Table rows must have the same number of elements")
    else:
        raise ValueError("Table must have at least 2 rows (first row is table header)")
    if len(table_data[0]) < 2:
        raise ValueError("Table must have at least 2 columns (first column is row label)")

    if first_col_fraction is not None and not (0 <= first_col_fraction < 1):
        raise ValueError("first_col_fraction must be between 0 and 1 (exclusive)")

    n_cols = len(table_data[0])

    # Calculate max content length for each column based on no. of chars
    max_lengths = []
    for col in range(n_cols):
        col_lengths = [len(str(row[col])) for row in table_data]
        max_lengths.append(max(col_lengths))

    if first_col_fraction and first_col_fraction > 0:
        # Set data columns to proportional widths based on content and first_col_fraction
        data_cols_total = sum(max_lengths[1:])
        base_width = (1 - first_col_fraction) / data_cols_total

        col_widths = [first_col_fraction]
        for length in max_lengths[1:]:
            col_widths.append(length * base_width)

    else:
        # Equal column width
        total_length = sum(max_lengths)
        col_widths = [length / total_length for length in max_lengths]

    return col_widths


def plot_bar_metrics(
    data: dict[str, list[float]],
    region_labels: list[str],
    metric: ProfilingMetric,
    show: bool = True,
) -> Figure:
    """Plots a grouped bar chart of a profiling metric over regions.

    Regions are placed along the x-axis. Within each region group, there is one bar per
    experiment, coloured by experiment name.

    Args:
        data (dict[str, list[float]]): Mapping of experiment name to a list of metric values,
            one per region (in the same order as ``region_labels``).
        region_labels (list[str]): Ordered list of region display labels for the x-axis.
        metric (ProfilingMetric): The metric being plotted (used for axis labels and title).
        show (bool): Whether to call ``plt.show()``. Default: True.

    Returns:
        Figure: The Matplotlib figure containing the bar chart.
    """
    exp_names = list(data.keys())
    n_experiments = len(exp_names)
    n_regions = len(region_labels)

    fig, ax = plt.subplots(figsize=(max(8, n_experiments * n_regions * 0.8), 6))
    bar_width = 0.8 / n_experiments
    group_positions = list(range(n_regions))

    for i, exp_name in enumerate(exp_names):
        offsets = [pos + (i - (n_experiments - 1) / 2) * bar_width for pos in group_positions]
        ax.bar(offsets, data[exp_name], width=bar_width, label=exp_name)

    ax.set_xticks(group_positions)
    ax.set_xticklabels(region_labels)
    ax.set_xlabel("Region")
    ax.set_ylabel(f"{metric.name} ({metric.units})")
    ax.set_title(f"{metric.description}")
    ax.legend(title="Experiment")
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    fig.tight_layout()

    if show:
        plt.show()

    return fig
