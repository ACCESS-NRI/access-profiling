# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The parts of the plots that are not about scaling.

The bar chart and the line plot that the plotting methods of the results draw, and the helper that sizes the columns
of the table beside a scaling plot. The regions a plot shows are named with `access.profiling.results.Region`.

Every plot returns the axes it drew on and shows nothing itself. The figure is made with pyplot, so a notebook shows
it once, at the end of the cell, and the axes are what a caller customises, as `ax.set(xscale="log")` does. A plot
given axes draws on them instead, which is how two plots share a figure. A script shows the figure with
`matplotlib.pyplot.show()`, or saves `ax.figure`.
"""

from collections.abc import Sequence

import matplotlib.pyplot as plt
from matplotlib.axes import Axes

from access.profiling.metrics import ProfilingMetric


def calculate_column_widths(table_data: list[list], first_col_fraction: float | None = None) -> list:
    """Calculate column widths based on content character length and required width for first column.

    Args:
        table_data (list[list]): Table data including headers. e.g.

            ```python
            [[  "ncpus",  "col1", "col2", "col3"],
             ["region1",    0.1,    0.2,    0.3],
             ["region2",    1. ,    2. ,    3. ]]
            ```

        first_col_fraction (float | None): If provided, controls the fraction of the table width
            assigned to the first column, the other columns sharing the rest in proportion to the length of their
            content. Default None. If set to 0.0 or None, every column's width is proportional to the length of
            its content, the first column's included. Must be between 0.0 (inclusive) and 1.0 (exclusive).

    Returns:
        list : Column width fractions, adding up to 1. An empty table has no columns, and gives an empty list.

    Raises:
        ValueError: If table_data has a single row, or fewer than 2 columns.
        ValueError: If table_data shape is not rectangular, i.e., all rows do not have the same number of columns.
        ValueError: If first_col_fraction is not between 0.0 (inclusive) and 1.0 (exclusive).
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
        # Every column proportional to its content, the first one included
        total_length = sum(max_lengths)
        col_widths = [length / total_length for length in max_lengths]

    return col_widths


def plot_bar_metrics(
    data: dict[str, list[float]],
    region_labels: list[str],
    metric: ProfilingMetric,
    ax: Axes | None = None,
) -> Axes:
    """Plots a grouped bar chart of a profiling metric over regions.

    Regions are placed along the x-axis. Within each region group, there is one bar per
    experiment, coloured by experiment name.

    Args:
        data (dict[str, list[float]]): Mapping of experiment name to a list of metric values,
            one per region (in the same order as `region_labels`).
        region_labels (list[str]): Ordered list of region display labels for the x-axis.
        metric (ProfilingMetric): The metric being plotted (used for axis labels and title).
        ax (Axes | None): The axes to draw on. None (the default) draws on a figure of its own, sized to the bars.

    Returns:
        Axes: The axes the bar chart is drawn on.
    """
    exp_names = list(data.keys())
    n_experiments = len(exp_names)
    n_regions = len(region_labels)

    own_figure = ax is None
    if ax is None:
        _, ax = plt.subplots(figsize=(max(8, n_experiments * n_regions * 0.8), 6))
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
    # The layout of a figure the caller made is the caller's.
    if own_figure:
        ax.figure.tight_layout()

    return ax


def plot_line_metrics(
    lines: Sequence[tuple[str, Sequence[float], Sequence[float]]],
    metric: ProfilingMetric,
    xlabel: str,
    ylabel: str | None = None,
    ax: Axes | None = None,
) -> Axes:
    """Plots a profiling metric against a quantity of the experiments, one line at a time.

    Both axes are linear and no ideal line is drawn, because the quantity along the x-axis need not be a number of
    cores: it may be a size of block or a number of threads, for which nothing says what ideal would be.
    `access.profiling.scaling.plot_component_scaling` is the plot of a metric against a component's own cores.

    The figure carries no title, as that plot carries none: the y-axis already names the metric.

    Args:
        lines (Sequence[tuple[str, Sequence[float], Sequence[float]]]): The lines to draw, in the order they are
            drawn and listed in the legend. Each is its label, its values along the x-axis and the value of the
            metric at each of them. They come as a sequence and not as a mapping, so that two lines given the same
            label are both drawn.
        metric (ProfilingMetric): The metric being plotted, which labels the y-axis unless ylabel is given.
        xlabel (str): Label for the x-axis: what the quantity along it is.
        ylabel (str | None): Optional label for the y-axis. If None, the metric names it.
        ax (Axes | None): The axes to draw on. None (the default) draws on a figure of its own.

    Returns:
        Axes: The axes the lines are drawn on.
    """
    own_figure = ax is None
    if ax is None:
        _, ax = plt.subplots(figsize=(10, 6))

    for label, x_values, values in lines:
        ax.plot(x_values, values, marker="o", label=label)

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel if ylabel is not None else f"{metric.name} ({metric.units})")
    ax.legend()
    ax.grid(linestyle="--", alpha=0.7)
    if own_figure:
        ax.figure.tight_layout()

    return ax
