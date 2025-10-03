# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Functions to calculate metrics related to parallel scaling of applications."""

import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import xarray as xr
from matplotlib.figure import Figure

from access.profiling.metrics import ProfilingMetric
from access.profiling.plotting_utils import calculate_column_widths


def parallel_speedup(stats: xr.Dataset, metric: ProfilingMetric) -> xr.DataArray:
    """Calculates the parallel speedup from scaling data.

    Args:
        stats (Dataset): Scaling data, stored as a xarray dataset.
        metric (ProfilingMetric): Metric to use for the speedup calculation.

    Returns:
        DataArray: Parallel speedup.
    Raises:
        ValueError: If metric units are not time (e.g., seconds).
    """
    if stats[metric].pint.dimensionality != "[time]":
        raise ValueError("Metric units must be time (e.g., seconds)!")
    speedup = stats[metric].sel(ncpus=stats["ncpus"].min()) / stats[metric]
    speedup.name = "speedup"
    return speedup


def parallel_efficiency(stats: xr.Dataset, metric: ProfilingMetric) -> xr.DataArray:
    """Calculates the parallel efficiency from scaling data.

    Args:
        stats (Dataset): Scaling data, stored as a xarray dataset.
        metric (ProfilingMetric): Metric to use for the efficiency calculation.

    Returns:
        DataArray: Parallel efficiency.
    """
    speedup = parallel_speedup(stats, metric)
    eff = speedup * (speedup.ncpus.min() / speedup.ncpus)
    eff = eff.pint.to("percent")
    eff.name = "parallel efficiency"
    return eff


def plot_scaling_metrics(
    stats: list[xr.Dataset],
    regions: list[list[str]],
    metric: ProfilingMetric,
    xcoordinate: str = "ncpus",
    region_relabel_map: dict = None,
    first_col_fraction: float = 0.4,
    show: bool = True,
) -> Figure:
    """Plots parallel speedup and efficiency from a list of datasets

    Args:
        stats (list[xr.Dataset]): The raw times to plot.
        regions (list[list[str]]): The list of regions to plot.
                                   regions[0][:] corresponds to regions to plot in stats[0].
        metric (ProfilinMetric): The metric to plot for each stat.
        xcoordinate (str): The x-axis variable e.g. "ncpus".
        region_relabel_map (dict): Mapping of labels to use for each region instead of the region name.
                                   If an element of "regions" is a key in this map, the region
                                   will be replaced by the corresponding value in the plot.
                                   Default: None.
        first_col_fraction (float): The fraction of table width to assign to the row labels. Default 0.4.
        show (bool): Whether to show the generated plot. Default: True.

    Returns:
        Figure: The Matplotlib figure on which the scaling plots and table are plotted on.

    Raises:
        ValueError: If region_labels is non-empty
    """

    # set default relabel map
    if region_relabel_map is None:
        region_relabel_map = {}

    # setup plots
    fig = plt.figure(figsize=(15, 6))
    # using gridspec so table can be added
    gs = gridspec.GridSpec(2, 2, height_ratios=[3, 1], hspace=0.3)
    ax1, ax2 = fig.add_subplot(gs[0, 0]), fig.add_subplot(gs[0, 1])
    ax_tbl = fig.add_subplot(gs[1, :])

    # add table of raw timings
    tbl = [[xcoordinate] + list(stats[0][xcoordinate].values)]  # first row
    for stat, region in zip(stats, regions, strict=True):
        # calculate efficiency and speedup
        efficiency = parallel_efficiency(stat, metric)
        speedup = parallel_speedup(stat, metric)

        # plots speedup and efficiency on their respective axes.
        max_eff = 100
        for r in region:
            label = region_relabel_map.get(r)
            label = label if label else r
            speedup.loc[r, :].plot.line(x=xcoordinate, ax=ax1, marker="o", label=label)
            efficiency.loc[r, :].plot.line(x=xcoordinate, ax=ax2, marker="o", label=label)
            # find max efficiency for setting efficiency axis
            max_eff = max(max_eff, efficiency.loc[r, :].max())

            tbl.append([label] + [f"{val:.2f}" for val in stat[metric].loc[:, r].pint.dequantify().values])

    # ideal speedup/scaling
    minx = stat[xcoordinate].values.min()
    nx = len(stat[xcoordinate].values)
    ideal_speedups = [i / minx for i in stat[xcoordinate].values]
    ax1.plot(stat[xcoordinate].values, ideal_speedups, "k:", label="ideal")
    ax2.plot(stat[xcoordinate].values, [100] * nx, "k:", label="ideal")

    # formatting
    ax1.legend()
    ax1.grid()
    ax2.grid()
    ax2.set_ylim((0, 1.1 * max_eff))
    ax1.set_title("Parallel Speedup")
    ax2.set_title("Parallel Efficiency")
    ax_tbl.axis("off")
    tbl_chart = ax_tbl.table(
        tbl,
        bbox=(0.05, 0, 0.9, 1),
        cellLoc="center",
        colWidths=calculate_column_widths(tbl, first_col_fraction),
    )
    ax_tbl.set_title(f"Timings ({stat[metric].pint.units})")
    for i in range(len(tbl[0])):
        tbl_chart[(0, i)].set_text_props(weight="bold")
    for i in range(len(tbl)):
        tbl_chart[(i, 0)].set_text_props(weight="bold")

    if show:
        plt.show()

    return fig
