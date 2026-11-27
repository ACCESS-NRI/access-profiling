# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from unittest import mock

import matplotlib.pyplot as plt
import pytest
from conftest import legend_labels
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle

from access.profiling.metrics import tavg
from access.profiling.plotting_utils import (
    calculate_column_widths,
    plot_bar_metrics,
    plot_line_metrics,
)


@pytest.fixture(scope="module")
def table_data():
    """Fixture returning sample table data."""
    return [
        ["Cell00", "Cell01", "Cell      02"],
        ["Cell10", "Cell11", "Cell  12"],
        ["Cell20", "Cell21", "Cell 22"],
        ["Cell30", "Cell31", "Cell32"],
    ]


@pytest.fixture(scope="module")
def nonrectangular_table_data():
    """Fixture returning sample invalid table data where table isn't rectangular."""
    return [
        ["Cell00", "Cell01", "Cell02"],
        ["Cell10", "Cell11"],
    ]


@pytest.fixture(scope="module")
def singlerow_table_data():
    """Fixture returning sample invalid table data where table only has 1 row."""
    return [
        ["Cell00", "Cell01", "Cell02"],
    ]


@pytest.fixture(scope="module")
def singlecol_table_data():
    """Fixture returning sample invalid table data where table only has 1 column."""
    return [
        ["Cell00"],
        ["Cell10"],
    ]


def test_calculate_column_widths_flexible(table_data):
    """Test the calculate_column_widths function."""
    # Test with empty table
    assert calculate_column_widths([]) == []

    # Test with multiple rows, multiple columns
    col_widths = calculate_column_widths(table_data, 0.4)
    assert abs(sum(col_widths) - 1.0) < 1e-6  # Sum to 1.0
    assert col_widths == pytest.approx([0.4, 0.2, 0.4])  # Proportional to content length with first column flexible


def test_calculate_column_widths_fixed(table_data):
    # Test with first_col_flexible=False
    col_widths = calculate_column_widths(table_data)
    assert abs(sum(col_widths) - 1.0) < 1e-6  # Sum to 1.0
    assert col_widths == [0.25, 0.25, 0.5]  # Proportional to content length


def test_wrong_column_widths(table_data):
    with pytest.raises(ValueError):
        calculate_column_widths(table_data, first_col_fraction=1.0)
    with pytest.raises(ValueError):
        calculate_column_widths(table_data, first_col_fraction=-1.0)


def test_invalid_tables(nonrectangular_table_data, singlerow_table_data, singlecol_table_data):
    with pytest.raises(ValueError):
        calculate_column_widths(nonrectangular_table_data)
    with pytest.raises(ValueError):
        calculate_column_widths(singlerow_table_data)
    with pytest.raises(ValueError):
        calculate_column_widths(singlecol_table_data)


def test_plot_bar_metrics_returns_figure():
    """Test that plot_bar_metrics returns a Figure with correct structure."""
    data = {
        "exp_A": [100.0, 50.0, 75.0, 30.0],
        "exp_B": [80.0, 40.0, 60.0, 20.0],
        "exp_C": [90.0, 45.0, 70.0, 25.0],
    }
    region_labels = ["Region 1", "Region 2", "Region 3", "Region 4"]

    fig = plot_bar_metrics(data, region_labels, tavg).figure

    assert isinstance(fig, Figure)
    ax = fig.axes[0]

    # Check axis labels and title
    assert ax.get_xlabel() == "Region"
    assert tavg.name in ax.get_ylabel()
    assert str(tavg.units) in ax.get_ylabel()
    assert ax.get_title() == tavg.description

    # Check x-tick labels match the region labels
    tick_labels = [t.get_text() for t in ax.get_xticklabels()]
    assert tick_labels == region_labels

    # Check legend shows experiment names
    labels = legend_labels(ax)
    assert labels == ["exp_A", "exp_B", "exp_C"]

    # Check correct number of bars: 3 experiments * 4 regions = 12 bars
    assert len(ax.patches) == 12

    # matplotlib groups bars by series: first all exp_A bars, then exp_B, then exp_C
    # Axes.patches is typed as the Patch base class; the bars a bar chart draws are Rectangles,
    # which is where get_height lives.
    heights = [p.get_height() for p in ax.patches if isinstance(p, Rectangle)]
    assert heights == pytest.approx([100.0, 50.0, 75.0, 30.0, 80.0, 40.0, 60.0, 20.0, 90.0, 45.0, 70.0, 25.0])


def test_plot_bar_metrics_single_experiment():
    """Test plot_bar_metrics with a single experiment."""
    data = {"exp_A": [10.0, 20.0, 30.0, 40.0, 50.0]}
    region_labels = ["R1", "R2", "R3", "R4", "R5"]

    fig = plot_bar_metrics(data, region_labels, tavg).figure
    ax = fig.axes[0]

    assert len(ax.patches) == 5
    labels = legend_labels(ax)
    assert labels == ["exp_A"]


@mock.patch("access.profiling.plotting_utils.plt.show")
def test_plot_bar_metrics_show(mock_show):
    """Test that plt.show() is never called: a notebook shows the figure at the end of the cell."""
    data = {"exp_A": [10.0]}
    region_labels = ["R1"]

    plot_bar_metrics(data, region_labels, tavg)
    assert mock_show.call_count == 0


def positions(fig):
    """Where each axes of a figure is, which laying the figure out changes."""
    return [ax.get_position().bounds for ax in fig.axes]


def test_plot_bar_metrics_draws_on_the_axes_it_is_given():
    """Which is how a chart shares a figure with another plot, whose layout it leaves alone."""

    fig, (left, right) = plt.subplots(1, 2)
    before = positions(fig)

    assert plot_bar_metrics({"exp_A": [10.0]}, ["R1"], tavg, ax=left) is left
    assert len(left.patches) == 1 and len(right.patches) == 0
    assert positions(fig) == before
    plt.close(fig)


def test_plot_bar_metrics_lays_out_a_figure_of_its_own():
    fig = plot_bar_metrics({"exp_A": [10.0]}, ["R1"], tavg).figure

    assert positions(fig) != positions(plt.subplots(figsize=fig.get_size_inches())[0])


class TestPlotLineMetrics:
    """A metric against a quantity of the experiments, one line at a time."""

    LINES = (("Ocean", [24, 48, 96], [300.0, 180.0, 120.0]), ("Sea ice", [24, 48], [80.0, 60.0]))

    def test_each_line_is_drawn_through_its_points(self):
        fig = plot_line_metrics(self.LINES, tavg, xlabel="Blocks per rank").figure

        assert isinstance(fig, Figure)
        lines = fig.axes[0].lines
        assert [list(line.get_xdata()) for line in lines] == [[24, 48, 96], [24, 48]]
        assert [list(line.get_ydata()) for line in lines] == [[300.0, 180.0, 120.0], [80.0, 60.0]]
        assert all(line.get_marker() == "o" for line in lines), "so that a line of one point can be seen"

    def test_the_legend_lists_the_lines_in_the_order_they_are_given(self):
        fig = plot_line_metrics(self.LINES, tavg, xlabel="Blocks per rank").figure

        assert legend_labels(fig.axes[0]) == ["Ocean", "Sea ice"]

    def test_two_lines_with_the_same_label_are_both_drawn(self):
        """Which is why the lines come as a sequence, and not keyed by their labels."""

        lines = [("Total", [1, 2], [10.0, 6.0]), ("Total", [1, 2], [4.0, 3.0])]
        fig = plot_line_metrics(lines, tavg, xlabel="Blocks per rank").figure

        assert legend_labels(fig.axes[0]) == ["Total", "Total"]
        assert len(fig.axes[0].lines) == 2

    def test_the_axes_are_linear(self):
        """The quantity along the x axis need not be a number of cores, so nothing here is logarithmic."""

        ax = plot_line_metrics(self.LINES, tavg, xlabel="Blocks per rank").figure.axes[0]

        assert ax.get_xscale() == "linear" and ax.get_yscale() == "linear"
        assert ax.xaxis.get_gridlines()[0].get_visible()

    def test_the_axes_are_labelled_and_the_figure_has_no_title(self):
        ax = plot_line_metrics(self.LINES, tavg, xlabel="Blocks per rank").figure.axes[0]

        assert ax.get_xlabel() == "Blocks per rank"
        assert ax.get_ylabel() == f"{tavg.name} ({tavg.units})"
        assert ax.get_title() == ""

    def test_a_label_for_the_y_axis_replaces_the_metric(self):
        fig = plot_line_metrics(self.LINES, tavg, xlabel="Blocks per rank", ylabel="Walltime (s)").figure

        assert fig.axes[0].get_ylabel() == "Walltime (s)"

    @mock.patch("access.profiling.plotting_utils.plt.show")
    def test_it_is_not_shown(self, mock_show):
        plot_line_metrics(self.LINES, tavg, xlabel="Blocks per rank")
        assert mock_show.call_count == 0

    def test_it_draws_on_the_axes_it_is_given(self):
        fig, ax = plt.subplots()
        before = positions(fig)

        assert plot_line_metrics(self.LINES, tavg, xlabel="Blocks per rank", ax=ax) is ax
        assert len(ax.lines) == 2
        assert positions(fig) == before, "the layout of a figure it did not make is left alone"
        plt.close(fig)

    def test_it_lays_out_a_figure_of_its_own(self):
        fig = plot_line_metrics(self.LINES, tavg, xlabel="Blocks per rank").figure

        assert positions(fig) != positions(plt.subplots(figsize=fig.get_size_inches())[0])
