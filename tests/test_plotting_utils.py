# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from unittest import mock

import pytest
from matplotlib.figure import Figure

from access.profiling.metrics import tavg
from access.profiling.plotting_utils import calculate_column_widths, plot_bar_metrics


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
        "exp_A": [100.0, 50.0],
        "exp_B": [80.0, 40.0],
    }
    region_labels = ["Region 1", "Region 2"]

    fig = plot_bar_metrics(data, region_labels, tavg, show=False)

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
    legend_labels = [t.get_text() for t in ax.get_legend().get_texts()]
    assert legend_labels == ["exp_A", "exp_B"]

    # Check correct number of bars: 2 experiments * 2 regions = 4 bars
    assert len(ax.patches) == 4

    # matplotlib groups bars by series: first all exp_A bars, then all exp_B bars
    heights = [p.get_height() for p in ax.patches]
    assert heights == pytest.approx([100.0, 50.0, 80.0, 40.0])


def test_plot_bar_metrics_single_experiment():
    """Test plot_bar_metrics with a single experiment."""
    data = {"exp_A": [10.0, 20.0, 30.0]}
    region_labels = ["R1", "R2", "R3"]

    fig = plot_bar_metrics(data, region_labels, tavg, show=False)
    ax = fig.axes[0]

    assert len(ax.patches) == 3
    legend_labels = [t.get_text() for t in ax.get_legend().get_texts()]
    assert legend_labels == ["exp_A"]


@mock.patch("access.profiling.plotting_utils.plt.show")
def test_plot_bar_metrics_show(mock_show):
    """Test that plt.show() is called when show=True and not called when show=False."""
    data = {"exp_A": [10.0]}
    region_labels = ["R1"]

    plot_bar_metrics(data, region_labels, tavg, show=True)
    assert mock_show.call_count == 1

    plot_bar_metrics(data, region_labels, tavg, show=False)
    assert mock_show.call_count == 1  # No additional call
