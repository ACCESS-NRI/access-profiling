# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access.profiling.plotting_utils import calculate_column_widths


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
