# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


def calculate_column_widths(table_data: list[list], first_col_fraction: float = None) -> list:
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
        raise ValueError("first_col_fration must be between 0 and 1 (exclusive)")

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
