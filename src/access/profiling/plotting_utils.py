# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


def calculate_column_widths(table_data: list[list], first_col_flexible: bool = True) -> list:
    """Calculate column widths based on content length

    Args:
        table_data (list[list]): Table data including headers
        first_col_flexible (bool): If True, first column gets remaining space after others are sized

    Returns:
        list : Column width ratios
    """
    if not table_data:
        return []

    n_cols = len(table_data[0])

    # Calculate max content length for each column
    max_lengths = []
    for col in range(n_cols):
        col_lengths = [len(str(row[col])) for row in table_data]
        max_lengths.append(max(col_lengths))

    if first_col_flexible:
        # Set data columns to proportional widths based on content
        # Give first column remaining space
        data_cols_total = sum(max_lengths[1:])
        base_width = 0.6 / data_cols_total if data_cols_total > 0 else 0.1

        col_widths = [0.4]  # First column gets 40% of space
        for length in max_lengths[1:]:
            col_widths.append(length * base_width)

        # Adjust to sum to 1.0
        current_sum = sum(col_widths)
        col_widths = [w / current_sum for w in col_widths]

        # Give first column any remaining space
        remaining = 1.0 - sum(col_widths[1:])
        col_widths[0] = remaining
    else:
        # Proportional to content length
        total_length = sum(max_lengths)
        col_widths = [length / total_length for length in max_lengths]

    return col_widths
