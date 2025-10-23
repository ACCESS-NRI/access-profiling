# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Parser for Cylc log files. The data to be parsed is written in the following form:

2025-10-17T00:51:12Z INFO - Suite server: url=... pid=152868
2025-10-17T00:51:12Z INFO - Run: (re)start=0 log=1
2025-10-17T00:51:12Z INFO - Cylc version: 7.9.9
2025-10-17T00:51:12Z INFO - Run mode: live
2025-10-17T00:51:12Z INFO - Initial point: 20220226T0000Z
2025-10-17T00:51:12Z INFO - Final point: 20220226T0300Z
2025-10-17T00:51:12Z INFO - Cold Start 20220226T0000Z
...
2025-10-17T01:36:23Z INFO - Suite shutting down - AUTOMATIC
2025-10-17T01:36:30Z INFO - DONE

The differences between the first and last time-stamp are used to determine the
total pipeline walltime.
"""

import os
import sqlite3
from datetime import datetime
from pathlib import Path

from access.profiling.metrics import tmax
from access.profiling.parser import ProfilingParser, _read_text_file, _test_file


class CylcProfilingParser(ProfilingParser):
    """Cylc log profiling parser."""

    _metrics = [tmax]

    def parse(self, file_path: str | Path | os.PathLike) -> dict:
        """Implements "parse" abstract method to parse the Cycle suite run log.

        Args:
            file_path (str | Path | os.PathLike): String containing the suite run log.

        Returns:
            dict: Parsed timing information.

        Raises:
            ValueError: when the last line does not contain "DONE".
        """
        lines = _read_text_file(file_path).splitlines()

        first_line = lines[0]
        last_line = lines[-1]

        if "DONE" not in last_line:
            raise ValueError("Cylc log is incomplete.")

        try:
            start_time = _extract_timestamp(first_line)
        except Exception as e:
            raise ValueError("First line of log doesn't contain a valid timestamp.") from e
        try:
            end_time = _extract_timestamp(last_line)
        except Exception as e:
            raise ValueError("Last line of log doesn't contain a valid timestamp.") from e

        return {
            "region": ["pipeline_elapsed_time"],
            tmax: [int((end_time - start_time).total_seconds())],
        }


class CylcDBReader(ProfilingParser):
    """Cylc database reader."""

    _table = "task_jobs"
    _required_cols = ("cycle", "name", "time_run", "time_run_exit", "run_status")
    _metrics = [tmax]

    def parse(self, file_path: str | Path | os.PathLike) -> dict:
        """Implements "read" abstract method of CylcDBReader to parse the Cylc Rose task database.

        Args:
            file_path (str | Path | os.PathLike): The path to the SQLite database.

        Returns:
            dict: Read timing information.

        Raises:
            FileNotFoundError: When the provided database file doesn't exist.
            RuntimeError: when the expected table is not present in the database or if the table doesn't have the
                          expected column names.
        """

        dbpath = _test_file(file_path)

        with sqlite3.connect(dbpath) as con:
            cur = con.cursor()

            # collect and validate table columns . Return type: list of tuples
            # where each list item corresponds to a column. Each tuple is (index, name, type, ?, ?, primary key)
            col_metadata = cur.execute(f"PRAGMA table_info({self._table})").fetchall()
            if col_metadata == []:
                raise RuntimeError(f"Table {self._table} not found in {dbpath}!")
            col_map = {col_data[1]: col_data[0] for col_data in col_metadata}
            columns_missing_from_tbl = set(self._required_cols) - set(col_map.keys())
            if columns_missing_from_tbl:
                raise RuntimeError(f"Expected table columns: {', '.join(columns_missing_from_tbl)}")

            # collect table data
            table_data = cur.execute(f"SELECT * FROM {self._table}").fetchall()

        # turn timestamps into time elapsed (seconds)
        data = {"region": []}
        for m in self._metrics:
            data[m] = []
        for row in table_data:
            # filter out tasks that haven't completed successfully
            if row[col_map["run_status"]] == 0:
                # region will look like <task>_<chunk no.>_cycle<cycle timestamp>
                region = row[col_map["name"]] + "_cycle" + row[col_map["cycle"]]
                start = row[col_map["time_run"]]
                end = row[col_map["time_run_exit"]]
                runtime = (_extract_timestamp(end) - _extract_timestamp(start)).total_seconds()
                data["region"].append(region)
                data[self._metrics[0]].append(runtime)

        return data


def _extract_timestamp(line: str) -> datetime:
    """Helper function to extra and convert timestamp to datetime object.

    Args:
        line (str): The line of text with the timestamp at the beginning.

    Raises:
        ValueError: When there is no timestamp or the timestamp is inavlid.
    """

    timestamp = line.split()[0]
    if timestamp.endswith("Z"):
        timestamp = timestamp[:-1] + "+00:00"
    try:
        time = datetime.fromisoformat(timestamp)
    except Exception as e:
        raise ValueError("Invalid or missing timestamp") from e

    return time
