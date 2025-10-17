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
from datetime import datetime
from pathlib import Path

from access.profiling.metrics import tmax
from access.profiling.parser import ProfilingParser, _read_text_file


class CylcProfilingParser(ProfilingParser):
    """Payu JSON job output profiling parser."""

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
