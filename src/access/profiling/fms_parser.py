# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Parser for FMS profiling data, such as output by MOM5 and MOM6.
The data to be parsed is written in the following form:

```text
                                   hits          tmin          tmax          tavg          tstd  tfrac grain pemin pemax
Total runtime                        1    138.600364    138.600366    138.600365      0.000001  1.000     0     0    11
Ocean Initialization                 2      2.344926      2.345701      2.345388      0.000198  0.017    11     0    11
Ocean                               23     86.869466     86.871652     86.870450      0.000744  0.627     1     0    11
Ocean dynamics                      96     43.721019     44.391032     43.957944      0.244785  0.317    11     0    11
Ocean thermodynamics and tracers    72     27.377185     33.281659     29.950144      1.792324  0.216    11     0    11
 MPP_STACK high water mark=          0
```

Each row is a clock: its name, truncated to 32 characters, then one number per column of the header. The `pemin`
and `pemax` columns are the lowest and highest PE of the clock's PE list, which are reported as `pelist_min` and
`pelist_max`. The FMS of MOM5 writes no `hits` column. When a clock is detailed, FMS writes a table of the messages
it timed after this one, under a header of its own, which is not read.
"""

import os
import re
from pathlib import Path

from pint import Unit

from access.profiling.metrics import ProfilingMetric, count, tavg, tfrac, tmax, tmin, tstd
from access.profiling.parser import ProfilingData, ProfilingParser, _convert_from_string, _read_text_file

grain = ProfilingMetric("grain", Unit("dimensionless"), "Grain")
"""Granularity of an FMS clock, as the `grain` column of FMS timings reports it."""
pelist_min = ProfilingMetric("lowest PE", Unit("dimensionless"), "Lowest PE of the clock's PE list")
"""Lowest PE of the list of PEs an FMS clock is kept on, as the `pemin` column of FMS timings reports it."""
pelist_max = ProfilingMetric("highest PE", Unit("dimensionless"), "Highest PE of the clock's PE list")
"""Highest PE of the list of PEs an FMS clock is kept on, as the `pemax` column of FMS timings reports it."""

# What FMS writes after the table of its clocks: the header of the table of messages of the detailed clocks, if
# there are any, and the line it writes once it has written all its timings.
_MESSAGES_HEADER = ["tmin", "tmax", "tavg", "tstd", "mmin", "mmax", "mavg", "mstd", "mavg/tavg"]
_FOOTER = "MPP_STACK high water mark="


class FMSProfilingParser(ProfilingParser):
    """FMS profiling output parser."""

    has_hits: bool  # whether FMS timings contains "hits" column.

    def __init__(self, has_hits: bool = True):
        """Instantiate FMS profiling parser.

        Args:
            has_hits (bool): whether FMS timings contains "hits" column.
        """
        super().__init__()

        self.has_hits = has_hits
        # FMS provides the following metrics, in the order of its columns:
        self._metrics = [count] if self.has_hits else []
        self._metrics += [tmin, tmax, tavg, tstd, tfrac, grain, pelist_min, pelist_max]

    def parse(self, file_path: str | Path | os.PathLike) -> dict:
        """Implements "parse" abstract method to parse profiling data in FMS log output.

        Every line between the header of the table and the end of it must be a row, so that a clock is never left
        out without notice. Blank lines are skipped.

        Args:
            file_path (str | Path | os.PathLike): file to parse.

        Returns:
            dict: Parsed timing information, with the time fraction as a percentage.

        Raises:
            ValueError: If no table of FMS timings is found, if a line of the table is not a row, or if the file
                ends before FMS has finished writing its timings.
            TypeError: If file_path cannot be converted to a valid Path object.
            FileNotFoundError: If file_path doesn't exist or isn't a file.
        """
        stream = _read_text_file(file_path)

        labels = ["hits"] if self.has_hits else []
        labels += ["tmin", "tmax", "tavg", "tstd", "tfrac", "grain", "pemin", "pemax"]

        # The name of a clock is anything that leaves exactly one number per column after it, digits and spaces
        # included, since a name may end in a number.
        profile_line = r"(?P<region>\S.*?)" + "".join(rf"\s+(?P<{label}>\d*\.?\d+)" for label in labels)
        profiling_region_p = re.compile(profile_line)

        lines = iter(stream.splitlines())
        for line in lines:
            if line.split() == labels:
                break
        else:
            raise ValueError(f"No FMS profiling data found in {file_path}")

        # Parse data
        stats: ProfilingData = {"region": []}
        stats.update({m: [] for m in self.metrics})
        for line in lines:
            if line.split() == _MESSAGES_HEADER or line.strip().startswith(_FOOTER):
                break
            if not line.strip():
                continue
            match = profiling_region_p.fullmatch(line.strip())
            if match is None:
                raise ValueError(f"Unexpected line in the FMS timings of {file_path}: {line!r}")
            stats["region"].append(match["region"])
            for label, metric in zip(labels, self.metrics, strict=True):
                stats[metric].append(_convert_from_string(match[label]))
        else:
            raise ValueError(f"The FMS timings of {file_path} end before FMS finished writing them.")

        # Convert time fraction to percentage
        stats[tfrac] = [val * 100 for val in stats[tfrac]]

        return stats
