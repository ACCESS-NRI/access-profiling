# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Parser for UM profiling data.
This routine parses the inclusive timers from the UM output log
(e.g. ``atm.fort6.pe0`` for UM7) and returns a dictionary of the
profiling data. Since UM7 and UM13 provides multiple sections with timer
output - we have chosen to use the 'Wallclock times' sub-section
within the Inclusive Timer Summary section.

The profiling data is assumed to have the following
format:

```
 MPP : Inclusive timer summary

 WALLCLOCK  TIMES
 <N>   ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)      MIN   (PE)
  1 AS3 Atmos_Phys2        1308.30  1308.30     0.02       0.00%  1308.33 ( 118)  1308.26 ( 221)
  2 AP2 Boundary Layer      956.50   956.13     3.26       0.34%   981.27 ( 136)   953.28 (  43)
  3 AS5-8 Updates           884.62   885.52     2.89       0.33%   889.49 (  48)   879.36 ( 212)

...

         CPU TIMES (sorted by wallclock times)
 <N>    ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)      MIN   (PE)
...

 ```

All columns in the first sub-section, except for the numeric index and the `% of mean`, are parsed and returned.
For UM versions 13.x, there is an extra 'N' column name that appears to the left of 'ROUTINE'; this 'N' is
not present in the output from UM v7.x .

"""

import logging
import os
import re
from pathlib import Path

from access.profiling.metrics import pemax, pemin, tavg, tmax, tmed, tmin, tstd
from access.profiling.parser import ProfilingParser, _convert_from_string, _read_text_file

logger = logging.getLogger(__name__)


class UMProfilingParser(ProfilingParser):
    """UM profiling output parser."""

    # The parsed column names that will be kept. The order needs to match
    # the order of the column names in the input data (defined as ``raw_headers``
    # in the ``read``` method), after discarding the ignored columns.
    _metrics = [tavg, tmed, tstd, tmax, pemax, tmin, pemin]

    def parse(self, file_path: str | Path | os.PathLike) -> dict:
        """Parse UM profiling data from a file path.

        Args:
            file_path (str | Path | os.PathLike): file to parse.

        Returns:
            stats (dict): dictionary of parsed profiling data.
                    Ignores two columns ``N``, and ``% over mean`` columns.

                    To keep consistent column names across all parsers, the following
                    mapping is used:
                        ==================  ==================
                        UM column name      Standard metric
                        ==================  ==================
                        N                   - (ignored)
                        ROUTINE             region
                        MEAN                tavg
                        MEDIAN              tmed
                        SD                  tstd
                        % of mean           - (ignored)
                        MAX                 tmax
                        (PE)                pemax
                        MIN                 tmin
                        (PE)                pemin
                        ==================  ==================
                    Each key returns a list of values, one for each region. For
                    example, if there are 20 regions, ``stats['tavg']`` will
                    return a list with 20 values, one each for each of the regions.

                    The assumption is that people will want to look at the same metric
                    for *all* regions at a time; if you want to look at all metrics for
                    a single region, then you will have to first find the index for the
                    ``region``, and then extract that index from *each* of the 'metric'
                    lists.

                    Any number of column headers can be present at the beginning, i.e., before
                    ``ROUTINE`` (including ``N`` for UM v13+) and will be ignored when the
                    header is being parsed. Such columns must contain integer values for the
                    profiling information parsing to work, otherwise (e.g., new columns with float
                    data), an ``AssertionError`` is raised.

                    Parsing will fail if there are any unexpected columns at the end, either
                    at the end of the expected header, or the end of the expected columns with
                    profiling data (regardless of type of the column value)

        Raises:
            ValueError: If the UM version number can not be found in the input string data.
            ValueError: If a match for any of header, footer or section (i.e., empty section)
                        is not found.
            AssertionError: If the expected format is not found in *all* of the lines within the
                            profiling section.
            TypeError: If file_path cannot be converted to a valid Path object.
            FileNotFoundError: If file_path doesn't exist or isn't a file.
        """

        stream = _read_text_file(file_path)

        # First create the local variable with the metrics list
        metrics = self.metrics
        raw_headers = ["ROUTINE", "MEAN", "MEDIAN", "SD", r"\% of mean", "MAX", r"\(PE\)", "MIN", r"\(PE\)"]

        header = r"MPP : Inclusive timer summary\s+WALLCLOCK  TIMES\s*"
        # UM 13 has an extra header 'N' for the numeric row index (that UM7 does not)
        # Writing the pattern this way avoids having to code in UM version dependent patterns
        header += r"\S*\s+"
        # Then skip over white-space-separated header names.
        header += r"\s*".join(raw_headers) + r"\s*"
        header_pattern = re.compile(header, re.MULTILINE)
        header_match = header_pattern.search(stream)
        if not header_match:
            logger.debug("Header pattern: %s", header)
            logger.debug("Input string: %s", stream)
            raise ValueError("No matching header found.")
        logger.debug("Found header: %s", header_match.group(0))

        # This line (and any preceeding whitespace) indicates
        # the end of the profiling data that we want to parse
        footer = r"CPU TIMES \(sorted by wallclock times\)\s*"
        footer_pattern = re.compile(footer, re.MULTILINE)
        footer_match = footer_pattern.search(stream)
        if not footer_match:
            logger.debug("Footer pattern: %s", footer)
            logger.debug("Input string: %s", stream)
            raise ValueError("No matching footer found.")
        logger.debug("Found footer: %s", footer_match.group(0))

        # Match *everything* between the header and footer (the match could be 0 characters)
        profiling_section_p = re.compile(header + r"(.*)" + footer, re.MULTILINE | re.DOTALL)
        profiling_section = profiling_section_p.search(stream)

        profiling_section = profiling_section.group(1)
        logger.debug("Found section: %s", profiling_section)

        # This is regex dark arts - seems to work, I roughly understood when I
        # was refining this named capture group, but I might not be able to in
        # the future. Made heavy use of the regex debugger at regex101.com :) - MS 19/9/2025
        profile_line = r"^\s*[\d\s]+\s+(?P<region>[a-zA-Z][a-zA-Z:()_/\-*&0-9\s\.]+(?<!\s))"
        for metric in metrics:
            logger.debug(f"Adding {metric.name=}")
            group_name = "".join(metric.name.split())  # remove any white-space from metric name to create group name
            if metric in [pemax, pemin]:
                # the pemax and pemin values are enclosed within brackets '()',
                # so we need to ignore both the opening and closing brackets
                add_pattern = r"\s+\(\s*(?P<" + group_name + r">[0-9.]+)\s*\)"
            elif metric == tstd:
                add_pattern = (
                    r"\s+(?P<" + group_name + r">[0-9.]+)\s+[\S]+"
                )  # SD is followed by % of mean -> ignore that column
            else:
                add_pattern = (
                    r"\s+(?P<" + group_name + r">[0-9.]+)"
                )  # standard white-space followed by a sequence of digits or '.'

            logger.debug(f"{add_pattern=} for {metric.name=}")
            profile_line += add_pattern
            logger.debug(f"{profile_line=} after {metric.name=}")

        profile_line += r"$"  # the regex should match till the end of line.
        profiling_region_p = re.compile(profile_line, re.MULTILINE)

        stats = {"region": []}
        stats.update({m: [] for m in self.metrics})
        for line in profiling_region_p.finditer(profiling_section):
            logger.debug(f"Matched line: {line.group(0)}")
            stats["region"].append(line.group("region"))
            for metric in metrics:
                group_name = "".join(metric.name.split())
                stats[metric].append(_convert_from_string(line.group(group_name)))

        # Parsing is done - let's run some checks
        num_lines = len(profiling_section.strip().split("\n"))
        logger.debug(f"Found {num_lines} lines in profiling section")
        if len(stats["region"]) != num_lines:
            raise AssertionError(f"Expected {num_lines} regions, found {len(stats['region'])}.")

        logger.info(f"Found {len(stats['region'])} regions with profiling info")
        return stats


"""Example UM7 runtime log snippet to be parsed for total wallclock runtime:

```
 END OF RUN - TIMER OUTPUT
 Timer information is for whole run
 PE                      0  Elapsed CPU Time:    3943.63426200007     
 PE                      0   Elapsed Wallclock Time:    3943.80157899974     
 
 Total Elapsed CPU Time:    820297.910506003     
 Maximum Elapsed Wallclock Time:    3944.07699399998     
 Speedup:    207.982225436750     
```
"""


class UMTotalRuntimeParser(ProfilingParser):
    """Parser for UM total runtime from the UM log file."""

    metrics = [tmax]

    def parse(self, file_path: str | Path | os.PathLike) -> float:
        """Parse UM total runtime from a file.

        Args:
            file_path (str | Path | os.PathLike): input string to parse.

        Returns:
            dict: dictionary of parsed profiling data.

        Raises:
            ValueError: If no matching total runtime line is found.
        """
        stream = _read_text_file(file_path)
        total_runtime_pattern = re.compile(
            r"Maximum\s+Elapsed\s+Wallclock\s+Time\s*:\s*(?P<total_time>[0-9.]+)\s*",
            re.MULTILINE,
        )
        total_runtime_match = total_runtime_pattern.search(stream)
        if not total_runtime_match:
            logger.debug("Total runtime pattern: %s", total_runtime_pattern)
            logger.debug("Input string: %s", stream)
            raise ValueError("No matching total runtime line found.")

        total_time = float(total_runtime_match.group("total_time"))
        logger.debug(f"Found total UM runtime: {total_time} seconds")

        return {"region": ["um_total_walltime"], tmax: [total_time]}
