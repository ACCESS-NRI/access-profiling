# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Parser for ESMF profiling text summary data, such as output by nuopc.
The data to be parsed is written in the following form:

Region                                       PETs   PEs    Count    Mean (s)    Min (s)     Min PET Max (s)     Max PET
  [ESMF]                                     1664   1664   1        2558.5684   2555.1450   279     2559.5801   817
    [ensemble] RunPhase1                     1664   1664   1        1879.7292   1872.5078   376     1905.4939   1
      [ESM0001] RunPhase1                    1664   1664   1        1879.7286   1872.5059   858     1905.4937   1
        [OCN] RunPhase1                      1300   1300   960      1850.4170   1848.3905   1023    1858.6404   364
        [OCN-TO-MED] RunPhase1               1664   1664   960      365.9532    0.1688      405     1673.2255   18
        [ICE] RunPhase1                      364    364    960      155.8202    154.7637    94      160.2443    0
          cice_run_total                     364    364    960      155.4648    154.3687    94      159.8980    0
            cice_run_import                  364    364    960      3.7565      3.5426      218     8.3892      1
              cice_imp_halo                  364    364    1920     2.1386      1.5405      111     6.9782      0
              cice_imp_t2u                   364    364    960      0.9242      0.5578      355     1.5026      111
              cice_imp_atm                   364    364    960      0.0579      0.0399      331     0.0834      18
              cice_imp_ocn                   364    364    960      0.0499      0.0419      50      0.0632      12
            cice_run_export                  364    364    960      1.1015      0.8846      361     1.3607      194
        [MED-TO-OCN] RunPhase1               1664   1664   960      16.7498     0.4588      256     23.2875     1023
        [MED] med_phases_restart_write       364    364    960      31.8234     31.8123     203     33.0513     363
          MED:(med_phases_restart_write)     364    364    960      31.7651     31.7559     203     32.9919     363
...

Where indentation depth indicates depth in the call-stack. Usually there is a header like
********
A warning about identifying load-imbalance.
********
Note that the profiling summary stats may contain identical region names
e.g. [ATM-TO-MED] RunPhase1 is found twice in OM3.
"""

from pint import Unit

from access.profiling.metrics import (
    ProfilingMetric,
    count,
    pemax,
    pemin,
    tavg,
    tmax,
    tmin,
)
from access.profiling.parser import ProfilingParser

pets = ProfilingMetric("PETs", Unit("dimensionless"), "ESMF Virtual Machine Persistent Execution Threads")
pes = ProfilingMetric("PEs", Unit("dimensionless"), "Processing Elements")


class ESMFSummaryProfilingParser(ProfilingParser):
    """ESMF text summary profiling output parser."""

    hierarchical: bool  # whether the call-stack hierarchy is parsed.

    def __init__(self, hierarchical: bool = False):
        """Instantiate ESMF profiling parser.

        Args:
            hierarchical (bool): Whether call-stack hierarchy is parsed.
        """
        super().__init__()

        self.hierarchical = hierarchical

        # ESMF provides the following metrics
        self._metrics = [pets, pes, count, tavg, tmin, pemin, tmax, pemax]

    def read(self, stream: str) -> dict:
        lines = stream.strip().split("\n")

        if self.hierarchical:
            result = {}
            stack = [(result, -1)]  # (current_dict, indent_level)
        else:
            result = {m: [] for m in self._metrics}
            result["region"] = []

        for line in lines:
            # Split the line into region name and statistics
            parts = line.split()
            if len(parts) < (1 + len(self._metrics)):  # Need at least 1 region + the number of stat columns
                continue

            # Extract region name and statistics
            region = " ".join(parts[:-8])
            stats = parts[-8:]

            # Validate that all statistics can be parsed correctly
            try:
                stats_dict = {
                    pets: int(stats[0]),
                    pes: int(stats[1]),
                    count: int(stats[2]),
                    tavg: float(stats[3]),
                    tmin: float(stats[4]),
                    pemin: int(stats[5]),
                    tmax: float(stats[6]),
                    pemax: int(stats[7]),
                }
            except (ValueError, IndexError):
                # Skip lines that don't match the expected format
                continue

            if self.hierarchical:
                # Calculate indentation level (each level is 2 spaces)
                indent = len(line) - len(line.lstrip())
                indent_level = indent // 2

                # Pop stack until we find the parent level
                while stack and stack[-1][1] >= indent_level:
                    stack.pop()

                # Get parent dictionary
                parent_dict = stack[-1][0]

                # Create entry for this region
                if region not in parent_dict:
                    parent_dict[region] = {}

                # Add statistics to this region
                parent_dict[region].update(stats_dict)

                # Push this level onto stack for potential children
                stack.append((parent_dict[region], indent_level))
            else:
                _update_flat_result(result, stats_dict, region)

        # fewer if statements to pass ruff checks
        if (self.hierarchical and not result) or (not self.hierarchical and len(result["region"]) == 0):
            raise ValueError("No ESMF summary profiling data found")

        return result


def _update_flat_result(result: dict, stats_dict: dict, region: str):
    """Helper function to update flat result.

    Besides appending results, this function also checks whether the region already exists
    and aggregates the metric values in result if possible.

    Args:
        result (dict): The flat result dictionary to update.
        stats_dict (dict): The stats to update the result with.
        region (str): The region to append the results to.

    Raises:
        NotImplementedError: If a stats_dict["region"] is already in result["region"],
                             but the PETs or PEs value aren't the same.
    """
    # Flat structure: just use region name as key
    try:
        idx = result["region"].index(region)
        # only update existing region if PETs and PEs are same
        if (
            result[pets][idx] == stats_dict[pets]
            and result[pes][idx] == stats_dict[pes]
            and stats_dict[pets] == stats_dict[pes]
        ):
            # new avg is weighted average using count as the weight
            result[tavg][idx] = (result[tavg][idx] * result[count][idx] + stats_dict[tavg] * stats_dict[count]) / (
                result[count][idx] + stats_dict[count]
            )
            result[count][idx] += stats_dict[count]
            if stats_dict[tmin] < result[tmin][idx]:
                result[tmin][idx] = stats_dict[tmin]
                result[pemin][idx] = stats_dict[pemin]
            if stats_dict[tmax] > result[tmax][idx]:
                result[tmax][idx] = stats_dict[tmax]
                result[pemax][idx] = stats_dict[pemax]
        else:
            raise NotImplementedError(
                "I don't know what to do with multiple regions with same name, but different PETs/PEs."
            )
    except ValueError:
        result["region"].append(region)
        for k, v in stats_dict.items():
            result[k].append(v)
