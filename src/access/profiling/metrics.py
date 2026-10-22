# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Classes and utilities to define profiling metrics.

A metric is one quantity a parser reports for each profiling region: a time, a count, or the processing element
(PE) on which something was recorded. A `ProfilingMetric` couples a name with units and a description, and parsed
data is keyed by the metric objects themselves, so `dataset[tavg]` rather than `dataset["average time"]`.

**What the predefined metrics are statistics of**

The timers the parsers in this package read - FMS's `mpp_clock`s, the UM's timer summary, ESMF's profile summary
and CICE's timers - all report the same kind of statistic. Each PE accumulates the time it spends in a region over
every call to it, and the timer then summarises those per-PE totals across the PEs that ran the region:

- `tmin`, `tmax`, `tavg`, `tmed` and `tstd` are the minimum, maximum, mean, median and standard deviation across
  PEs of the total time spent in the region.
- `tfrac` is that mean as a fraction of the total runtime.
- `count` is the number of times the region was entered.
- `pemin` and `pemax` are the PEs on which the minimum and maximum times were recorded.

Logs that record one elapsed time per region rather than a distribution over PEs - Payu's job timings, the Cylc
suite log and task database, and the UM's total walltime - store it as `tmax`.

**Naming new metrics**

Metrics are classified by the dimension(s) they aggregate over:

- `call`: a single invocation of a profiling region. Statistics over this dimension describe how timing varies
  across repeated calls to the same region within one run.
- `pe`: a processing element (MPI process). Statistics over this dimension describe how timing varies across
  parallel processes for a given call (or a call-aggregated value).
- `call_pe`: both dimensions simultaneously - global statistics across all calls and PEs.

New metric names encode the aggregation dimension(s) as underscore-separated suffixes, `<base>_<stat>_<dimension>`:

| Name | Meaning |
|---|---|
| `tavg_max_pe` | maximum across PEs of the per-call average time |
| `t_min_call_pe` | global minimum across both calls and PEs |

The pre-defined constants below keep their short legacy names. In this convention they are statistics over the
`pe` dimension of a value aggregated over calls, so their implicit suffix is `_pe`. New metrics should always make
the dimension suffix explicit.
"""

from pint import Unit


class ProfilingMetric:
    """A quantity reported for each profiling region, with its units and a description.

    Metrics are compared by identity: the parsed data is keyed by the metric objects, so a metric is looked up with
    the object itself rather than with its name. The description is used as the title of the bar charts plotting
    the metric.
    """

    def __init__(self, name: str, units: Unit, description: str):
        """Class representing a profiling metric.

        Args:
            name (str): Name of the metric.
            units (pint.Unit): Units of the metric.
            description (str): Description of the metric.

        Raises:
            ValueError: If name or description are empty or whitespace-only strings.
        """

        if not name.strip():
            raise ValueError("Metric name cannot be empty!")

        if not description.strip():
            raise ValueError("Metric description cannot be empty!")

        self._name = name
        self._units = units
        self._description = description

    @property
    def name(self) -> str:
        """Returns the name of the metric, which is also what the metric prints as."""
        return self._name

    @property
    def units(self) -> Unit:
        """Returns the units the metric's values are in."""
        return self._units

    @property
    def description(self) -> str:
        """Returns the description of the metric."""
        return self._description

    def __str__(self) -> str:
        return self._name


# The metrics the parsers in this package report; the module docstring says what each one is a statistic of.
count = ProfilingMetric("count", Unit("dimensionless"), "Number of calls to region")
"""Number of times the region was entered."""
tmin = ProfilingMetric("minimum time", Unit("second"), "Minimum across PEs of the total time spent in the region")
"""Minimum across PEs of the total time spent in the region."""
tmax = ProfilingMetric("maximum time", Unit("second"), "Maximum across PEs of the total time spent in the region")
"""Maximum across PEs of the total time spent in the region, or the one elapsed time of logs that record no more."""
pemin = ProfilingMetric("minimum PE", Unit("dimensionless"), "PE on which the minimum time was recorded")
"""PE on which the minimum time was recorded."""
pemax = ProfilingMetric("maximum PE", Unit("dimensionless"), "PE on which the maximum time was recorded")
"""PE on which the maximum time was recorded."""
tavg = ProfilingMetric("average time", Unit("second"), "Mean across PEs of the total time spent in the region")
"""Mean across PEs of the total time spent in the region."""
tmed = ProfilingMetric("median time", Unit("second"), "Median across PEs of the total time spent in the region")
"""Median across PEs of the total time spent in the region."""
tstd = ProfilingMetric(
    "time std", Unit("second"), "Standard deviation across PEs of the total time spent in the region"
)
"""Standard deviation across PEs of the total time spent in the region."""
tfrac = ProfilingMetric("time fraction", Unit("%"), "Mean time in the region as a fraction of the total runtime")
"""Mean across PEs of the total time spent in the region, as a percentage of the total runtime."""
