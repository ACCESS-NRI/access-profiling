# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Classes and utilities to define profiling metrics.

Metrics are classified by which dimension(s) they aggregate over.

Aggregation dimensions
----------------------
call
    A single invocation of a profiling region. Statistics over this dimension
    describe how timing varies across repeated calls to the same region within one run.
pe
    A processing element (MPI process). Statistics over this dimension describe how
    timing varies across parallel processes for a given call (or a call-aggregated value).
call_pe
    Both dimensions simultaneously — global statistics across all calls and PEs.

Metric naming convention
------------------------
New metric names encode aggregation dimension(s) as underscore-separated suffixes:

    <base>_<stat>_<dimension>

Examples:
    tmin            minimum time over calls (implicit _call suffix; legacy name)
    tavg_max_pe     maximum across PEs of the per-call average time
    t_min_call_pe   global minimum across both calls and PEs

The pre-defined constants below (tmin, tmax, tavg, …) carry an implicit _call
dimension for backward compatibility. New metrics should always make the dimension
suffix explicit.
"""

from pint import Unit


class ProfilingMetric:
    def __init__(self, name: str, units: Unit, description: str):
        """Class representing a profiling metric.

        Args:
            name (str): Name of the metric.
            units (pint.Unit): Units of the metric.
            description (str): Description of the metric.
        Raises:
            ValueError: If name, units or description are empty or whitespace-only strings.
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
        return self._name

    @property
    def units(self) -> Unit:
        return self._units

    @property
    def description(self) -> str:
        return self._description

    def __str__(self) -> str:
        return self._name


# Per-call statistics (reduced over repeated invocations of the same region)
count = ProfilingMetric("count", Unit("dimensionless"), "Number of calls to region")
tmin = ProfilingMetric("minimum time", Unit("second"), "Minimum time over calls to region")
tmax = ProfilingMetric("maximum time", Unit("second"), "Maximum time over calls to region")
pemin = ProfilingMetric("minimum PE", Unit("dimensionless"), "Processing element where minimum call time was recorded")
pemax = ProfilingMetric("maximum PE", Unit("dimensionless"), "Processing element where maximum call time was recorded")
tavg = ProfilingMetric("average time", Unit("second"), "Mean time over calls to region")
tmed = ProfilingMetric("median time", Unit("second"), "Median time over calls to region")
tstd = ProfilingMetric("time std", Unit("second"), "Standard deviation of time over calls to region")
tfrac = ProfilingMetric("time fraction", Unit("%"), "Fraction of total time over calls to region")
