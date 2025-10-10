# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Classes and utilities to define profiling metrics."""

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


# Define common metrics
count = ProfilingMetric("count", Unit("dimensionless"), "Number of calls to region")
tmin = ProfilingMetric("minimum time", Unit("second"), "Minimum time spent in region")
tmax = ProfilingMetric("maximum time", Unit("second"), "Maximum time spent in region")
pemin = ProfilingMetric("minimum PE", Unit("dimensionless"), "Processing element where minimum time was recorded")
pemax = ProfilingMetric("maximum PE", Unit("dimensionless"), "Processing element where maximum time was recorded")
tavg = ProfilingMetric("average time", Unit("second"), "Average time spent in region")
tmed = ProfilingMetric("median time", Unit("second"), "Median time spent in region")
tstd = ProfilingMetric("time std", Unit("second"), "Standard deviation of time spent in region")
tfrac = ProfilingMetric("time fraction", Unit("%"), "Fraction of total time spent in region")
