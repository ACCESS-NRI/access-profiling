# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Classes and utilities to define profiling metrics."""


class ProfilingMetric:
    def __init__(self, name: str, units: str, description: str):
        """Class representing a profiling metric.

        Args:
            name (str): Name of the metric.
            units (str): Units of the metric. Should be a valid pint unit name ()
            description (str): Description of the metric.
        Raises:
            ValueError: If name, units or description are empty or whitespace-only strings.
        """

        if not name.strip():
            raise ValueError("Metric name cannot be empty!")

        if not units.strip():
            raise ValueError("Metric units cannot be empty!")

        if not description.strip():
            raise ValueError("Metric description cannot be empty!")

        self._name = name
        self._units = units
        self._description = description

    @property
    def name(self) -> str:
        return self._name

    @property
    def units(self) -> str | None:
        return self._units

    @property
    def description(self) -> str:
        return self._description


# Define common metrics
count = ProfilingMetric("count", "dimensionless", "Number of calls to region")
tmin = ProfilingMetric("time_minimum", "second", "Minimum time spent in region")
tmax = ProfilingMetric("time_maximum", "second", "Maximum time spent in region")
pemin = ProfilingMetric("pe_minimum", "dimensionless", "Processing element where minimum time was recorded")
pemax = ProfilingMetric("pe_maximum", "dimensionless", "Processing element where maximum time was recorded")
tavg = ProfilingMetric("time_average", "second", "Average time spent in region")
tmed = ProfilingMetric("time_mediam", "second", "Median time spent in region")
tstd = ProfilingMetric("time_std", "second", "Standard deviation of time spent in region")
tfrac = ProfilingMetric("time_fraction", "%", "Fraction of total time spent in region")
