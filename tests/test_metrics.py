# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import re

import pytest
import xarray as xr
from pint import Unit

from access.profiling.metrics import ProfilingMetric, tmax


def test_metric():
    """Test basic initialization of ProfilingMetric class"""
    metric = ProfilingMetric("test_name", Unit("second"), "test_description")
    assert metric.name == "test_name"
    assert metric.units == "second"
    assert metric.description == "test_description"
    assert f"{metric}" == "test_name"


def test_metric_empty_name():
    """Test initialization with empty name"""
    with pytest.raises(ValueError):
        ProfilingMetric("", Unit("second"), "test_description")


def test_metric_empty_description():
    """Test initialization with empty description"""
    with pytest.raises(ValueError):
        ProfilingMetric("test_name", Unit("second"), "")


class TestRepr:
    """A metric is shown by its name and its units, since a dataset is keyed by the metric objects."""

    def test_it_names_the_metric_and_its_units(self):
        """Not as a call: one made from it would be another key, a metric being a key by identity."""
        assert repr(tmax) == "<ProfilingMetric 'maximum time' [second]>"

    def test_a_key_a_dataset_does_not_hold_names_the_metrics_it_does(self):
        """What xarray suggests for a string is then the metric to use, not an address in memory."""
        ds = xr.Dataset({tmax: ("region", [1.0])}, coords={"region": ["Total"]})

        with pytest.raises(
            KeyError, match=re.escape("Did you mean one of (<ProfilingMetric 'maximum time' [second]>,)")
        ):
            ds["maximum time"]
