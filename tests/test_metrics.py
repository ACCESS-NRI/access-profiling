# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access.profiling.metrics import ProfilingMetric


def test_metric():
    """Test basic initialization of ProfilingMetric class"""
    metric = ProfilingMetric("test_name", "test_units", "test_description")
    assert metric.name == "test_name"
    assert metric.units == "test_units"
    assert metric.description == "test_description"


def test_metric_empty_name():
    """Test initialization with empty name"""
    with pytest.raises(ValueError):
        ProfilingMetric("", "test_units", "test_description")


def test_metric_empty_units():
    """Test initialization with empty units"""
    with pytest.raises(ValueError):
        ProfilingMetric("test_name", "", "test_description")


def test_metric_empty_description():
    """Test initialization with empty description"""
    with pytest.raises(ValueError):
        ProfilingMetric("test_name", "test_units", "")
