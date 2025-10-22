# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access.profiling.metrics import tmax
from access.profiling.payujson_parser import PayuJSONProfilingParser


@pytest.fixture(scope="module")
def payujson_parser():
    """Fixture instantiating the Payu JSON parser."""
    return PayuJSONProfilingParser()


@pytest.fixture(scope="module")
def payujson_profiling():
    """Fixture returning a dict holding the parsed Payu JSON timing content."""
    return {
        "region": [
            "payu_setup_duration_seconds",
            "payu_model_run_duration_seconds",
            "payu_run_duration_seconds",
            "payu_archive_duration_seconds",
            "payu_total_duration_seconds",
        ],
        tmax: [47.73822930175811, 6776.044810215011, 6779.385873348918, 8.063649574294686, 6838.225644],
    }


@pytest.fixture(scope="module")
def payujson_log_text():
    """Fixture returning the Payu JSON timing content."""
    return """{
    "scheduler_job_id": "149764665.gadi-pbs",
    "timings": {
        "payu_start_time": "2025-09-16T08:52:50.748807",
        "payu_setup_duration_seconds": 47.73822930175811,
        "payu_model_run_duration_seconds": 6776.044810215011,
        "payu_run_duration_seconds": 6779.385873348918,
        "payu_archive_duration_seconds": 8.063649574294686,
        "payu_finish_time": "2025-09-16T10:46:48.974451",
        "payu_total_duration_seconds": 6838.225644
    },
    "payu_run_id": "5c9027104cc39a5d39814624537c21440b68beb7",
    "payu_model_run_status": 0,
    "model_finish_time": "1844-01-01T00:00:00",
    "model_start_time": "1843-01-01T00:00:00",
    "model_calendar": "proleptic_gregorian",
    "payu_run_status": 0
}
"""


def test_payujson_profiling(tmp_path, payujson_parser, payujson_log_text, payujson_profiling):
    """Test the correct parsing of Payu JSON timing information."""
    payujson_log_file = tmp_path / "payu.json"
    payujson_log_file.write_text(payujson_log_text)
    assert payujson_parser.metrics == [tmax], "tmax metric not found in parsed log."
    parsed_log = payujson_parser.parse(payujson_log_file)
    for idx, region in enumerate(payujson_profiling["region"]):
        assert region in parsed_log["region"], f"{region} not found in Payu JSON parsed log."
        assert payujson_profiling[tmax][idx] == parsed_log[tmax][idx], (
            f"Incorrect walltime for region {region} (idx: {idx})."
        )


def test_payujson_incorrect_profiling(tmp_path, payujson_parser):
    """Test that exceptions get raised appropriately."""
    wrong_content = {
        '{"a": 123}',  # missing "timings" key
        '{"timings": {"payu_start_time": "2025-09-16T08:52:50.748807"}}',  # invalid "timings" value
        "abc def",  # invalid JSON altogether
    }
    for content in wrong_content:
        payu_log_file = tmp_path / "payu.json"
        payu_log_file.write_text(content)
        with pytest.raises(ValueError):
            payujson_parser.parse(payu_log_file)
        payu_log_file.unlink()
