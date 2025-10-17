# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access.profiling import CylcProfilingParser
from access.profiling.metrics import tmax


@pytest.fixture(scope="module")
def cylc_parser():
    "Fixture Instantiating the Cylc parser."
    return CylcProfilingParser()


@pytest.fixture(scope="module")
def cylc_profiling():
    "Fixture instantiating parsed Cylc log timing."
    return {
        "region": ["pipeline_elapsed_time"],
        tmax: [976632499],
    }


@pytest.fixture(scope="module")
def cylc_log():
    "Fixture instantiating a valid Cylc run log."
    return """1994-11-05T23:30:01Z INFO - Suite server: url=http://localhost:1234 pid=12345
1994-11-05T23:30:01Z INFO - Run: (re)start=0 log=1
1994-11-05T23:30:01Z INFO - Cylc version: 7.9.9
1994-11-05T23:30:01Z INFO - Run mode: live
1994-11-05T23:30:01Z INFO - Hello
2025-10-17T14:18:13Z INFO - Suite shutting down - AUTOMATIC
2025-10-17T14:18:20Z INFO - DONE
"""


@pytest.fixture(scope="module")
def cylc_log_no_first_timestamp():
    "Fixture instantiating an invalid Cylc log that is missing a timestamp in the first line."
    return "Not a valid start to the log\n" + cylc_log()


@pytest.fixture(scope="module")
def cylc_log_no_last_timestamp():
    "Fixture instantiating an invalid Cylc log that is missing a timestamp in the last line."
    return cylc_log() + "\nNot a valid end to the log\n"


def test_cylc_profiling(cylc_parser, cylc_profiling, cylc_log):
    "Tests that a Cylc log file is correctly parsed."
    parsed_log = cylc_parser.read(cylc_log)
    for idx, region in enumerate(cylc_profiling["region"]):
        assert region in parsed_log["region"], f"{region} not found in Cylc parsed log"
        for metric in (tmax,):
            assert cylc_profiling[metric][idx] == parsed_log[metric][idx], (
                f"Incorrect {metric} for region {region} (idx: {idx})."
            )


def test_cylc_invalid_logs(cylc_parser, cylc_log):
    """Tests that an exceptions are raised when:
    * last line doesn't contain "DONE".
    * invalid timestamp in either the first or last line.
    """
    with pytest.raises(ValueError):
        cylc_parser.read(cylc_log.replace("DONE", "potato"))
    with pytest.raises(ValueError):
        cylc_parser.read("Invalid start to log\n" + cylc_log)
    with pytest.raises(ValueError):
        cylc_parser.read(cylc_log + "\n1234-56-78T90:12:34Z INFO - DONE\n")
