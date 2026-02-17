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
def cylc_log_text():
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
def cylc_log_no_first_timestamp(cylc_log_text):
    "Fixture instantiating an invalid Cylc log that is missing a timestamp in the first line."
    return "Not a valid start to the log\n" + cylc_log_text


@pytest.fixture(scope="module")
def cylc_log_no_last_timestamp(cylc_log_text):
    "Fixture instantiating an invalid Cylc log that is missing a timestamp in the last line."
    return cylc_log_text + "\nNot a valid end to the log\n"


def test_cylc_profiling(tmp_path, cylc_parser, cylc_profiling, cylc_log_text):
    "Tests that a Cylc log file is correctly parsed."
    cylc_log_file = tmp_path / "cylc.log"
    cylc_log_file.write_text(cylc_log_text)
    parsed_log = cylc_parser.parse(cylc_log_file)
    for idx, region in enumerate(cylc_profiling["region"]):
        assert region in parsed_log["region"], f"{region} not found in Cylc parsed log"
        for metric in (tmax,):
            assert cylc_profiling[metric][idx] == parsed_log[metric][idx], (
                f"Incorrect {metric} for region {region} (idx: {idx})."
            )


def test_cylc_invalid_logs(tmp_path, cylc_parser, cylc_log_text):
    """Tests that an exceptions are raised when:
    * last line doesn't contain "DONE".
    * invalid timestamp in either the first or last line.
    """
    erroneous_content = (
        cylc_log_text.replace("DONE", "potato"),  # last line is not done (indicating job is incomplete)
        "Invalid start to log\n" + cylc_log_text,  # first line has no timestamp
        cylc_log_text + "1234-56-78T90:12:34Z INFO - DONE",  # invalid timestamp
    )
    cylc_log_file = tmp_path / "cylc.log"
    for content in erroneous_content:
        cylc_log_file.write_text(content)
        with pytest.raises(ValueError):
            cylc_parser.parse(cylc_log_file)
        cylc_log_file.unlink()
