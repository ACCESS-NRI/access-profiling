# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access.profiling import ESMFSummaryProfilingParser
from access.profiling.metrics import count, pemax, pemin, tavg, tmax, tmin


@pytest.fixture(scope="module")
def flat_esmf_summary_parser():
    """Fixture instantiating the ESMF summary parser where parsed results are flat."""
    return ESMFSummaryProfilingParser()


@pytest.fixture(scope="module")
def hierarchical_esmf_summary_parser():
    """Fixture instantiating the ESMF summary parser where parsed results are hierarchical."""
    return ESMFSummaryProfilingParser(hierarchical=True)


@pytest.fixture(scope="module")
def flat_esmf_summary_profiling():
    """Fixture returning a flat dict holding the parsed ESMF summary timing content."""
    return {
        "region": [
            "[ESMF]",
            "[ICE] RunPhase1",
            "cice_run_total",
            "cice_run_import",
            "cice_run_export",
            "[MED-TO-OCN] RunPhase1",
        ],
        count: [1, 960, 960, 960, 960, 960],
        tavg: [
            2558.5684,
            155.8202,
            155.4648,
            3.7565,
            1.1015,
            16.7498,
        ],
        tmin: [
            2555.1450,
            154.7637,
            154.3687,
            3.5426,
            0.8846,
            0.4588,
        ],
        pemin: [279, 94, 94, 218, 361, 256],
        tmax: [
            2559.5801,
            160.2443,
            159.8980,
            8.3892,
            1.3607,
            23.2875,
        ],
        pemax: [817, 0, 0, 1, 194, 1023],
    }


@pytest.fixture(scope="module")
def hierarchical_esmf_summary_profiling():
    """Fixture returning a hierarchical dict holding the parsed ESMF summary timing content."""
    return {
        "[ESMF]": {
            count: 1,
            tavg: 2558.5684,
            tmin: 2555.1450,
            pemin: 279,
            tmax: 2559.5801,
            pemax: 817,
            "[ICE] RunPhase1": {
                count: 960,
                tavg: 155.8202,
                tmin: 154.7637,
                pemin: 94,
                tmax: 160.2443,
                pemax: 0,
                "cice_run_total": {
                    count: 960,
                    tavg: 155.4648,
                    tmin: 154.3687,
                    pemin: 94,
                    tmax: 159.8980,
                    pemax: 0,
                    "cice_run_import": {
                        count: 960,
                        tavg: 3.7565,
                        tmin: 3.5426,
                        pemin: 218,
                        tmax: 8.3892,
                        pemax: 1,
                    },
                    "cice_run_export": {
                        count: 960,
                        tavg: 1.1015,
                        tmin: 0.8846,
                        pemin: 361,
                        tmax: 1.3607,
                        pemax: 194,
                    },
                },
            },
            "[MED-TO-OCN] RunPhase1": {
                count: 960,
                tavg: 16.7498,
                tmin: 0.4588,
                pemin: 256,
                tmax: 23.2875,
                pemax: 1023,
            },
        }
    }


@pytest.fixture(scope="module")
def esmf_log_text():
    """Fixture returning the ESMF summary timing content."""
    return """********
IMPORTANT: Large deviations between Connector times on different PETs
are typically indicators of load imbalance in the system. The following
Connectors in this profile may indicate a load imbalance:
     - [OCN-TO-MED] RunPhase1
********

Region                         PETs   PEs    Count    Mean (s)    Min (s)     Min PET Max (s)     Max PET
  [ESMF]                       1664   1664   1        2558.5684   2555.1450   279     2559.5801   817    
    [ICE] RunPhase1            364    364    960      155.8202    154.7637    94      160.2443    0      
      cice_run_total           364    364    960      155.4648    154.3687    94      159.8980    0      
        cice_run_import        364    364    960      3.7565      3.5426      218     8.3892      1      
        cice_run_export        364    364    960      1.1015      0.8846      361     1.3607      194    
    [MED-TO-OCN] RunPhase1     1664   1664   960      16.7498     0.4588      256     23.2875     1023   
"""


@pytest.fixture(scope="module")
def incorrect_esmf_log_text():
    """Fixture returning an ESMF summary timing output with missing values."""
    return """
  [ESMF]                      1664   1        2558.5684   2555.1450   279     2559.5801   817    
    [ensemble] RunPhase1      1664   1664   1        1879.7292               376     1905.4939   1      
      [ESM0001] RunPhase1     1664   1664   1.0      1879.7286   1872.5059   858     1905.4937   1      
    """


@pytest.fixture(scope="module")
def duplicate_region_log_text():
    return """
Region                         PETs   PEs    Count    Mean (s)    Min (s)     Min PET Max (s)     Max PET
  [ESMF]                       1664   1664   1        10.0000     9.0000      0       11.0000     1663    
    [NEST1] region1            364    364    960      4.0000      3.0000      0       5.0000      1663    
    [NEST1] region1            364    364    1920     6.0000      2.0000      1       6.5000      1662    
"""


@pytest.fixture(scope="module")
def duplicate_region_profiling():
    return {
        "region": ["[ESMF]", "[NEST1] region1"],
        count: [1, 2880],
        tavg: [10.0, (960 * 4.0 + 1920 * 6.0) / (960 + 1920)],
        tmin: [9.0, 2.0],
        pemin: [0, 1],
        tmax: [11.0, 6.5],
        pemax: [1663, 1662],
    }


@pytest.fixture(scope="module")
def duplicate_region_with_nonmatching_pes_log_text():
    return """
Region                         PETs   PEs    Count    Mean (s)    Min (s)     Min PET Max (s)     Max PET
  [ESMF]                       1664   1664   1        10.0000     9.0000      0       11.0000     1663    
    [NEST1] region1            364    364    960      4.0000      3.0000      0       5.0000      1663    
    [NEST1] region1            728    728    1920     6.0000      2.0000      1       6.5000      1662    
"""


def check_nested_dict(
    input_dict: dict,
    correct_dict: dict,
    metric_keys: set = None,
    region: str = "[ESMF]",
    depth: int = 1,
):
    """Helper function to check that all key-value pairs of correct_dict are in input_dict.

    Args:
        input_dict (dict): The dict to check.
        correct_dict (dict): The correct dict used to check input_dict.
        metric_keys (set): Expected metrics at each level of the dict (except root).
        region (str): The region currently being checked.
        depth (int): The depth currently being checked.
    """

    # set default metric_keys
    if metric_keys is None:
        metric_keys = {count, tavg, tmin, pemin, tmax, pemax}

    # check that all keys in correct_dict are in input_dict
    if input_dict.keys() < correct_dict.keys():
        raise ValueError(f"Missing keys for {region} (depth: {depth}): {set(correct_dict.keys()) - input_dict.keys()}")
    region_keys = set(correct_dict.keys()) - metric_keys

    # first check metric values
    for k in metric_keys:
        assert input_dict[k] == correct_dict[k], (
            f"Incorrect {k} value at {region} (depth: {depth}): expected {correct_dict[k]}, but got {input_dict[k]}."
        )

    # recursively check children dicts
    for k in region_keys:
        check_nested_dict(input_dict[k], correct_dict[k], metric_keys, k, depth + 1)


def test_flat_esmf_profiling(tmp_path, flat_esmf_summary_parser, esmf_log_text, flat_esmf_summary_profiling):
    """Test the correct parsing of ESMF timing summary information with flat structure."""
    esmf_log_file = tmp_path / "esmf.log"
    esmf_log_file.write_text(esmf_log_text)
    parsed_log = flat_esmf_summary_parser.parse(esmf_log_file)
    for idx, region in enumerate(flat_esmf_summary_profiling["region"]):
        assert region in parsed_log["region"], f"{region} not found in ESMF parsed summary timings."
        for metric in (tavg, tmin, pemin, tmax, pemax):
            assert flat_esmf_summary_profiling[metric][idx] == parsed_log[metric][idx], (
                f"Incorrect {metric.name} for region {region} (idx: {idx}). \
Expected {flat_esmf_summary_profiling[metric][idx]}, got {parsed_log[metric][idx]}."
            )


def test_hierarchical_esmf_profiling(
    tmp_path, hierarchical_esmf_summary_parser, esmf_log_text, hierarchical_esmf_summary_profiling
):
    """Test the correct parsing of ESMF timing summary information with hierarchical structure."""
    esmf_log_file = tmp_path / "esmf.log"
    esmf_log_file.write_text(esmf_log_text)
    parsed_log = hierarchical_esmf_summary_parser.parse(esmf_log_file)
    check_nested_dict(parsed_log["[ESMF]"], hierarchical_esmf_summary_profiling["[ESMF]"])


def test_esmf_missing_values(
    tmp_path, flat_esmf_summary_parser, hierarchical_esmf_summary_parser, incorrect_esmf_log_text
):
    """Tests that row isn't picked up when values are missing."""
    esmf_log_file = tmp_path / "esmf.log"
    esmf_log_file.write_text(incorrect_esmf_log_text)
    # check flat parser
    with pytest.raises(ValueError):
        flat_esmf_summary_parser.parse(esmf_log_file)
    # check hierarchical parser
    with pytest.raises(ValueError):
        hierarchical_esmf_summary_parser.parse(esmf_log_file)


def test_esmf_repeat_region(tmp_path, flat_esmf_summary_parser, duplicate_region_log_text, duplicate_region_profiling):
    """Tests that duplicate regions are aggregated correctly."""
    esmf_log_file = tmp_path / "esmf.log"
    esmf_log_file.write_text(duplicate_region_log_text)
    parsed_log = flat_esmf_summary_parser.parse(esmf_log_file)
    for idx, region in enumerate(duplicate_region_profiling["region"]):
        assert region in parsed_log["region"], f"{region} not found in ESMF parsed summary timings."
        for metric in (tavg, tmin, pemin, tmax, pemax):
            assert duplicate_region_profiling[metric][idx] == parsed_log[metric][idx], (
                f"Incorrect {metric.name} for region {region} (idx: {idx}). \
                    {duplicate_region_profiling[metric][idx]}, got {parsed_log[metric][idx]}."
            )


def test_esmf_repeat_region_nonmatching_pes(
    tmp_path, flat_esmf_summary_parser, duplicate_region_with_nonmatching_pes_log_text
):
    esmf_log_file = tmp_path / "esmf.log"
    esmf_log_file.write_text(duplicate_region_with_nonmatching_pes_log_text)
    with pytest.raises(NotImplementedError):
        flat_esmf_summary_parser.parse(esmf_log_file)
