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


@pytest.fixture(scope="module")
def second_occurrence_with_higher_minimum_log_text():
    """Fixture returning a log whose repeated region is slower, not faster, on its second occurrence.

    The second [ATM-TO-MED] RunPhase1 row has a higher minimum (3.0 against 1.0) but a higher maximum
    (12.0 against 9.0), so only the maximum should move when the two rows are merged.
    """
    return """
Region                         PETs   PEs    Count    Mean (s)    Min (s)     Min PET Max (s)     Max PET
  [ESMF]                       64     64     1        20.0000     19.0000     0       21.0000     63
    [ATM-TO-MED] RunPhase1     64     64     100      4.0000      1.0000      7       9.0000      11
    [ATM-TO-MED] RunPhase1     64     64     300      8.0000      3.0000      42      12.0000     57
"""


@pytest.fixture(scope="module")
def second_occurrence_with_lower_maximum_log_text():
    """Fixture returning a log whose repeated region is uniformly faster on its second occurrence.

    The second [ATM-TO-MED] RunPhase1 row has a lower minimum (0.5 against 1.0) and a lower maximum
    (6.0 against 9.0), so only the minimum should move when the two rows are merged.
    """
    return """
Region                         PETs   PEs    Count    Mean (s)    Min (s)     Min PET Max (s)     Max PET
  [ESMF]                       64     64     1        20.0000     19.0000     0       21.0000     63
    [ATM-TO-MED] RunPhase1     64     64     100      4.0000      1.0000      7       9.0000      11
    [ATM-TO-MED] RunPhase1     64     64     300      8.0000      0.5000      42      6.0000      57
"""


@pytest.fixture(scope="module")
def second_occurrence_tying_both_extremes_log_text():
    """Fixture returning a log whose repeated region ties on both extremes but reports different PETs.

    Both [ATM-TO-MED] RunPhase1 rows bottom out at 1.0 s and peak at 9.0 s, but blame different PETs for
    those times - the only way to tell a strict comparison from a non-strict one.
    """
    return """
Region                         PETs   PEs    Count    Mean (s)    Min (s)     Min PET Max (s)     Max PET
  [ESMF]                       64     64     1        20.0000     19.0000     0       21.0000     63
    [ATM-TO-MED] RunPhase1     64     64     100      4.0000      1.0000      7       9.0000      11
    [ATM-TO-MED] RunPhase1     64     64     300      8.0000      1.0000      42      9.0000      57
"""


@pytest.fixture(scope="module")
def repeated_region_with_children_log_text():
    """Fixture returning a log where a repeated region is given different children each time it appears.

    ESMF nests a connector's phases underneath it, so the two occurrences of [ATM-TO-MED] RunPhase1 carry
    one child apiece. Keeping both children is what makes the hierarchy usable for such regions.
    """
    return """
Region                         PETs   PEs    Count    Mean (s)    Min (s)     Min PET Max (s)     Max PET
  [ESMF]                       64     64     1        20.0000     19.0000     0       21.0000     63
    [ATM-TO-MED] RunPhase1     64     64     100      4.0000      1.0000      7       9.0000      11
      first_phase              64     64     100      2.0000      0.5000      3       3.0000      5
    [ATM-TO-MED] RunPhase1     64     64     300      8.0000      3.0000      42      12.0000     57
      second_phase             64     64     300      6.0000      2.5000      9       7.0000      13
"""


class TestMergingRepeatedFlatRegions:
    """How the flat parser folds a region that the summary reports more than once into a single row.

    ESMF genuinely lists some regions twice - [ATM-TO-MED] RunPhase1 appears twice in ACCESS-OM3 - so the
    merge has to end up holding the extremes over both occurrences together with the PET that recorded
    each one. A merge that moved a time without moving its PET, or vice versa, would quietly point the
    load-imbalance analysis at a rank that was never the slowest, and nothing downstream could detect it.
    """

    def test_a_second_occurrence_with_a_higher_minimum_leaves_the_minimum_where_it_was(
        self, tmp_path, flat_esmf_summary_parser, second_occurrence_with_higher_minimum_log_text
    ):
        """The fastest call over the whole run is the fastest of the occurrences, not the latest one.

        Without the comparison, a second occurrence that never ran as fast as the first would overwrite
        the minimum and report the run as slower than it was.
        """
        esmf_log_file = tmp_path / "esmf.log"
        esmf_log_file.write_text(second_occurrence_with_higher_minimum_log_text)

        parsed_log = flat_esmf_summary_parser.parse(esmf_log_file)
        idx = parsed_log["region"].index("[ATM-TO-MED] RunPhase1")

        assert parsed_log[tmin][idx] == 1.0, "The higher minimum of the second occurrence replaced the lower one."
        assert parsed_log[pemin][idx] == 7, "The minimum PET followed the second occurrence despite its slower time."
        assert parsed_log[tmax][idx] == 12.0, "The higher maximum of the second occurrence was not taken up."
        assert parsed_log[pemax][idx] == 57, "The maximum moved to the second occurrence but its PET did not."

    def test_a_second_occurrence_with_a_lower_maximum_leaves_the_maximum_where_it_was(
        self, tmp_path, flat_esmf_summary_parser, second_occurrence_with_lower_maximum_log_text
    ):
        """The slowest call over the whole run is the slowest of the occurrences, not the latest one.

        Without the comparison, a second, uniformly quicker occurrence would hide the worst call of the
        run - exactly the call the profiling is meant to surface.
        """
        esmf_log_file = tmp_path / "esmf.log"
        esmf_log_file.write_text(second_occurrence_with_lower_maximum_log_text)

        parsed_log = flat_esmf_summary_parser.parse(esmf_log_file)
        idx = parsed_log["region"].index("[ATM-TO-MED] RunPhase1")

        assert parsed_log[tmax][idx] == 9.0, "The lower maximum of the second occurrence replaced the higher one."
        assert parsed_log[pemax][idx] == 11, "The maximum PET followed the second occurrence despite its faster time."
        assert parsed_log[tmin][idx] == 0.5, "The lower minimum of the second occurrence was not taken up."
        assert parsed_log[pemin][idx] == 42, "The minimum moved to the second occurrence but its PET did not."

    def test_an_occurrence_that_only_ties_the_extremes_does_not_claim_them(
        self, tmp_path, flat_esmf_summary_parser, second_occurrence_tying_both_extremes_log_text
    ):
        """A tie is resolved in favour of the first occurrence, so the reported PETs stay put.

        Both occurrences are equally extreme here, so either PET would be defensible - but the choice has
        to be stable, otherwise the PET reported for a region would depend on the order of the summary.
        """
        esmf_log_file = tmp_path / "esmf.log"
        esmf_log_file.write_text(second_occurrence_tying_both_extremes_log_text)

        parsed_log = flat_esmf_summary_parser.parse(esmf_log_file)
        idx = parsed_log["region"].index("[ATM-TO-MED] RunPhase1")

        assert parsed_log[tmin][idx] == 1.0
        assert parsed_log[tmax][idx] == 9.0
        assert parsed_log[pemin][idx] == 7, "A tied minimum handed the PET over to the second occurrence."
        assert parsed_log[pemax][idx] == 11, "A tied maximum handed the PET over to the second occurrence."

    def test_the_merged_mean_is_weighted_by_the_calls_each_occurrence_made(
        self, tmp_path, flat_esmf_summary_parser, second_occurrence_with_higher_minimum_log_text
    ):
        """The mean of a merged region has to be weighted by call count, not a plain average of the means.

        Here 100 calls averaging 4 s merge with 300 calls averaging 8 s: the answer is 7 s, whereas an
        unweighted average would report 6 s and understate the region by a sixth.
        """
        esmf_log_file = tmp_path / "esmf.log"
        esmf_log_file.write_text(second_occurrence_with_higher_minimum_log_text)

        parsed_log = flat_esmf_summary_parser.parse(esmf_log_file)
        idx = parsed_log["region"].index("[ATM-TO-MED] RunPhase1")

        assert parsed_log[count][idx] == 400, "The call counts of the two occurrences were not added up."
        assert parsed_log[tavg][idx] == pytest.approx((100 * 4.0 + 300 * 8.0) / 400)


class TestRepeatedRegionsInTheHierarchy:
    """How the hierarchical parser treats a region name that appears twice under the same parent.

    The hierarchy keys regions by name, so the two occurrences have to share one node. The children hung
    off the first occurrence must survive the second one being read, or half of the call-stack below a
    repeated connector would silently disappear from the parsed tree.
    """

    def test_a_region_repeated_under_one_parent_keeps_the_children_of_both_occurrences(
        self, tmp_path, hierarchical_esmf_summary_parser, repeated_region_with_children_log_text
    ):
        """Re-entering an existing node must extend it, not start it again from scratch.

        If the second occurrence created a fresh dictionary, first_phase would be dropped and the tree
        would claim the connector only ever ran one phase.
        """
        esmf_log_file = tmp_path / "esmf.log"
        esmf_log_file.write_text(repeated_region_with_children_log_text)

        parsed_log = hierarchical_esmf_summary_parser.parse(esmf_log_file)
        connector = parsed_log["[ESMF]"]["[ATM-TO-MED] RunPhase1"]

        assert {k for k in parsed_log["[ESMF]"] if isinstance(k, str)} == {"[ATM-TO-MED] RunPhase1"}, (
            "The repeated region was stored as two sibling nodes instead of one."
        )
        assert {k for k in connector if isinstance(k, str)} == {"first_phase", "second_phase"}, (
            "The children of the two occurrences were not gathered under the shared node."
        )
        assert connector["first_phase"][tavg] == 2.0, "The first occurrence's child lost its statistics."
        assert connector["second_phase"][tavg] == 6.0, "The second occurrence's child lost its statistics."

    def test_a_region_repeated_under_one_parent_reports_its_last_occurrence(
        self, tmp_path, hierarchical_esmf_summary_parser, repeated_region_with_children_log_text
    ):
        """The hierarchy overwrites the node's statistics rather than merging them as the flat result does.

        This is worth pinning down because the two modes disagree: the flat result would report 400 calls
        here, while the tree reports the 300 of the last row read. Anyone comparing the two needs to know
        the tree is not an aggregate.
        """
        esmf_log_file = tmp_path / "esmf.log"
        esmf_log_file.write_text(repeated_region_with_children_log_text)

        parsed_log = hierarchical_esmf_summary_parser.parse(esmf_log_file)
        connector = parsed_log["[ESMF]"]["[ATM-TO-MED] RunPhase1"]

        assert connector[count] == 300, "The repeated region's call count was aggregated rather than overwritten."
        assert connector[tavg] == 8.0
        assert connector[tmin] == 3.0
        assert connector[pemin] == 42
        assert connector[tmax] == 12.0
        assert connector[pemax] == 57
