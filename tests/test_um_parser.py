# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access.profiling.metrics import pemax, pemin, tavg, tmax, tmed, tmin, tstd
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser


@pytest.fixture(scope="module")
def um_parser():
    """Fixture for the UM parser"""
    return UMProfilingParser()


@pytest.fixture(scope="module")
def um_required_metrics():
    """Fixture for required metrics for the UM parser"""
    return {tavg, tmed, tstd, tmax, pemax, tmin, pemin}


@pytest.fixture(scope="module")
def um7_raw_profiling_data():
    """Fixture with UM v7.x raw profiling data"""
    return r"""
 MPP Timing information :
                   240  processors in configuration                     16  x
                    15

 MPP : Inclusive timer summary

 WALLCLOCK  TIMES
    ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)      MIN   (PE)
  1 AS3 Atmos_Phys2        1308.30  1308.30     0.02       0.00%  1308.33 ( 118)  1308.26 ( 221)
  2 AP2 Boundary Layer      956.50   956.14     3.26       0.34%   981.28 ( 136)   953.28 (  43)
  3 AS5-8 Updates           884.63   885.53     2.89       0.33%   889.49 (  48)   879.37 ( 212)
  4 AS2 S-L Advection       746.73   746.73     0.01       0.00%   746.74 (  47)   746.71 ( 181)
  5 AS1 Atmos_Phys1         561.27   562.54    10.63       1.89%   580.32 (  42)   538.58 ( 212)
  6 AP2 Convection          493.73   493.82     0.18       0.04%   493.93 (  76)   493.34 (  20)

 CPU TIMES (sorted by wallclock times)
    ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)      MIN   (PE)
  1 AS3 Atmos_Phys2        1308.30  1308.30     0.02       0.00%  1308.33 ( 118)  1308.26 ( 221)
  2 AP2 Boundary Layer      956.50   956.13     3.26       0.34%   981.27 ( 136)   953.28 (  43)
  3 AS5-8 Updates           884.62   885.52     2.89       0.33%   889.49 (  48)   879.36 ( 212)
  4 AS2 S-L Advection       746.72   746.73     0.01       0.00%   746.74 (  47)   746.71 ( 181)
  5 AS1 Atmos_Phys1         561.27   562.53    10.63       1.89%   580.32 (  42)   538.58 ( 212)
  6 AP2 Convection          493.73   493.82     0.18       0.04%   493.93 (  76)   493.34 (  20)
  7 AP1 Radiation           315.24   315.24     0.02       0.01%   315.30 (  66)   315.19 (  13)
  8 AS4 Solver              208.71   208.71     0.01       0.00%   208.72 (  42)   208.69 ( 120)
  9 AS9 End TStep Diags     140.76   140.79     0.12       0.08%   141.00 ( 155)   140.57 (  15)
 10 AP1 Microphysics         65.56    65.56     0.01       0.02%    65.59 (  64)    65.53 (  15)
 11 AS Swap_Bounds           60.83    60.98     1.38       2.27%    62.74 ( 128)    58.21 ( 228)
 12 AP2 River Routing        56.46    56.48     0.09       0.16%    56.66 ( 239)    56.29 (   2)
 13 AEROSOL MODELLING        48.82    48.82     0.02       0.03%    48.87 ( 216)    48.79 ( 109)
 14 DUMPCTL                  13.52    16.88     5.84      43.20%    27.81 (   0)     3.42 (  47)
 15 AP1 G-wave drag           6.17     6.27     0.58       9.45%     7.05 ( 163)     3.71 ( 105)
 16 TIMER                     5.66     5.67     0.13       2.31%     6.02 ( 159)     5.28 ( 115)
 17 AS3 Diffusion             4.64     4.64     0.01       0.11%     4.67 (  15)     4.63 (  16)
 18 AP2 Hydrology             0.50     0.48     0.37      75.19%     1.16 ( 163)     0.03 (  68)
 19 INITDUMP                  0.99     0.99     0.01       0.59%     1.00 ( 198)     0.98 ( 106)
 20 AS9 Energy mass           0.77     0.77     0.00       0.09%     0.78 (   0)     0.77 ( 194)
 21 AP2 Conv Eng Corr         0.58     0.58     0.00       0.84%     0.59 (  15)     0.57 ( 190)
 22 AP1 Conv Eng Corr         0.57     0.57     0.00       0.61%     0.58 (   5)     0.56 ( 135)
 23 AP1 Energy Correct.       0.42     0.42     0.02       3.73%     0.46 (  83)     0.38 ( 215)
 24 AS18 Assimilation         0.01     0.01     0.00       9.45%     0.02 (  64)     0.01 ( 196)

 PARALLEL SPEEDUP SUMMARY (sorted by wallclock times)
    ROUTINE              CPU TOTAL   WALLCLOCK MAX   SPEEDUP   PARALLEL EFFICIENCY
  1 AS3 Atmos_Phys2       ********         1308.33    239.99                  1.00
  2 AP2 Boundary Layer    ********          981.28    233.94                  0.97
  3 AS5-8 Updates         ********          889.49    238.69                  0.99
  4 AS2 S-L Advection     ********          746.74    240.00                  1.00
  5 AS1 Atmos_Phys1       ********          580.32    232.12                  0.97
  6 AP2 Convection        ********          493.93    239.90                  1.00
  7 AP1 Radiation         75658.68          315.30    239.96                  1.00

        """


@pytest.fixture(scope="module")
def um7_malformed_profiling_data_missing_header():
    """Fixture with UM7 raw profiling data that is missing ``` WALLCLOCK  TIMES``` in the header"""
    return r"""
 MPP : Inclusive timer summary

    ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)      MIN   (PE)
  1 AS3 Atmos_Phys2        1308.30  1308.30     0.02       0.00%  1308.33 ( 118)  1308.26 ( 221)
  2 AP2 Boundary Layer      956.50   956.14     3.26       0.34%   981.28 ( 136)   953.28 (  43)

 CPU TIMES (sorted by wallclock times)
    """


@pytest.fixture(scope="module")
def um7_malformed_profiling_data_missing_footer():
    """Fixture with UM7 raw profiling data that is missing the footer line with `` CPU TIMES (sorted by wallclock
    times)``"""
    return r"""
 MPP : Inclusive timer summary

 WALLCLOCK  TIMES
     ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)      MIN   (PE)
  1 AS3 Atmos_Phys2        1308.30   1308.30      0.02       0.00%  1308.33 ( 118)  1308.26 ( 221)

    """


@pytest.fixture(scope="module")
def um7_malformed_profiling_data_missing_profiling_section():
    """Fixture with UM7 raw profiling data that is missing the section with profiling data"""
    return r"""
 MPP : Inclusive timer summary

 WALLCLOCK  TIMES
    ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)      MIN   (PE)

 CPU TIMES (sorted by wallclock times)
    """


@pytest.fixture(scope="module")
def um7_malformed_data_extra_final_column():
    """Fixture with UM7 raw profiling data but with an extra column at the end"""
    return r"""
 MPP : Inclusive timer summary

 WALLCLOCK  TIMES
     ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)      MIN   (PE)  UNEXPECTED_COLUMN
  1 AS3 Atmos_Phys2        1308.30   1308.30     0.02       0.00%  1308.33 ( 118)  1308.26 ( 221)   2378

 CPU TIMES (sorted by wallclock times)
    """


@pytest.fixture(scope="module")
def um7_malformed_data_extra_middle_column():
    """Fixture with UM7 raw profiling data but with an extra column in the middle"""
    return r"""
 MPP : Inclusive timer summary

 WALLCLOCK  TIMES
     ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)  UNEXPECTED_COLUMN    MIN   (PE)
  1 AS3 Atmos_Phys2        1308.30   1308.30      0.02       0.00%  1308.33 ( 118)    2378           1308.26 ( 221)

 CPU TIMES (sorted by wallclock times)
    """


@pytest.fixture(scope="module")
def um7_malformed_data_extra_front_column_with_float_data():
    """Fixture with UM7 raw profiling data but with an extra column in the front containing a floating value"""
    return r"""
 MPP : Inclusive timer summary

 WALLCLOCK  TIMES
UNEXPECTED_COLUMN     ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)    MIN     (PE)
 2378.23             1 AS3 Atmos_Phys2        1308.30   1308.30      0.02    0.00%    1308.33 ( 118)  1308.26 ( 221)

 CPU TIMES (sorted by wallclock times)
    """


@pytest.fixture(scope="module")
def um7_malformed_data_extra_front_column_with_integer_data():
    """Fixture with UM7 raw profiling data but with an extra column in the front containing integer values"""
    return r"""
 MPP : Inclusive timer summary

 WALLCLOCK  TIMES
UNEXPECTED_COLUMN     ROUTINE                 MEAN   MEDIAN       SD   % of mean      MAX   (PE)    MIN     (PE)
 2378             1 AS3 Atmos_Phys2        1308.30  1308.30     0.02       0.00%  1308.33 ( 118)  1308.26 ( 221)
 1232             2 AP2 Boundary Layer      956.50   956.14     3.26       0.34%   981.28 ( 136)   953.28 (  43)
 12343            3 AS5-8 Updates           884.63   885.53     2.89       0.33%   889.49 (  48)   879.37 ( 212)
 12223            4 AS2 S-L Advection       746.73   746.73     0.01       0.00%   746.74 (  47)   746.71 ( 181)
 947586           5 AS1 Atmos_Phys1         561.27   562.54    10.63       1.89%   580.32 (  42)   538.58 ( 212)
 87462            6 AP2 Convection          493.73   493.82     0.18       0.04%   493.93 (  76)   493.34 (  20)

 CPU TIMES (sorted by wallclock times)
    """


@pytest.fixture(scope="module")
def um7_malformed_data_extra_front_column_with_string_data():
    """Fixture with UM7 raw profiling data but with an extra column in the front containing strings"""
    return r"""
 MPP : Inclusive timer summary

 WALLCLOCK  TIMES
UNEXPECTED_COLUMN     ROUTINE                   MEAN   MEDIAN       SD   % of mean      MAX   (PE)    MIN     (PE)
 ***             1 AS3 Atmos_Phys2        1308.30   1308.30      0.02    0.00%    1308.33 ( 118)  1308.26 ( 221)

 CPU TIMES (sorted by wallclock times)
    """


@pytest.fixture(scope="module")
def um7_malformed_profiling_data_bad_columndata():
    """Fixture with UM7 raw profiling data but with asterisks in one of the columns"""
    return r"""
 MPP : Inclusive timer summary

 WALLCLOCK  TIMES
    ROUTINE                   MEAN   MEDIAN        SD   % of mean      MAX   (PE)      MIN   (PE)
  1 AS3 Atmos_Phys2        1308.30   ******      0.02       0.00%  1308.33 ( 118)  1308.26 ( 221)
  2 AP2 Boundary Layer      956.50   956.14      3.26       0.34%   981.28 ( 136)   953.28 (  43)

 CPU TIMES (sorted by wallclock times)
    """


@pytest.fixture(scope="module")
def um13_raw_profiling_data():
    """Fixture with UM v13.x raw profiling data."""
    return r"""
MPP : Inclusive timer summary

WALLCLOCK  TIMES
N  ROUTINE                                MEAN       MEDIAN        SD   % of mean          MAX  (PE)          MIN  (PE)
01 U_MODEL_4A                          1314.55      1314.55      0.06       0.00%      1315.88 (  0)      1314.55 (433)
02 Atm_Step_4A (AS)                    1272.16      1273.09      4.60       0.36%      1279.04 (240)      1257.69 ( 27)
03 AS Atmos_Phys1 (AP1)                 466.83       466.81      0.21       0.04%       467.36 ( 83)       466.37 (377)
04 AS S-L Advect (AA)                   180.79       181.45      1.67       0.92%       183.17 (104)       175.98 ( 21)
05 AS UKCA_MAIN1                        173.52       174.45      4.60       2.65%       180.40 (240)       159.06 ( 27)
06 AS Atmos_Phys2 (AP2)                 144.45       144.33      2.71       1.87%       150.18 (390)       138.25 (160)

CPU TIMES (sorted by wallclock times)
N  ROUTINE                                MEAN       MEDIAN        SD   % of mean          MAX  (PE)          MIN  (PE)
01 U_MODEL_4A                          1313.66      1314.19      1.65       0.13%      1314.52 (394)      1299.05 (  0)
02 Atm_Step_4A (AS)                    1271.36      1271.79      4.59       0.36%      1278.86 (244)      1249.70 (  0)
03 AS Atmos_Phys1 (AP1)                 466.75       466.79      0.47       0.10%       467.36 ( 83)       462.79 (  0)
04 AS S-L Advect (AA)                   180.78       181.46      1.69       0.93%       183.21 (104)       175.02 (  0)
05 AS UKCA_MAIN1                        173.51       174.45      4.61       2.65%       180.25 (244)       159.06 ( 27)
06 AS Atmos_Phys2 (AP2)                 144.45       144.38      2.70       1.87%       150.25 (390)       138.30 (160)

?  Caution This run generated 27 warnings

        """


@pytest.fixture(scope="module")
def um7_parsed_profile_data():
    """Fixture containing the parsed UM v7.x data with regions, and the associated metrics"""
    return {
        "region": [
            "AS3 Atmos_Phys2",
            "AP2 Boundary Layer",
            "AS5-8 Updates",
            "AS2 S-L Advection",
            "AS1 Atmos_Phys1",
            "AP2 Convection",
        ],
        tavg: [1308.3, 956.5, 884.63, 746.73, 561.27, 493.73],
        tmed: [1308.3, 956.14, 885.53, 746.73, 562.54, 493.82],
        tstd: [0.02, 3.26, 2.89, 0.01, 10.63, 0.18],
        tmax: [1308.33, 981.28, 889.49, 746.74, 580.32, 493.93],
        pemax: [118, 136, 48, 47, 42, 76],
        tmin: [1308.26, 953.28, 879.37, 746.71, 538.58, 493.34],
        pemin: [221, 43, 212, 181, 212, 20],
    }


@pytest.fixture(scope="module")
def um13_parsed_profile_data():
    """Fixture containing the parsed UM v13.x data with regions, and the associated metrics"""
    return {
        "region": [
            "U_MODEL_4A",
            "Atm_Step_4A (AS)",
            "AS Atmos_Phys1 (AP1)",
            "AS S-L Advect (AA)",
            "AS UKCA_MAIN1",
            "AS Atmos_Phys2 (AP2)",
        ],
        tavg: [1314.55, 1272.16, 466.83, 180.79, 173.52, 144.45],
        tmed: [1314.55, 1273.09, 466.81, 181.45, 174.45, 144.33],
        tstd: [0.06, 4.6, 0.21, 1.67, 4.6, 2.71],
        tmax: [1315.88, 1279.04, 467.36, 183.17, 180.4, 150.18],
        pemax: [0, 240, 83, 104, 240, 390],
        tmin: [1314.55, 1257.69, 466.37, 175.98, 159.06, 138.25],
        pemin: [433, 27, 377, 21, 27, 160],
    }


def test_um_metrics(um_parser, um_required_metrics):
    """Test that parsed metrics *exactly* match the expected metrics"""
    assert set(um_parser.metrics) == um_required_metrics, (
        f"Expected to find *exactly* these metrics = {um_required_metrics},"
        f" instead found = {um_parser.metrics}. "
        f"Affected field(s) = {um_required_metrics.symmetric_difference(set(um_parser.metrics))}"
    )


def test_um7_parsing(tmp_path, um_parser, um7_raw_profiling_data, um7_parsed_profile_data):
    """Test that parsed UM7 profiling data *exactly* matches the known-correct profiling data"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_raw_profiling_data)
    stats = um_parser.parse(um7_log_file)

    # Might also be worthwhile to check that the 'region' key exists first
    assert len(stats["region"]) == len(um7_parsed_profile_data["region"]), (
        f"Number of matched regions should be *exactly* {len(um7_parsed_profile_data['region'])}"
    )

    for metric in um_parser.metrics:
        for idx, region in enumerate(stats["region"]):
            assert stats[metric][idx] == um7_parsed_profile_data[metric][idx], (
                f"Incorrect {metric.name} for region {region} (index: {idx})."
            )


def test_um7_parser_missing_header(tmp_path, um_parser, um7_malformed_profiling_data_missing_header):
    """Test that UM7 parsing fails when the header is missing"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_malformed_profiling_data_missing_header)
    with pytest.raises(ValueError):
        um_parser.parse(um7_log_file)


def test_um7_parser_missing_footer(tmp_path, um_parser, um7_malformed_profiling_data_missing_footer):
    """Test that UM7 parsing fails when the footer is missing"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_malformed_profiling_data_missing_footer)
    with pytest.raises(ValueError):
        um_parser.parse(um7_log_file)


def test_um7_parser_missing_section(tmp_path, um_parser, um7_malformed_profiling_data_missing_profiling_section):
    """Test that UM7 parsing fails when the profiling section is empty (but both header and footer are present)"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_malformed_profiling_data_missing_profiling_section)
    with pytest.raises(AssertionError):
        um_parser.parse(um7_log_file)


def test_um7_parser_extra_final_column(tmp_path, um_parser, um7_malformed_data_extra_final_column):
    """Test that UM7 parsing fails when there is an extra column at the end"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_malformed_data_extra_final_column)
    with pytest.raises(AssertionError):
        um_parser.parse(um7_log_file)


def test_um7_parser_extra_middle_column(tmp_path, um_parser, um7_malformed_data_extra_middle_column):
    """Test that UM7 parsing fails when there is an extra column in the middle"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_malformed_data_extra_middle_column)
    with pytest.raises(ValueError):
        um_parser.parse(um7_log_file)


def test_um7_parser_extra_front_column_float(
    tmp_path, um_parser, um7_malformed_data_extra_front_column_with_float_data
):
    """Test that UM7 parsing fails when there is an extra column, with float values, at the beginning of a line"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_malformed_data_extra_front_column_with_float_data)
    with pytest.raises(AssertionError):
        um_parser.parse(um7_log_file)


def test_um7_parser_extra_front_column_integer(
    tmp_path, um_parser, um7_malformed_data_extra_front_column_with_integer_data, um7_parsed_profile_data
):
    """Test that UM7 parsing *works* when there is an extra column, with integer values, at the beginning of a line"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_malformed_data_extra_front_column_with_integer_data)
    stats = um_parser.parse(um7_log_file)

    # Might also be worthwhile to check that the 'region' key exists first
    assert len(stats["region"]) == len(um7_parsed_profile_data["region"]), (
        f"Number of matched regions should be *exactly* {len(um7_parsed_profile_data['region'])}"
    )

    for metric in um_parser.metrics:
        for idx, region in enumerate(stats["region"]):
            assert stats[metric][idx] == um7_parsed_profile_data[metric][idx], (
                f"Incorrect {metric} for region {region} (index: {idx})."
            )


def test_um7_parser_extra_front_column_string(
    tmp_path, um_parser, um7_malformed_data_extra_front_column_with_string_data
):
    """Test that UM7 parsing fails when there is an extra column, with string values, at the beginning of a line"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_malformed_data_extra_front_column_with_string_data)
    with pytest.raises(AssertionError):
        um_parser.parse(um7_log_file)


def test_um7_parser_malformed_columns(tmp_path, um_parser, um7_malformed_profiling_data_bad_columndata):
    """Test that UM7 parsing fails when the column data is not representable as a number"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_malformed_profiling_data_bad_columndata)
    with pytest.raises(AssertionError):
        um_parser.parse(um7_log_file)


# UM13 parsing tests below
def test_um13_parsing(tmp_path, um_parser, um13_raw_profiling_data, um13_parsed_profile_data):
    """Test that parsed UM13 profiling data *exactly* matches the known-correct profiling data"""
    um13_log_file = tmp_path / "um7.log"
    um13_log_file.write_text(um13_raw_profiling_data)
    stats = um_parser.parse(um13_log_file)

    # Might also be worthwhile to check that the 'region' key exists first
    assert len(stats["region"]) == len(um13_parsed_profile_data["region"]), (
        f"Number of matched regions should be *exactly* {len(um13_parsed_profile_data['region'])}"
    )

    for metric in um_parser.metrics:
        for idx, region in enumerate(stats["region"]):
            assert stats[metric][idx] == um13_parsed_profile_data[metric][idx], (
                f"Incorrect {metric} for region {region} (index: {idx})."
            )


@pytest.fixture(scope="module")
def um_total_runtime_raw_profiling_data():
    """Fixture with UM total runtime raw profiling data"""
    return r"""
 END OF RUN - TIMER OUTPUT
 Timer information is for whole run
 PE                      0  Elapsed CPU Time:    3943.63426200007     
 PE                      0   Elapsed Wallclock Time:    3943.80157899974     
 
 Total Elapsed CPU Time:    820297.910506003     
 Maximum Elapsed Wallclock Time:    3944.07699399998     
 Speedup:    207.982225436750     
"""


@pytest.fixture(scope="module")
def um_total_runtime_parser():
    """Fixture for the UM total runtime parser"""
    return UMTotalRuntimeParser()


def test_um_total_runtime_parsing(tmp_path, um_total_runtime_parser, um_total_runtime_raw_profiling_data):
    """Test that parsed UM total runtime data *exactly* matches the known-correct profiling data"""
    um_log_file = tmp_path / "um.log"
    um_log_file.write_text(um_total_runtime_raw_profiling_data)
    parsed_log = um_total_runtime_parser.parse(um_log_file)

    assert "um_total_walltime" in parsed_log["region"]
    assert len(parsed_log["region"]) == 1, (
        f"Incorrect number of regions. Found {len(parsed_log['region'])} instead of 1."
    )
    assert parsed_log[tmax][0] == 3944.07699399998, (
        f"Incorrect total wallclock time. Expected 3944.07699399998, got {parsed_log[tmax][0]}"
    )


def test_um_total_runtime_parsing_missing_section(tmp_path, um7_raw_profiling_data, um_total_runtime_parser):
    """Test that UM total runtime parsing fails when the max. elapsed wallclock phrase is missing"""
    um7_log_file = tmp_path / "um7.log"
    um7_log_file.write_text(um7_raw_profiling_data)
    with pytest.raises(ValueError):
        um_total_runtime_parser.parse(um7_log_file)
