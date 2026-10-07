# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from unittest import mock

import pytest

from access.profiling.parser import ProfilingParser
from access.profiling.rose_configuration import RoseSuiteConfiguration


def fake_parser() -> ProfilingParser:
    """Returns a stand-in parser.

    A rose suite declares no logs in an output directory, so what the parser reads never matters here and a
    real one would only tie this test to a log format.

    Returns:
        ProfilingParser: The stand-in.
    """
    return mock.Mock(spec=ProfilingParser)


class TestRoseSuiteConfiguration:
    """A suite described by data alone: what its variables are called, and which parsers read its task logs."""

    def test_a_configuration_with_no_name_is_refused(self):
        """The name is a defaulted field only so that the dataclass satisfies the abstract property.

        That makes an unnamed configuration constructible unless `__post_init__` refuses it, and an unnamed
        suite is one that cannot be told from any other in a working directory holding several.
        """
        with pytest.raises(ValueError, match="name must be non-empty"):
            RoseSuiteConfiguration(layout_variable="um_layout")

    def test_a_configuration_naming_no_layout_variable_is_refused(self):
        """The layout variable is the whole of what the configuration contributes to counting a suite's CPUs.

        Without one, `occupied_cpus` would look up the empty string and report the suite as missing the key,
        far from the point where the configuration was built wrong.
        """
        with pytest.raises(ValueError, match="layout_variable must name at least one"):
            RoseSuiteConfiguration(name="am3")

    def test_a_rose_suite_declares_no_logs_in_an_output_directory(self, tmp_path):
        """Its logs are one per task per cycle, so they are found by walking the run directory, not named here.

        Declaring any would commit the suite to a fixed layout of files on disk that Cylc does not promise;
        the manager walks instead, using `parsers`.
        """
        configuration = RoseSuiteConfiguration(
            name="am3", layout_variable="um_layout", parsers={"UM_regions": fake_parser()}
        )
        (tmp_path / "job.out").write_text("")

        assert configuration.logs == ()
        assert configuration.component_logs(tmp_path) == {}, "even with a log file sitting right there"
