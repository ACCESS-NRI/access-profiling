# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


import pytest
from conftest import fake_parser

from access.profiling.rose_configuration import RoseSuiteConfiguration


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


class TestExtraCpusVariables:
    """The cores of the components a suite runs beside the atmosphere, as ACCESS-CM3 adds them to its request."""

    CONFIGURATION = RoseSuiteConfiguration(
        name="cm3",
        layout_variable=("ATM_NPROCX", "ATM_NPROCY"),
        cpus_per_proc_variable="OMPTHR",
        extra_cpus_variables=("OCN_NPROC", "ICE_NPROC"),
    )

    def test_they_are_added_after_the_threads_are_counted(self):
        rose_conf = {"ATM_NPROCX": "24", "ATM_NPROCY": "24", "OMPTHR": "2", "OCN_NPROC": "1152", "ICE_NPROC": "192"}

        assert self.CONFIGURATION.occupied_cpus(rose_conf) == 24 * 24 * 2 + 1152 + 192

    def test_a_missing_one_is_an_error(self):
        """As for the other variables: the configuration then describes a different suite from the one read."""

        rose_conf = {"ATM_NPROCX": "24", "ATM_NPROCY": "24", "OMPTHR": "2", "OCN_NPROC": "1152"}

        with pytest.raises(ValueError, match="Cannot find component cores key, ICE_NPROC"):
            self.CONFIGURATION.occupied_cpus(rose_conf)
