# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The configuration axis itself: how a declared log is found, and what a data-only configuration refuses.

The model configurations have their own test modules. What is tested here is the machinery they are built out
of - the log locators, LogSpec, and RoseSuiteConfiguration, which is a configuration with no code of its own.
"""

from pathlib import Path
from unittest import mock

import pytest

from access.profiling.configuration import LogSpec, RoseSuiteConfiguration, log_at, payu_model_stdout, um_stdout
from access.profiling.parser import ProfilingParser


def fake_parser() -> ProfilingParser:
    """Returns a stand-in parser.

    Resolving a log only has to carry a parser through to the log it builds, never run it, so what the parser
    reads is irrelevant here and a real one would only tie these tests to a log format.

    Returns:
        ProfilingParser: The stand-in.
    """
    return mock.Mock(spec=ProfilingParser)


def write_um_env(output_dir: Path, text: str) -> Path:
    """Writes the UM's environment file into an output directory, and returns the directory.

    Args:
        output_dir (Path): The output directory to write into.
        text (str): Contents of `atmosphere/um_env.yaml`.

    Returns:
        Path: The output directory.
    """
    (output_dir / "atmosphere").mkdir()
    (output_dir / "atmosphere" / "um_env.yaml").write_text(text)
    return output_dir


class TestLogAt:
    """Logs whose place inside an output directory the configuration simply knows."""

    def test_the_relative_path_is_joined_onto_whichever_directory_the_locator_is_given(self, tmp_path):
        """One locator is declared once and then used for every output directory of every experiment.

        If it closed over a directory instead of over the relative path, every run would be read out of the
        first directory looked at, which would silently plot one cycle's numbers for all of them.
        """
        locate = log_at("ice/ice_diag.d")

        assert locate(tmp_path / "output000") == tmp_path / "output000" / "ice" / "ice_diag.d"
        assert locate(tmp_path / "output001") == tmp_path / "output001" / "ice" / "ice_diag.d"

    def test_a_fixed_path_is_named_whether_or_not_the_file_is_there(self, tmp_path):
        """Whether the log exists is resolve's question.

        A locator that answered it too would make "this run did not write that log" indistinguishable from
        "this configuration does not say where that log is", and only the second is worth reporting.
        """
        assert log_at("access.out")(tmp_path) == tmp_path / "access.out"


class TestPayuModelStdout:
    """Payu names the model's standard output after the model it ran, so the name has to be read, not assumed."""

    def test_the_log_is_named_after_the_model_the_archived_config_records(self, tmp_path):
        """Two models archived side by side write differently named files, so the name follows the config."""

        (tmp_path / "config.yaml").write_text("model: access-om3\njobname: whatever\n")

        assert payu_model_stdout()(tmp_path) == tmp_path / "access-om3.out"

    def test_an_output_directory_holding_no_config_yaml_names_no_log(self, tmp_path):
        """A directory Payu did not archive a config into says nothing about where the log is.

        That is a missing log rather than an error: a run that produced no output at all still has to be
        skipped over rather than bring down the parse of every other run.
        """
        assert payu_model_stdout()(tmp_path) is None

    def test_a_config_yaml_stating_no_model_names_no_log(self, tmp_path):
        """Payu lets the model key be absent, and then nothing names the file, so there is nothing to read."""

        (tmp_path / "config.yaml").write_text("jobname: whatever\n")

        assert payu_model_stdout()(tmp_path) is None

    def test_a_directory_called_config_yaml_is_not_a_config_yaml(self, tmp_path):
        """Only a file can be read, so a locator testing mere existence would raise instead of declining."""

        (tmp_path / "config.yaml").mkdir()

        assert payu_model_stdout()(tmp_path) is None


class TestUMStdout:
    """The UM writes one standard output file per rank, under a name its own environment file states."""

    def test_rank_zeros_file_is_the_one_named(self, tmp_path):
        """The timings are printed by rank 0 alone, so any other rank's file would parse to nothing."""

        write_um_env(tmp_path, "UM_STDOUT_FILE: pe_output/atm.fort6.pe\n")

        assert um_stdout()(tmp_path) == tmp_path / "atmosphere" / "pe_output" / "atm.fort6.pe0"

    def test_an_output_directory_with_no_um_environment_file_names_no_log(self, tmp_path):
        """A configuration without an atmosphere archives no `um_env.yaml`, and that is not an error."""

        assert um_stdout()(tmp_path) is None

        (tmp_path / "atmosphere").mkdir()
        assert um_stdout()(tmp_path) is None, "the directory is there, the environment file is not"

    def test_a_um_environment_stating_no_stdout_file_names_no_log(self, tmp_path):
        """Nothing then says what the per-rank files are called, and guessing a name would read someone else's."""

        write_um_env(tmp_path, "UM_ATM_NPROCX: '16'\n")

        assert um_stdout()(tmp_path) is None


class TestLogSpecResolve:
    """Turning a log a configuration declares into one that is actually in a given output directory."""

    @pytest.mark.parametrize("optional", [True, False])
    def test_a_log_that_is_there_carries_the_parser_and_the_flag_it_was_declared_with(self, tmp_path, optional):
        """The spec is the only place that knows how to read the log and whether its absence is allowed.

        If resolve dropped either, every log would be read with some default parser, and a missing optional
        log would be reported as a failed run.
        """
        parser = fake_parser()
        (tmp_path / "access.out").write_text("")
        spec = LogSpec("MOM5", log_at("access.out"), parser, component="ocean", optional=optional)

        log = spec.resolve(tmp_path)

        assert log.filepath == tmp_path / "access.out"
        assert log.parser is parser
        assert log.optional is optional

    def test_a_log_whose_file_is_not_there_resolves_to_nothing(self, tmp_path):
        """A run that did not write a log is absent rather than present and unreadable."""

        spec = LogSpec("MOM5", log_at("access.out"), fake_parser())

        assert spec.resolve(tmp_path) is None

    def test_a_locator_that_names_no_file_at_all_resolves_to_nothing(self, tmp_path):
        """A locator declines when the run's own configuration files do not say where its log is.

        That is the same answer as a missing file, which is why resolve has to accept a locator returning None
        rather than join it onto the output directory.
        """
        spec = LogSpec("MOM5", lambda _: None, fake_parser())

        assert spec.resolve(tmp_path) is None

    def test_a_directory_where_the_log_should_be_is_not_a_log(self, tmp_path):
        """Handing a directory to a parser would raise, and this one is only a log that was never written."""

        (tmp_path / "access.out").mkdir()
        spec = LogSpec("MOM5", log_at("access.out"), fake_parser())

        assert spec.resolve(tmp_path) is None


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
