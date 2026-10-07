# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The configuration axis itself: how a declared log is found, and what a data-only configuration refuses.

The model configurations have their own test modules. What is tested here is the machinery they are built out
of - the log locators, LogSpec, and RoseSuiteConfiguration, which is a configuration with no code of its own.
"""

from unittest import mock

import pytest

from access.profiling.application import LogSpec, log_at
from access.profiling.parser import ProfilingParser


def fake_parser() -> ProfilingParser:
    """Returns a stand-in parser.

    Resolving a log only has to carry a parser through to the log it builds, never run it, so what the parser
    reads is irrelevant here and a real one would only tie these tests to a log format.

    Returns:
        ProfilingParser: The stand-in.
    """
    return mock.Mock(spec=ProfilingParser)


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
