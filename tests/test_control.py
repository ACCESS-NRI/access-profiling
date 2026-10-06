# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import dataclasses
from pathlib import Path

import pytest

from access.profiling.control import ControlSource, ExistingDirectoryControlSource, GitControlSource

ESM16_CONFIGS = "git@github.com:ACCESS-NRI/access-esm1.6-configs.git"
PI_CONTROL_RELEASE = "release-piControl-2.1"


class TestTheSourcesAreControls:
    """The two members exist to be handed to a manager as a ControlSource, so the contract has to hold."""

    def test_the_base_class_cannot_be_instantiated(self):
        """It records nothing itself - a source that answered none of the four coordinates would leave a
        runner with no repository to clone and a study with nothing to say it ran against."""

        with pytest.raises(TypeError, match="abstract"):
            ControlSource()

    def test_both_sources_are_control_sources(self, tmp_path):
        assert isinstance(GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE), ControlSource)
        assert isinstance(ExistingDirectoryControlSource(tmp_path), ControlSource)


class TestAGitControlNeedsBothCoordinates:
    """A control is a particular state of a particular repository, so half of that is not a control at all."""

    def test_a_source_with_no_repository_is_refused(self):
        with pytest.raises(ValueError, match="repository must be non-empty"):
            GitControlSource("", PI_CONTROL_RELEASE)

    def test_a_source_with_an_empty_start_point_is_refused(self):
        with pytest.raises(ValueError, match="start_point must be non-empty"):
            GitControlSource(ESM16_CONFIGS, "")

    def test_omitting_the_start_point_is_refused_although_the_field_has_a_default(self):
        """The empty default is there only so the field overrides the abstract start_point of the base class.
        Were it taken at face value, a study would silently profile whatever the repository's default branch
        happened to be on the day it ran, rather than the release it meant."""

        with pytest.raises(ValueError, match="start_point must be non-empty"):
            GitControlSource(ESM16_CONFIGS)

    def test_a_source_with_both_coordinates_is_accepted(self):
        """The guard rejects what is missing and nothing else."""

        assert GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE).start_point == PI_CONTROL_RELEASE


class TestWhatAGitControlAnswersTo:
    """What a runner that clones the control itself is given, and what a study says it was run against."""

    def test_the_origin_is_the_repository_to_clone(self):
        assert GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE).origin == ESM16_CONFIGS

    def test_the_start_point_is_the_revision_the_experiments_branch_from(self):
        assert GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE).start_point == PI_CONTROL_RELEASE

    def test_the_directory_defaults_to_config(self):
        """Which is the name Payu-driven studies clone into, and what the generated experiments refer to."""

        assert GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE).directory == "config"

    def test_the_directory_can_be_overridden(self):
        control = GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE, _directory="am3-suite")

        assert control.directory == "am3-suite"

    def test_the_label_defaults_to_the_start_point(self):
        """For a released configuration the start point is the release, which is what a plot should say."""

        assert GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE).label == PI_CONTROL_RELEASE

    def test_the_label_can_be_overridden(self):
        """A commit hash makes a poor axis label, so a study may give the control a name of its own."""

        control = GitControlSource(ESM16_CONFIGS, "9a3f21c", _label="pre-release fix")

        assert control.label == "pre-release fix"
        assert control.start_point == "9a3f21c", "the override renames the control, it does not move it"

    def test_an_empty_label_is_kept_rather_than_replaced_by_the_start_point(self):
        """Only an unset label falls back - a caller that asked for no label gets no label."""

        assert GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE, _label="").label == ""

    def test_the_provenance_is_the_two_coordinates_and_nothing_else(self):
        """It is written beside the results, so it has to be enough to clone the same control again."""

        control = GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE, _directory="config", _label="piControl")

        assert control.provenance() == {"repository": ESM16_CONFIGS, "start_point": PI_CONTROL_RELEASE}


class TestAnExistingDirectoryMustBeFoundAgain:
    """The control outlives the working directory a study runs in, so where it is cannot be ambiguous."""

    def test_a_relative_path_is_refused(self):
        with pytest.raises(ValueError, match="path must be absolute"):
            ExistingDirectoryControlSource(Path("roses/u-dg768"))

    def test_the_refusal_names_the_path_and_says_why_it_cannot_be_used(self):
        """A bare "must be absolute" leaves the reader guessing which of several controls was the bad one."""

        with pytest.raises(ValueError) as refusal:
            ExistingDirectoryControlSource(Path("roses/u-dg768"))

        assert "'roses/u-dg768'" in str(refusal.value)
        assert "different directories" in str(refusal.value)

    def test_an_absolute_path_is_accepted(self, tmp_path):
        """The guard rejects relative paths and nothing else - an existing checkout is the normal case."""

        suite = tmp_path / "u-dg768"
        suite.mkdir()

        assert ExistingDirectoryControlSource(suite).origin == str(suite)


class TestWhatAnExistingDirectoryAnswersTo:
    """ACCESS-rAM3 works this way: `rosie checkout` has already run, and the suite is simply there."""

    def test_the_directory_is_the_name_the_checkout_already_has(self, tmp_path):
        """Nothing clones it into place, so the name is whatever is on disk rather than a name we choose."""

        suite = tmp_path / "u-dg768"
        suite.mkdir()

        assert ExistingDirectoryControlSource(suite).directory == "u-dg768"

    def test_the_origin_is_the_path_as_a_string(self, tmp_path):
        """A runner is given a string here whichever source it was handed, so a Path would not do."""

        origin = ExistingDirectoryControlSource(tmp_path / "u-dg768").origin

        assert origin == str(tmp_path / "u-dg768")
        assert isinstance(origin, str)

    def test_the_label_defaults_to_the_directory_name(self, tmp_path):
        assert ExistingDirectoryControlSource(tmp_path / "u-dg768").label == "u-dg768"

    def test_the_label_can_be_overridden(self, tmp_path):
        """A suite id says nothing to a reader of the plot, so a study may name it after the science."""

        control = ExistingDirectoryControlSource(tmp_path / "u-dg768", _label="rAM3 trial")

        assert control.label == "rAM3 trial"
        assert control.directory == "u-dg768", "the override renames the control, it does not move it"

    def test_a_known_revision_is_the_start_point(self, tmp_path):
        """A runner that has to start branches from somewhere is given this."""

        assert ExistingDirectoryControlSource(tmp_path / "u-dg768", revision="1234").start_point == "1234"

    def test_a_directory_of_unknown_revision_states_no_start_point(self, tmp_path):
        """None means the directory is taken as it stands, which a runner has to be able to tell apart from
        a revision, rather than being handed an empty string it would try to check out."""

        assert ExistingDirectoryControlSource(tmp_path / "u-dg768").start_point is None

    def test_the_provenance_records_the_revision_when_it_is_known(self, tmp_path):
        control = ExistingDirectoryControlSource(tmp_path / "u-dg768", revision="1234")

        assert control.provenance() == {"path": str(tmp_path / "u-dg768"), "revision": "1234"}

    def test_the_provenance_leaves_the_revision_out_when_it_is_not(self, tmp_path):
        """Recording "revision": None would claim a revision was looked up and found to be nothing, when in
        fact none was ever known."""

        assert ExistingDirectoryControlSource(tmp_path / "u-dg768").provenance() == {"path": str(tmp_path / "u-dg768")}


class TestBothSourcesAreValues:
    """Both are frozen dataclasses, so the same control handed to two managers compares equal and hashes
    together - which is what lets results be grouped by what they were run against."""

    def test_two_git_controls_with_the_same_coordinates_are_interchangeable(self):
        one = GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE)
        other = GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE)

        assert one == other
        assert len({one, other}) == 1, "and so group as one control"
        assert one != GitControlSource(ESM16_CONFIGS, "release-piControl-2.0")

    def test_two_existing_directory_controls_compare_by_value(self, tmp_path):
        one = ExistingDirectoryControlSource(tmp_path / "u-dg768", revision="1234")
        other = ExistingDirectoryControlSource(tmp_path / "u-dg768", revision="1234")

        assert one == other
        assert len({one, other}) == 1
        assert one != ExistingDirectoryControlSource(tmp_path / "u-dg768", revision="1235")

    def test_a_git_control_cannot_be_moved_after_a_study_has_been_built_on_it(self):
        """Experiments are generated from the control, so changing it afterwards would leave the manager
        describing a control the experiments on disk never came from."""

        control = GitControlSource(ESM16_CONFIGS, PI_CONTROL_RELEASE)

        with pytest.raises(dataclasses.FrozenInstanceError):
            control.start_point = "release-piControl-2.0"

    def test_an_existing_directory_control_cannot_be_moved_either(self, tmp_path):
        control = ExistingDirectoryControlSource(tmp_path / "u-dg768")

        with pytest.raises(dataclasses.FrozenInstanceError):
            control.path = tmp_path / "u-dg769"
