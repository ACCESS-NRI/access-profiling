# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


from access.profiling.payu_configuration import payu_model_stdout


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
