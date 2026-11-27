# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
import re
import runpy
import sys
from importlib.resources import files
from pathlib import Path
from unittest import mock

import pytest
from matplotlib.axes import Axes

from access.profiling.__main__ import main
from access.profiling.payu_manager import PayuManager
from access.profiling.results import ProfilingResults

GUIDE = files("access.profiling").joinpath("guide.md").read_text()


def code_cells():
    """The Python blocks of the guide, in order: the code cells of its notebook."""
    return re.findall(r"```python\n(.*?)```", GUIDE, flags=re.DOTALL)


class TestPrintingTheGuide:
    """`python -m access.profiling guide` prints the guide that ships with the package."""

    def test_the_command_prints_the_guide(self, capsys):
        assert main(["guide"]) == 0

        assert capsys.readouterr().out == GUIDE
        assert GUIDE.startswith("# Writing a study notebook\n")

    @pytest.mark.parametrize("arguments", [[], ["help"], ["guide", "again"]])
    def test_any_other_command_prints_the_usage(self, capsys, arguments):
        assert main(arguments) == 2

        captured = capsys.readouterr()
        assert (captured.out, captured.err) == ("", "Usage: python -m access.profiling guide\n")

    def test_the_module_runs_as_a_program(self, capsys):
        with (
            mock.patch.object(sys, "argv", ["access.profiling", "guide"]),
            mock.patch.dict(sys.modules),
            pytest.raises(SystemExit) as exit_,
        ):
            # As `python -m` runs it: the module this test imported is not the one run.
            del sys.modules["access.profiling.__main__"]
            runpy.run_module("access.profiling", run_name="__main__")

        assert exit_.value.code == 0
        assert capsys.readouterr().out == GUIDE

    def test_the_documentation_includes_it(self):
        """The page of the user guide is the file itself, so that the two cannot say different things."""
        page = Path(__file__).parents[1] / "documentation/docs/pages/user-guide/writing-a-notebook.md"

        assert 'include-markdown "../../../../src/access/profiling/guide.md"' in page.read_text()


class TestTheNotebookOfTheGuide:
    """The code cells of the guide run, in order, with the workflow engine faked and the logs not parsed.

    So a name the guide imports, a method it calls, or an argument it passes that the package no longer has fails
    here. The parsed results are an autospec of `ProfilingResults`, which checks each call against the signature of
    the method, and a selection returns the same results.
    """

    @pytest.fixture
    def results(self):
        results = mock.create_autospec(ProfilingResults, instance=True)
        results.select_best_experiments.return_value = results
        results.plot_scaling_data.return_value = (
            mock.create_autospec(Axes, instance=True),
            mock.create_autospec(Axes, instance=True),
        )
        results.plot_component_scaling_data.return_value = mock.create_autospec(Axes, instance=True)
        return results

    @mock.patch("access.profiling.payu_manager.ExperimentRunner")
    @mock.patch("access.profiling.payu_manager.ExperimentGenerator")
    def test_the_cells_run_in_order(self, mock_experiment_generator, mock_experiment_runner, tmp_path, results):
        namespace = {}
        environment = {name: value for name, value in os.environ.items() if name != "PAYU_LAB_PATH"}
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.object(PayuManager, "parse_profiling_data", return_value=results),
        ):
            for cell in code_cells():
                # The directories of the guide are examples on Gadi, which the test keeps under tmp_path.
                cell = cell.replace('"/scratch/tm70/', f'"{tmp_path}/scratch/').replace(
                    '"/g/data/tm70/', f'"{tmp_path}/'
                )
                exec(compile(cell, "guide.md", "exec"), namespace)

        manager, plan = namespace["manager"], namespace["plan"]
        assert len(plan.experiments) > 0, "the strategy of the guide finds layouts at every size"
        assert list(manager.experiments) == [f"exp-{number:04d}" for number in range(1, len(plan.experiments) + 1)]
        mock_experiment_generator.return_value.run.assert_called_once_with()
        mock_experiment_runner.return_value.run.assert_called_once_with()
        results.table.assert_called_once()
        namespace["ax"].set.assert_called_once()

    def test_the_cells_name_no_experiment(self):
        """As the guide asks: an experiment is picked out by a coordinate, never by its name."""
        assert not any(re.search(r"exp-\d{4}", cell) for cell in code_cells())
