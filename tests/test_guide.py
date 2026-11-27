# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import builtins
import inspect
import os
import re
import runpy
import sys
from importlib.resources import files
from pathlib import Path
from unittest import mock

import pytest
import xarray as xr

import access.profiling
from access.profiling.__main__ import main
from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.experiment import StudyPlan
from access.profiling.metrics import tmax
from access.profiling.models.om3 import OM3Configuration
from access.profiling.payu_manager import PayuManager
from access.profiling.results import ParsedExperiment, ProfilingResults

GUIDE = files("access.profiling").joinpath("guide.md").read_text(encoding="utf-8")


def code_cells():
    """The Python blocks of the guide, in order: the code cells of its notebook."""
    return re.findall(r"```python\n(.*?)```", GUIDE, flags=re.DOTALL)


def prose():
    """The guide without its code blocks."""
    return re.sub(r"```.*?```", "", GUIDE, flags=re.DOTALL)


def api_names():
    """Every name the guide could mean by an identifier: what the package exports, the members of the types a study
    calls, and the arguments of their methods."""
    names = set(dir(access.profiling)) | set(dir(builtins))
    for kind in (PayuManager, CylcRoseManager, ProfilingResults, StudyPlan, OM3Configuration):
        names |= set(dir(kind))
        for base in kind.__mro__:
            for member in vars(base).values():
                function = member.fget if isinstance(member, property) else member
                if inspect.isfunction(function):
                    names |= set(inspect.signature(function).parameters)
    # The names the guide's own notebook gives its values.
    return names | {"manager", "plan"}


def parsed(manager):
    """Results of the experiments the manager holds, as if each had run, with the ESMF regions the guide reads.

    Each time comes from the layout of its experiment, so that the regions are read against the real components of
    the application, and no two experiments tie.
    """
    experiments = {}
    for name, experiment in manager.experiments.items():
        cores = experiment.layout.n_cores
        times = [1e5 / cores, 9e4 / cores, 1e4 / cores + len(experiments)]
        regions = ["[ensemble] RunPhase1", "[OCN] RunPhase1", "[ICE] RunPhase1"]
        summary = xr.Dataset(
            {tmax: xr.DataArray(times, dims=["region"]).pint.quantify("second")}, coords={"region": regions}
        )
        experiments[name] = ParsedExperiment({"ESMF": summary}, dict(experiment.labels), experiment.layout, cores)
    return ProfilingResults(manager.application, experiments)


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
            pytest.raises(SystemExit, match="^0$") as exit_,
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
    """The code cells of the guide run, in order, with the workflow engine faked and the logs made up.

    The analysis cells run on real results, made from the layouts the guide's plan finds, so a name the guide imports,
    a method it calls, an argument it passes, or a component or a region it names, that the package or the application
    no longer has, fails here.
    """

    @mock.patch("access.profiling.payu_manager.ExperimentRunner")
    @mock.patch("access.profiling.payu_manager.ExperimentGenerator")
    def test_the_cells_run_in_order(self, mock_experiment_generator, mock_experiment_runner, tmp_path):
        namespace = {}
        environment = {name: value for name, value in os.environ.items() if name != "PAYU_LAB_PATH"}
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.object(PayuManager, "parse_profiling_data", autospec=True, side_effect=parsed),
        ):
            for cell in code_cells():
                # The directories of the guide are examples on Gadi, which the test keeps under tmp_path.
                cell = cell.replace('"/scratch/tm70/', f'"{tmp_path}/scratch/').replace(
                    '"/g/data/tm70/', f'"{tmp_path}/'
                )
                exec(compile(cell, "guide.md", "exec"), namespace)

        manager, plan = namespace["manager"], namespace["plan"]
        assert set(plan.table["nodes"]) == {1, 2, 3, 4}, "the strategy of the guide finds layouts at every size"
        assert list(manager.experiments) == [f"exp-{number:04d}" for number in range(1, len(plan.experiments) + 1)]
        mock_experiment_generator.return_value.run.assert_called_once_with()
        mock_experiment_runner.return_value.run.assert_called_once_with()
        assert namespace["ax"].get_xlabel() == "Cores of the component"
        assert len(namespace["ax"].lines) > 0

    def test_the_cells_name_no_experiment(self):
        """As the guide asks: an experiment is picked out by a coordinate, never by its name."""
        assert not any(re.search(r"exp-\d{4}", cell) for cell in code_cells())

    def test_every_name_the_prose_gives_is_one_the_package_has(self):
        """A method or an argument the guide names outside its code, such as `nruns`, is checked too."""
        known = api_names()
        unknown = set()
        for token in re.findall(r"`([^`]+)`", prose()):
            # Text that is not code: a name of an experiment, a string, a magic, a command.
            if re.search(r"[\s%-]", token) or token.startswith(('"', "'")):
                continue
            unknown |= set(re.findall(r"[A-Za-z_]\w*", re.sub(r"\"[^\"]*\"", "", token))) - known
        assert unknown == set()
