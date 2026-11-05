# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass

import pytest
from access.config.parallel_allocation_strategies import FixedAllocation, FreeAllocation, RootAllocation
from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import FixedThreadsPerRankConstraint, MaxWastedCoreFractionConstraint
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid
from conftest import fake_parser, grid_of, layout_of, select_layouts

from access.profiling.application import LogSpec, log_at
from access.profiling.payu_configuration import (
    MAX_WASTED_CORE_FRACTION,
    FixedRanksConstraint,
    PayuComponent,
    PayuSubmodel,
    PlainComponent,
    SubmodelsConfiguration,
    control_name,
    payu_model_stdout,
    read_submodels,
    require_process_grid,
)


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


def _layout(name: str, n_ranks: int, grid: tuple[int, ...] | None = None) -> ComponentLayout:
    """A component's layout, with a decomposition over the given process grid where one is given."""

    decomposition = None
    if grid is not None:
        decomposition = DomainDecompositionSpec(Domain(tuple(8 * ranks for ranks in grid)), MPICartesianGrid(grid))
    return ComponentLayout(name=name, n_cores=n_ranks, n_ranks=n_ranks, threads_per_rank=1, decomposition=decomposition)


class TestFixedRanksConstraint:
    """A rank count the model fixes, for a component the search can give nothing else."""

    def test_it_is_satisfied_by_that_rank_count_alone(self):
        constraint = FixedRanksConstraint(n_ranks=12)

        assert constraint.is_satisfied(_layout("ice", 12))
        assert not constraint.is_satisfied(_layout("ice", 6))
        assert not constraint.is_satisfied(_layout("ice", 24))

    def test_it_refuses_a_rank_count_nothing_could_run_on(self):
        with pytest.raises(ValueError, match="must be positive"):
            FixedRanksConstraint(n_ranks=0)


class TestPlainComponent:
    """A component the search chooses a rank count for and nothing else."""

    def test_it_carries_no_domain_and_writes_nothing(self):
        component = PlainComponent(name="atm", token="atm")

        assert component.domain is None
        assert component.local_constraints == ()
        assert component.logs == ()
        assert component.config_changes(_layout("atm", 24)) == {}

    def test_a_fixed_rank_count_becomes_the_one_requirement(self):
        component = PlainComponent(name="YATM", token="atm", n_ranks=1)

        assert component.local_constraints == (FixedRanksConstraint(n_ranks=1),)

    def test_it_refuses_a_pin_to_no_ranks(self):
        """Refused when the component is built, so that a configuration is, rather than when the search runs."""

        with pytest.raises(ValueError, match="must be positive"):
            PlainComponent(name="YATM", token="atm", n_ranks=0)

    def test_its_logs_are_whatever_it_is_handed(self):
        spec = LogSpec("ocean", log_at("ocean.log"), fake_parser(), component="ocn")

        assert PlainComponent(name="ocn", token="ocn", logs=(spec,)).logs == (spec,)


class TestRequireProcessGrid:
    """What config_changes needs from a layout, said in the ValueError the contract promises."""

    def test_it_returns_the_two_extents(self):
        assert require_process_grid("atm", _layout("atm", 6, (3, 2))) == (3, 2)

    def test_it_refuses_a_layout_read_back_from_a_configuration(self):
        """Such a layout states cores and not grids, and reaching through the missing decomposition would be an
        AttributeError from deep inside config_changes rather than the ValueError its docstring promises."""

        with pytest.raises(ValueError, match="states no decomposition"):
            require_process_grid("atm", _layout("atm", 6))


@dataclass(frozen=True)
class GridComponent(PayuComponent):
    """A component decomposing a grid over a process grid it writes into a file of its own, for the tests here."""

    grid: Domain
    name: str = "grid"
    token: str = "g"

    @property
    def domain(self) -> Domain:
        return self.grid

    @property
    def local_constraints(self) -> tuple:
        return ()

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        return (LogSpec("grid", log_at("grid.log"), fake_parser(), component=self.name),)

    def config_changes(self, layout: ComponentLayout) -> dict:
        nx, ny = require_process_grid(self.name, layout)
        return {"grid.nml": {"grid": f"{nx}, {ny}"}}


@dataclass(frozen=True, kw_only=True)
class ToyConfiguration(SubmodelsConfiguration):
    """A model of a gridded component, a serial one and a coupler, as a control might declare them."""

    name: str
    grid: Domain | None = Domain((8, 8))
    serial: bool = True
    model_type: str = "toy"
    model_name: str = "Toy"
    experiment_prefix: str = "toy-layout"

    @property
    def submodels(self) -> tuple[PayuSubmodel, ...]:
        submodels = []
        if self.grid is not None:
            submodels.append(PayuSubmodel("grid", "model", GridComponent(self.grid), exe="grid.exe"))
        if self.serial:
            submodels.append(PayuSubmodel("serial", "model", PlainComponent(name="serial", token="s", n_ranks=1)))
        submodels.append(PayuSubmodel("coupler", "oasis"))
        return tuple(submodels)


TOY = ToyConfiguration(name="demo")
TOY_ALLOCATIONS = RootAllocation(subcomponents={"grid": FixedAllocation(4), "serial": FixedAllocation(1)})


def _toy_layout(grid: tuple[int, int]) -> ComponentLayout:
    """The layout of the toy on five cores whose gridded component is split over the given process grid."""

    layouts = select_layouts(TOY, 5, allocations=TOY_ALLOCATIONS)
    picked = [layout for layout in layouts if grid_of(layout.sub_layouts[0]) == grid]
    assert len(picked) == 1, f"the search should find the {grid} process grid exactly once"
    return picked[0]


class TestSubmodelsConfiguration:
    """A Payu model whose components run side by side, each a submodel of config.yaml."""

    def test_its_tree_is_its_components_side_by_side(self):
        tree = TOY.parallel_component

        assert tree.name == "Toy demo"
        assert [component.name for component in tree.subcomponents] == ["grid", "serial"]
        assert tree.subcomponents[0].domain == Domain((8, 8))
        assert tree.subcomponents[1].domain is None
        assert tree.local_constraints == (MaxWastedCoreFractionConstraint(max_fraction=MAX_WASTED_CORE_FRACTION),)

    def test_every_component_runs_one_thread_per_rank(self):
        """Which is what lets parse_layout read cores back as ranks, so it is the tree's to require."""

        for component in TOY.parallel_component.subcomponents:
            assert component.local_constraints[0] == FixedThreadsPerRankConstraint(n_threads=1)

    def test_a_fixed_rank_count_holds_whatever_the_strategy_allows(self):
        """The model's requirement, not the study's: a band around it still yields that count alone.

        Of the splits of six cores the bands admit, four and two would be the only one to idle nothing but for
        the pin; with it, five and one is the one found.
        """

        allocations = RootAllocation(
            subcomponents={
                "grid": FreeAllocation(min_cores=4, max_cores=5),
                "serial": FreeAllocation(min_cores=1, max_cores=2),
            }
        )
        layouts = select_layouts(TOY, 6, allocations=allocations)

        assert layouts
        assert {(layout.sub_layouts[0].n_cores, layout.sub_layouts[1].n_ranks) for layout in layouts} == {(5, 1)}

    def test_its_logs_are_its_components_logs(self):
        assert [spec.name for spec in TOY.logs] == ["grid"]
        assert TOY.component_for_log("grid") == "grid"
        assert TOY.component_for_log("serial") is None, "a component writing no log"

    def test_an_experiment_is_named_after_what_each_component_received(self):
        """A process grid where a component decomposes one, and a rank count otherwise."""

        assert TOY.experiment_name(_toy_layout((2, 2))) == "toy-layout_demo_g_2x2_s_1"
        assert TOY.experiment_name(_toy_layout((4, 1))) == "toy-layout_demo_g_4x1_s_1"

    def test_it_writes_a_slot_for_every_submodel_the_control_declares(self):
        """The coupler's slot is marked PRESERVE rather than left out, since the generator truncates."""

        changes = TOY.config_changes(_toy_layout((2, 2)))

        assert changes == {
            "config.yaml": {"ncpureq": 5, "submodels": [[{"ncpus": 4, "exe": ["grid.exe"]}, {"ncpus": 1}, "PRESERVE"]]},
            "grid.nml": {"grid": "2, 2"},
        }

    def test_it_refuses_a_layout_of_another_model(self):
        other = ComponentLayout(
            name="other-model",
            n_cores=4,
            n_ranks=4,
            threads_per_rank=None,
            decomposition=None,
            sub_layouts=(_layout("other", 4, (2, 2)),),
        )
        with pytest.raises(ValueError, match="runs 2"):
            TOY.experiment_name(other)
        with pytest.raises(ValueError, match="runs 2"):
            TOY.config_changes(other)

    def test_it_needs_a_name_and_a_component(self):
        with pytest.raises(ValueError, match="name must be non-empty"):
            ToyConfiguration(name="")
        with pytest.raises(ValueError, match="runs no component"):
            ToyConfiguration(name="empty", grid=None, serial=False)

    def test_it_reads_the_cores_of_every_submodel_it_declares(self, tmp_path):
        (tmp_path / "config.yaml").write_text(
            "model: toy\nsubmodels:\n  - name: grid\n    ncpus: 4\n  - name: serial\n    ncpus: 1\n"
        )

        layout = layout_of(TOY.parse_layout(tmp_path))

        assert layout.name == "Toy demo"
        assert [(sub.name, sub.n_cores, sub.n_ranks, sub.threads_per_rank) for sub in layout.sub_layouts] == [
            ("grid", 4, 4, 1),
            ("serial", 1, 1, 1),
        ]
        assert layout.n_cores == 5 and layout.idle_cores == 0

    def test_a_submodel_it_does_not_declare_or_one_stating_no_cores_is_left_out(self, tmp_path):
        (tmp_path / "config.yaml").write_text(
            "model: toy\nsubmodels:\n  - name: grid\n    ncpus: 4\n  - name: serial\n  - name: other\n    ncpus: 8\n"
        )

        assert [sub.name for sub in layout_of(TOY.parse_layout(tmp_path)).sub_layouts] == ["grid"]

    def test_a_control_naming_no_component_of_it_states_no_layout(self, tmp_path):
        (tmp_path / "config.yaml").write_text("model: toy\nsubmodels:\n  - name: other\n    ncpus: 8\n")
        assert TOY.parse_layout(tmp_path) is None

        (tmp_path / "config.yaml").write_text("model: toy\n")
        assert TOY.parse_layout(tmp_path) is None


class TestReadingAControl:
    """What the readers of the models take from a control's config.yaml."""

    CONFIG = (
        "jobname: piControl\nmodel: access-esm1.6\nsubmodels:\n"
        "  - name: atmosphere\n    model: um\n    ncpus: 256\n    exe: um_hg3.exe\n"
        "  - name: coupler\n    model: oasis\n"
    )

    def test_the_submodels_come_back_in_order_with_their_types(self, tmp_path):
        (tmp_path / "config.yaml").write_text(self.CONFIG)

        submodels = read_submodels(tmp_path)

        assert [(s.name, s.model, s.ncpus, s.exe) for s in submodels] == [
            ("atmosphere", "um", 256, "um_hg3.exe"),
            ("coupler", "oasis", None, None),
        ]

    def test_a_control_without_a_config_yaml_or_without_submodels_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="has no config.yaml"):
            read_submodels(tmp_path)

        (tmp_path / "config.yaml").write_text("model: access-om3\n")
        with pytest.raises(ValueError, match="declares no submodels"):
            read_submodels(tmp_path)

    def test_a_submodel_without_a_name_or_a_model_is_refused(self, tmp_path):
        (tmp_path / "config.yaml").write_text("submodels:\n  - name: atmosphere\n    ncpus: 256\n")

        with pytest.raises(ValueError, match="states no name or no model"):
            read_submodels(tmp_path)

    def test_the_name_is_the_one_given_or_else_the_jobname(self, tmp_path):
        (tmp_path / "config.yaml").write_text(self.CONFIG)

        assert control_name(tmp_path, "historical") == "historical"
        assert control_name(tmp_path) == "piControl"

    def test_a_control_without_a_jobname_needs_a_name(self, tmp_path):
        (tmp_path / "config.yaml").write_text("model: access-om3\n")

        with pytest.raises(ValueError, match="states no jobname"):
            control_name(tmp_path)
