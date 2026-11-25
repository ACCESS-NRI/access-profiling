# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest
from access.config.parallel_allocation_strategies import FreeAllocation, RootAllocation
from access.config.parallel_component import ComponentLayout, CoreSharing
from access.config.parallel_constraints import FixedThreadsPerRankConstraint, ProcessGridDimEvenConstraint
from access.config.parallel_domain import Domain
from access.config.parallel_layouts import iter_layouts
from conftest import fake_parser, write_suite_conf

from access.profiling.rose_configuration import (
    RoseProcessGrid,
    RoseRanks,
    RoseSuiteConfiguration,
    read_section,
)

UM = RoseProcessGrid(
    name="UM",
    token="atm",
    variables=("PROCX", "PROCY"),
    grid=Domain((12, 8)),
    threads_variable="OMPTHR",
    io_server_variable="IOS",
    local_constraints=(ProcessGridDimEvenConstraint(dim=0),),
)
OCEAN = RoseRanks(name="ocean", token="ocn", variable="OCN_NPROC")
SUITE = RoseSuiteConfiguration(name="mock", model_name="Mock", components=(UM, OCEAN))


def searched(configuration, total_cores, thread_range=(1, 1)):
    """Returns the layouts of a configuration at a size, each component free to take any share."""
    allocations = RootAllocation(
        thread_range=thread_range,
        subcomponents={component.name: FreeAllocation() for component in configuration.components},
    )
    return list(iter_layouts(configuration.parallel_component, total_cores, allocations=allocations))


def sub_layout(layout, name):
    """Returns what a layout gave the named component."""
    return next(sub for sub in layout.sub_layouts if sub.name == name)


class TestReadSection:
    """The settings of one section of a Rose configuration file that Rose uses."""

    def test_the_settings_in_use_are_returned(self, tmp_path):
        """An ignored setting is one the suite does not run with, and an empty one reads as Rose reads it."""

        conf = tmp_path / "rose-suite.conf"
        conf.write_text(
            "root-dir=*=/scratch\n[jinja2:suite.rc]\nPROCX=6\n!!IOS=48\n!OMPTHR=2\nEMPTY=\n[env]\nPROCY=4\n"
        )

        assert read_section(conf, "jinja2:suite.rc") == {"PROCX": "6", "EMPTY": ""}

    @pytest.mark.parametrize("text", ["[env]\nPROCX=6\n", "[!jinja2:suite.rc]\nPROCX=6\n"], ids=["absent", "ignored"])
    def test_a_section_not_in_use_is_refused(self, tmp_path, text):
        conf = tmp_path / "rose-suite.conf"
        conf.write_text(text)

        with pytest.raises(ValueError, match=r"has no section \[jinja2:suite.rc\] in use"):
            read_section(conf, "jinja2:suite.rc")

    def test_a_file_that_is_not_there_is_refused(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="rose-suite.conf"):
            read_section(tmp_path / "rose-suite.conf", "jinja2:suite.rc")

    def test_a_setting_both_in_use_and_ignored_is_refused(self, tmp_path):
        """Rose uses whichever is written last, which the two keys the parser reads them as do not say.

        Taking the one in use would count the 48 ranks of an I/O server that Rose ignores, here.
        """
        conf = tmp_path / "rose-suite.conf"
        conf.write_text("[jinja2:suite.rc]\nIOS=48\n!!IOS=0\n!OMPTHR=2\n!!OMPTHR=1\n")

        with pytest.raises(ValueError, match=r"holds \['IOS'\] both in use and ignored"):
            read_section(conf, "jinja2:suite.rc")

    def test_a_setting_ignored_twice_is_left_out(self, tmp_path):
        """Both forms are ignored, so Rose uses neither, whichever is written last."""
        conf = tmp_path / "rose-suite.conf"
        conf.write_text("[jinja2:suite.rc]\nPROCX=6\n!OMPTHR=2\n!!OMPTHR=1\n")

        assert read_section(conf, "jinja2:suite.rc") == {"PROCX": "6"}


class TestRoseProcessGrid:
    """A code decomposing a two-dimensional grid over the process grid the suite's variables state."""

    @pytest.fixture()
    def layout(self):
        """What a search gives the UM on 16 cores with two threads a rank: a 4 by 2 process grid."""
        layouts = searched(RoseSuiteConfiguration(name="atm", model_name="Mock", components=(UM,)), 16, (2, 2))
        return next(
            sub_layout(layout, "UM")
            for layout in layouts
            if tuple(sub_layout(layout, "UM").decomposition.grid) == (4, 2)
        )

    def test_two_variables_are_written_and_read_back(self, layout):
        changes = UM.variable_changes(layout)

        assert changes == {"PROCX": "4", "PROCY": "2", "OMPTHR": "2"}
        assert UM.read_layout(changes, "the file") == layout

    def test_one_variable_holds_both_extents(self, layout):
        nest = RoseProcessGrid(
            name="UM", token="atm", variables=("NPROC",), grid=Domain((12, 8)), threads_variable="OMPTHR"
        )

        changes = nest.variable_changes(layout)

        assert changes == {"NPROC": "4,2", "OMPTHR": "2"}
        assert nest.read_layout(changes, "the file") == layout

    def test_the_io_server_is_counted_and_never_written(self, layout):
        """The I/O server runs beside the process grid, with as many threads a rank, and the search never chooses it."""

        assert "IOS" not in UM.variable_changes(layout)
        assert UM.occupied_cpus({"PROCX": "4", "PROCY": "2", "OMPTHR": "2", "IOS": "3"}, "the file") == (8 + 3) * 2

    def test_a_suite_stating_no_threads_runs_one_a_rank(self):
        """So its leaf runs one, and a layout with more is refused rather than written as if it ran one."""

        plain = RoseProcessGrid(name="UM", token="atm", variables=("NPROC",), grid=Domain((12, 8)))

        assert isinstance(plain.leaf.local_constraints[0], FixedThreadsPerRankConstraint)
        assert UM.leaf.local_constraints == UM.local_constraints, "the search chooses them where the suite states them"
        assert plain.occupied_cpus({"NPROC": "4,2"}, "the file") == 8

    def test_a_layout_with_threads_the_suite_cannot_state_is_refused(self, layout):
        plain = RoseProcessGrid(name="UM", token="atm", variables=("NPROC",), grid=Domain((12, 8)))

        with pytest.raises(ValueError, match="runs 2 threads per rank, but the suite states no threads"):
            plain.variable_changes(layout)

    def test_without_a_grid_it_is_read_but_not_searched(self):
        unknown = RoseProcessGrid(name="UM", token="atm", variables=("PROCX", "PROCY"))

        with pytest.raises(ValueError, match="UM states no grid, so its process grids cannot be searched for"):
            _ = unknown.leaf
        layout = unknown.read_layout({"PROCX": "4", "PROCY": "2"}, "the file")
        assert (layout.n_cores, layout.n_ranks, layout.decomposition) == (8, 8, None)
        with pytest.raises(ValueError, match="states no decomposition"):
            unknown.variable_changes(layout)

    @pytest.mark.parametrize(
        ("variables", "message"),
        [
            ({"PROCX": "4", "OMPTHR": "1"}, "Cannot find the variable PROCY in the file"),
            ({"PROCX": "4", "PROCY": "two", "OMPTHR": "1"}, "PROCY of the file is 'two', which is not 1 whole number"),
            ({"PROCX": "4,2", "PROCY": "2", "OMPTHR": "1"}, "PROCX of the file is '4,2', which is not 1 whole number"),
        ],
        ids=["missing", "not a number", "two numbers"],
    )
    def test_a_variable_that_does_not_say_is_refused(self, variables, message):
        """A configuration naming a variable the file does not hold describes another suite than the one read."""

        with pytest.raises(ValueError, match=message):
            UM.read_layout(variables, "the file")

    def test_one_variable_holds_two_numbers(self):
        nest = RoseProcessGrid(name="UM", token="atm", variables=("NPROC",), grid=Domain((12, 8)))

        with pytest.raises(ValueError, match="NPROC of the file is '8', which is not 2 whole number"):
            nest.read_layout({"NPROC": "8"}, "the file")

    @pytest.mark.parametrize("variables", [(), ("X", "Y", "Z")])
    def test_it_names_one_or_two_variables(self, variables):
        with pytest.raises(ValueError, match="Name two variables, one for each extent, or one variable stating both"):
            RoseProcessGrid(name="UM", token="atm", variables=variables)

    def test_its_grid_is_two_dimensional(self):
        with pytest.raises(ValueError, match="UM is given a 3-dimensional grid"):
            RoseProcessGrid(name="UM", token="atm", variables=("NPROC",), grid=Domain((12, 8, 4)))


class TestRoseRanks:
    """A code given the number of ranks one variable states, and nothing else."""

    def test_its_ranks_are_written_and_read_back(self):
        layout = ComponentLayout(name="ocean", n_cores=6, n_ranks=6, threads_per_rank=1, decomposition=None)

        assert OCEAN.variable_changes(layout) == {"OCN_NPROC": "6"}
        assert OCEAN.read_layout({"OCN_NPROC": "6"}, "the file") == layout
        assert OCEAN.occupied_cpus({"OCN_NPROC": "6"}, "the file") == 6

    def test_no_ranks_is_not_running(self):
        """A layout cannot give a component no cores, so it is left out of the layout, and counts none."""

        assert OCEAN.read_layout({"OCN_NPROC": "0"}, "the file") is None
        assert OCEAN.occupied_cpus({"OCN_NPROC": "0"}, "the file") == 0

    def test_its_leaf_runs_one_thread_a_rank_over_no_grid(self):
        leaf = OCEAN.leaf

        assert leaf.domain is None
        assert leaf.local_constraints == (FixedThreadsPerRankConstraint(n_threads=1),)


class TestRoseSuiteConfiguration:
    """A suite put together from components, which says how the variables of its `rose-suite.conf` are read and
    written."""

    def test_a_configuration_with_no_name_is_refused(self):
        """An unnamed suite cannot be told from another in a working directory holding several."""

        with pytest.raises(ValueError, match="name must be non-empty"):
            RoseSuiteConfiguration(name="", model_name="Mock", components=(UM,))

    def test_a_configuration_running_no_component_is_refused(self):
        with pytest.raises(ValueError, match="Mock configuration 'mock' runs no component"):
            RoseSuiteConfiguration(name="mock", model_name="Mock", components=())

    @pytest.mark.parametrize(
        "components",
        [
            (
                RoseProcessGrid(name="outer", token="rs01", variables=("RS01",), threads_variable="OMPTHR"),
                RoseProcessGrid(name="inner", token="rs02", variables=("RS02",), threads_variable="OMPTHR"),
            ),
            (RoseProcessGrid(name="UM", token="atm", variables=("NPROC",), io_server_variable="NPROC"),),
            (UM, RoseRanks(name="ice", token="ice", variable="OMPTHR")),
        ],
        ids=["two components", "one component", "a process grid and a rank count"],
    )
    def test_a_variable_read_for_two_settings_is_refused(self, components):
        """Two values written into one variable would overwrite each other, and the layout read back would differ.

        Two nests sharing a threads variable, given two and one threads, would both run one.
        """
        with pytest.raises(ValueError, match=r"reads the variables \['\w+'\] for more than one setting"):
            RoseSuiteConfiguration(name="mock", model_name="Mock", components=components)

    def test_a_rose_suite_declares_no_logs_in_an_output_directory(self, tmp_path):
        """Its logs are one per task per cycle, so they are found by walking the run directory, not named here."""

        configuration = RoseSuiteConfiguration(
            name="am3", model_name="Mock", components=(UM,), parsers={"UM_regions": fake_parser()}
        )
        (tmp_path / "job.out").write_text("")

        assert configuration.logs == ()
        assert configuration.component_logs(tmp_path) == {}, "even with a log file sitting right there"

    def test_the_tree_is_named_after_the_model_and_the_configuration(self):
        shared = RoseSuiteConfiguration(
            name="nests", model_name="Mock", components=(UM,), core_sharing=CoreSharing.SHARED
        )

        assert SUITE.parallel_component.name == "Mock mock"
        assert [component.name for component in SUITE.parallel_component.subcomponents] == ["UM", "ocean"]
        assert SUITE.parallel_component.core_sharing is CoreSharing.PARTITIONED
        assert shared.parallel_component.core_sharing is CoreSharing.SHARED

    def test_a_key_says_what_each_component_was_given(self):
        layout = next(
            layout
            for layout in searched(SUITE, 12, (1, 2))
            if sub_layout(layout, "UM").threads_per_rank == 2
            and sub_layout(layout, "UM").n_ranks == 4
            and sub_layout(layout, "ocean").n_ranks == 4
        )
        um_grid = "x".join(map(str, sub_layout(layout, "UM").decomposition.grid))

        assert SUITE.layout_key(layout) == f"rose-layout_mock_atm_{um_grid}_t2_ocn_4"

    def test_every_layout_has_a_key_of_its_own(self):
        """Over every size, the threads included: a key two layouts shared would make one of them never run."""

        layouts = {
            layout.sub_layouts: SUITE.layout_key(layout)
            for total_cores in range(2, 17)
            for layout in searched(SUITE, total_cores, (1, 2))
        }

        assert len(set(layouts.values())) == len(layouts)

    def test_a_layout_of_another_configuration_is_refused(self):
        other = RoseSuiteConfiguration(name="other", model_name="Mock", components=(UM,))
        (layout, *_) = searched(other, 8)

        with pytest.raises(
            ValueError, match=r"gives cores to \['UM'\], but Mock configuration 'mock' runs \['UM', 'ocean'\]"
        ):
            SUITE.layout_key(layout)
        with pytest.raises(ValueError, match="gives cores to"):
            SUITE.config_changes(layout)

    def test_a_written_layout_is_read_back(self, tmp_path):
        """config_changes and parse_layout are inverses over the same variables, the grids included."""

        for layout in searched(SUITE, 12, (1, 2)):
            if layout.idle_cores:
                continue  # what the root held beyond what it gave is not in the file
            changes = SUITE.config_changes(layout)
            assert list(changes) == ["rose-suite.conf"] and list(changes["rose-suite.conf"]) == ["jinja2:suite.rc"]
            write_suite_conf(tmp_path, {**changes["rose-suite.conf"]["jinja2:suite.rc"], "IOS": "0"})

            assert SUITE.parse_layout(tmp_path) == layout

    def test_components_side_by_side_add_up(self, tmp_path):
        conf = write_suite_conf(tmp_path, {"PROCX": "4", "PROCY": "2", "OMPTHR": "2", "IOS": "1", "OCN_NPROC": "6"})

        assert SUITE.occupied_cpus(conf) == (8 + 1) * 2 + 6
        layout = SUITE.parse_layout(tmp_path)
        assert (layout.n_cores, layout.n_ranks) == (16 + 6, 8 + 6), "the I/O server is not part of the layout"

    def test_components_sharing_the_cores_occupy_as_many_as_the_largest(self, tmp_path):
        """As ACCESS-rAM3's nests do, each a job of its own on the same nodes."""

        nests = RoseSuiteConfiguration(
            name="nests",
            model_name="Mock",
            components=(
                RoseProcessGrid(name="outer", token="rs01", variables=("RS01",), grid=Domain((12, 8))),
                RoseProcessGrid(name="inner", token="rs02", variables=("RS02",), grid=Domain((12, 8))),
            ),
            core_sharing=CoreSharing.SHARED,
        )
        conf = write_suite_conf(tmp_path, {"RS01": "6,4", "RS02": "4,4"})

        assert nests.occupied_cpus(conf) == 24
        layout = nests.parse_layout(tmp_path)
        assert (layout.n_cores, layout.n_ranks, layout.core_sharing) == (24, 24, CoreSharing.SHARED)
        assert [sub.n_cores for sub in layout.sub_layouts] == [24, 16]

    def test_a_suite_giving_no_component_cores_states_no_layout(self, tmp_path):
        ranks_only = RoseSuiteConfiguration(name="ranks", model_name="Mock", components=(OCEAN,))
        write_suite_conf(tmp_path, {"OCN_NPROC": "0"})

        assert ranks_only.parse_layout(tmp_path) is None
