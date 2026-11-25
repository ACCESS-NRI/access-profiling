# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import replace

import pytest
from access.config.parallel_allocation_strategies import FixedAllocation, RootAllocation, WholeRangeAllocation
from access.config.parallel_component import CoreSharing
from access.config.parallel_domain import Domain
from access.config.parallel_layouts import iter_layouts
from conftest import grid_of, write_suite_conf

from access.profiling.models import um_suites
from access.profiling.models.components import UM_CONSTRAINTS
from access.profiling.models.um_suites import (
    AM3_N96E,
    AM3_N96E_SOURCE,
    AM3_N512E,
    AM3_N512E_SOURCE,
    CM3_O25KM,
    CM3_O25KM_SOURCE,
    RAM3,
    UM_SUITE_PARSERS,
    am3_from_control,
)
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser


def test_the_um_suites_read_their_logs_with_both_um_parsers():
    """One for the regions, one for the total run time, which is what every suite's task logs hold."""

    for configuration in (AM3_N96E, RAM3, CM3_O25KM):
        assert isinstance(configuration.parsers["UM_regions"], UMProfilingParser)
        assert isinstance(configuration.parsers["UM_total"], UMTotalRuntimeParser)
        assert configuration.parsers is UM_SUITE_PARSERS, "the same two parsers, not a copy of them"


def test_am3_states_its_process_grid_in_two_variables_and_ram3_its_nests_in_one_each():
    """Which, with the nests sharing the cores, is the whole of what tells the two suites apart."""

    (um,) = AM3_N96E.components
    assert um.variables == ("MAIN_ATM_PROCX", "MAIN_ATM_PROCY")
    assert (um.threads_variable, um.io_server_variable) == ("MAIN_OMPTHR_ATM", "MAIN_IOS_NPROC")
    assert um.local_constraints == UM_CONSTRAINTS

    assert [nest.variables for nest in RAM3.components] == [("rg01_rs01_m01_nproc",), ("rg01_rs02_m01_nproc",)]
    assert all(nest.threads_variable is None and nest.io_server_variable is None for nest in RAM3.components)
    assert RAM3.core_sharing is CoreSharing.SHARED


def test_am3_counts_the_cpus_its_pbs_macro_asks_for(tmp_path):
    """cpus(x, y, i, nt) = (x*y + i)*nt, as site/nci_gadi.rc states it."""

    conf = write_suite_conf(
        tmp_path, {"MAIN_ATM_PROCX": "32", "MAIN_ATM_PROCY": "24", "MAIN_OMPTHR_ATM": "2", "MAIN_IOS_NPROC": "48"}
    )

    assert AM3_N96E.occupied_cpus(conf) == (32 * 24 + 48) * 2


def test_ram3_occupies_the_cpus_of_its_larger_nest(tmp_path):
    """Each nest is a job of its own, so the suite never holds the two nests' cores at once."""

    conf = write_suite_conf(tmp_path, {"rg01_rs01_m01_nproc": "32,24", "rg01_rs02_m01_nproc": "16,24"})

    assert RAM3.occupied_cpus(conf) == 32 * 24


def test_am3_n512e_is_n96e_on_another_grid():
    """The n512e release states its process grid, its threads and its I/O server in the same variables."""

    assert AM3_N512E.name == "n512e"
    assert AM3_N512E.components[0].grid == Domain((1024, 768))
    assert replace(AM3_N512E.components[0], grid=Domain((192, 144))) == AM3_N96E.components[0]
    assert AM3_N512E.experiment_prefix == AM3_N96E.experiment_prefix
    assert (AM3_N96E_SOURCE.start_point, AM3_N512E_SOURCE.start_point) == ("release-n96e-3.0", "release-n512e-1.0")


def test_cm3_adds_the_ocean_and_the_sea_ice_to_the_um_s_request(tmp_path):
    """NPROC = ATM_NPROCX*ATM_NPROCY + OCN_NPROC + ICE_NPROC + FLUME_IOS_NPROC, as the cm3_O25km suite.rc states it."""

    conf = write_suite_conf(
        tmp_path,
        {"ATM_NPROCX": "24", "ATM_NPROCY": "24", "OCN_NPROC": "1152", "ICE_NPROC": "192", "FLUME_IOS_NPROC": "0"},
    )

    assert CM3_O25KM.occupied_cpus(conf) == 1920
    layout = CM3_O25KM.parse_layout(tmp_path)
    assert [(sub.name, sub.n_cores) for sub in layout.sub_layouts] == [("UM", 576), ("MOM6", 1152), ("CICE6", 192)]
    assert CM3_O25KM.components[0].threads_variable is None, "OMP_NUM_THREADS is fixed at 1 in its suite.rc"
    assert CM3_O25KM_SOURCE.start_point == "cm3_O25km", "a branch: ACCESS-CM3 has no release yet"


def test_cm3_is_read_but_not_searched():
    """The grid its UM decomposes is not stated, so there is no process grid to choose."""

    with pytest.raises(ValueError, match="UM states no grid"):
        _ = CM3_O25KM.parallel_component


def test_every_source_names_its_suite_s_repository():
    sources = {name: value for name, value in vars(um_suites).items() if name.endswith("_SOURCE")}

    assert set(sources) == {"AM3_N96E_SOURCE", "AM3_N512E_SOURCE", "CM3_O25KM_SOURCE"}
    assert sources["AM3_N512E_SOURCE"].repository == sources["AM3_N96E_SOURCE"].repository
    assert all(source.start_point for source in sources.values())


class TestSearches:
    """The layouts the search finds for the suites, with the allocations a study of each would give."""

    def test_am3_on_the_whole_of_96_cores(self):
        """The UM takes every core, so only its process grid varies: one even along x, with subdomains not too long."""

        allocations = RootAllocation(subcomponents={"UM": FixedAllocation(core_fraction=1.0)})

        layouts = list(iter_layouts(AM3_N96E.parallel_component, 96, allocations=allocations))

        assert [AM3_N96E.layout_key(layout) for layout in layouts] == [
            "am3-layout_n96e_atm_6x16",
            "am3-layout_n96e_atm_8x12",
            "am3-layout_n96e_atm_12x8",
            "am3-layout_n96e_atm_16x6",
        ]

    def test_am3_with_two_threads_a_rank(self):
        allocations = RootAllocation(thread_range=(2, 2), subcomponents={"UM": FixedAllocation(core_fraction=1.0)})

        layouts = list(iter_layouts(AM3_N96E.parallel_component, 96, allocations=allocations))

        assert [AM3_N96E.layout_key(layout) for layout in layouts] == [
            "am3-layout_n96e_atm_4x12_t2",
            "am3-layout_n96e_atm_6x8_t2",
            "am3-layout_n96e_atm_8x6_t2",
            "am3-layout_n96e_atm_12x4_t2",
            "am3-layout_n96e_atm_16x3_t2",
        ]
        assert AM3_N96E.config_changes(layouts[0]) == {
            "rose-suite.conf": {
                "jinja2:suite.rc": {"MAIN_ATM_PROCX": "4", "MAIN_ATM_PROCY": "12", "MAIN_OMPTHR_ATM": "2"}
            }
        }

    @pytest.mark.parametrize(("total_cores", "count"), [(48, 16), (576, 49)])
    def test_ram3_runs_each_nest_on_the_whole_budget(self, total_cores, count):
        """Each nest is offered every process grid of the budget, so the two choose theirs independently."""

        allocations = RootAllocation(
            subcomponents={"UM rs01": WholeRangeAllocation(), "UM rs02": WholeRangeAllocation()}
        )

        layouts = list(iter_layouts(RAM3.parallel_component, total_cores, allocations=allocations))

        assert len(layouts) == count
        assert all(sub.n_cores == total_cores for layout in layouts for sub in layout.sub_layouts)
        assert all(grid_of(sub)[0] % 2 == 0 for layout in layouts for sub in layout.sub_layouts)
        assert RAM3.config_changes(layouts[0]) == {
            "rose-suite.conf": {
                "jinja2:suite.rc": {
                    "rg01_rs01_m01_nproc": ",".join(map(str, grid_of(layouts[0].sub_layouts[0]))),
                    "rg01_rs02_m01_nproc": ",".join(map(str, grid_of(layouts[0].sub_layouts[1]))),
                }
            }
        }


class TestAM3FromControl:
    """An ACCESS-AM3 configuration read from a suite checked out by hand, its grid stated where the UM reads it."""

    @staticmethod
    def suite(directory, nlsizes):
        """Writes the UM app of a suite, its sizes as `nlsizes` gives them."""
        conf = directory / "app/um/rose-app.conf"
        conf.parent.mkdir(parents=True)
        sizes = "".join(f"{key}={value}\n" for key, value in nlsizes.items())
        conf.write_text(f"meta=um-atmos/vn13.8\n\n[namelist:nlsizes]\nbl_levels=70\n{sizes}model_levels=85\n")
        return directory

    @pytest.mark.parametrize(
        ("grid", "preset"), [((192, 144), AM3_N96E), ((1024, 768), AM3_N512E)], ids=["n96e", "n512e"]
    )
    def test_the_releases_give_back_their_presets(self, tmp_path, grid, preset):
        suite = self.suite(tmp_path, {"global_row_length": grid[0], "global_rows": grid[1]})

        assert am3_from_control(suite) == preset

    def test_a_name_of_your_own(self, tmp_path):
        suite = self.suite(tmp_path, {"global_row_length": 192, "global_rows": 144})

        assert am3_from_control(suite, name="n96e-amip") == replace(AM3_N96E, name="n96e-amip")

    def test_sizes_it_does_not_state_are_refused(self, tmp_path):
        suite = self.suite(tmp_path, {"global_row_length": 192, "!!global_rows": 144})

        with pytest.raises(ValueError, match=r"\[namelist:nlsizes\] of .* states no \['global_rows'\]"):
            am3_from_control(suite)

    def test_a_suite_without_a_um_app_is_refused(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            am3_from_control(tmp_path)
