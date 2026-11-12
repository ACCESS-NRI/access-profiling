# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from access.profiling.models import um_suites
from access.profiling.models.um_suites import (
    AM3_N96E,
    AM3_N96E_SOURCE,
    AM3_N512E,
    AM3_N512E_SOURCE,
    CM3_O25KM,
    CM3_O25KM_SOURCE,
    RAM3,
    UM_SUITE_PARSERS,
)
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser


def test_the_um_suites_read_their_logs_with_both_um_parsers():
    """One for the regions, one for the total run time, which is what both suites' task logs hold."""

    for parsers in (AM3_N96E.parsers, RAM3.parsers):
        assert isinstance(parsers["UM_regions"], UMProfilingParser)
        assert isinstance(parsers["UM_total"], UMTotalRuntimeParser)
    assert AM3_N96E.parsers is UM_SUITE_PARSERS, "the same two parsers, not a copy of them"


def test_am3_states_its_layout_in_two_variables_and_ram3_in_one():
    """Which is the whole of what tells the two suites apart."""

    assert AM3_N96E.layout_variable == ("MAIN_ATM_PROCX", "MAIN_ATM_PROCY")
    assert AM3_N96E.cpus_per_proc_variable == "MAIN_OMPTHR_ATM"
    assert AM3_N96E.io_server_variable == "MAIN_IOS_NPROC"

    assert RAM3.layout_variable == "rg01_rs01_m01_nproc"
    assert RAM3.cpus_per_proc_variable is None, "one thread per rank"
    assert RAM3.io_server_variable is None, "no I/O server"


def test_am3_counts_the_cpus_its_pbs_macro_asks_for():
    """cpus(x, y, i, nt) = (x*y + i)*nt, as site/nci_gadi.rc states it."""

    rose_conf = {
        "MAIN_ATM_PROCX": "32",
        "MAIN_ATM_PROCY": "24",
        "MAIN_OMPTHR_ATM": "2",
        "MAIN_IOS_NPROC": "48",
    }
    assert AM3_N96E.occupied_cpus(rose_conf) == (32 * 24 + 48) * 2


def test_ram3_counts_the_product_of_its_one_variable():
    assert RAM3.occupied_cpus({"rg01_rs01_m01_nproc": "32,24"}) == 32 * 24


def test_am3_n512e_is_n96e_under_another_name():
    """The n512e release states its layout, its threads and its I/O server in the same variables."""

    assert AM3_N512E.name == "n512e"
    assert (AM3_N512E.layout_variable, AM3_N512E.cpus_per_proc_variable, AM3_N512E.io_server_variable) == (
        AM3_N96E.layout_variable,
        AM3_N96E.cpus_per_proc_variable,
        AM3_N96E.io_server_variable,
    )
    assert AM3_N512E.experiment_prefix == AM3_N96E.experiment_prefix
    assert (AM3_N96E_SOURCE.start_point, AM3_N512E_SOURCE.start_point) == ("release-n96e-3.0", "release-n512e-1.0")


def test_cm3_adds_the_ocean_and_the_sea_ice_to_the_um_s_request():
    """NPROC = ATM_NPROCX*ATM_NPROCY + OCN_NPROC + ICE_NPROC + FLUME_IOS_NPROC, as the cm3_O25km suite.rc states it."""

    rose_conf = {
        "ATM_NPROCX": "24",
        "ATM_NPROCY": "24",
        "OCN_NPROC": "1152",
        "ICE_NPROC": "192",
        "FLUME_IOS_NPROC": "0",
    }

    assert CM3_O25KM.occupied_cpus(rose_conf) == 1920
    assert CM3_O25KM.parsers is UM_SUITE_PARSERS, "the UM's timers, read as for the other suites"
    assert CM3_O25KM.cpus_per_proc_variable is None, "OMP_NUM_THREADS is fixed at 1 in its suite.rc"
    assert CM3_O25KM_SOURCE.start_point == "cm3_O25km", "a branch: ACCESS-CM3 has no release yet"


def test_every_source_names_its_suite_s_repository():
    sources = {name: value for name, value in vars(um_suites).items() if name.endswith("_SOURCE")}

    assert set(sources) == {"AM3_N96E_SOURCE", "AM3_N512E_SOURCE", "CM3_O25KM_SOURCE"}
    assert sources["AM3_N512E_SOURCE"].repository == sources["AM3_N96E_SOURCE"].repository
    assert all(source.start_point for source in sources.values())
