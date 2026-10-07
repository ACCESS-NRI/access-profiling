# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from access.profiling.models.um_suites import AM3_N96E, RAM3, UM_SUITE_PARSERS
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
