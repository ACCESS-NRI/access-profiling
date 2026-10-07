# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The UM-based ACCESS suites driven by Cylc and Rose: ACCESS-AM3 and ACCESS-rAM3.

What tells the two apart, for profiling purposes, is the names their `rose-suite.conf` gives the variables
stating their parallelism. ACCESS-AM3 states its process grid as two variables and has an I/O server and
OpenMP threads besides; ACCESS-rAM3 states its as one comma-separated pair and has neither. Both read their
task logs with the same two UM parsers. None of that is code, so neither suite has any.
"""

from access.profiling.control import GitControlSource
from access.profiling.rose_configuration import RoseSuiteConfiguration
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

# The UM writes one profiling log per task, read twice: once for its regions and once for its total run time.
UM_SUITE_PARSERS: dict = {
    "UM_regions": UMProfilingParser(),
    "UM_total": UMTotalRuntimeParser(),
}

# ACCESS-AM3, whose `rose-suite.conf` names the two extents of the atmosphere's process grid separately, adds
# the I/O server's ranks to their product, and multiplies the result by the OpenMP threads each rank runs.
# This is the arithmetic of the pbs_cpus macro in site/nci_gadi.rc: cpus(x, y, i, nt) = (x*y + i)*nt.
AM3_N96E: RoseSuiteConfiguration = RoseSuiteConfiguration(
    name="n96e",
    layout_variable=("MAIN_ATM_PROCX", "MAIN_ATM_PROCY"),
    parsers=UM_SUITE_PARSERS,
    experiment_prefix="am3-layout",
    cpus_per_proc_variable="MAIN_OMPTHR_ATM",
    io_server_variable="MAIN_IOS_NPROC",
)

# The release that configuration describes. Cylc takes its control the same way Payu does - ACCESS-AM3's
# configurations are a git repository whose releases are tags - but nothing here clones it: the suite is
# expected to be in the working directory already, so this records what a study was run against rather than
# fetching it.
AM3_CONFIGS_REPOSITORY: str = "git@github.com:ACCESS-NRI/ACCESS-AM3-configs.git"
AM3_N96E_SOURCE: GitControlSource = GitControlSource(
    repository=AM3_CONFIGS_REPOSITORY,
    start_point="release-n96e-3.0",
)

# ACCESS-rAM3, whose suite states both extents in one comma-separated variable, runs one thread per rank and
# has no I/O server. Its suites are checked out by hand - `rosie checkout` after `mosrs-auth`, or `git clone`
# now that the configurations are moving to git - so a study supplies its own control rather than taking one
# from here.
RAM3: RoseSuiteConfiguration = RoseSuiteConfiguration(
    name="ram3",
    layout_variable="rg01_rs01_m01_nproc",
    parsers=UM_SUITE_PARSERS,
    experiment_prefix="ram3-layout",
)
