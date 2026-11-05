# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The UM-based ACCESS suites driven by Cylc and Rose: ACCESS-AM3, ACCESS-rAM3 and ACCESS-CM3.

What tells them apart, for profiling purposes, is the names their `rose-suite.conf` gives the variables stating
their parallelism. ACCESS-AM3 states its process grid as two variables and has an I/O server and OpenMP threads
besides; ACCESS-rAM3 states its as one comma-separated pair and has neither; ACCESS-CM3 states the UM's as two
variables and adds the cores of the ocean and the sea ice it couples through NUOPC. All read their task logs with
the same two UM parsers. None of that is code, so no suite has any.
"""

from dataclasses import replace

from access.profiling.control import GitControlSource
from access.profiling.rose_configuration import RoseSuiteConfiguration
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

UM_SUITE_PARSERS: dict = {
    "UM_regions": UMProfilingParser(),
    "UM_total": UMTotalRuntimeParser(),
}
"""The parsers the UM suites read their task logs with, keyed by name.

The UM writes one profiling log per task, read twice: once for its regions and once for its total run time.
"""

AM3_N96E: RoseSuiteConfiguration = RoseSuiteConfiguration(
    name="n96e",
    layout_variable=("MAIN_ATM_PROCX", "MAIN_ATM_PROCY"),
    parsers=UM_SUITE_PARSERS,
    experiment_prefix="am3-layout",
    cpus_per_proc_variable="MAIN_OMPTHR_ATM",
    io_server_variable="MAIN_IOS_NPROC",
)
"""ACCESS-AM3, whose `rose-suite.conf` names the two extents of the atmosphere's process grid separately.

It adds the I/O server's ranks to their product, and multiplies the result by the OpenMP threads each rank runs.
This is the arithmetic of the pbs_cpus macro in site/nci_gadi.rc: `cpus(x, y, i, nt) = (x*y + i)*nt`.
"""

AM3_CONFIGS_REPOSITORY: str = "git@github.com:ACCESS-NRI/ACCESS-AM3-configs.git"
"""The repository holding the released ACCESS-AM3 configurations."""
AM3_N96E_SOURCE: GitControlSource = GitControlSource(
    repository=AM3_CONFIGS_REPOSITORY,
    start_point="release-n96e-3.0",
)
"""The release `AM3_N96E` describes, as a control to profile it against.

Cylc takes its control the same way Payu does - ACCESS-AM3's configurations are a git repository whose releases
are tags - but nothing here clones it: the suite is expected to be in the working directory already, so this
records what a study was run against rather than fetching it.
"""

AM3_N512E: RoseSuiteConfiguration = replace(AM3_N96E, name="n512e")
"""ACCESS-AM3 at N512e, which states its parallelism in the same variables as N96e: the same suite under another
name.
"""
AM3_N512E_SOURCE: GitControlSource = GitControlSource(
    repository=AM3_CONFIGS_REPOSITORY,
    start_point="release-n512e-1.0",
)
"""The release `AM3_N512E` describes, as a control to profile it against."""

RAM3: RoseSuiteConfiguration = RoseSuiteConfiguration(
    name="ram3",
    layout_variable="rg01_rs01_m01_nproc",
    parsers=UM_SUITE_PARSERS,
    experiment_prefix="ram3-layout",
)
"""ACCESS-rAM3, whose suite states both extents in one comma-separated variable.

It runs one thread per rank and has no I/O server. Its suites are checked out by hand - `rosie checkout` after
`mosrs-auth`, or `git clone` now that the configurations are moving to git - so a study supplies its own control
rather than taking one from here.
"""


CM3_O25KM: RoseSuiteConfiguration = RoseSuiteConfiguration(
    name="O25km",
    layout_variable=("ATM_NPROCX", "ATM_NPROCY"),
    parsers=UM_SUITE_PARSERS,
    experiment_prefix="cm3-layout",
    io_server_variable="FLUME_IOS_NPROC",
    extra_cpus_variables=("OCN_NPROC", "ICE_NPROC"),
)
"""ACCESS-CM3 with its ocean at 25 km: the UM coupled to MOM6 and CICE6 through NUOPC, as a Cylc suite.

Its `suite.rc` asks PBS for `ATM_NPROCX * ATM_NPROCY + OCN_NPROC + ICE_NPROC + FLUME_IOS_NPROC` cores, with one
thread per rank, so the UM's process grid is the layout and the ocean's and the sea ice's cores are added to it.
The UM's timers are read from the `atmos` task's `job.out`, with `ltimer` on, as for ACCESS-AM3; the logs the
ocean and the sea ice write are not read by the Cylc manager. The older `cm3_O100km` branch states the ocean's
cores as `OCN_NPROCX` and `OCN_NPROCY` instead, and is not described here.
"""

CM3_CONFIGS_REPOSITORY: str = "git@github.com:ACCESS-NRI/access-cm3-configs.git"
"""The repository holding the ACCESS-CM3 configurations, one per branch, which is restricted like ACCESS-AM3's."""
CM3_O25KM_SOURCE: GitControlSource = GitControlSource(
    repository=CM3_CONFIGS_REPOSITORY,
    start_point="cm3_O25km",
)
"""The standard branch `CM3_O25KM` describes, as a control to profile it against; ACCESS-CM3 has no release yet."""
