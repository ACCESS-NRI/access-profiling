# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The UM-based ACCESS suites driven by Cylc and Rose: ACCESS-AM3, ACCESS-rAM3 and ACCESS-CM3.

What tells them apart, for profiling purposes, is the components they run and the variables of their
`rose-suite.conf` that state the parallelism of each. ACCESS-AM3 runs one UM, whose process grid it states as two
variables, with an I/O server and OpenMP threads besides. ACCESS-rAM3 runs two nests of the UM as jobs of their own,
each stating its process grid as one comma-separated pair. ACCESS-CM3 runs the UM beside an ocean and a sea ice it
couples through NUOPC, each given its ranks by a variable. All read their task logs with the same two UM parsers.
None of that is code, so no suite has any.
"""

from pathlib import Path

from access.config.parallel_component import CoreSharing
from access.config.parallel_domain import Domain

from access.profiling.control import GitControlSource
from access.profiling.models.components import UM_CONSTRAINTS
from access.profiling.rose_configuration import RoseProcessGrid, RoseRanks, RoseSuiteConfiguration, read_section
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

UM_SUITE_PARSERS: dict = {
    "UM_regions": UMProfilingParser(),
    "UM_total": UMTotalRuntimeParser(),
}
"""The parsers the UM suites read their task logs with, keyed by name.

The UM writes one profiling log per task, read twice: once for its regions and once for its total run time.
"""

# Where an ACCESS-AM3 suite states the UM's grid, relative to the suite directory, and the section and the settings
# that state it there: the namelist the UM itself reads its sizes from.
_UM_APP_CONF: str = "app/um/rose-app.conf"
_UM_SIZES: str = "namelist:nlsizes"
_UM_GRID_SETTINGS: tuple[str, str] = ("global_row_length", "global_rows")


def _am3(name: str, grid: Domain) -> RoseSuiteConfiguration:
    """Returns the ACCESS-AM3 configuration of a resolution: one UM, over the given grid.

    Args:
        name (str): The configuration's name.
        grid (Domain): The UM's grid, longitudes first.

    Returns:
        RoseSuiteConfiguration: The configuration.
    """
    return RoseSuiteConfiguration(
        name=name,
        model_name="ACCESS-AM3",
        components=(
            RoseProcessGrid(
                name="UM",
                token="atm",
                variables=("MAIN_ATM_PROCX", "MAIN_ATM_PROCY"),
                grid=grid,
                threads_variable="MAIN_OMPTHR_ATM",
                io_server_variable="MAIN_IOS_NPROC",
                local_constraints=UM_CONSTRAINTS,
            ),
        ),
        parsers=UM_SUITE_PARSERS,
        experiment_prefix="am3-layout",
    )


AM3_N96E: RoseSuiteConfiguration = _am3("n96e", Domain((192, 144)))
"""ACCESS-AM3 at N96e: the UM on a grid of 192 longitudes by 144 latitudes.

Its `rose-suite.conf` names the two extents of the UM's process grid separately, adds the I/O server's ranks to their
product, and multiplies the result by the OpenMP threads each rank runs. This is the arithmetic of the pbs_cpus macro
in site/nci_gadi.rc: `cpus(x, y, i, nt) = (x*y + i)*nt`.
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

AM3_N512E: RoseSuiteConfiguration = _am3("n512e", Domain((1024, 768)))
"""ACCESS-AM3 at N512e: the UM on a grid of 1024 longitudes by 768 latitudes, its parallelism stated in the same
variables as N96e.
"""
AM3_N512E_SOURCE: GitControlSource = GitControlSource(
    repository=AM3_CONFIGS_REPOSITORY,
    start_point="release-n512e-1.0",
)
"""The release `AM3_N512E` describes, as a control to profile it against."""


def am3_from_control(directory: Path, name: str | None = None) -> RoseSuiteConfiguration:
    """Returns the ACCESS-AM3 configuration a suite already checked out describes, read from its own files.

    The UM's grid is read from the sizes the UM itself reads, `global_row_length` and `global_rows` in the
    `[namelist:nlsizes]` section of `app/um/rose-app.conf`. Everything else is what every ACCESS-AM3 suite states, so
    the suites of the two releases give back `AM3_N96E` and `AM3_N512E`.

    Args:
        directory (Path): The suite directory, as `rosie checkout` or `git clone` left it.
        name (str | None): The configuration's name. None (the default) names it after the resolution, `n<N>e` for
            a grid of 2N longitudes, as the presets are named.

    Returns:
        RoseSuiteConfiguration: The configuration.

    Raises:
        FileNotFoundError: If the suite has no `app/um/rose-app.conf`.
        ValueError: If the file has no `[namelist:nlsizes]` section in use, or if the section does not state the two
            sizes as positive whole numbers.
    """
    conf_path = directory / _UM_APP_CONF
    sizes = read_section(conf_path, _UM_SIZES)
    missing = [setting for setting in _UM_GRID_SETTINGS if setting not in sizes]
    if missing:
        raise ValueError(f"[{_UM_SIZES}] of {conf_path} states no {missing}, so the UM's grid cannot be read from it.")
    try:
        grid = Domain(tuple(int(sizes[setting]) for setting in _UM_GRID_SETTINGS))
    except ValueError as error:
        stated = {setting: sizes[setting] for setting in _UM_GRID_SETTINGS}
        raise ValueError(
            f"[{_UM_SIZES}] of {conf_path} states the UM's grid as {stated}, which are not two positive whole numbers."
        ) from error
    return _am3(name if name is not None else f"n{grid.shape[0] // 2}e", grid)


RAM3: RoseSuiteConfiguration = RoseSuiteConfiguration(
    name="ram3",
    model_name="ACCESS-rAM3",
    components=tuple(
        RoseProcessGrid(
            name=f"UM {nest}",
            token=nest,
            variables=(f"rg01_{nest}_m01_nproc",),
            grid=Domain((450, 450)),
            local_constraints=UM_CONSTRAINTS,
        )
        for nest in ("rs01", "rs02")
    ),
    parsers=UM_SUITE_PARSERS,
    core_sharing=CoreSharing.SHARED,
    experiment_prefix="ram3-layout",
)
"""ACCESS-rAM3's Regional Nesting Suite: two nests of the UM, each on a grid of 450 by 450 points.

Each nest states both extents of its process grid in one comma-separated variable, `rg01_rs01_m01_nproc` and
`rg01_rs02_m01_nproc`, and runs one thread per rank with no I/O server. The nests run as tasks of their own, each a
job of its own, so they share the cores rather than divide them: the suite occupies as many as the larger nest. Its
suites are checked out by hand - `rosie checkout` after `mosrs-auth`, or `git clone` now that the configurations are
moving to git - so a study supplies its own control rather than taking one from here.
"""


CM3_O25KM: RoseSuiteConfiguration = RoseSuiteConfiguration(
    name="O25km",
    model_name="ACCESS-CM3",
    components=(
        RoseProcessGrid(
            name="UM", token="atm", variables=("ATM_NPROCX", "ATM_NPROCY"), io_server_variable="FLUME_IOS_NPROC"
        ),
        RoseRanks(name="MOM6", token="ocn", variable="OCN_NPROC"),
        RoseRanks(name="CICE6", token="ice", variable="ICE_NPROC"),
    ),
    parsers=UM_SUITE_PARSERS,
    experiment_prefix="cm3-layout",
)
"""ACCESS-CM3 with its ocean at 25 km: the UM coupled to MOM6 and CICE6 through NUOPC, as a Cylc suite.

Its `suite.rc` asks PBS for `ATM_NPROCX * ATM_NPROCY + OCN_NPROC + ICE_NPROC + FLUME_IOS_NPROC` cores, with one
thread per rank, so the UM's process grid, the ocean's ranks and the sea ice's ranks are its three components, and
the I/O server's ranks are added to the UM's. Its layouts are read but not searched for, since the grid the UM
decomposes is not stated here. The UM's timers are read from the `atmos` task's `job.out`, with `ltimer` on, as for
ACCESS-AM3; the logs the ocean and the sea ice write are not read by the Cylc manager. The older `cm3_O100km` branch
states the ocean's cores as `OCN_NPROCX` and `OCN_NPROCY` instead, and is not described here.
"""

CM3_CONFIGS_REPOSITORY: str = "git@github.com:ACCESS-NRI/access-cm3-configs.git"
"""The repository holding the ACCESS-CM3 configurations, one per branch, which is restricted like ACCESS-AM3's."""
CM3_O25KM_SOURCE: GitControlSource = GitControlSource(
    repository=CM3_CONFIGS_REPOSITORY,
    start_point="cm3_O25km",
)
"""The standard branch `CM3_O25KM` describes, as a control to profile it against; ACCESS-CM3 has no release yet."""
