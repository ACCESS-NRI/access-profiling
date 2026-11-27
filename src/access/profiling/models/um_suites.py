# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The UM-based ACCESS suites driven by Cylc and Rose: ACCESS-AM3, ACCESS-rAM3 and ACCESS-CM3.

What tells them apart, for profiling purposes, is the components they run and the variables of their
`rose-suite.conf` that state the parallelism of each. ACCESS-AM3 runs one UM, whose process grid it states as two
variables, with an I/O server and OpenMP threads besides. ACCESS-rAM3 runs two nests of the UM as jobs of their own,
each stating its process grid as one comma-separated pair. ACCESS-CM3 runs the UM beside an ocean and a sea ice it
couples through NUOPC, each given its ranks by a variable. All read their task logs with the same two UM parsers.
None of that is code, so no suite has any.

The components and their variables, the models' names and the repositories of their configurations are Payu's
description of the ACCESS models, `payu.access_models`, which the other tools of the models share. What only
profiling knows stays here: the token of each component in a layout key, the grids and the constraints.
"""

import re
from pathlib import Path

from access.config import RoseConfParser
from access.config.parallel_component import CoreSharing
from access.config.parallel_domain import Domain
from payu.access_models import ACCESS_AM3, ACCESS_CM3, ACCESS_RAM3, Model, Ranks, Setting

from access.profiling.control import GitControlSource
from access.profiling.models.components import UM_CONSTRAINTS
from access.profiling.rose_configuration import (
    SUITE_CONF,
    SUITE_VARIABLES,
    RoseComponent,
    RoseProcessGrid,
    RoseRanks,
    RoseSuiteConfiguration,
    read_section,
)
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

UM_SUITE_PARSERS: dict = {
    "UM_regions": UMProfilingParser(),
    "UM_total": UMTotalRuntimeParser(),
}
"""The parsers the UM suites read their task logs with, keyed by name.

The UM writes one profiling log per task, read twice: once for its regions and once for its total run time.
"""

# Where an ACCESS-AM3 suite states the UM's grid, relative to the suite directory, and the section and the settings
# that state it there: the namelist the UM itself reads its sizes from. The optional configuration of the UM app for
# the suite's resolution, which the variable of rose-suite.conf names, changes them where the suite has one.
_UM_GRID: tuple[Setting, Setting] = ACCESS_AM3.component("UM").parallelism.grid
_UM_APP_CONF: str = _UM_GRID[0].file
_UM_OPT_CONF: str = "app/um/opt/rose-app-{}.conf"
_UM_RESOLUTION_VARIABLE: str = "EXPT_HORIZ_ATM"
_UM_SIZES: str = _UM_GRID[0].section
_UM_GRID_SETTINGS: tuple[str, str] = (_UM_GRID[0].key, _UM_GRID[1].key)


def _rose_component(model: Model, name: str, token: str, **kwargs) -> RoseComponent:
    """Returns one of a suite's components, stating its parallelism in the variables Payu's description names.

    Args:
        model (Model): The model, as `payu.access_models` describes it.
        name (str): The component's name, in the model and in the component tree.
        token (str): How `RoseSuiteConfiguration.layout_key` abbreviates it.
        **kwargs: What only profiling knows about a process grid: its `grid` and its `local_constraints`.

    Returns:
        RoseComponent: A `RoseRanks` for a component given its ranks, and a `RoseProcessGrid` for one decomposing a
            process grid, its two extents stated apart or as one comma-separated pair.
    """
    parallelism = model.component(name).parallelism
    if isinstance(parallelism, Ranks):
        return RoseRanks(name=name, token=token, variable=parallelism.ranks.key)
    return RoseProcessGrid(
        name=name,
        token=token,
        variables=tuple(setting.key for setting in (parallelism.x, parallelism.y) if setting is not None),
        threads_variable=None if parallelism.threads is None else parallelism.threads.key,
        io_server_variable=None if parallelism.io_server is None else parallelism.io_server.key,
        **kwargs,
    )


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
        model_name=ACCESS_AM3.name,
        components=(_rose_component(ACCESS_AM3, "UM", "atm", grid=grid, local_constraints=UM_CONSTRAINTS),),
        parsers=UM_SUITE_PARSERS,
        experiment_prefix="am3-layout",
        walltime_variable="MAIN_CLOCK",
    )


AM3_N96E: RoseSuiteConfiguration = _am3("n96e", Domain((192, 144)))
"""ACCESS-AM3 at N96e: the UM on a grid of 192 longitudes by 144 latitudes.

Its `rose-suite.conf` names the two extents of the UM's process grid separately, adds the I/O server's ranks to their
product, and multiplies the result by the OpenMP threads each rank runs. This is the arithmetic of the pbs_cpus macro
in site/nci_gadi.rc: `cpus(x, y, i, nt) = (x*y + i)*nt`. The walltime of the UM's job is `MAIN_CLOCK`.
"""

AM3_CONFIGS_REPOSITORY: str = ACCESS_AM3.configs_ssh_url
"""The repository holding the released ACCESS-AM3 configurations."""
AM3_N96E_SOURCE: GitControlSource = GitControlSource(
    repository=AM3_CONFIGS_REPOSITORY,
    start_point="release-n96e-3.0",
)
"""The release `AM3_N96E` describes, as a control to profile it against.

Cylc takes its control the same way Payu does - ACCESS-AM3's configurations are a git repository whose releases
are tags - but nothing here clones it: the suite is expected in the working directory already, under `config`, where
generating a study copies it from, so this records what a study was run against rather than fetching it.
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
    `[namelist:nlsizes]` section of `app/um/rose-app.conf`, as the optional configuration of the suite's resolution
    changes them. The suite names its resolution in the variable `EXPT_HORIZ_ATM` of its `rose-suite.conf`, and has
    the UM's task apply `app/um/opt/rose-app-<resolution>.conf` over `rose-app.conf` where that file exists. This is
    where the N512e release states its grid, its `rose-app.conf` stating N96e's. The suite applies the optional
    configurations other variables name too, but neither release states the grid in them. Everything else is what
    every ACCESS-AM3 suite states, so the suites of the two releases give back `AM3_N96E` and `AM3_N512E`.

    Args:
        directory (Path): The suite directory, as `rosie checkout` or `git clone` left it.
        name (str | None): The configuration's name. None (the default) names it after the resolution, `n<N>e` for
            a grid of 2N longitudes, as the presets are named.

    Returns:
        RoseSuiteConfiguration: The configuration.

    Raises:
        FileNotFoundError: If the suite has no `app/um/rose-app.conf` or no `rose-suite.conf`.
        ValueError: If `app/um/rose-app.conf` has no `[namelist:nlsizes]` section in use, if `rose-suite.conf` has no
            `[jinja2:suite.rc]` section in use or states `EXPT_HORIZ_ATM` as anything but a quoted string, or if the
            sizes, with those the optional configuration changes, are not two positive whole numbers.
    """
    conf_path = directory / _UM_APP_CONF
    sizes = read_section(conf_path, _UM_SIZES)
    source = str(conf_path)
    suite_conf = directory / SUITE_CONF
    resolution = read_section(suite_conf, SUITE_VARIABLES).get(_UM_RESOLUTION_VARIABLE)
    if resolution is not None:
        # A Jinja2 string: anything else is an expression, whose value, and so the file it selects, cannot be told.
        quoted = re.fullmatch(r"""(['"])(.+)\1""", resolution)
        if quoted is None:
            raise ValueError(
                f"{suite_conf} states {_UM_RESOLUTION_VARIABLE} as {resolution}, which is not a quoted string, so the "
                "optional configuration of the UM it selects, which may state the UM's grid, cannot be told."
            )
        opt_path = directory / _UM_OPT_CONF.format(quoted.group(2))
        # Rose skips an optional configuration the suite does not have, and one need not change the sizes.
        if opt_path.is_file() and _UM_SIZES in RoseConfParser().parse(opt_path.read_text()):
            sizes |= read_section(opt_path, _UM_SIZES)
            source = f"{conf_path} with {opt_path}"
    missing = [setting for setting in _UM_GRID_SETTINGS if setting not in sizes]
    if missing:
        raise ValueError(f"[{_UM_SIZES}] of {source} states no {missing}, so the UM's grid cannot be read from it.")
    try:
        grid = Domain(tuple(int(sizes[setting]) for setting in _UM_GRID_SETTINGS))
    except ValueError as error:
        stated = {setting: sizes[setting] for setting in _UM_GRID_SETTINGS}
        raise ValueError(
            f"[{_UM_SIZES}] of {source} states the UM's grid as {stated}, which are not two positive whole numbers."
        ) from error
    return _am3(name if name is not None else f"n{grid.shape[0] // 2}e", grid)


RAM3: RoseSuiteConfiguration = RoseSuiteConfiguration(
    name="ram3",
    model_name=ACCESS_RAM3.name,
    components=tuple(
        # A nest is named "UM rs01", and its token is the nest's.
        _rose_component(
            ACCESS_RAM3,
            component.name,
            component.name.split()[-1],
            grid=Domain((450, 450)),
            local_constraints=UM_CONSTRAINTS,
        )
        for component in ACCESS_RAM3.components
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
moving to git - so a study supplies its own control rather than taking one from here. It states the walltime of its
jobs in `site/nci-gadi/suite-adds.rc` rather than in a variable, so it names no `walltime_variable`.
"""


CM3_O25KM: RoseSuiteConfiguration = RoseSuiteConfiguration(
    name="O25km",
    model_name=ACCESS_CM3.name,
    components=(
        _rose_component(ACCESS_CM3, "UM", "atm"),
        _rose_component(ACCESS_CM3, "MOM6", "ocn"),
        _rose_component(ACCESS_CM3, "CICE6", "ice"),
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

CM3_CONFIGS_REPOSITORY: str = ACCESS_CM3.configs_ssh_url
"""The repository holding the ACCESS-CM3 configurations, one per branch, which is restricted like ACCESS-AM3's."""
CM3_O25KM_SOURCE: GitControlSource = GitControlSource(
    repository=CM3_CONFIGS_REPOSITORY,
    start_point="cm3_O25km",
)
"""The standard branch `CM3_O25KM` describes, as a control to profile it against; ACCESS-CM3 has no release yet."""
