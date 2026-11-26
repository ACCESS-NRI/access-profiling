# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""What a Cylc Rose suite states about itself.

Everything here knows that a rose suite is the runner: that its parallelism is stated as variables in the
`[jinja2:suite.rc]` section of `rose-suite.conf`, and that which variables those are is what tells one suite from
another. None of it belongs in the generic contract.

A suite is put together from components, as a Payu model is: a component is one code whose parallelism the suite's
variables state, and says once how those variables are read and written. `RoseProcessGrid` is a code that decomposes
a two-dimensional grid over a process grid, as the UM does, and `RoseRanks` one that is given a number of ranks and
nothing else. A configuration arranges components, so a new suite is a new value of `RoseSuiteConfiguration` rather
than new code.

The files are read and written with `RoseConfParser` from access-config-utils. A setting Rose is told to ignore,
written `!KEY=VALUE` or `!!KEY=VALUE`, is one the suite does not run with, so it is never read as the setting.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from access.config import RoseConfParser
from access.config.parallel_component import ComponentLayout, CoreSharing, LocalConstraint, ParallelComponent
from access.config.parallel_constraints import FixedThreadsPerRankConstraint
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid

from access.profiling.application import Application, LogSpec
from access.profiling.parser import ProfilingParser

SUITE_CONF: str = "rose-suite.conf"
"""The file of a suite that states its variables, relative to the suite directory."""

SUITE_VARIABLES: str = "jinja2:suite.rc"
"""The section of `rose-suite.conf` holding the variables the suite's `suite.rc` is written with."""


def read_section(conf_path: Path, section: str) -> dict[str, str]:
    """Returns the settings of one section of a Rose configuration file that Rose uses.

    A setting Rose is told to ignore is left out, since the suite does not run with it. A setting with nothing after
    its `=` reads as the empty string, as Rose reads it. A value continued over several lines is one string, its
    lines joined by newlines.

    A setting written both in use and ignored is refused. Rose uses the one written last, but `RoseConfParser`
    reads the two as two keys, so which one that is cannot be told here.

    Args:
        conf_path (Path): The file, such as a suite's `rose-suite.conf` or an app's `rose-app.conf`.
        section (str): The section, as its header names it without the brackets, such as `jinja2:suite.rc`.

    Returns:
        dict[str, str]: The values, keyed by setting.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file holds no such section, or only an ignored one, or if the section holds a setting
            both in use and ignored.
    """
    conf = RoseConfParser().parse(conf_path.read_text())
    if section not in conf:
        raise ValueError(f"The Rose configuration {conf_path} has no section [{section}] in use.")
    settings = conf[section]
    in_use = {key for key in settings if not key.startswith("!")}
    both = sorted(in_use & {key.lstrip("!") for key in settings if key.startswith("!")})
    if both:
        raise ValueError(
            f"[{section}] of {conf_path} holds {both} both in use and ignored. Rose uses the one written last, which "
            "cannot be told here: keep one of the two."
        )
    return {key: "" if value is None else value for key, value in settings.items() if key in in_use}


def _integers(variables: Mapping[str, str], name: str, source: str, count: int = 1) -> tuple[int, ...]:
    """Returns the whole numbers a variable of a suite holds, separated by commas where it holds several.

    A variable the configuration names and the file does not hold is an error rather than a zero: it means the
    configuration describes a different suite from the one being read, and a plausible-looking core count from the
    wrong suite is worse than a refusal.

    Args:
        variables (Mapping[str, str]): The variables of the suite.
        name (str): The variable.
        source (str): What to call the file in a message.
        count (int): How many numbers the variable holds.

    Returns:
        tuple[int, ...]: The numbers.

    Raises:
        ValueError: If the variable is not set, or does not hold `count` whole numbers.
    """
    if name not in variables:
        raise ValueError(f"Cannot find the variable {name} in {source}.")
    try:
        numbers = tuple(int(part) for part in variables[name].split(","))
    except ValueError:
        numbers = ()
    if len(numbers) != count:
        raise ValueError(
            f"The variable {name} of {source} is {variables[name]!r}, which is not {count} whole number(s) separated "
            "by commas."
        )
    return numbers


class RoseComponent(ABC):
    """Abstract base class for one code a Rose suite runs, whose parallelism variables of the suite state.

    A component says how its variables are read and written, so that a configuration arranges components rather than
    restating them: the leaf of the component tree the search decomposes, what a layout of it writes into the
    suite's variables, and what the variables say back.

    Attributes:
        name (str): How the component tree names it, and so the key a caller uses for it in an allocation strategy
            or a RegionGroup.
        token (str): How `RoseSuiteConfiguration.layout_key` abbreviates it. Changing it changes the key of every
            layout, so that the experiments on disk are no longer recognised as the experiments of their layouts.

    These two are declared as attributes rather than abstract properties, for the reason `Application` gives.
    """

    name: str
    token: str

    @property
    @abstractmethod
    def suite_variables(self) -> tuple[str, ...]:
        """Returns the variables of the suite this component reads, every one of them.

        Returns:
            tuple[str, ...]: The names of the variables.
        """

    @property
    @abstractmethod
    def leaf(self) -> ParallelComponent:
        """Returns the leaf of the component tree this component becomes.

        Returns:
            ParallelComponent: The leaf, carrying the grid the search decomposes, if any, and the requirements every
                layout of the component meets.

        Raises:
            ValueError: If the component states too little for its layouts to be searched for.
        """

    @abstractmethod
    def variable_changes(self, layout: ComponentLayout) -> dict[str, str]:
        """Returns the values of the suite's variables that run a given layout of this component.

        Args:
            layout (ComponentLayout): What the component was given, as the layout search returned it.

        Returns:
            dict[str, str]: The values, as the file is to state them, keyed by variable.

        Raises:
            ValueError: If the layout cannot be written into the variables.
        """

    @abstractmethod
    def read_layout(self, variables: Mapping[str, str], source: str) -> ComponentLayout | None:
        """Returns the layout of this component that the suite's variables state.

        Args:
            variables (Mapping[str, str]): The variables of the suite.
            source (str): What to call the file in a message.

        Returns:
            ComponentLayout | None: The layout, or None where the variables give the component no cores.

        Raises:
            ValueError: If a variable the component names is not set, or does not hold whole numbers.
        """

    @abstractmethod
    def occupied_cpus(self, variables: Mapping[str, str], source: str) -> int:
        """Returns the number of CPUs this component occupies, as the suite's variables state it.

        Args:
            variables (Mapping[str, str]): The variables of the suite.
            source (str): What to call the file in a message.

        Returns:
            int: The number of CPUs, including those of anything the component runs beside its layout.

        Raises:
            ValueError: If a variable the component names is not set, or does not hold whole numbers.
        """


@dataclass(frozen=True, kw_only=True)
class RoseProcessGrid(RoseComponent):
    """A code decomposing a two-dimensional grid over the process grid the suite's variables state, as the UM does.

    The process grid is stated either as two variables, one for each extent, or as one variable holding both,
    separated by a comma. The code may also run OpenMP threads, and an I/O server whose ranks the job asks for beside
    the process grid. The search chooses the process grid and, where the suite states them, the threads. It never
    chooses the I/O server, whose ranks are counted as the suite states them and never written.

    Args:
        name (str): How the component tree names it.
        token (str): How `RoseSuiteConfiguration.layout_key` abbreviates it.
        variables (tuple[str, ...]): The names of the two variables stating the extents of the process grid, or the
            name of the one variable stating both.
        grid (Domain | None): The grid the code decomposes, or None where it is not known. A component without one
            is read but not searched for, and a layout read back from it carries no decomposition.
        threads_variable (str | None): The variable stating the OpenMP threads of each rank. None (the default)
            runs one thread per rank, and every layout of the component has one.
        io_server_variable (str | None): The variable stating the ranks of the I/O server, which the job runs on
            cores of their own, each running as many threads as a rank of the process grid. None (the default) runs
            no I/O server.
        local_constraints (tuple[LocalConstraint, ...]): The requirements every layout of the code meets, such as
            `UM_CONSTRAINTS` in `access.profiling.models.components`.

    Raises:
        ValueError: If it names other than one or two variables for the process grid, or if its grid is not
            two-dimensional.
    """

    name: str
    token: str
    variables: tuple[str, ...]
    grid: Domain | None = None
    threads_variable: str | None = None
    io_server_variable: str | None = None
    local_constraints: tuple[LocalConstraint, ...] = ()

    def __post_init__(self) -> None:
        if len(self.variables) not in (1, 2):
            raise ValueError(
                f"{self.name} names the variables {self.variables} for its process grid. Name two variables, one for "
                "each extent, or one variable stating both."
            )
        if self.grid is not None and self.grid.ndim != 2:
            raise ValueError(
                f"{self.name} is given a {self.grid.ndim}-dimensional grid {self.grid.shape}. Its decomposition is "
                "written as a two-dimensional process grid, so the grid it decomposes has to be one too."
            )

    @property
    def suite_variables(self) -> tuple[str, ...]:
        optional = (self.threads_variable, self.io_server_variable)
        return (*self.variables, *(variable for variable in optional if variable is not None))

    @property
    def leaf(self) -> ParallelComponent:
        if self.grid is None:
            raise ValueError(
                f"{self.name} states no grid, so its process grids cannot be searched for. Give it the grid it "
                "decomposes."
            )
        # A suite stating no threads runs one per rank, so a layout with more could not be written.
        threads = () if self.threads_variable is not None else (FixedThreadsPerRankConstraint(n_threads=1),)
        return ParallelComponent(
            name=self.name, domain=self.grid, local_constraints=(*threads, *self.local_constraints)
        )

    def variable_changes(self, layout: ComponentLayout) -> dict[str, str]:
        if layout.decomposition is None:
            raise ValueError(
                f"The layout of {self.name!r} states no decomposition, so there is no process grid to write into "
                "the suite's variables."
            )
        if self.threads_variable is None and layout.threads_per_rank != 1:
            raise ValueError(
                f"The layout of {self.name!r} runs {layout.threads_per_rank} threads per rank, but the suite states "
                "no threads, so it runs one."
            )
        nx, ny = layout.decomposition.grid
        if len(self.variables) == 1:
            changes = {self.variables[0]: f"{nx},{ny}"}
        else:
            changes = {self.variables[0]: str(nx), self.variables[1]: str(ny)}
        if self.threads_variable is not None:
            changes[self.threads_variable] = str(layout.threads_per_rank)
        return changes

    def _process_grid(self, variables: Mapping[str, str], source: str) -> tuple[int, int]:
        """Returns the extents of the process grid the variables state."""
        if len(self.variables) == 1:
            nx, ny = _integers(variables, self.variables[0], source, count=2)
        else:
            (nx,), (ny,) = (_integers(variables, name, source) for name in self.variables)
        return nx, ny

    def _threads(self, variables: Mapping[str, str], source: str) -> int:
        """Returns the threads of each rank the variables state, one where the suite states none."""
        return 1 if self.threads_variable is None else _integers(variables, self.threads_variable, source)[0]

    def read_layout(self, variables: Mapping[str, str], source: str) -> ComponentLayout:
        nx, ny = self._process_grid(variables, source)
        threads = self._threads(variables, source)
        decomposition = None if self.grid is None else DomainDecompositionSpec(self.grid, MPICartesianGrid((nx, ny)))
        return ComponentLayout(
            name=self.name,
            n_cores=nx * ny * threads,
            n_ranks=nx * ny,
            threads_per_rank=threads,
            decomposition=decomposition,
        )

    def occupied_cpus(self, variables: Mapping[str, str], source: str) -> int:
        nx, ny = self._process_grid(variables, source)
        io_ranks = 0 if self.io_server_variable is None else _integers(variables, self.io_server_variable, source)[0]
        return (nx * ny + io_ranks) * self._threads(variables, source)


@dataclass(frozen=True, kw_only=True)
class RoseRanks(RoseComponent):
    """A code given the number of ranks one variable of the suite states, and nothing else, one thread each.

    ACCESS-CM3 gives its ocean and its sea ice their cores this way. A variable holding 0 gives the code no cores,
    which is read as the suite not running it.

    Args:
        name (str): How the component tree names it.
        token (str): How `RoseSuiteConfiguration.layout_key` abbreviates it.
        variable (str): The variable stating its ranks.
    """

    name: str
    token: str
    variable: str

    @property
    def suite_variables(self) -> tuple[str, ...]:
        return (self.variable,)

    @property
    def leaf(self) -> ParallelComponent:
        return ParallelComponent(name=self.name, local_constraints=(FixedThreadsPerRankConstraint(n_threads=1),))

    def variable_changes(self, layout: ComponentLayout) -> dict[str, str]:
        return {self.variable: str(layout.n_ranks)}

    def read_layout(self, variables: Mapping[str, str], source: str) -> ComponentLayout | None:
        (ranks,) = _integers(variables, self.variable, source)
        if ranks == 0:
            return None
        return ComponentLayout(name=self.name, n_cores=ranks, n_ranks=ranks, threads_per_rank=1, decomposition=None)

    def occupied_cpus(self, variables: Mapping[str, str], source: str) -> int:
        return _integers(variables, self.variable, source)[0]


@dataclass(frozen=True, kw_only=True)
class RoseSuiteConfiguration(Application):
    """A configuration of a model driven by a Cylc Rose suite: the codes it runs, and which parsers read its logs.

    The codes run side by side, each on cores of its own, as the UM, the ocean and the sea ice of ACCESS-CM3 do. A
    suite may instead run each code as a job of its own, as ACCESS-rAM3 runs its two nests one after the other: the
    codes then share the cores, and the suite occupies as many as the largest of them.

    Args:
        name (str): Short name for this configuration, which is part of the key of every layout of it.
        model_name (str): The model's name, which names the root of the component tree together with `name`.
        components (tuple[RoseComponent, ...]): The codes the suite runs, in the order the component tree declares
            them.
        parsers (Mapping[str, ProfilingParser]): The parsers this suite's task logs are read with, keyed by name.
        core_sharing (CoreSharing): Whether the codes divide the cores between them (`CoreSharing.PARTITIONED`, the
            default), or each runs as a job of its own on the same number of cores (`CoreSharing.SHARED`).
        experiment_prefix (str): Prefix of the key of every layout of this configuration.
        walltime_variable (str | None): The variable of `[jinja2:suite.rc]` stating the walltime of the job that runs
            the layout, as an ISO 8601 duration, such as `MAIN_CLOCK` in ACCESS-AM3. None (the default) for a suite
            that states it elsewhere, which a study then changes as a change of its own.

    Raises:
        ValueError: If the name is empty, if the configuration runs no component, or if it uses one variable for
            two settings.
    """

    name: str
    model_name: str
    components: tuple[RoseComponent, ...]
    parsers: Mapping[str, ProfilingParser] = field(default_factory=dict)
    core_sharing: CoreSharing = CoreSharing.PARTITIONED
    experiment_prefix: str = "rose-layout"
    walltime_variable: str | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("RoseSuiteConfiguration.name must be non-empty: it is what tells one suite from another.")
        if not self.components:
            raise ValueError(
                f"{self.model_name} configuration {self.name!r} runs no component. There is nothing to profile."
            )
        # A variable states one setting: two settings written into it would overwrite each other, and the layout
        # read back would not be the one written.
        names = [name for component in self.components for name in component.suite_variables]
        if self.walltime_variable is not None:
            names.append(self.walltime_variable)
        shared = sorted({name for name in names if names.count(name) > 1})
        if shared:
            raise ValueError(
                f"{self.model_name} configuration {self.name!r} uses the variables {shared} for more than one "
                "setting. Give each setting a variable of its own."
            )

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        """Returns no logs, since a rose suite's are not found by name.

        A rose suite's logs are one per task per cycle, so they are found by walking the run directory rather
        than by naming files in an output directory. The manager does that walking, using `parsers`. A group of
        their regions names its component, since no log here declares one.

        Returns:
            tuple[LogSpec, ...]: An empty tuple.
        """
        return ()

    @property
    def _root_name(self) -> str:
        """Returns the name of the root of the component tree: the model's and the configuration's."""
        return f"{self.model_name} {self.name}"

    @property
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how this suite is parallelised.

        Returns:
            ParallelComponent: Root of the component tree, the components under it as the suite runs them.

        Raises:
            ValueError: If a component states too little for its layouts to be searched for, as a process grid with
                no grid does.
        """
        return ParallelComponent(
            name=self._root_name,
            subcomponents=tuple(component.leaf for component in self.components),
            core_sharing=self.core_sharing,
        )

    def _component_layouts(self, layout: ComponentLayout) -> tuple[tuple[RoseComponent, ComponentLayout], ...]:
        """Returns what each component was given, paired with the component.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            tuple[tuple[RoseComponent, ComponentLayout], ...]: Each component and its layout, in tree order.

        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        names = [component.name for component in self.components]
        given = [sub_layout.name for sub_layout in layout.sub_layouts]
        if given != names:
            raise ValueError(
                f"The layout {layout.name!r} gives cores to {given}, but {self.model_name} configuration "
                f"{self.name!r} runs {names}."
            )
        return tuple(zip(self.components, layout.sub_layouts, strict=True))

    def layout_key(self, layout: ComponentLayout) -> str:
        """Returns the key identifying a given layout.

        The key records the configuration and then what each component received: its process grid where it
        decomposes one, and its ranks otherwise, followed by its threads where it runs more than one per rank.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            str: The key, such as `am3-layout_n96e_atm_16x6` or `am3-layout_n96e_atm_8x6_t2`.

        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        parts = []
        for component, sub_layout in self._component_layouts(layout):
            decomposition = sub_layout.decomposition
            size = str(sub_layout.n_ranks) if decomposition is None else "x".join(map(str, decomposition.grid))
            threads = "" if sub_layout.threads_per_rank == 1 else f"_t{sub_layout.threads_per_rank}"
            parts.append(f"{component.token}_{size}{threads}")
        return f"{self.experiment_prefix}_{self.name}_{'_'.join(parts)}"

    def config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the changes to the suite's variables needed to run a given layout.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            dict: The values of the variables, as the file is to state them, keyed by variable, under the section of
                `rose-suite.conf` that holds them: `{"rose-suite.conf": {"jinja2:suite.rc": {...}}}`.

        Raises:
            ValueError: If the layout is not a layout of this configuration, or cannot be written into the
                variables, as a layout read back from a component with no grid cannot.
        """
        variables: dict[str, str] = {}
        for component, sub_layout in self._component_layouts(layout):
            variables.update(component.variable_changes(sub_layout))
        return {SUITE_CONF: {SUITE_VARIABLES: variables}}

    def parse_layout(self, output_dir: Path) -> ComponentLayout | None:
        """Returns the layout a suite runs, read back out of the variables of its `rose-suite.conf`.

        `CylcRoseManager` reads a suite that ran from the configuration its run recorded instead, with
        `read_layout`.

        Args:
            output_dir (Path): The suite directory. Must contain a `rose-suite.conf`.

        Returns:
            ComponentLayout | None: The layout, or None if the variables give no component any cores.

        Raises:
            FileNotFoundError: If the suite has no `rose-suite.conf`.
            ValueError: If the file has no `[jinja2:suite.rc]` section, or if a variable a component names is not
                set in it or does not hold whole numbers.
        """
        return self.read_layout(output_dir / SUITE_CONF)

    def read_layout(self, conf_path: Path) -> ComponentLayout | None:
        """Returns the layout the variables of a suite state, from the file they are read from.

        A process grid whose component states its grid comes back with its decomposition, so that a layout written
        with `config_changes` is read back as it was, the cores of its root aside: what the root held beyond what its
        components were given is not in the file. A component the variables give no cores is left out. The manager
        finds the file, as for `occupied_cpus`.

        Args:
            conf_path (Path): The suite's `rose-suite.conf`, or the copy of it the run recorded.

        Returns:
            ComponentLayout | None: The layout, or None if the variables give no component any cores.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file has no `[jinja2:suite.rc]` section, or if a variable a component names is not
                set in it or does not hold whole numbers.
        """
        variables = read_section(conf_path, SUITE_VARIABLES)
        sub_layouts = tuple(
            sub_layout
            for component in self.components
            if (sub_layout := component.read_layout(variables, str(conf_path))) is not None
        )
        if not sub_layouts:
            return None
        # Components sharing the cores each sit on all of them, so the root holds as many as the largest.
        combine = max if self.core_sharing is CoreSharing.SHARED else sum
        return ComponentLayout(
            name=self._root_name,
            n_cores=combine(sub_layout.n_cores for sub_layout in sub_layouts),
            n_ranks=combine(sub_layout.n_ranks for sub_layout in sub_layouts),
            threads_per_rank=None,
            decomposition=None,
            sub_layouts=sub_layouts,
            core_sharing=self.core_sharing,
        )

    def occupied_cpus(self, conf_path: Path) -> int:
        """Returns the number of CPUs a suite occupies, from the variables it states them in.

        The manager finds the file, since where it is and how it is written is the runner's business; what its
        variables are called, and so what they mean, is the suite's. Components that divide the cores add up, and
        components that share them occupy as many as the largest of them.

        Args:
            conf_path (Path): The suite's `rose-suite.conf`, or the copy of it the run recorded.

        Returns:
            int: The number of CPUs.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file has no `[jinja2:suite.rc]` section, or if a variable a component names is not
                set in it or does not hold whole numbers.
        """
        variables = read_section(conf_path, SUITE_VARIABLES)
        counts = [component.occupied_cpus(variables, str(conf_path)) for component in self.components]
        return max(counts) if self.core_sharing is CoreSharing.SHARED else sum(counts)
