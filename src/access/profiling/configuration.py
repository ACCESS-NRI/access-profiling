# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""What is being profiled, as opposed to what runs it.

A profiling study varies along two axes that are not the same kind of thing. How a model is *run* - where the
engine puts its output, what the scheduler recorded, how a job is submitted - is behaviour, and it belongs to a
manager. What is being run - which components exist, on which grids, which files state the parallelism, which
logs to read - is data, and it belongs here.

The rule that places a member is which side of that line it reads from:

    The manager reads what the engine and the scheduler wrote.
    The configuration reads what the model's own configuration files say.

So finding `archive/output000` is the manager's, because the engine chose that name, while reading
`nuopc.runconfig` is the configuration's, because the model chose what that file means. The pair that writes a
layout into the model's files and reads one back out of them therefore lives here, together: they are inverses
over the same text, and keeping them apart is how they drift.

A model is a *value* of this type, not a subclass of a manager. Two configurations of one model differ in data,
and a new model is a new module of data rather than a new class in the hierarchy.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from access.config import YAMLParser
from access.config.parallel_component import ComponentLayout, ParallelComponent

from access.profiling.experiment import ProfilingLog
from access.profiling.parser import ProfilingParser

# How to find one log inside an output directory. Takes the directory, returns the file, or None when the
# configuration that wrote the directory does not say where it is.
LogLocator = Callable[[Path], Path | None]


def log_at(relative_path: str) -> LogLocator:
    """Returns a locator for a log at a fixed place inside an output directory.

    Args:
        relative_path (str): Path of the log, relative to the output directory.

    Returns:
        LogLocator: The locator.
    """

    def locate(output_dir: Path) -> Path | None:
        return output_dir / relative_path

    return locate


def payu_model_stdout() -> LogLocator:
    """Returns a locator for the standard output Payu captured from the model.

    Payu names it after the model it ran, which the archived `config.yaml` records, so the name is read from
    there rather than assumed.

    Returns:
        LogLocator: The locator.
    """

    def locate(output_dir: Path) -> Path | None:
        config_path = output_dir / "config.yaml"
        if not config_path.is_file():
            return None
        payu_config = YAMLParser().parse(config_path.read_text())
        model = payu_config.get("model")
        return None if model is None else output_dir / f"{model}.out"

    return locate


def um_stdout() -> LogLocator:
    """Returns a locator for the UM's standard output.

    The UM writes one file per rank and names them from `UM_STDOUT_FILE` in its own environment file; the
    timings are in rank 0's.

    Returns:
        LogLocator: The locator.
    """

    def locate(output_dir: Path) -> Path | None:
        um_env_path = output_dir / "atmosphere" / "um_env.yaml"
        if not um_env_path.is_file():
            return None
        um_env = YAMLParser().parse(um_env_path.read_text())
        stem = um_env.get("UM_STDOUT_FILE")
        return None if stem is None else output_dir / "atmosphere" / f"{stem}0"

    return locate


@dataclass(frozen=True)
class LogSpec:
    """One profiling log a configuration writes, declared rather than discovered.

    Args:
        name (str): What the log is called in the parsed data, and so the `component` argument a caller passes
            to the plotting and selection methods. User-facing: changing it changes a published name.
        locator (LogLocator): How to find the file inside one output directory.
        parser (ProfilingParser): What reads it.
        component (str | None): The component of the layout whose cores this log's regions ran on, named as
            the component tree names it. None when the log is not one component's alone - a coupled profile
            covering several - in which case a caller naming regions says which component each group is.
        optional (bool): Whether a run may legitimately not produce this log.
    """

    name: str
    locator: LogLocator
    parser: ProfilingParser
    component: str | None = None
    optional: bool = False

    def resolve(self, output_dir: Path) -> ProfilingLog | None:
        """Returns this log in a given output directory, or None if it is not there.

        Args:
            output_dir (Path): The output directory to look in.

        Returns:
            ProfilingLog | None: The log, or None if the file does not exist.
        """
        path = self.locator(output_dir)
        if path is None or not path.is_file():
            return None
        return ProfilingLog(path, self.parser, optional=self.optional)


@dataclass(frozen=True)
class ExperimentPlan:
    """One experiment a manager has decided to create, before any engine has touched it.

    Args:
        name (str): What identifies the experiment. It is the key this manager holds it under, the branch or
            directory it lives in, and the name written into the experiment's own configuration, so it has to
            be distinct for every distinct layout and stable from one session to the next.
        layout (ComponentLayout): The layout the experiment runs.
        changes (dict): The configuration file changes realising that layout, keyed by the path of each file
            relative to the control directory.
        walltime_hours (float): Walltime to request.
        regenerating (bool): Whether this plan corrects an experiment the manager already holds, rather than
            describing a new one.
    """

    name: str
    layout: ComponentLayout
    changes: dict
    walltime_hours: float
    regenerating: bool = False


class ModelConfiguration(ABC):
    """Abstract base class for one configuration of one ACCESS model.

    A configuration is a model set up a particular way: which components it runs, on which grids, and what its
    own configuration files have to say for a given layout. It is deliberately not a subclass of anything that
    runs experiments - the same configuration can be profiled by any manager whose runner drives it.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns a short name for this configuration, distinguishing it from others of the same model.

        It appears in the name of every experiment generated for it, which is how two configurations sharing a
        grid and a component set stay apart on disk.

        Returns:
            str: The name.
        """

    @property
    @abstractmethod
    def experiment_prefix(self) -> str:
        """Returns the prefix every experiment of this configuration's model is named with.

        Shared by every configuration of one model, so that a working directory holding several models can
        tell which experiments are whose.

        Returns:
            str: The prefix.
        """

    @property
    @abstractmethod
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how this configuration is parallelised.

        Returns:
            ParallelComponent: Root of the tree, holding the domains and the requirements every valid layout of
                this configuration must satisfy. What a particular study wants belongs in the allocation
                strategy passed to the layout search instead.
        """

    @property
    @abstractmethod
    def logs(self) -> tuple[LogSpec, ...]:
        """Returns the profiling logs this configuration's components write.

        Returns:
            tuple[LogSpec, ...]: One entry per log. A configuration without a component writes none of its
                logs, so this follows the components rather than the model.
        """

    @abstractmethod
    def experiment_name(self, layout: ComponentLayout) -> str:
        """Returns the name identifying the experiment that runs a given layout.

        Must be distinct for every distinct layout and the same every time for the same one: it is what tells
        a manager whether it already holds an experiment, across sessions as well as within one.

        Args:
            layout (ComponentLayout): The layout, as returned by the layout search.

        Returns:
            str: The name.

        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """

    @abstractmethod
    def config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run a given layout.

        Args:
            layout (ComponentLayout): The layout, as returned by the layout search.

        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control
                directory.

        Raises:
            ValueError: If the layout is not a layout of this configuration, or cannot be realised by it.
        """

    @abstractmethod
    def parse_layout(self, output_dir: Path) -> ComponentLayout | None:
        """Returns the layout an experiment ran, read back out of its configuration files.

        The inverse of `config_changes`, over the same files. Kept beside it deliberately: a change to what one
        writes is a change to what the other reads.

        Args:
            output_dir (Path): Directory holding the configuration files the experiment ran with.

        Returns:
            ComponentLayout | None: The layout, or None if the files do not say.
        """

    def component_logs(self, output_dir: Path) -> dict[str, ProfilingLog]:
        """Returns the logs present in one output directory.

        Args:
            output_dir (Path): The output directory.

        Returns:
            dict[str, ProfilingLog]: The logs that are there, keyed by their names. A log a run did not write
                is absent rather than present and empty.
        """
        found = {}
        for spec in self.logs:
            log = spec.resolve(output_dir)
            if log is not None:
                found[spec.name] = log
        return found

    def component_for_log(self, log_name: str) -> str | None:
        """Returns the component whose cores a log's regions ran on.

        Args:
            log_name (str): Name of the log.

        Returns:
            str | None: The component, as the component tree names it, or None if the log is not one
                component's alone or is not one of this configuration's.
        """
        for spec in self.logs:
            if spec.name == log_name:
                return spec.component
        return None


class PayuConfiguration(ModelConfiguration):
    """Abstract base class for a configuration of a model driven by Payu."""

    @property
    @abstractmethod
    def model_type(self) -> str:
        """Returns the model type identifier, as Payu defines it.

        Returns:
            str: The identifier, e.g. "access-esm1.6".
        """


@dataclass(frozen=True)
class RoseSuiteConfiguration(ModelConfiguration):
    """A configuration of a model driven by a Cylc Rose suite.

    What distinguishes one rose suite from another, for profiling purposes, is the names its
    `rose-suite.conf` gives the variables stating its parallelism, and which parsers read its task logs. Both
    are data, which is why ACCESS-rAM3 and ACCESS-AM3 need no code of their own to tell them apart.

    Args:
        name (str): Short name for this configuration.
        layout_variable (str | tuple[str, str]): Name of the variable stating the process layout, or the pair
            of names stating its two extents separately.
        parsers (Mapping[str, ProfilingParser]): The parsers this suite's task logs are read with, keyed by
            name.
        experiment_prefix (str): Prefix for the names of experiments generated for this configuration.
        cpus_per_proc_variable (str | None): Name of the variable stating OpenMP threads per MPI process, if
            the suite has one.
        io_server_variable (str | None): Name of the variable stating the number of I/O server ranks, if the
            suite has one.

    Raises:
        ValueError: If no name or no layout variable is given.
    """

    # These three are dataclass fields rather than properties because that is all it takes to satisfy the
    # abstract properties of the base class: a field with a default leaves a value in the class namespace, and
    # a plain value is not a data descriptor, so each instance reads back its own. A field with no default
    # would leave the member abstract and the class uninstantiable, which is why name is defaulted and then
    # checked below rather than simply required.
    name: str = ""
    layout_variable: str | tuple[str, str] = ""
    parsers: Mapping[str, ProfilingParser] = field(default_factory=dict)
    experiment_prefix: str = "rose-layout"
    cpus_per_proc_variable: str | None = None
    io_server_variable: str | None = None

    _unsupported = (
        "Layout generation is not supported for Cylc Rose configurations yet: a rose suite states its "
        "parallelism in rose-suite.conf, and nothing here yet knows how to search over that."
    )

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("RoseSuiteConfiguration.name must be non-empty: it is what tells one suite from another.")
        if not self.layout_variable:
            raise ValueError("RoseSuiteConfiguration.layout_variable must name at least one variable.")

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        # A rose suite's logs are one per task per cycle, so they are found by walking the run directory
        # rather than by naming files in an output directory. The manager does that walking, using `parsers`.
        return ()

    @property
    def parallel_component(self) -> ParallelComponent:
        raise NotImplementedError(self._unsupported)

    def experiment_name(self, layout: ComponentLayout) -> str:
        raise NotImplementedError(self._unsupported)

    def config_changes(self, layout: ComponentLayout) -> dict:
        raise NotImplementedError(self._unsupported)

    def parse_layout(self, output_dir: Path) -> ComponentLayout | None:
        raise NotImplementedError(self._unsupported)

    def occupied_cpus(self, rose_conf: dict, source: str = "the rose configuration") -> int:
        """Returns the number of CPUs a suite occupies, from the variables it states them in.

        The manager finds and reads `rose-suite.conf`, since where it is and how it is written is the
        runner's business; what its variables are called, and so what they mean, is the suite's.

        A variable this configuration names and the file does not hold is an error rather than a zero: it
        means the configuration describes a different suite from the one being read, and a plausible-looking
        core count from the wrong suite is worse than a refusal.

        Args:
            rose_conf (dict): The parsed `rose-suite.conf`.
            source (str): What to call the file in an error message. The manager knows the path; this class
                only knows the variable names.

        Returns:
            int: The number of CPUs.

        Raises:
            ValueError: If a variable this configuration names is not in the file.
        """
        if isinstance(self.layout_variable, tuple):
            x_key, y_key = self.layout_variable
            missing = [key for key in (x_key, y_key) if key not in rose_conf]
            if missing:
                raise ValueError(f"Cannot find layout key(s) {missing} in {source}.")
            n_cpus = int(rose_conf[x_key]) * int(rose_conf[y_key])
        else:
            if self.layout_variable not in rose_conf:
                raise ValueError(f"Cannot find layout key, {self.layout_variable}, in {source}.")
            nx, ny = rose_conf[self.layout_variable].split(",")
            n_cpus = int(nx.strip()) * int(ny.strip())

        if self.io_server_variable is not None:
            if self.io_server_variable not in rose_conf:
                raise ValueError(f"Cannot find I/O server key, {self.io_server_variable}, in {source}.")
            n_cpus += int(rose_conf[self.io_server_variable])

        if self.cpus_per_proc_variable is not None:
            if self.cpus_per_proc_variable not in rose_conf:
                raise ValueError(f"Cannot find cpus-per-process key, {self.cpus_per_proc_variable}, in {source}.")
            n_cpus *= int(rose_conf[self.cpus_per_proc_variable])

        return n_cpus
