# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""What is being profiled, as opposed to what runs it.

A profiling study varies along two axes that are not the same kind of thing. How an application is *run* -
where the engine puts its output, what the scheduler recorded, how a job is submitted - is behaviour, and it
belongs to a manager. What is being run - which components exist, over which domains, which files state the
parallelism, which logs to read - is data, and it belongs here.

The rule that places a member is which side of that line it reads from:

    The manager reads what the engine and the scheduler wrote.
    The application reads what its own input files say.

So finding an engine's output directory is the manager's, because the engine chose that name, while reading
the file that states how the work is divided is the application's, because the application chose what that
file means. The pair that writes a layout into those files and reads one back out of them therefore lives
here, together: they are inverses over the same text, and keeping them apart is how they drift.

Nothing here knows which engine runs the application, and nothing here names a particular application. An
application is a *value* of this type, not a subclass of a manager: two setups of one application differ in
data, and a new application is a new module of data rather than a new class in the hierarchy. Anything that
has to know what a particular runner or code writes belongs in a module named for it.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

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


class Application(ABC):
    """Abstract base class for one application, set up one particular way.

    An application states which components it runs, over which domains, and what its own input files have to
    say for a given layout. It is deliberately not a subclass of anything that runs experiments - the same
    application can be profiled by any manager whose runner drives it.

    Attributes:
        name (str): A short name for this setup, distinguishing it from others of the same application. It
            appears in the name of every experiment generated for it, which is how two setups sharing a
            domain and a component set stay apart on disk.
        experiment_prefix (str): The prefix every experiment of this application is named with. Shared by
            every setup of one application, so that a working directory holding several applications can
            tell which experiments are whose.

    These two are declared as attributes rather than abstract properties so that a subclass can satisfy them
    with a plain dataclass field, which is what every one of them does. The cost is that nothing refuses a
    subclass that forgets one until something reads it.
    """

    name: str
    experiment_prefix: str

    @property
    @abstractmethod
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how this application is parallelised.

        Returns:
            ParallelComponent: Root of the tree, holding the domains and the requirements every valid layout
                of this application must satisfy. What a particular study wants belongs in the allocation
                strategy passed to the layout search instead.
        """

    @property
    @abstractmethod
    def logs(self) -> tuple[LogSpec, ...]:
        """Returns the profiling logs this application's components write.

        Returns:
            tuple[LogSpec, ...]: One entry per log. An application without a component writes none of its
                logs, so this follows the components rather than the application as a whole.
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
