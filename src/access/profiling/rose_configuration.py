# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""What a Cylc Rose suite states about itself.

Everything here knows that a rose suite is the runner: that its parallelism is stated as variables in
`rose-suite.conf`, and that which variables those are is what tells one suite from another. None of it belongs
in the generic contract.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from access.config.parallel_component import ComponentLayout, ParallelComponent

from access.profiling.configuration import LogSpec, ModelConfiguration
from access.profiling.parser import ProfilingParser


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
