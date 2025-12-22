# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from pathlib import Path

from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.experiment import ProfilingLog
from access.profiling.manager import ProfilingManager
from access.profiling.parser import ProfilingParser

logger = logging.getLogger(__name__)


class CylcRoseManager(ProfilingManager, ABC):
    """Abstract base class to handle profiling data for Cylc Rose configurations."""

    @abstractmethod
    def parse_ncpus(self, path: Path) -> int:
        """Parses the number of CPUs used in a given Cylc experiment.

        Args:
            path (Path): Path to the Payu experiment directory. Must contain a rose-suite.conf file.

        Returns:
            int: Number of CPUs used in the experiment. If multiple submodels are defined, returns the sum of their
                 cpus.
        """

    @property
    @abstractmethod
    def known_parsers(self) -> dict[str, ProfilingParser]:
        """Returns the parsers that this model configuration knows about.

        Returns:
            dict[str, ProfilingParser]: a dictionary of known parsers with names as keys.
        """

    def profiling_logs(self, path: Path) -> dict[str, ProfilingLog]:
        """Returns all profiling logs from the specified path.

        Args:
            path (Path): Path to the experiment directory.
        Returns:
            dict[str, ProfilingLog]: Dictionary of profiling logs.
        """
        logs = {}

        # setup log paths
        suite_log = path / "log/suite/log"  # cylc log file
        cylcdb = path / "cylc-suite.db"  # database with task runtimes
        jobdir = path / "log/job"  # where task logs are stored

        logs["cylc_suite_log"] = ProfilingLog(suite_log, CylcProfilingParser())
        # cylcdb.read_text = lambda x: x # hack to make log work
        logs["cylc_tasks"] = ProfilingLog(cylcdb, CylcDBReader())

        # Search for available profiling logs for the components in the configuration.
        # matches <cycle> / <task> / NN / job.out e.g. 20220226T0000Z/Lismore_d1100_GAL9_um_fcst_000/NN/job.out
        # NN is the last attempt
        # job.out is the stdout
        # this pattern is followed for all cylc workflows.
        # as tasks of interest will likely have their own logging regions e.g. UM each task_cycle is
        # treated as a "component" of the configuration.
        possible_component_logs = list(jobdir.glob("*/*/NN/job.out"))
        if not possible_component_logs:
            raise RuntimeError(f"Could not find any known logs in {jobdir}")

        for logfile in possible_component_logs:
            cycle, task = logfile.parts[-4:-2]
            for parser_name, parser in self.known_parsers.items():
                logs[f"{task}_cycle{cycle}_{parser_name}"] = ProfilingLog(logfile, parser, optional=True)

        return logs
