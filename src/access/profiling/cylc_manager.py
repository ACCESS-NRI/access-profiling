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
    """Abstract base class to handle profiling data for Cylc Rose configurations.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        layout_variable (str): Name of the variable in rose-suite-run.conf file that defines the layout.
    """

    _layout_variable: str  # Name of the variable in rose-suite-run.conf file that defines the layout.

    def __init__(self, work_dir: Path, archive_dir: Path, layout_variable: str):
        super().__init__(work_dir, archive_dir)
        self._layout_variable = layout_variable

    @property
    @abstractmethod
    def known_parsers(self) -> dict[str, ProfilingParser]:
        """Returns the parsers that this model configuration knows about.

        Returns:
            dict[str, ProfilingParser]: a dictionary of known parsers with names as keys.
        """

    def parse_ncpus(self, path: Path) -> int:
        # this is a symlink
        config_path = path / "log/rose-suite-run.conf"

        if not config_path.is_file():
            raise FileNotFoundError(f"Could not find suitable config file in {config_path}")

        for line in config_path.read_text().splitlines():
            if not line.startswith("!!"):
                keypair = line.split("=")
                if keypair[0].strip() == self._layout_variable:
                    layout = keypair[1].split(",")
                    return int(layout[0].strip()) * int(layout[1].strip())

        raise ValueError(f"Cannot find layout key, {self._layout_variable}, in {config_path}.")

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
