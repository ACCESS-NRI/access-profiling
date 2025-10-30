# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from pathlib import Path

import xarray as xr

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

    def find_component_datasets(self, path: Path) -> dict[str, ProfilingLog]:
        """Returns available profiling logs for the components in the configuration.

        Args:
            path (Path): Path to the output directory.

        Returns:
            dict[str, ProfilingLog]: Dictionary mapping component names to their ProfilingLog instances.

        Raises:
            RuntimeError: If no logs could be found.
        """
        datasets = {}
        # matches <cycle> / <task> / NN / job.out e.g. 20220226T0000Z/Lismore_d1100_GAL9_um_fcst_000/NN/job.out
        # NN is the last attempt
        # job.out is the stdout
        # this pattern is followed for all cylc workflows.
        # as tasks of interest will likely have their own logging regions e.g. UM each task_cycle is
        # treated as a "component" of the configuration.
        for logfile in path.glob("*/*/NN/job.out"):
            cycle, task = logfile.parts[-4:-2]
            for parser_name, parser in self.known_parsers.items():
                # parsers raise an error if the log doesn't contain valid data.
                # skip log if parsing fails.
                try:
                    datasets[f"{task}_cycle{cycle}_{parser_name}"] = ProfilingLog(logfile, parser).parse()
                    continue
                except ValueError:  # all the parsers raise a ValueError if they can't find matching data
                    pass

        if datasets == {}:
            raise RuntimeError(f"Could not find any known logs in {path}")

        return datasets

    def parse_profiling_data(self, path: Path) -> dict[str, xr.Dataset]:
        """Parses profiling data from a Cylc Rose experiment directory.

        Args:
            path (Path): Path to the Cylc Rose experiment directory.

        Returns:
            dict[str, xr.Dataset]: Dictionary mapping component names to their profiling datasets.

        Raises:
            FileNotFoundError: If the suite log or cylc-suite.db files are missing.
            RuntimeError: If the expected cylc task table is not found in the cylc-suite.db file.
        """
        datasets = {}
        logs = {}

        # setup log paths
        suite_log = path / "log/suite/log"  # cylc log file
        cylcdb = path / "cylc-suite.db"  # database with task runtimes
        jobdir = path / "log/job"  # where task logs are stored

        logs["cylc_suite_log"] = ProfilingLog(suite_log, CylcProfilingParser())
        # cylcdb.read_text = lambda x: x # hack to make log work
        logs["cylc_tasks"] = ProfilingLog(cylcdb, CylcDBReader())

        for name, log in logs.items():
            logger.info(f"Parsing {name} profiling log: {log.filepath}.")
            datasets[name] = log.parse()
            logger.info(" Done.")

        # find known component datasets
        datasets.update(self.find_component_datasets(jobdir))

        return datasets
