# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import shutil
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path

from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.experiment import ProfilingExperiment, ProfilingExperimentStatus, ProfilingLog
from access.profiling.manager import ProfilingManager
from access.profiling.parser import ProfilingParser

logger = logging.getLogger(__name__)


class CylcRoseManager(ProfilingManager, ABC):
    """Abstract base class to handle profiling data for Cylc Rose configurations.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        layout_variable (str | tuple[str, str]): Name(s) of the variable(s) in rose-suite-run.conf that define the
            layout. A single name (e.g. rAM3's "rg01_rs01_m01_nproc") is treated as a comma-separated "x,y" tuple.
            A pair of names (e.g. AM3's ("MAIN_ATM_PROCX", "MAIN_ATM_PROCY")) is treated as separate x and y
            variables to multiply together.
        cpus_per_proc_variable (str | None): Name of the variable in rose-suite-run.conf that defines the number of
            OpenMP threads per MPI process (e.g. AM3's "MAIN_OMPTHR_ATM"). If given, the layout size is multiplied
            by this value to give the total number of CPUs. Defaults to None, i.e. one thread per process.
        io_server_variable (str | None): Name of the variable in rose-suite-run.conf that defines the number of I/O
            server ranks (e.g. AM3's "MAIN_IOS_NPROC"). If given, these ranks are added to the layout size before
            applying cpus_per_proc_variable, matching how PBS resources are requested (see the pbs_cpus/pbs_mem
            Jinja2 macros in site/nci_gadi.rc: cpus(x,y,i,nt) = (x*y+i)*nt). Defaults to None, i.e. no I/O server
            ranks.
    """

    # Name(s) of the variable(s) in rose-suite-run.conf file that define the layout.
    _layout_variable: str | tuple[str, str]
    # Name of the variable in rose-suite-run.conf that defines the number of OpenMP threads per MPI process.
    _cpus_per_proc_variable: str | None
    # Name of the variable in rose-suite-run.conf that defines the number of I/O server ranks.
    _io_server_variable: str | None

    def __init__(
        self,
        work_dir: Path,
        archive_dir: Path,
        layout_variable: str | tuple[str, str],
        cpus_per_proc_variable: str | None = None,
        io_server_variable: str | None = None,
    ):
        super().__init__(work_dir, archive_dir)
        self._layout_variable = layout_variable
        self._cpus_per_proc_variable = cpus_per_proc_variable
        self._io_server_variable = io_server_variable

    @property
    @abstractmethod
    def known_parsers(self) -> dict[str, ProfilingParser]:
        """Returns the parsers that this model configuration knows about.

        Returns:
            dict[str, ProfilingParser]: a dictionary of known parsers with names as keys.
        """

    def parse_ncpus(self, path: Path, run_path: Path | None = None) -> int:
        # both the run and original config will store cpu information
        config_paths = []
        if run_path is not None:
            config_paths.append(run_path / "log/rose-suite-run.conf")
        config_paths.append(path / "rose-suite.conf")

        config_path = next((candidate for candidate in config_paths if candidate.is_file()), None)
        if config_path is None:
            tried = ", ".join(str(p) for p in config_paths)
            raise FileNotFoundError(f"Could not find suitable config file. Tried: {tried}")

        config = self._parse_rose_conf(config_path)

        if isinstance(self._layout_variable, tuple):
            x_key, y_key = self._layout_variable
            missing = [key for key in (x_key, y_key) if key not in config]
            if missing:
                raise ValueError(f"Cannot find layout key(s) {missing} in {config_path}.")
            layout_size = int(config[x_key]) * int(config[y_key])
        else:
            if self._layout_variable not in config:
                raise ValueError(f"Cannot find layout key, {self._layout_variable}, in {config_path}.")
            nx, ny = config[self._layout_variable].split(",")
            layout_size = int(nx.strip()) * int(ny.strip())

        if self._io_server_variable is not None:
            if self._io_server_variable not in config:
                raise ValueError(f"Cannot find I/O server key, {self._io_server_variable}, in {config_path}.")
            layout_size += int(config[self._io_server_variable])

        if self._cpus_per_proc_variable is not None:
            if self._cpus_per_proc_variable not in config:
                raise ValueError(f"Cannot find cpus-per-process key, {self._cpus_per_proc_variable}, in {config_path}.")
            layout_size *= int(config[self._cpus_per_proc_variable])

        return layout_size

    # TODO: use "real" parser from access-config-utils once implemented.
    @staticmethod
    def _parse_rose_conf(config_path: Path) -> dict[str, str]:
        """Parses a rose-suite.conf-style file into a dict mapping variable name to its raw string value."""
        config = {}
        for line in config_path.read_text().splitlines():
            if not line.startswith("!!") and "=" in line:
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()
        return config

    def add_rose_experiment(self, rose: str, run_path: Path | None = None) -> None:
        """Adds the given rose as an experiment to this manager.

        Args:
            rose (str): The rose to add as an experiment.
            run_path (Path | None): Path to the Cylc run directory holding the results. If not provided, or if the
                provided directory does not exist, archiving will only include the experiment files.

        Raises:
            ValueError: If the experiment path does not exist.
        """
        experiment_path = self.work_dir / rose
        if not experiment_path.is_dir():
            raise ValueError(f"Experiment path '{experiment_path}' does not exist or is not a directory.")

        if run_path is not None and not run_path.is_dir():
            logger.warning(f"Run path '{run_path}' does not exist. Archiving will only include experiment files.")
            run_path = None

        self.experiments[rose] = ProfilingExperiment(path=experiment_path, run_path=run_path)
        self.experiments[rose].status = ProfilingExperimentStatus.DONE

    def run_experiments(self) -> None:
        """Runs Rose Cylc experiments via `rose suite-run` for profiling data generation."""

        to_run = {name: exp for name, exp in self.experiments.items() if exp.status == ProfilingExperimentStatus.NEW}

        if not to_run:
            logger.info("No new experiments to run. Will skip execution.")
            return

        for name, exp in to_run.items():
            logger.info(f"Running experiment '{name}' via rose suite-run in '{exp.path}'.")
            try:
                result = subprocess.run(["rose", "suite-run"], cwd=exp.path, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                for line in e.stdout.splitlines():
                    logger.info(f"[{name}] {line}")
                for line in e.stderr.splitlines():
                    logger.error(f"[{name}] {line}")
                raise
            for line in result.stdout.splitlines():
                logger.info(f"[{name}] {line}")
            for line in result.stderr.splitlines():
                logger.warning(f"[{name}] {line}")
            exp.status = ProfilingExperimentStatus.RUNNING

        # TODO: properly detect when running experiments have completed rather than marking them done immediately.
        for exp_name in self.experiments:
            if self.experiments[exp_name].status == ProfilingExperimentStatus.RUNNING:
                self.experiments[exp_name].status = ProfilingExperimentStatus.DONE

    def _delete_experiment(self, name: str, dry_run: bool) -> None:
        """Deletes the experiment and run directories of a single Rose Cylc experiment.

        Args:
            name (str): Name of the experiment to delete.
            dry_run (bool): If True, logs what would be deleted without making any changes.
        """
        exp = self.experiments[name]
        exp_path = exp.path
        run_path = exp.run_path
        if dry_run:
            logger.info(f"Dry run: would delete experiment directory '{exp_path}' and run directory '{run_path}'.")
            return
        if exp_path.is_dir():
            logger.info(f"Deleting experiment directory '{exp_path}'.")
            shutil.rmtree(exp_path)
        else:
            logger.warning(f"Experiment directory '{exp_path}' does not exist. Skipping deletion.")
        if run_path is not None:
            if run_path.is_dir():
                logger.info(f"Deleting run directory '{run_path}'.")
                shutil.rmtree(run_path)
            else:
                logger.warning(f"Run directory '{run_path}' does not exist. Skipping deletion.")

    def archive_experiments(
        self,
        exclude_dirs: list[str] | None = None,
        exclude_files: list[str] | None = None,
        follow_symlinks: bool = False,
        overwrite: bool = False,
    ) -> None:
        """Archives completed experiments to the specified archive path.

        Args:
            exclude_dirs (list[str] | None): Directory patterns to exclude when archiving. Defaults to
                [".svn", "share"] if not provided.
            exclude_files (list[str] | None): File patterns to exclude when archiving. Defaults to
                ["*.nc"] if not provided.
            follow_symlinks (bool): Whether to follow symlinks when archiving. Defaults to False.
            overwrite (bool): Whether to overwrite existing archives. Defaults to False.
        """
        if exclude_dirs is None:
            exclude_dirs = [".svn", "share"]
        if exclude_files is None:
            exclude_files = ["*.nc"]
        super().archive_experiments(
            exclude_dirs=exclude_dirs,
            exclude_files=exclude_files,
            follow_symlinks=follow_symlinks,
            overwrite=overwrite,
        )

    def profiling_logs(self, path: Path, run_path: Path | None = None) -> dict[str, dict[int, ProfilingLog]]:
        """Returns all profiling logs from the specified path.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Path to the Cylc run directory.
        Returns:
            dict[str, dict[int, ProfilingLog]]: Dictionary mapping log names to their logs, keyed by run number.
                Cylc workflows have no concept of repeated runs, so every log is returned as the single run 0.
        """
        if run_path is None:
            raise ValueError("Cylc run_path is required to locate profiling logs.")

        logs = {}

        # setup log paths
        suite_log = run_path / "log/suite/log"  # cylc log file
        cylcdb = run_path / "cylc-suite.db"  # database with task runtimes
        jobdir = run_path / "log/job"  # where task logs are stored

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

        # Cylc workflows have no concept of repeated runs, so every log is registered as the single run 0.
        return {name: {0: log} for name, log in logs.items()}
