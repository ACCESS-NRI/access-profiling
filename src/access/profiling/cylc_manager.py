# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import shutil
import sqlite3
import subprocess
from pathlib import Path

from access.profiling.control import ControlSource
from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.experiment import (
    ExperimentPlan,
    ProfilingExperiment,
    ProfilingExperimentStatus,
    ProfilingLog,
)
from access.profiling.manager import ProfilingManager
from access.profiling.rose_configuration import RoseSuiteConfiguration

logger = logging.getLogger(__name__)


class CylcRoseManager(ProfilingManager):
    """Profiling of any ACCESS model driven by a Cylc Rose suite.

    One class for every rose suite: what tells ACCESS-AM3 from ACCESS-rAM3 is the names their
    `rose-suite.conf` files give the variables stating their parallelism, and which parsers read their task
    logs, both of which arrive as a RoseSuiteConfiguration. What is written here is the engine - `rose
    suite-run`, the Cylc run directory and the suite database the scheduler writes - and none of it is any one
    suite's.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        configuration (RoseSuiteConfiguration): The suite configuration being profiled.
        control (ControlSource | None): Where the control suite comes from. Recorded rather than acted on:
            unlike the Payu side, nothing here clones or checks out, so the suite is expected to be in the
            working directory already - `rosie checkout` or `git clone` having been run in a terminal, where
            a credentials prompt can be answered. None (the default) records nothing.
    """

    def __init__(
        self,
        work_dir: Path,
        archive_dir: Path,
        configuration: RoseSuiteConfiguration,
        control: ControlSource | None = None,
    ):
        super().__init__(work_dir, archive_dir, configuration, control)

    def _create_experiments(self, plans: list[ExperimentPlan]) -> dict[str, ProfilingExperiment]:
        """Raises NotImplementedError: layout experiments are not generated for Cylc Rose suites yet.

        Args:
            plans (list[ExperimentPlan]): Unused.

        Raises:
            NotImplementedError: Always. Unreachable in practice, generate_scaling_experiments asking the
                configuration for a layout search first and that raising the same way.
        """
        raise NotImplementedError(
            "Layout generation is not supported for Cylc Rose configurations yet: a rose suite states its "
            "parallelism in rose-suite.conf, and nothing here yet knows how to search over that."
        )

    def parse_ncpus(self, path: Path, run_path: Path | None = None) -> int:
        """Parses the number of CPUs used in a given Cylc/Rose experiment, from the model layout.

        Finding the configuration file is this manager's, since where a rose suite keeps it and how it is
        written is the runner's business; what its variables are called, and so how many CPUs they come to, is
        the suite's and is answered by the configuration.

        Unlike the Payu manager, this reports the cores the layout puts to work rather than the ones the job
        occupied: it does not yet round up to whole compute nodes the way PayuManager.parse_ncpus does, though
        the configuration does state the node size. Scaling studies whose sizes are not multiples of the node
        size will therefore group experiments by a slightly smaller count than they were charged for, until
        this is implemented.

        Args:
            path (Path): Path to the experiment directory. Must contain a rose-suite.conf file.
            run_path (Path | None): Optional path to a separate runs directory. Its run configuration is
                preferred over the one in path when present, since it records what the run actually used.

        Returns:
            int: Number of CPUs the layout puts to work.

        Raises:
            FileNotFoundError: If neither configuration file exists.
            ValueError: If a variable the configuration names is not set in the file that was found.
        """
        # TODO: round up to whole compute nodes, as PayuManager.parse_ncpus does, using the node size from the
        # Rose/Cylc configuration.
        # both the run and original config will store cpu information
        config_paths = []
        if run_path is not None:
            config_paths.append(run_path / "log/rose-suite-run.conf")
        config_paths.append(path / "rose-suite.conf")

        config_path = next((candidate for candidate in config_paths if candidate.is_file()), None)
        if config_path is None:
            tried = ", ".join(str(p) for p in config_paths)
            raise FileNotFoundError(f"Could not find suitable config file. Tried: {tried}")

        return self.configuration.occupied_cpus(self._parse_rose_conf(config_path), source=str(config_path))

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

    def run_experiments(self, retry_failed: bool = False) -> None:
        """Runs Rose Cylc experiments via `rose suite-run` for profiling data generation.

        An experiment whose suite failed is run where retry_failed asks for it. Note that what it is offered
        is the same `rose suite-run` as any other, not a restart: whether a suite that has already run
        accepts that is the suite's own affair, and one that does not says so and stops this method rather
        than being worked around here.

        Args:
            retry_failed (bool): Whether to run the experiments whose suites failed as well as the new ones.
                False (the default) runs only the new ones.
        """

        # An experiment is only new as far as this manager knows: a status does not outlive the session, so
        # one generated and run in an earlier session comes back NEW. Ask before deciding what to submit.
        self.update_statuses()

        wanted = {ProfilingExperimentStatus.NEW}
        if retry_failed:
            wanted.add(ProfilingExperimentStatus.FAILED)

        to_run = {name: exp for name, exp in self.experiments.items() if exp.status in wanted}

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

    def _delete_experiment(self, name: str, dry_run: bool, **kwargs) -> None:
        """Deletes the experiment and run directories of a single Rose Cylc experiment.

        Args:
            name (str): Name of the experiment to delete.
            dry_run (bool): If True, logs what would be deleted without making any changes.
            **kwargs: Ignored. Accepted so that this stays substitutable for the base method, whose callers
                may pass options other runners take.
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

    def parse_status(self, path: Path, run_path: Path | None = None) -> ProfilingExperimentStatus | None:
        """Parses the state the tasks of a Rose Cylc suite reached.

        Cylc records a run status per task attempt in the task_jobs table of the suite database: 0 where the
        task succeeded, non-zero where it failed, and nothing at all while it is still going. A suite is done
        once every task it has recorded has succeeded, and failed as soon as any of them has not.

        Args:
            path (Path): Path to the experiment directory. Unused: the database lives with the run.
            run_path (Path | None): Path to the runs directory, which holds cylc-suite.db.

        Returns:
            ProfilingExperimentStatus | None: The state of the suite, or None if the database says nothing
                readable - no run directory, no database yet, or no task recorded in it.
        """
        if run_path is None:
            logger.debug("No Cylc run directory, so there is no suite database to read a status from.")
            return None

        cylcdb = run_path / "cylc-suite.db"
        if not cylcdb.is_file():
            logger.debug(f"No Cylc suite database at {cylcdb}.")
            return None

        try:
            with sqlite3.connect(f"file:{cylcdb}?mode=ro", uri=True) as connection:
                statuses = [row[0] for row in connection.execute("SELECT run_status FROM task_jobs")]
        except sqlite3.Error as error:
            logger.debug(f"Could not read task statuses from {cylcdb}: {error}.")
            return None

        if not statuses:
            logger.debug(f"The Cylc suite database at {cylcdb} records no task yet.")
            return None

        if any(status is not None and status != 0 for status in statuses):
            return ProfilingExperimentStatus.FAILED
        if any(status is None for status in statuses):
            return ProfilingExperimentStatus.RUNNING
        return ProfilingExperimentStatus.DONE

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
            for parser_name, parser in self.configuration.parsers.items():
                logs[f"{task}_cycle{cycle}_{parser_name}"] = ProfilingLog(logfile, parser, optional=True)

        # Cylc workflows have no concept of repeated runs, so every log is registered as the single run 0.
        return {name: {0: log} for name, log in logs.items()}
