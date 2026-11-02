# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Profiling studies of applications driven by a Cylc Rose suite, such as the ACCESS models ACCESS-AM3 and ACCESS-rAM3.

`CylcRoseManager` is the manager for every rose suite: it runs experiments with `rose suite-run`, and reads back
what the suite and Cylc recorded about each run - the CPUs its jobs occupied, the state its tasks reached and
the profiling logs they wrote. Experiments are not generated for a suite: they are added from suites already
in the working directory. What is being profiled arrives as a `RoseSuiteConfiguration`.
"""

import logging
import re
import shutil
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path

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

# The PBS request for CPUs in a job script, as Cylc writes the directive - `#PBS -l ncpus=576` - possibly after
# other resources in the same list.
_PBS_NCPUS = re.compile(r"^#PBS\s+-l\s+(?:[^\s,]+,)*ncpus=(?P<ncpus>\d+)\b", re.MULTILINE)


class CylcRoseManager(ProfilingManager[RoseSuiteConfiguration]):
    """Profiling of any application driven by a Cylc Rose suite, such as the ACCESS models.

    One class for every rose suite: what tells ACCESS-AM3 from ACCESS-rAM3 is the names their
    `rose-suite.conf` files give the variables stating their parallelism, and which parsers read their task
    logs, both of which arrive as a RoseSuiteConfiguration. What is written here is the engine - `rose
    suite-run`, the Cylc run directory and the suite database the scheduler writes - and none of it is any one
    suite's.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        application (RoseSuiteConfiguration): The suite being profiled.
        control (ControlSource | None): Where the control suite comes from. Recorded rather than acted on:
            unlike the Payu side, nothing here clones or checks out, so the suite is expected to be in the
            working directory already - `rosie checkout` or `git clone` having been run in a terminal, where
            a credentials prompt can be answered. None (the default) records nothing.
    """

    def _create_experiments(self, plans: list[ExperimentPlan], **runner_options) -> dict[str, ProfilingExperiment]:
        """Raises NotImplementedError: layout experiments are not generated for Cylc Rose suites yet.

        Args:
            plans (list[ExperimentPlan]): Unused.
            **runner_options: Ignored. Accepted so that this stays substitutable for the base method, whose
                callers may pass options other runners take.

        Raises:
            NotImplementedError: Always. Unreachable in practice, generate_scaling_experiments asking the
                configuration for a layout search first and that raising the same way.
        """
        raise NotImplementedError(
            "Layout generation is not supported for Cylc Rose configurations yet: a rose suite states its "
            "parallelism in rose-suite.conf, and nothing here yet knows how to search over that."
        )

    def parse_ncpus(self, path: Path, run_path: Path | None = None) -> int:
        """Parses the number of CPUs a given Cylc/Rose experiment occupied.

        The layout is read first: finding the configuration file is this manager's, since where a rose suite
        keeps it and how it is written is the runner's business, while what its variables are called, and so how
        many CPUs they come to, is the suite's and is answered by the configuration.

        The job that runs the layout may occupy more CPUs than the layout puts to work, to fill whole compute
        nodes, and the scheduler charges for those. So, as PayuManager.parse_ncpus reads what the scheduler was
        asked for, this reads the PBS requests in the job scripts Cylc submitted, and reports the smallest that
        covers the layout: that of the job that ran it. Where no job script covers the layout - no run directory,
        no job submitted to PBS - the layout's own count is reported. See _requested_ncpus.

        Args:
            path (Path): Path to the experiment directory. Must contain a rose-suite.conf file.
            run_path (Path | None): Optional path to a separate runs directory. Its run configuration is
                preferred over the one in path when present, since it records what the run actually used, and
                its job scripts say what the run's jobs asked PBS for.

        Returns:
            int: Number of CPUs the experiment occupied, including any left idle to fill whole compute nodes.

        Raises:
            FileNotFoundError: If neither configuration file exists.
            ValueError: If a variable the configuration names is not set in the file that was found.
        """
        layout_cpus = self._layout_ncpus(path, run_path)
        if run_path is None:
            return layout_cpus

        covering = [ncpus for ncpus in self._requested_ncpus(run_path) if ncpus >= layout_cpus]
        if not covering:
            logger.debug(f"No job in {run_path} asked PBS for the {layout_cpus} CPUs of the layout, or more.")
            return layout_cpus
        return min(covering)

    @staticmethod
    def _requested_ncpus(run_path: Path) -> list[int]:
        """Returns the numbers of CPUs the jobs of a suite's run asked PBS for.

        Cylc writes the directives of a task's job into the script it submits, at
        log/job/<cycle>/<task>/NN/job for the latest attempt at it, so a job submitted to PBS has a `#PBS -l
        ncpus=<n>` line there. A job asking for chunks with `select` instead, or not submitted to PBS, says
        nothing here.

        Args:
            run_path (Path): Path to the Cylc run directory.

        Returns:
            list[int]: The request of every job script that makes one, in no particular order.
        """
        requests = []
        for script in run_path.glob("log/job/*/*/NN/job"):
            match = _PBS_NCPUS.search(script.read_text(errors="replace"))
            if match is not None:
                requests.append(int(match["ncpus"]))
        return requests

    def _layout_ncpus(self, path: Path, run_path: Path | None) -> int:
        """Returns the number of CPUs the layout of a suite puts to work, from its rose-suite.conf.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory, whose run configuration is
                preferred.

        Returns:
            int: Number of CPUs the layout puts to work.

        Raises:
            FileNotFoundError: If neither configuration file exists.
            ValueError: If a variable the configuration names is not set in the file that was found.
        """
        # both the run and original config will store cpu information
        config_paths = []
        if run_path is not None:
            config_paths.append(run_path / "log/rose-suite-run.conf")
        config_paths.append(path / "rose-suite.conf")

        config_path = next((candidate for candidate in config_paths if candidate.is_file()), None)
        if config_path is None:
            tried = ", ".join(str(p) for p in config_paths)
            raise FileNotFoundError(f"Could not find suitable config file. Tried: {tried}")

        return self.application.occupied_cpus(self._parse_rose_conf(config_path), source=str(config_path))

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

        The experiment takes the status the suite database in its run directory records, as parse_status reads
        it: DONE once every task has succeeded, FAILED once one has not, and RUNNING while some are still going. A
        suite with no run recorded there is NEW, which is what run_experiments runs.

        Args:
            rose (str): The rose to add as an experiment.
            run_path (Path | None): Path to the Cylc run directory of the suite, where its results are, or will be
                once it runs: it does not have to exist yet. Without it, nothing the suite records can be read, so
                the suite stays NEW, a run of it is never seen to finish, and its logs cannot be parsed.

        Raises:
            ValueError: If the experiment path does not exist.
        """
        experiment_path = self.work_dir / rose
        if not experiment_path.is_dir():
            raise ValueError(f"Experiment path '{experiment_path}' does not exist or is not a directory.")

        experiment = ProfilingExperiment(path=experiment_path, run_path=run_path)
        status = self.parse_status(experiment_path, run_path)
        experiment.status = status if status is not None else ProfilingExperimentStatus.NEW
        self.experiments[rose] = experiment

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

    def _delete_experiments(self, names: list[str], dry_run: bool, **kwargs) -> None:
        """Deletes the experiment and run directories of Rose Cylc experiments.

        Args:
            names (list[str]): Names of the experiments to delete.
            dry_run (bool): If True, logs what would be deleted without making any changes.
            **kwargs: Ignored. Accepted so that this stays substitutable for the base method, whose callers
                may pass options other runners take.
        """
        for name in names:
            exp = self.experiments[name]
            exp_path = exp.path
            run_path = exp.run_path
            if dry_run:
                logger.info(f"Dry run: would delete experiment directory '{exp_path}' and run directory '{run_path}'.")
                continue
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

        # A connection used as a context manager only ends a transaction, so it is closed explicitly.
        try:
            with closing(sqlite3.connect(f"file:{cylcdb}?mode=ro", uri=True)) as connection:
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

        Raises:
            ValueError: If no run directory is given.
            RuntimeError: If the run directory holds no task job logs.
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
            for parser_name, parser in self.application.parsers.items():
                logs[f"{task}_cycle{cycle}_{parser_name}"] = ProfilingLog(logfile, parser, optional=True)

        # Cylc workflows have no concept of repeated runs, so every log is registered as the single run 0.
        return {name: {0: log} for name, log in logs.items()}
