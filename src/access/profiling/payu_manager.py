# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from pathlib import Path

from access.config import YAMLParser
from experiment_runner.experiment_runner import ExperimentRunner

from access.profiling.experiment import ProfilingLog
from access.profiling.manager import ProfilingExperiment, ProfilingExperimentStatus, ProfilingManager
from access.profiling.payujson_parser import PayuJSONProfilingParser

logger = logging.getLogger(__name__)


class PayuManager(ProfilingManager, ABC):
    """Abstract base class to handle profiling of Payu configurations."""

    _repository_directory: str = "config"  # Repository directory name needed by the experiment generator and runner.
    _nruns: int = 1  # Number of repetitions for the Payu experiments.
    _startfrom_restart: str = "cold"  # Restart option for the Payu experiments.

    @abstractmethod
    def get_component_logs(self, path: Path) -> dict[str, ProfilingLog]:
        """Returns available profiling logs for the components in the configuration.

        Args:
            path (Path): Path to the output directory.
        Returns:
            dict[str, ProfilingLog]: Dictionary mapping component names to their ProfilingLog instances.
        """

    @property
    def nruns(self) -> int:
        """Returns the number of repetitions for the Payu experiments.

        Returns:
            int: Number of repetitions.
        """
        return self._nruns

    @nruns.setter
    def nruns(self, value: int) -> None:
        """Sets the number of repetitions for the Payu experiments.

        Args:
            value (int): Number of repetitions.
        """
        if value < 1:
            raise ValueError("Number of runs must be at least 1.")
        self._nruns = value

    @property
    def startfrom_restart(self) -> str:
        """Returns the restart option for the Payu experiments.

        Returns:
            str: Restart option.
        """
        return self._startfrom_restart

    @startfrom_restart.setter
    def startfrom_restart(self, value: str) -> None:
        """Sets the restart option for the Payu experiments.

        Args:
            value (str): Restart option.
        """
        self._startfrom_restart = value

    def generate_experiments(self, branches: list[str]) -> None:
        """Generates Payu experiments for profiling data generation.

        Args:
            branches (list[str]): List of branches to generate experiments for.
        """

        for branch in branches:
            if branch in self.experiments:
                logger.info(f"Experiment for branch {branch} already exists. Skipping addition.")
            else:
                self.experiments[branch] = ProfilingExperiment(self.work_dir / branch / self._repository_directory)

    def run_experiments(self) -> None:
        """Runs Payu experiments for profiling data generation."""

        runner_config = {
            "test_path": self.work_dir,
            "repository_directory": self._repository_directory,
            "running_branches": [],
            "keep_uuid": True,
            "nruns": [],
            "startfrom_restart": [],
        }

        for path, exp in self.experiments.items():
            if exp.status == ProfilingExperimentStatus.NEW:
                runner_config["running_branches"].append(path)
                runner_config["nruns"].append(self.nruns)
                runner_config["startfrom_restart"].append(self.startfrom_restart)
                exp.status = ProfilingExperimentStatus.RUNNING

        # Run the experiment runner
        if runner_config["running_branches"]:
            ExperimentRunner(runner_config).run()
        else:
            logger.info("No new experiments to run. Will skip execution.")

        # We are marking all running experiments as done here, but later this should be implemented properly
        # so that an actual check is performed, probably somewhere else.
        for exp in self.experiments.values():
            if exp.status == ProfilingExperimentStatus.RUNNING:
                exp.status = ProfilingExperimentStatus.DONE

    def purge_experiments(
        self,
        branches: list[str] | None = None,
        hard: bool = False,
        dry_run: bool = False,
        remove_repo_dir: bool = False,
    ) -> None:
        """Purges Payu experiments from the work directory.

        Args:
            branches (list[str] | None): List of branches to purge. If None, purges all existing branches.
            dry_run (bool): If True, performs a dry run without deleting files. Defaults to False.
            hard (bool): If True, performs a hard purge removing all files. Defaults to False.
            remove_repo_dir (bool): If True, removes the base repository directory if no branches are using it.
        """
        if branches is None:
            branches = list(self.experiments.keys())

        runner_config = {
            "test_path": self.work_dir,
            "repository_directory": self._repository_directory,
        }

        runner = ExperimentRunner(runner_config)
        runner.purge_experiments(
            branches=branches,
            hard=hard,
            dry_run=dry_run,
            remove_repo_dir=remove_repo_dir,
        )

    def archive_experiments(
        self,
        exclude_dirs: list[str] | None = None,
        exclude_files: list[str] | None = None,
        follow_symlinks: bool = True,
        overwrite: bool = False,
    ) -> None:
        """Archives completed experiments to the specified archive path.

        Args:
            exclude_dirs (list[str] | None): Directory patterns to exclude when archiving experiments. Defaults to
                [".git", "restart*"] if not provided.
            exclude_files (list[str] | None): File patterns to exclude when archiving experiments. Defaults to
                ["*.nc"] if not provided.
            follow_symlinks (bool): Whether to follow symlinks when archiving experiments. Defaults to True.
            overwrite (bool): Whether to overwrite existing archives. Defaults to False.
        """
        if exclude_dirs is None:
            exclude_dirs = [".git", "restart*"]
        if exclude_files is None:
            exclude_files = ["*.nc"]
        super().archive_experiments(
            exclude_dirs=exclude_dirs, exclude_files=exclude_files, follow_symlinks=follow_symlinks, overwrite=overwrite
        )

    def parse_ncpus(self, path: Path) -> int:
        """Parses the number of CPUs used in a given Payu experiment.

        Args:
            path (Path): Path to the Payu experiment directory. Must contain a config.yaml file.
        Returns:
            int: Number of CPUs used in the experiment. If multiple submodels are defined, returns the sum of their
                 ncpus.
        """
        config_path = path / "config.yaml"
        payu_config = YAMLParser().parse(config_path.read_text())
        if "submodels" in payu_config:
            return sum(submodel["ncpus"] for submodel in payu_config["submodels"])
        else:
            return payu_config["ncpus"]

    def profiling_logs(self, path: Path) -> dict[str, ProfilingLog]:
        """Returns all profiling logs from the specified path.
        Args:
            path (Path): Path to the experiment directory.
        Returns:
            dict[str, ProfilingLog]: Dictionary of profiling logs.
        """
        logs = {}

        # Check archive directory exists
        archive = path / "archive"
        if not archive.is_dir():
            raise FileNotFoundError(f"Directory {archive} does not exist!")

        # Parse payu json profiling data if available
        matches = sorted(archive.glob("payu_jobs/*/run/*.json"))
        if len(matches) > 1:
            logger.warning(f"Multiple payu json logs found in {path}! Using the first one found.")
        if len(matches) >= 1:
            logs["payu"] = ProfilingLog(matches[0], PayuJSONProfilingParser())

        # Find how many output directories are available and get logs from each component
        matches = sorted(archive.glob("output*"))
        if len(matches) == 0:
            raise FileNotFoundError(f"No output files found in {path}!")
        elif len(matches) > 1:
            logger.warning(f"Multiple output directories found in {path}! Using the first one found.")
        logs.update(self.get_component_logs(matches[0]))

        return logs
