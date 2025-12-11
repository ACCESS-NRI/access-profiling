# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path
from typing import Callable

import xarray as xr
from access.config import YAMLParser
from access.config.esm1p6_layout_input import (
    LayoutSearchConfig,
    generate_esm1p6_core_layouts_from_node_count,
    generate_esm1p6_perturb_block,
)
from experiment_generator.experiment_generator import ExperimentGenerator
from experiment_runner.experiment_runner import ExperimentRunner

from access.profiling.experiment import ProfilingLog
from access.profiling.manager import ProfilingExperiment, ProfilingExperimentStatus, ProfilingManager
from access.profiling.payujson_parser import PayuJSONProfilingParser

logger = logging.getLogger(__name__)


class PayuManager(ProfilingManager, ABC):
    """Abstract base class to handle profiling of Payu configurations."""

    _repository_directory: str = "."  # Repository directory name needed by the experiment generator and runner.
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
    @abstractmethod
    def model_type(self) -> str:
        """Returns the model type identifier, as defined in Payu."""
        pass

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

    def set_control(self, repository, commit) -> None:
        """Sets the control experiment from an existing Payu configuration.

        Args:
            repository: Git repository URL or path.
            commit: Git commit hash or identifier.
        """
        self._repository = repository
        self._control_commit = commit

    def generate_experiments_directories(
        self,
        num_nodes_list: list[float],
        control_options: dict,
        cores_per_node: int,
        tol_around_ctrl_ratio: float,
        max_wasted_ncores_frac: float | Callable[[float], float],
        walltime: float | Callable[[float], float],
    ) -> list[str]:
        """Generates scaling experiments for ACCESS-ESM1.6.

        Args:
            num_nodes_list (list[int]): List of number of nodes to generate experiments for.
        """

        generator_config = {
            "model_type": self.model_type,
            "repository_url": self._repository,
            "start_point": self._control_commit,
            "test_path": str(self.work_dir),
            "repository_directory": self._repository_directory,
            "control_branch_name": "ctrl",
            "Control_Experiment": control_options,
        }

        branch_name_prefix = "esm1p6-layout"

        seen_layouts = set()
        seqnum = 1
        generator_config["Perturbation_Experiment"] = {}
        branches = []
        for num_nodes in num_nodes_list:
            mwf = max_wasted_ncores_frac(num_nodes) if callable(max_wasted_ncores_frac) else max_wasted_ncores_frac
            layout_config = LayoutSearchConfig(tol_around_ctrl_ratio=tol_around_ctrl_ratio, max_wasted_ncores_frac=mwf)
            layouts = generate_esm1p6_core_layouts_from_node_count(
                num_nodes,
                cores_per_node=cores_per_node,
                layout_search_config=layout_config,
            )[0]
            if not layouts:
                logger.warning(f"No layouts found for {num_nodes} nodes")
                continue

            layouts = [x for x in layouts if x not in seen_layouts]
            seen_layouts.update(layouts)
            logger.info(f"Generated {len(layouts)} layouts for {num_nodes} nodes. Layouts: {layouts}")

            branch_name = f"{branch_name_prefix}-unused-cores-to-cice-{layout_config.allocate_unused_cores_to_ice}"
            walltime_hrs = walltime(num_nodes) if callable(walltime) else walltime

            for layout in layouts:
                pert_config = generate_esm1p6_perturb_block(layout=layout, branch_name_prefix=branch_name)

                pert_config["config.yaml"]["walltime"] = str(timedelta(hours=walltime_hrs))
                generator_config["Perturbation_Experiment"][f"Experiment_{seqnum}"] = pert_config

                branches.add(pert_config["branches"][0])
                seqnum += 1

        from ruamel.yaml import YAML

        ryaml = YAML()
        ryaml.dump(generator_config, Path("/home/156/mo1833/1.Projects/Scaling/esm16_test/layout_input_config.yaml"))

        ExperimentGenerator(generator_config).run()

        return branches

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

    def parse_profiling_data(self, path: Path) -> dict[str, xr.Dataset]:
        """Parses profiling data from a Payu experiment directory.

        Args:
            path (Path): Path to the Payu experiment directory.
        Returns:
            dict[str, xr.Dataset]: Dictionary mapping component names to their profiling datasets.
        Raises:
            FileNotFoundError: If the archive or output directories are missing.
        """
        datasets = {}
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

        # Parse all logs
        for name, log in logs.items():
            logger.info(f"Parsing {name} profiling log: {log.filepath}. ")
            datasets[name] = log.parse()
            logger.info(" Done.")

        return datasets
