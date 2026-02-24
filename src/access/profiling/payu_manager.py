# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path

from access.config import YAMLParser
from access.config.accessom3_layout_input import OM3ConfigLayout, OM3LayoutSearchConfig
from access.config.esm1p6_layout_input import LayoutSearchConfig
from access.config.layout_config import LayoutTuple
from experiment_generator.experiment_generator import ExperimentGenerator
from experiment_runner.experiment_runner import ExperimentRunner

from access.profiling.experiment import ProfilingLog
from access.profiling.manager import ProfilingExperiment, ProfilingExperimentStatus, ProfilingManager
from access.profiling.payujson_parser import PayuJSONProfilingParser

logger = logging.getLogger(__name__)

# Type aliases for better readability
SearchConfig = LayoutSearchConfig | OM3LayoutSearchConfig
Layout = LayoutTuple | OM3ConfigLayout


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
    @abstractmethod
    def model_type(self) -> str:
        """Returns the model type identifier, as defined in Payu."""

    @abstractmethod
    def generate_core_layouts_from_node_count(
        self,
        num_nodes: float,
        cores_per_node: int,
        layout_search_config: SearchConfig | None = None,
    ) -> list[Layout]:
        """Generates core layouts from the given number of nodes.

        Args:
            num_nodes (float): Number of nodes.
            cores_per_node (int): Number of cores per node.
            layout_search_config (SearchConfig | None): Configuration for layout search.
        """

    @abstractmethod
    def generate_perturbation_block(
        self,
        layout: Layout,
        num_nodes: float,
        branch_name_prefix: str,
        walltime_hrs: float,
        layout_search_config: SearchConfig | None = None,
        block_overrides: dict | None = None,
    ) -> dict:
        """Generates a perturbation block for the given layout to be passed to the experiment generator.

        Args:
            layout (Layout): Core layout tuple.
            num_nodes (float): Number of nodes.
            branch_name_prefix (str): Branch name prefix.
            walltime_hrs (float): Walltime in hours.
            layout_search_config (SearchConfig | None): Configuration for layout search.
            block_overrides (dict | None): Overrides for the perturbation block.
        Returns:
            dict: Perturbation block configuration.
        """

    @abstractmethod
    def layout_key(self, layout: Layout) -> tuple:
        """Returns a stable key for a layout so PayuManager can deduplicate layouts."""

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
        if value < 0:
            raise ValueError("Number of runs must be at least 0.")
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

    def generate_scaling_experiments(
        self,
        num_nodes_list: list[float],
        control_options: dict,
        cores_per_node: int,
        branch_name_prefix: str,
        walltime: float | Callable[[float], float],
        layout_search_config_builder: Callable[[float], SearchConfig] | None = None,
        block_overrides_builder: Callable[[Layout, float], dict] | None = None,
    ) -> None:
        """Generates scaling experiments using ExperimentGenerator.

        Args:
            num_nodes_list (list[int]): List of number of nodes to generate experiments for.
            control_options (dict): Options for the control experiment.
            cores_per_node (int): Number of cores per node.
            branch_name_prefix (str): Branch name prefix for the generated experiments.
            walltime (float | Callable[[float], float]): Walltime in hours for each experiment.
            layout_search_config_builder: optional function (num_nodes)->OM3LayoutSearchConfig
            block_overrides_builder: optional function (layout, num_nodes)->dict.
        """

        generator_config = {
            "model_type": self.model_type,
            "repository_url": self._repository,
            "start_point": self._control_commit,
            "test_path": str(self.work_dir),
            "repository_directory": self._repository_directory,
            "control_branch_name": "ctrl",
            "Control_Experiment": control_options,
            "Perturbation_Experiment": {},
        }

        seen_layouts = set()
        seqnum = 1

        for num_nodes in num_nodes_list:
            layout_search_config = layout_search_config_builder(num_nodes) if layout_search_config_builder else None
            layouts = self.generate_core_layouts_from_node_count(
                num_nodes,
                cores_per_node=cores_per_node,
                layout_search_config=layout_search_config,
            )

            if not layouts:
                logger.warning(f"No layouts found for {num_nodes} nodes")
                continue

            unique_layouts: list[Layout] = []
            for layout in layouts:
                key = self.layout_key(layout)
                if key in seen_layouts:
                    continue
                seen_layouts.add(key)
                unique_layouts.append(layout)

            logger.info(
                f"Generated {len(layouts)} layouts for {num_nodes} nodes "
                f"with {len(unique_layouts)} unique layouts: {unique_layouts}"
            )

            for layout in unique_layouts:
                walltime_hrs = walltime(num_nodes) if callable(walltime) else walltime
                block_overrides = block_overrides_builder(layout, num_nodes) if block_overrides_builder else None
                perturb_block = self.generate_perturbation_block(
                    layout=layout,
                    num_nodes=num_nodes,
                    branch_name_prefix=branch_name_prefix,
                    walltime_hrs=walltime_hrs,
                    layout_search_config=layout_search_config,
                    block_overrides=block_overrides,
                )

                generator_config["Perturbation_Experiment"][f"Experiment_{seqnum}"] = perturb_block

                branch = perturb_block["branches"][0]
                self.experiments[branch] = ProfilingExperiment(self.work_dir / branch / self._repository_directory)

                seqnum += 1

        ExperimentGenerator(generator_config).run()

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
