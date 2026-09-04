# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

from access.config import YAMLParser
from access.config.esm1p6_layout_input import LayoutSearchConfig
from access.config.layout_config import LayoutTuple
from experiment_generator.experiment_generator import ExperimentGenerator
from experiment_runner.experiment_runner import ExperimentRunner

from access.profiling.experiment import ProfilingLog
from access.profiling.manager import ProfilingExperiment, ProfilingExperimentStatus, ProfilingManager
from access.profiling.payujson_parser import PayuJSONProfilingParser

logger = logging.getLogger(__name__)

# Payu's own defaults, from payu.subcommands.run_cmd, which _requested_ncpus mirrors. A config that declares
# neither is submitted on 48 core nodes, so that is what this package has to assume as well.
_PAYU_DEFAULT_NODE_SIZE = 48
_PAYU_DEFAULT_NCPUS = 1


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
        layout_search_config: LayoutSearchConfig | None = None,
    ) -> list:
        """Generates core layouts from the given number of nodes.

        Args:
            num_nodes (float): Number of nodes.
            cores_per_node (int): Number of cores per node.
            layout_search_config (LayoutSearchConfig | None): Configuration for layout search.
        """

    @abstractmethod
    def generate_perturbation_block(self, layout: LayoutTuple, branch_name_prefix: str) -> dict:
        """Generates a perturbation block for the given layout to be passed to the experiment generator.

        Args:
            layout (LayoutTuple): Core layout tuple.
            branch_name_prefix (str): Branch name prefix.
        Returns:
            dict: Perturbation block configuration.
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
        tol_around_ctrl_ratio: float,
        max_wasted_ncores_frac: float | Callable[[float], float],
        walltime: float | Callable[[float], float],
    ) -> None:
        """Generates scaling experiments using the ExperimentGenerator.

        Args:
            num_nodes_list (list[int]): List of number of nodes to generate experiments for.
            control_options (dict): Options for the control experiment.
            cores_per_node (int): Number of cores per node.
            tol_around_ctrl_ratio (float): Tolerance around control core ratio for layout generation.
            max_wasted_ncores_frac (float | Callable[[float], float]): Maximum fraction of wasted cores allowed.
            walltime (float | Callable[[float], float]): Walltime in hours for each experiment.
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

        seen_layouts = set()
        seqnum = 1
        generator_config["Perturbation_Experiment"] = {}
        for num_nodes in num_nodes_list:
            mwf = max_wasted_ncores_frac(num_nodes) if callable(max_wasted_ncores_frac) else max_wasted_ncores_frac
            layout_config = LayoutSearchConfig(tol_around_ctrl_ratio=tol_around_ctrl_ratio, max_wasted_ncores_frac=mwf)
            layouts = self.generate_core_layouts_from_node_count(
                num_nodes,
                cores_per_node=cores_per_node,
                layout_search_config=layout_config,
            )
            if not layouts:
                logger.warning(f"No layouts found for {num_nodes} nodes")
                continue

            layouts = [x for x in layouts if x not in seen_layouts]
            seen_layouts.update(layouts)
            logger.info(f"Generated {len(layouts)} layouts for {num_nodes} nodes. Layouts: {layouts}")

            # TODO: the branch name needs to be simpler and model agnostic
            branch_name = f"layout-unused-cores-to-cice-{layout_config.allocate_unused_cores_to_ice}"
            walltime_hrs = walltime(num_nodes) if callable(walltime) else walltime

            for layout in layouts:
                pert_config = self.generate_perturbation_block(layout=layout, branch_name_prefix=branch_name)
                branch = pert_config["branches"][0]
                pert_config["config.yaml"]["walltime"] = str(timedelta(hours=walltime_hrs))

                generator_config["Perturbation_Experiment"][f"Experiment_{seqnum}"] = pert_config
                self.experiments[branch] = ProfilingExperiment(path=self.work_dir / branch / self._repository_directory)

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

    def delete_experiments(
        self,
        experiments: list[str] | None = None,
        all_experiments: bool = False,
        dry_run: bool = False,
        remove_repo_dir: bool = False,
    ) -> None:
        """Deletes Payu experiments from the work directory and remove them from the manager.

        Args:
            experiments (list[str] | None): List of experiments (branches) to delete.
            all_experiments (bool): If True, deletes all experiments managed by this instance.
            dry_run (bool): If True, performs a dry run without deleting files. Defaults to False.
            remove_repo_dir (bool): If True, removes the base repository directory if no branches are using it.
        """
        # remove_repo_dir would already be forwarded to _delete_experiment via the base class **kwargs, but this
        # override declares it explicitly so it stays a documented, discoverable and typo-checked argument of the
        # public Payu API rather than a hidden keyword convention.
        super().delete_experiments(
            experiments=experiments,
            all_experiments=all_experiments,
            dry_run=dry_run,
            remove_repo_dir=remove_repo_dir,
        )

    def _delete_experiment(self, name: str, dry_run: bool, remove_repo_dir: bool = False) -> None:
        """Deletes a single Payu experiment (branch) via the experiment runner.

        Args:
            name (str): Name of the experiment (branch) to delete.
            dry_run (bool): If True, performs a dry run without deleting files.
            remove_repo_dir (bool): If True, removes the base repository directory if no branches are using it.
        """
        runner_config = {
            "test_path": self.work_dir,
            "repository_directory": self._repository_directory,
        }

        runner = ExperimentRunner(runner_config)

        runner.delete_experiments(
            branches=[name],
            hard=True,
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

    def parse_ncpus(self, path: Path, run_path: Path | None = None) -> int:
        """Parses the number of CPUs a given Payu experiment occupied.

        There are two ways to know this, and both are used. What the scheduler recorded is the better answer, so
        it is tried first; failing that, the request Payu would have made is worked out from the configuration.
        The two agree whenever both are available, since the first is the result of submitting the second.

        Args:
            path (Path): Path to the Payu experiment directory. Must contain a config.yaml file.
            run_path (Path | None): Optional path to a separate runs directory. Unused for Payu experiments.
        Returns:
            int: Number of CPUs the experiment occupied, including any left idle to fill whole compute nodes.
        """
        recorded = self._recorded_ncpus(path)
        if recorded is not None:
            return recorded

        config_path = path / "config.yaml"
        return self._requested_ncpus(YAMLParser().parse(config_path.read_text()))

    @staticmethod
    def _recorded_ncpus(path: Path) -> int | None:
        """Returns the number of CPUs the scheduler recorded for the most recent run, if it recorded any.

        Payu writes one job file per run, holding the job information it read back from the scheduler. Under PBS
        that is the output of ``qstat -f -F json``, whose Resource_List.ncpus is the request the job was charged
        for - the whole-node figure, not the sum over the submodels.

        Every way of not finding it is a missing answer rather than an error: experiments archived before Payu
        recorded job information, runs under a scheduler that reports none, and jobs whose scheduler query
        failed all fall back to reading the configuration instead. The reason is logged at DEBUG level.

        Args:
            path (Path): Path to the Payu experiment directory.

        Returns:
            int | None: Recorded number of CPUs, or None if no job file records one.
        """
        # Payu names the directory holding each job file after the run number, as in profiling_logs().
        job_files = sorted(path.glob("archive/payu_jobs/*/run/*.json"), key=lambda p: int(p.parts[-3]))
        if not job_files:
            logger.debug(f"No Payu job file found under {path / 'archive/payu_jobs'}.")
            return None

        # The most recent run: an experiment run several times keeps one job file per run, and they all describe
        # the same configuration, so the newest is as good as any and is the one that certainly ran.
        job_file = job_files[-1]
        try:
            job_info = json.loads(job_file.read_text())
        except (OSError, json.JSONDecodeError) as error:
            logger.debug(f"Could not read the Payu job file {job_file}: {error}.")
            return None

        if job_info.get("scheduler_type") != "pbs":
            logger.debug(f"Job file {job_file} records no PBS job information, so it states no CPU count.")
            return None

        try:
            job_id = job_info["scheduler_job_id"]
            return int(job_info["scheduler_job_info"]["Jobs"][job_id]["Resource_List"]["ncpus"])
        except (KeyError, TypeError, ValueError) as error:
            logger.debug(f"Job file {job_file} has no usable Resource_List.ncpus: {error}.")
            return None

    @staticmethod
    def _requested_ncpus(payu_config: dict) -> int:
        """Returns the number of CPUs Payu requests from the scheduler for a given configuration.

        This mirrors what Payu itself does in payu.subcommands.run_cmd before submitting: it works out the cores
        the model needs, then rounds the request up to fill whole compute nodes, warning about the ones that go
        unused. Reproducing the rule rather than reading the summed submodel counts is what makes the answer
        agree with the job that was actually submitted, whether or not the configuration names a node size.

        Two details are Payu's rather than this package's, and are kept deliberately. A job fitting within a
        single node is not rounded up at all, so small runs report exactly what they asked for. And ncpureq
        overrides everything, including the rounding, since it is Payu's hard override of the request.

        Args:
            payu_config (dict): Parsed contents of the experiment's config.yaml.

        Returns:
            int: Number of CPUs the request comes to.
        """
        node_size = payu_config.get("platform", {}).get("nodesize", _PAYU_DEFAULT_NODE_SIZE)

        if "ncpureq" in payu_config:
            # A hard override of the request, which Payu passes to the scheduler untouched.
            return payu_config["ncpureq"]
        if "submodels" in payu_config and "ncpus" not in payu_config:
            n_cpus = sum(submodel.get("ncpus", 0) for submodel in payu_config["submodels"])
        else:
            # Note the precedence: a top-level ncpus wins over the submodels, as it does in Payu.
            n_cpus = payu_config.get("ncpus", _PAYU_DEFAULT_NCPUS)

        cpus_per_node = payu_config.get("npernode", node_size)
        if n_cpus > node_size and (cpus_per_node < node_size or n_cpus % node_size):
            n_cpus = node_size * (1 + (n_cpus - 1) // cpus_per_node)
        return n_cpus

    def profiling_logs(self, path: Path, run_path: Path | None = None) -> dict[str, dict[int, ProfilingLog]]:
        """Returns all profiling logs from the specified path.

        Payu can be asked to submit the same experiment several times, in which case each run produces its own
        output directory and its own telemetry log. Payu numbers both after the same run counter, so the logs of
        every run are returned, keyed by that number.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory. Unused for Payu experiments.
        Returns:
            dict[str, dict[int, ProfilingLog]]: Dictionary mapping log names to their logs, keyed by run number.
        """
        logs: dict[str, dict[int, ProfilingLog]] = {}

        # Check archive directory exists
        archive = path / "archive"
        if not archive.is_dir():
            raise FileNotFoundError(f"Directory {archive} does not exist!")

        # Parse payu json profiling data if available. Payu names the directory holding each log after the run number.
        for json_path in archive.glob("payu_jobs/*/run/*.json"):
            logs.setdefault("payu", {})[int(json_path.parts[-3])] = ProfilingLog(json_path, PayuJSONProfilingParser())

        # Get the logs of each component of every output directory. Payu names these outputNNN, NNN being the run
        # number, so output003 holds the same run as payu_jobs/3.
        output_dirs = sorted(archive.glob("output*"))
        if not output_dirs:
            raise FileNotFoundError(f"No output files found in {path}!")
        for output_dir in output_dirs:
            run = int(output_dir.name.removeprefix("output"))
            for name, log in self.get_component_logs(output_dir).items():
                logs.setdefault(name, {})[run] = log

        return logs
