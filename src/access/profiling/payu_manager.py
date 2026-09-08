# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

from access.config import YAMLParser
from access.config.parallel_allocation_strategies import RootAllocation
from experiment_generator.experiment_generator import ExperimentGenerator
from experiment_runner.experiment_runner import ExperimentRunner

from access.profiling.experiment import ProfilingLog
from access.profiling.manager import ProfilingExperiment, ProfilingExperimentStatus, ProfilingManager
from access.profiling.payujson_parser import PayuJSONProfilingParser

logger = logging.getLogger(__name__)

# The defaults Payu applies to platform.nodesize and ncpus in payu.subcommands.run_cmd.runcmd, which
# _requested_ncpus mirrors. They are copied rather than imported because Payu states them as bare literals in
# the body of that function, and exposes them nowhere: if Payu ever changes them, these have to follow.
_PAYU_DEFAULT_NODE_SIZE = 48
_PAYU_DEFAULT_NCPUS = 1


class PayuManager(ProfilingManager, ABC):
    """Abstract base class to handle profiling of Payu configurations."""

    _repository_directory: str = "config"  # Repository directory name needed by the experiment generator and runner.
    _nruns: int = 1  # Number of repetitions for the Payu experiments.
    _startfrom_restart: str = "cold"  # Restart option for the Payu experiments.
    _repository: str  # Git repository URL or path of the control experiment. Set by set_control.
    _control_commit: str  # Git commit of the control experiment. Set by set_control.

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
        walltime: float | Callable[[float], float],
        allocations: RootAllocation | Callable[[float], RootAllocation] | None = None,
        max_layouts: int | None = None,
    ) -> None:
        """Generates scaling experiments, one per valid layout of the model.

        For each requested number of nodes, the valid layouts of the model are enumerated and each one becomes a
        perturbation experiment. Layouts whose branch is already known to this manager are skipped, so the same
        layout found for two different numbers of nodes only generates one experiment.

        Args:
            num_nodes_list (list[float]): Numbers of nodes to generate experiments for. Fractional values are
                allowed; the number of cores the layouts are searched for is the product with cores_per_node,
                truncated to an integer.
            control_options (dict): Options of the control experiment, passed to the experiment generator.
            cores_per_node (int): Number of cores available on each node. Must be a positive integer.
            walltime (float | Callable[[float], float]): Walltime in hours to request for each experiment, either as
                a fixed value or as a function of the number of nodes.
            allocations (RootAllocation | Callable[[float], RootAllocation] | None): Allocation strategy deciding
                how many cores each component may receive, either as a single strategy or as a function of the
                number of nodes. A single strategy is usually enough, since the bounds of an allocation may be
                written as fractions of the total core count, which resolve to a different number of cores at every
                size in the study. Pass a function only where a bound cannot be expressed that way; note that
                allocation bounds are in cores, so it typically multiplies by cores_per_node itself. None (the
                default) leaves every component unconstrained.
            max_layouts (int | None): Maximum number of layouts to enumerate for each number of nodes. None (the
                default) enumerates all of them.

        Raises:
            ValueError: If cores_per_node is not a positive integer, or if any of the node counts is not positive.
        """

        if not isinstance(cores_per_node, int) or cores_per_node <= 0:
            raise ValueError(f"Cores per node must be a positive integer. Got {cores_per_node} instead")

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

        seqnum = 1
        for num_nodes in num_nodes_list:
            if num_nodes <= 0:
                raise ValueError(f"Number of nodes must be > 0. Got {num_nodes} instead")

            total_cores = int(num_nodes * cores_per_node)
            layouts = self.select_layouts(
                total_cores,
                allocations=allocations(num_nodes) if callable(allocations) else allocations,
                max_layouts=max_layouts,
            )
            if not layouts:
                logger.warning(
                    f"No layouts found for {num_nodes} nodes ({total_cores} cores). Check the bounds and the "
                    "constraints of the allocation strategy."
                )
                continue
            logger.info(f"Found {len(layouts)} layouts for {num_nodes} nodes ({total_cores} cores).")

            walltime_hrs = walltime(num_nodes) if callable(walltime) else walltime

            for layout in layouts:
                branch = self.layout_branch_name(layout)
                if branch in self.experiments:
                    logger.info(f"Experiment for branch {branch} already exists. Skipping addition.")
                    continue

                pert_config = {"branches": [branch], **self.layout_config_changes(layout)}
                pert_config.setdefault("config.yaml", {})["walltime"] = str(timedelta(hours=walltime_hrs))

                generator_config["Perturbation_Experiment"][f"Experiment_{seqnum}"] = pert_config
                self.experiments[branch] = ProfilingExperiment(path=self.work_dir / branch / self._repository_directory)

                seqnum += 1

        if not generator_config["Perturbation_Experiment"]:
            logger.warning("No new experiments to generate. Will skip generation.")
            return

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
        agree with the job that was actually submitted.

        Two details are Payu's rather than this package's, and are kept deliberately. A job fitting within a
        single node is not rounded up at all, so small runs report exactly what they asked for. And ncpureq
        replaces the count the model asks for, but is still rounded up like any other: it overrides the request,
        not the node arithmetic.

        The answer hinges on the size of a compute node, which only ``platform.nodesize`` states. A
        configuration that leaves it out is submitted by Payu on nodes of _PAYU_DEFAULT_NODE_SIZE cores, so
        taking the same default here reproduces the request Payu made rather than guessing at it.

        Args:
            payu_config (dict): Parsed contents of the experiment's config.yaml.

        Returns:
            int: Number of CPUs the request comes to.
        """
        node_size = payu_config.get("platform", {}).get("nodesize", _PAYU_DEFAULT_NODE_SIZE)

        if "ncpureq" in payu_config:
            # A hard override of the count the model asks for; Payu still rounds it up to whole nodes.
            n_cpus = payu_config["ncpureq"]
        elif "submodels" in payu_config and "ncpus" not in payu_config:
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
