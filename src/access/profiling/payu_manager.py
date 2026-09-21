# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
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


def _walltime_string(hours: float) -> str:
    """Returns a walltime in hours as the HH:MM:SS string a Payu configuration states it in.

    The seconds are rounded rather than truncated, and the hours are left to run past 24 rather than becoming
    a count of days. Formatting a timedelta gets both wrong: an hour count whose seconds are not whole leaves
    a fraction behind, so 0.12345 hours reads as "0:07:24.420000", and a day or more is written out as
    "1 day, 1:00:00". A scheduler reads neither.

    Args:
        hours (float): Walltime to request, in hours.

    Returns:
        str: The walltime as HH:MM:SS, the hours unpadded and counting past 24.
    """
    hours_part, remainder = divmod(round(hours * 3600), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours_part}:{minutes:02d}:{seconds:02d}"


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

    @staticmethod
    def _validate_sizing(num_nodes_list: list[float], cores_per_node: int) -> None:
        """Rejects the sizes no layout search could be made for.

        Everything is checked before the first search, so that a bad value late in the list costs nothing and
        generate_scaling_experiments either generates all the experiments it was asked for or none of them.

        Args:
            num_nodes_list (list[float]): Numbers of nodes to generate experiments for.
            cores_per_node (int): Number of cores available on each node.

        Raises:
            ValueError: If cores_per_node is not a positive integer, or if any of the node counts is not
                positive.
        """
        if not isinstance(cores_per_node, int) or cores_per_node <= 0:
            raise ValueError(f"Cores per node must be a positive integer. Got {cores_per_node} instead")

        for num_nodes in num_nodes_list:
            if num_nodes <= 0:
                raise ValueError(f"Number of nodes must be > 0. Got {num_nodes} instead")

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
        perturbation experiment. Layouts whose branch is already known to this manager, or was already found
        earlier in the same call, are skipped, so the same layout found for two different numbers of nodes only
        generates one experiment.

        That happens whenever a layout fits both budgets, which is a matter of how much waste the component tree
        tolerates: a layout spending 508 cores is valid on 520 and on 546, leaving 2.3% and 7.0% of them idle.
        Note that it does not arise for an allocation strategy written in fractions of the total, since its
        bounds move with the budget; it is strategies stated in cores, and callables, that repeat themselves.

        Experiments become known to this manager only once the generator has returned, since an experiment it
        holds is one that can be run, archived and deleted. A generation that fails therefore registers
        nothing, not even the branches the generator managed to create before failing, and it is the whole
        call that is abandoned: correcting the cause and calling this method again generates the rest, the
        generator leaving the branches that already exist alone.

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
            Exception: Whatever the experiment generator raises, unchanged. No experiment is registered when it
                does.
        """

        self._validate_sizing(num_nodes_list, cores_per_node)

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
        # Nothing reaches the manager until the generator has returned. An entry in self.experiments is a
        # claim that a branch exists to be run, archived and deleted, and until then none of them do.
        new_experiments: dict[str, ProfilingExperiment] = {}
        for num_nodes in num_nodes_list:
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
                # Against what this call has found as well as what the manager already holds: with the
                # registration deferred, self.experiments on its own no longer says whether a layout has
                # been seen, and the same layout is regularly valid at two different node counts.
                if branch in self.experiments or branch in new_experiments:
                    logger.info(f"Experiment for branch {branch} already exists. Skipping addition.")
                    continue

                # Everything this experiment changes in the control configuration, keyed by the file each
                # change applies to.
                changes_by_file = self.layout_config_changes(layout)
                if "config.yaml" not in changes_by_file:
                    # Not every model has something of its own to change in there.
                    changes_by_file["config.yaml"] = {}
                changes_by_file["config.yaml"]["walltime"] = _walltime_string(walltime_hrs)
                changes_by_file["config.yaml"]["experiment"] = branch

                pert_config = {"branches": [branch]}
                pert_config.update(changes_by_file)

                generator_config["Perturbation_Experiment"][f"Experiment_{seqnum}"] = pert_config
                new_experiments[branch] = ProfilingExperiment(path=self.work_dir / branch / self._repository_directory)

                seqnum += 1

        if not generator_config["Perturbation_Experiment"]:
            logger.warning("No new experiments to generate. Will skip generation.")
            return

        try:
            ExperimentGenerator(generator_config).run()
        except Exception:
            # The generator sets up one branch at a time and says nothing about how far it got, so which
            # branches survive a failure is unknown. None of them are registered: an experiment this manager
            # has never heard of is simply generated again on the next call, and the generator leaves
            # branches that already exist alone, whereas one registered without its branch would be
            # submitted by run_experiments() and would never be generated, the check above having claimed it.
            logger.warning(
                f"Experiment generation failed, so none of {sorted(new_experiments)} were added to this "
                "manager. Some of their branches may already exist in the control clone; correcting the "
                "cause and calling this method again generates the rest and leaves those alone."
            )
            raise

        self.experiments.update(new_experiments)

    def run_experiments(self) -> None:
        """Runs Payu experiments for profiling data generation.

        Every experiment this manager holds as NEW is submitted, and only those that the runner returned
        from having submitted are marked RUNNING. A submission that fails therefore leaves them all NEW,
        which is what lets this method be called again to submit them: the runner leaves a branch it has
        already cloned alone, refuses to submit an experiment whose job is still live, and tops each one up
        to nruns rather than starting it afresh.

        Raises:
            Exception: Whatever the experiment runner raises, unchanged. No status is advanced when it does.
        """

        # An experiment is only new as far as this manager knows: a status does not outlive the session, so
        # one generated and run in an earlier session comes back NEW. Ask before deciding what to submit.
        self.update_statuses()

        # No keep_uuid: it would have every clone reuse one experiment_uuid, so each run's metadata.yaml
        # would claim to be the same experiment as all the others. Each experiment is named outright in
        # generate_scaling_experiments, so nothing here depends on the uuid to tell the runs apart.
        runner_config = {
            "test_path": self.work_dir,
            "repository_directory": self._repository_directory,
            "running_branches": [],
            "nruns": [],
            "startfrom_restart": [],
        }

        for branch, exp in self.experiments.items():
            if exp.status == ProfilingExperimentStatus.NEW:
                runner_config["running_branches"].append(branch)
                runner_config["nruns"].append(self.nruns)
                runner_config["startfrom_restart"].append(self.startfrom_restart)

        if not runner_config["running_branches"]:
            logger.info("No new experiments to run. Will skip execution.")
            return

        ExperimentRunner(runner_config).run()

        # Only a runner that returned has submitted anything. An experiment left NEW by a failed submission
        # is one this method offers the runner again, which is safe: the runner leaves a branch it has
        # already cloned alone, its PBS job manager refuses to submit an experiment that already has a job
        # neither finished nor suspended, and it only tops the run count up to nruns. RUNNING, by contrast,
        # sticks - parse_status reports nothing about an experiment that never ran, so update_statuses
        # leaves it be and this method would never offer it again.
        for branch in runner_config["running_branches"]:
            self.experiments[branch].status = ProfilingExperimentStatus.RUNNING

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
        job_info = PayuManager._latest_run_job(path)
        if job_info is None:
            return None

        if job_info.get("scheduler_type") != "pbs":
            logger.debug(f"The most recent Payu job file for {path} records no PBS job information.")
            return None

        try:
            job_id = job_info["scheduler_job_id"]
            return int(job_info["scheduler_job_info"]["Jobs"][job_id]["Resource_List"]["ncpus"])
        except (KeyError, TypeError, ValueError) as error:
            logger.debug(f"The most recent Payu job file has no usable Resource_List.ncpus: {error}.")
            return None

    @staticmethod
    def _latest_run_job(path: Path) -> dict | None:
        """Returns what Payu recorded for the most recent run of an experiment, if it recorded anything.

        Payu writes one job file per run, under a directory named after the run number, and fills it in as the
        run proceeds: the scheduler's job information, then the model's return code, then its own. The most
        recent run is the one asked about here - an experiment run several times keeps a file per run, and it
        is the newest that says where the experiment now stands.

        Every way of not finding it is a missing answer rather than an error, logged at DEBUG: an experiment
        archived before Payu recorded job information, one that has not run yet, and a file that cannot be
        read all mean the same thing to a caller, which is that Payu has nothing to say.

        Args:
            path (Path): Path to the Payu experiment directory.

        Returns:
            dict | None: The parsed job file of the most recent run, or None if there is none to read.
        """
        # Payu names the directory holding each job file after the run number, as in profiling_logs(). Only
        # *.json: Payu leaves its lock files and, when a lock times out, timestamped .tmp copies beside them.
        job_files = sorted(path.glob("archive/payu_jobs/*/run/*.json"), key=lambda p: int(p.parts[-3]))
        if not job_files:
            logger.debug(f"No Payu job file found under {path / 'archive/payu_jobs'}.")
            return None

        job_file = job_files[-1]
        try:
            return json.loads(job_file.read_text())
        except (OSError, json.JSONDecodeError) as error:
            logger.debug(f"Could not read the Payu job file {job_file}: {error}.")
            return None

    def parse_status(self, path: Path, run_path: Path | None = None) -> ProfilingExperimentStatus | None:
        """Parses the state the most recent Payu run of an experiment reached.

        Payu records its own verdict on a run as payu_run_status, 0 or 1, and writes it from the finally block
        of the run command, so it lands whether the run succeeded or raised. Its presence is therefore what
        says the run is over; the stage a job file reports only says how far it had got when last written, and
        a run job never reaches the "exited" stage that collate and sync jobs end at.

        A run killed outright - a walltime limit, a node failure - never reaches that finally block, so its
        job file stays at the stage it died in and this keeps reporting it as running. Telling that apart from
        a run still going needs the scheduler, which this does not ask.

        Args:
            path (Path): Path to the Payu experiment directory.
            run_path (Path | None): Optional path to a separate runs directory. Unused for Payu experiments.

        Returns:
            ProfilingExperimentStatus | None: The state of the run, or None if Payu recorded nothing readable.
        """
        job_info = self._latest_run_job(path)
        if job_info is None:
            return None

        run_status = job_info.get("payu_run_status")
        if run_status is None:
            # Still under way, as far as anything written down says.
            return ProfilingExperimentStatus.RUNNING

        if run_status == 0:
            return ProfilingExperimentStatus.DONE

        # Payu's own status covers the whole run, so a non-zero one is a failure wherever it happened. The
        # model's return code is reported alongside it to say whether the model itself was what failed.
        model_status = job_info.get("payu_model_run_status")
        logger.debug(f"Payu reports run status {run_status} (model run status {model_status}) for {path}.")
        return ProfilingExperimentStatus.FAILED

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
