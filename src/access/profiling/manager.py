# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The part of a profiling study that is the same whichever workflow engine runs it.

`ProfilingManager` decides which experiments a study is made of, follows the status they reach, archives them,
and parses their profiling data into results, which `access.profiling.results` selects, tabulates and plots. What
depends on the engine - creating, running and deleting experiments, and reading what the engine and the scheduler
recorded - is left to a subclass for each engine, `PayuManager` or `CylcRoseManager`.

The experiments a manager generates are numbered, `exp-0001` onwards, and a name says nothing about its experiment:
an experiment can also be added under any other name, so nothing is ever read out of one. What an experiment is, is
written in its record, a file in its directory (see `access.profiling.experiment`). What is true of the whole study
is written in the study file, `STUDY_FILENAME` in the working directory: the highest number given so far, and one
entry for each call that created, added or deleted experiments. A manager stores nothing else, and never a status.
Each of the two files states the version of its own format, and a file of another version is refused when it is read.
Both are written whole, to a temporary file that then replaces them, so that a session stopped part way through a
write leaves the file as it was.
"""

import copy
import dataclasses
import itertools
import json
import logging
import os
import re
import shutil
import tarfile
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Generic, TypeVar

import pandas as pd
import xarray as xr
from access.config.parallel_allocation_strategies import RootAllocation
from access.config.parallel_component import ComponentLayout, ParallelComponent
from access.config.parallel_layouts import iter_layouts

from access.profiling.application import Application
from access.profiling.control import ControlSource
from access.profiling.experiment import (
    RECORD_FILENAME,
    ConfigurationChoice,
    ExperimentPlan,
    ProfilingExperiment,
    ProfilingExperimentStatus,
    ProfilingLog,
    StudyPlan,
    read_archived_record,
    read_record,
    read_versioned,
)
from access.profiling.results import ParsedExperiment, ProfilingResults

logger = logging.getLogger(__name__)

AppT = TypeVar("AppT", bound=Application)
"""The kind of application a manager profiles.

A manager is written once per workflow engine, and each engine drives a particular kind of application - Payu
drives a PayuConfiguration, a Cylc Rose suite a RoseSuiteConfiguration - so the engine knows the concrete type
and should not have to rediscover it.
"""

STUDY_FILENAME: str = "access-profiling-study.json"
"""The name of the study file, in the working directory and, once experiments are archived, in the archive directory."""

STUDY_VERSION: int = 1
"""The version of the format of the study file, which the file states."""

_STUDY_KEYS = ("version", "last_number", "history")

# The names given to generated experiments. Kept apart from the names experiments are added under, so that a
# number is only ever one this manager gave.
_NUMBERED_NAME = re.compile(r"exp-(\d+)")


def _write_whole(path: Path, text: str) -> None:
    """Writes a file whole or not at all.

    The text goes to a temporary file beside the target, which then replaces it in one step. A write that stops part
    way, because the disk is full or the session is killed, therefore leaves the file as it was, rather than cut
    short. A record cut short would stop the study being opened, and a study file cut short would stop every call
    that numbers or logs experiments.

    Args:
        path (Path): The file to write.
        text (str): What it is to hold.
    """
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _package_version() -> str:
    """Returns the version of this package, or "unknown" where it is run without being installed."""
    try:
        return version("access-profiling")
    except PackageNotFoundError:
        return "unknown"


def _merge_changes(
    layout_changes: dict,
    stated: dict,
    above: tuple[str, ...] = (),
    sources: tuple[str, str] = ("the layout", "the configuration choice"),
) -> dict:
    """Returns the changes that realise a layout with the changes a configuration choice states merged over them.

    Mappings are merged key by key, and where the two disagree the choice wins, being what the study asked for.
    Two lists are refused instead: the workflow engine merges a list into the control's position by position, by
    rules of its own that are not reproduced here, so there is no saying what merging two of them would come to.
    The control options of a study are merged over the control changes of its application in the same way.

    Args:
        layout_changes (dict): The changes realising the layout, keyed by file. Left as they are.
        stated (dict): The changes the choice states, keyed by file. Left as they are.
        above (tuple[str, ...]): The keys leading to these mappings, for the message.
        sources (tuple[str, str]): What the two sets of changes are, for the message.

    Returns:
        dict: The merged changes.

    Raises:
        ValueError: If both give a list for the same key.
    """
    merged = dict(layout_changes)
    for key, value in stated.items():
        ours = merged.get(key)
        if isinstance(ours, dict) and isinstance(value, dict):
            merged[key] = _merge_changes(ours, value, (*above, key), sources)
        elif isinstance(ours, list) and isinstance(value, list):
            raise ValueError(
                f"Both {sources[0]} and {sources[1]} give a list for {' > '.join((*above, key))}, and two lists "
                f"cannot be merged here. Leave the list out of {sources[1]}, and if every experiment needs the change, "
                "make it in the control instead."
            )
        else:
            merged[key] = value
    return merged


class ProfilingManager(ABC, Generic[AppT]):
    """Abstract base class to handle profiling data and workflows.

    This high-level class generates the experiments of a profiling study, one for each layout of every size the
    study asks for or one for each configuration choice it states, or takes in experiments that already exist. It
    finds the experiments of its working directory and of its archive directory again when it is built. It keeps
    track of the status they reach, archives them, and parses their profiling data into a `ProfilingResults`,
    which selects, tabulates and plots it: the best performing experiment for each number of CPUs, the best of
    several runs, how each region scales, how experiments compare region by region, and how a region changes with
    whatever the study varied. What depends on the workflow engine - creating, running and deleting the
    experiments, and reading what the engine recorded about them - is left to each subclass.

    A manager is the workflow engine, and nothing about any one application: what is being profiled arrives
    as an Application and what it is profiled against as a ControlSource. So a subclass is written once
    per engine rather than once per application, and a new application, or a new setup of one, is data rather
    than code.

    Which of the two answers a question is decided by what wrote the thing being read: the manager reads what
    the engine and the scheduler wrote - the logs, the job records, the CPU count the scheduler charged for -
    and the application reads what its own input files say.

    Args:
        work_dir (Path | str): Working directory where profiling experiments will be generated and run.
        archive_dir (Path | str): Directory where completed experiments will be archived.
        application (AppT): The application being profiled, which says how it is parallelised, what it
            writes and how a layout is realised in its own input files.
        control (ControlSource | None): Where the control every experiment perturbs comes from.
            None (the default) is enough to read and plot experiments that already exist, and is rejected by
            generate, which has nothing to perturb without it.
        control_options (dict | None): Changes made to the control before any experiment is created from it, keyed
            by file, in the words of the workflow engine. They are merged over the control changes the application
            needs (`Application.control_changes`), and win where both change one setting. None (the default) makes
            none beyond the application's.

    Raises:
        ValueError: If the study file records calls made with another application than this one, or if the record
            of an experiment, in the working directory or in an archive, is not a JSON object, states no format
            version or another one than this version of the package reads, or lacks one of its entries.

    Attributes:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        experiments (dict[str, ProfilingExperiment]): The experiments this manager holds, keyed by name: those
            found in the working directory and in the archive directory when it was built, and those it has
            generated or been given since.
        control_options (dict): The study's own changes to the control, as given.
    """

    work_dir: Path
    archive_dir: Path
    experiments: dict[str, ProfilingExperiment]
    control_options: dict

    def __init__(
        self,
        work_dir: Path | str,
        archive_dir: Path | str,
        application: AppT,
        control: ControlSource | None = None,
        *,
        control_options: dict | None = None,
    ):
        super().__init__()
        self.work_dir = Path(work_dir)
        self.archive_dir = Path(archive_dir)
        self._application = application
        self._control = control
        # Copied, so that a dictionary the notebook goes on editing does not change what the study generates.
        self.control_options = copy.deepcopy(control_options) if control_options is not None else {}
        self.experiments = {}
        self._refuse_another_application()

        # The experiments of the working directory, each found again from the record in its directory.
        for record_file in sorted(self.work_dir.glob(f"*/{RECORD_FILENAME}")):
            name = record_file.parent.name
            logger.info(f"Found experiment: {name}")
            record = read_record(record_file)
            path = Path(record["path"])
            self.experiments[name] = ProfilingExperiment.from_record(
                record,
                path if path.is_absolute() else self.work_dir / path,
                None if record["run_path"] is None else Path(record["run_path"]),
            )

        # Discover experiments in the archive directory. An archive is what an experiment is read from once it
        # has one, so it replaces the experiment found in the working directory. The archives are taken in the order
        # of their names, as the records are, so that the order of the experiments is not the file system's.
        if self.archive_dir.is_dir():
            for branch_path in sorted(self.archive_dir.glob("*.tar.gz")):
                if branch_path.is_file():
                    branch_name = branch_path.name[: -len(".tar.gz")]
                    try:
                        record = read_archived_record(branch_path)
                    except (tarfile.TarError, EOFError, OSError) as error:
                        # An archive_experiments stopped part way leaves such a file. Skipping it keeps the
                        # experiment of the working directory, which can then be archived again over it.
                        logger.warning(
                            f"Skipping the archive '{branch_path}', which cannot be read: {error}. An archiving that "
                            "was stopped part way leaves such a file. Archive the experiment again with "
                            "overwrite=True, or remove the file."
                        )
                        continue
                    logger.info(f"Found archived experiment: {branch_name}")
                    self.experiments[branch_name] = (
                        ProfilingExperiment(path=branch_path)
                        if record is None
                        else ProfilingExperiment.from_record(record, branch_path)
                    )

    def __repr__(self) -> str:
        """Returns a string representation of the ProfilingManager."""

        indent = "    "
        summary = f"<{type(self).__name__}>\n"
        summary += indent + f"Application: {self.application.name}\n"
        if self.control is not None:
            summary += indent + f"Control: {self.control.label}\n"
        # Both are said, since what the application adds to the control is not in the notebook that made the manager.
        if self.application.control_changes:
            summary += indent + f"Control changes of the application: {self.application.control_changes}\n"
        if self.control_options:
            summary += indent + f"Control options: {self.control_options}\n"
        summary += indent + f"Working directory: {self.work_dir!r}\n"
        summary += indent + f"Archive directory: {self.archive_dir!r}\n"
        summary += indent + "Experiments:\n"
        for name, exp in self.experiments.items():
            summary += indent * 2 + f"'{name}': {exp!r}\n"
            # The name says nothing about the experiment, so the layout it was given is said here. Printing a
            # manager is how the experiments it found are looked at, so a layout of another application is said
            # to be one, and nothing is raised.
            try:
                key = self.layout_key(name)
            except ValueError:
                key = f"not a layout of {self.application.name}"
            if key is not None:
                summary += indent * 3 + f"Layout: {key}\n"
        return summary

    @property
    def application(self) -> AppT:
        """Returns the application being profiled.

        Returns:
            AppT: The application, as the concrete type this manager's runner drives.
        """
        return self._application

    @property
    def control(self) -> ControlSource | None:
        """Returns where the control configuration every experiment perturbs comes from.

        Returns:
            ControlSource | None: The control, or None if this manager was given none.
        """
        return self._control

    @control.setter
    def control(self, value: ControlSource | None) -> None:
        """Sets where the control configuration comes from.

        Settable so that one manager can profile the same application against several controls in turn -
        two releases, or a release and a fork - without being rebuilt and losing the experiments it holds.

        Args:
            value (ControlSource | None): The control.
        """
        self._control = value

    def _read_study(self) -> dict:
        """Returns what the study file holds.

        The file is read from the working directory, and failing that from the archive directory, where
        archive_experiments copies it, so that a study whose working directory is gone still has its history.

        Returns:
            dict: The study: the version of the format, the highest number given so far and the history. A study
                with no file yet starts at zero, with an empty history.

        Raises:
            ValueError: If the file is not a JSON object, if it states no format version or another one than
                `STUDY_VERSION`, or if it lacks one of its entries.
        """
        for directory in (self.work_dir, self.archive_dir):
            study_file = directory / STUDY_FILENAME
            if study_file.is_file():
                return read_versioned(study_file.read_text(), study_file, "study file", STUDY_VERSION, _STUDY_KEYS)
        return {"version": STUDY_VERSION, "last_number": 0, "history": []}

    def _refuse_another_application(self) -> None:
        """Refuses a study that was made with another application than this manager's, as its study file records.

        A study has one application. A manager of another one would take the study's experiments for its own, since
        two configurations of a model can key the same layout without error: it would skip generating those
        layouts, and read the study's runs as runs of its own configuration. The record of an experiment does not
        say which application made it, since that is the same for every experiment of a study, but the study file
        records the application of every call.

        Raises:
            ValueError: If a call the study file records was made with another application.
        """
        own = {"class": type(self.application).__name__, "name": self.application.name}
        others = []
        for entry in self._read_study()["history"]:
            if entry["application"] != own and entry["application"] not in others:
                others.append(entry["application"])
        if others:
            made = " and ".join(f"{other['class']} '{other['name']}'" for other in others)
            raise ValueError(
                f"The study of the working directory '{self.work_dir}' and the archive directory '{self.archive_dir}' "
                f"was made with {made}, as its study file records, but this manager's application is "
                f"{own['class']} '{own['name']}'. A study has one application: build the manager with that one, or "
                "give it working and archive directories of its own."
            )

    def _write_study(self, study: dict) -> None:
        """Writes the study file into the working directory, whole or not at all.

        Args:
            study (dict): The study, as _read_study returned it.
        """
        self.work_dir.mkdir(parents=True, exist_ok=True)
        _write_whole(self.work_dir / STUDY_FILENAME, json.dumps(study, indent=2, default=str) + "\n")

    def _control_options(self) -> dict:
        """Returns the changes made to the control: those the application needs, with the study's merged over them.

        Returns:
            dict: The changes, keyed by file.

        Raises:
            ValueError: If the application and the study both give a list for the same key.
        """
        return _merge_changes(
            self.application.control_changes,
            self.control_options,
            sources=("the control changes of the application", "the control options"),
        )

    def _engine_record(self) -> dict:
        """Returns what the workflow engine adds to the history entry of a call that generated experiments.

        Returns:
            dict: The changes made to the control, which are part of what every experiment of the call is, where
                there are any. A subclass adds what else only its engine can say.
        """
        options = self._control_options()
        return {"control_options": options} if options else {}

    def _log(self, call: str, experiments: list[str], generated: bool = False) -> None:
        """Adds a call to the history the study file keeps.

        What is true of every experiment of a call is written once here rather than in each experiment's
        record: when it was, what made it, and for a call that generated experiments, what they perturb.

        Args:
            call (str): The method called.
            experiments (list[str]): The experiments it created, added or deleted.
            generated (bool): Whether the call generated the experiments, from this manager's control. False (the
                default) records no control and nothing of the engine, which is what an experiment added or
                deleted has to say of them.
        """
        study = self._read_study()
        study["history"].append(
            {
                "when": datetime.now(UTC).isoformat(timespec="seconds"),
                "call": call,
                "access_profiling_version": _package_version(),
                "manager": type(self).__name__,
                "application": {"class": type(self.application).__name__, "name": self.application.name},
                "control": self.control.provenance() if generated and self.control is not None else None,
                "engine": self._engine_record() if generated else {},
                "experiments": experiments,
            }
        )
        self._write_study(study)

    def _write_record(self, name: str) -> None:
        """Writes an experiment's record into its directory under the working directory, whole or not at all.

        The directory is created if need be.

        Args:
            name (str): Name of the experiment.
        """
        directory = self.work_dir / name
        directory.mkdir(parents=True, exist_ok=True)
        record = self.experiments[name].record(self.work_dir)
        _write_whole(directory / RECORD_FILENAME, json.dumps(record, indent=2) + "\n")

    def _remove_record(self, name: str) -> None:
        """Removes an experiment's record, and its directory if the record was all it held.

        Args:
            name (str): Name of the experiment.
        """
        directory = self.work_dir / name
        (directory / RECORD_FILENAME).unlink(missing_ok=True)
        if directory.is_dir() and not any(directory.iterdir()):
            directory.rmdir()

    @staticmethod
    def _refuse_added_name(name: str) -> None:
        """Refuses a name that an experiment cannot be added under.

        The record of an experiment is kept in `<work_dir>/<name>`, and a manager finds the records of the
        directories directly under the working directory, so the name is that of one directory.

        Args:
            name (str): The name the experiment is being added under.

        Raises:
            ValueError: If the name is not that of a single directory, or is `exp-<digits>`, the form of the names
                generated experiments are given.
        """
        if name in ("", ".", "..") or Path(name).name != name:
            raise ValueError(
                f"Experiment name '{name}' is not the name of a single directory, which its record is kept in under "
                "the working directory. Add the experiment under a name without a '/'."
            )
        if _NUMBERED_NAME.fullmatch(name) is not None:
            raise ValueError(
                f"Experiment name '{name}' is of the form this manager gives the experiments it generates. Add "
                "the experiment under another name."
            )

    def _last_number(self) -> int:
        """Returns the highest number given to an experiment of this study so far.

        A number is never given twice, not even that of a deleted experiment: an engine may keep something
        under the name, as Payu keeps the branch, and a new experiment would be created on top of it. So the
        study file counts, and the names held are counted too, for a study whose file is gone.

        Returns:
            int: The number, or 0 where none has been given.
        """
        numbers = [self._read_study()["last_number"]]
        for name in self.experiments:
            match = _NUMBERED_NAME.fullmatch(name)
            if match is not None:
                numbers.append(int(match.group(1)))
        return max(numbers)

    def _held_identities(self) -> dict[tuple[str | None, str], str]:
        """Returns the experiments whose record says what they are, keyed by what identifies them.

        An experiment is identified by the layout it was given and by what the study changed beyond it. One
        whose changes are unknown, as an added one's are, is identified by nothing and is left out.

        Returns:
            dict[tuple[str | None, str], str]: The name of each such experiment, keyed by the key of its layout,
                or None if it was given none, and by its changes, written out with their keys in order.
        """
        held = {}
        for name, experiment in self.experiments.items():
            if experiment.changes is None:
                continue
            held[(self.layout_key(name), json.dumps(experiment.changes, sort_keys=True))] = name
        return held

    def layout_key(self, name: str) -> str | None:
        """Returns the key of the layout an experiment was given, which says what the experiment's name does not.

        Args:
            name (str): Name of the experiment.

        Returns:
            str | None: The key, as the application states it, or None for an experiment that was given no
                layout: one added by hand, or archived before experiments had records. A layout read back from
                such an experiment's configuration has no key here, since it is not what identifies it.

        Raises:
            ValueError: If the layout is not a layout of this manager's application, as the layout of an
                experiment generated with another application is not.
        """
        experiment = self.experiments[name]
        if experiment.layout is None or not experiment.layout_given:
            return None
        try:
            return self.application.layout_key(experiment.layout)
        except ValueError as error:
            raise ValueError(
                f"Experiment '{name}' was given a layout that is not a layout of '{self.application.name}', the "
                f"application of this manager. {error} A study has one application: build the manager with the "
                "application its experiments were generated with, or give it working and archive directories of "
                "its own."
            ) from error

    @abstractmethod
    def profiling_logs(self, path: Path, run_path: Path | None = None) -> dict[str, dict[int, ProfilingLog]]:
        """Returns all profiling logs from the specified path.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            dict[str, dict[int, ProfilingLog]]: Dictionary mapping log names to their logs, keyed by run number.
                Configurations with no concept of repeated runs should return a single run, numbered 0.
        """

    @abstractmethod
    def parse_ncpus(self, path: Path, run_path: Path | None = None) -> int:
        """Parses the number of CPUs a given experiment occupied.

        This is what the experiment cost, not what it put to work: a scheduler that hands out whole compute
        nodes charges a run that gives its components 402 cores on 104 core nodes for all 416 cores it
        occupies.
        Two layouts that fill the same nodes are the same size for the purposes of a scaling study, however
        differently they divide the cores among the components.

        How to find that number is up to each subclass, since it depends on the workflow engine and on what the
        scheduler recorded.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            int: Number of CPUs the experiment occupied.
        """

    @abstractmethod
    def parse_status(self, path: Path, run_path: Path | None = None) -> ProfilingExperimentStatus | None:
        """Parses the state a given experiment's run actually reached.

        Submitting a run says nothing about how it ended, so this reads what the workflow engine recorded and
        reports DONE, FAILED or RUNNING accordingly. How to find that out is up to each subclass, since it
        depends on the engine and on what it writes.

        Returning None means the state could not be determined - no record written yet, or one that cannot be
        read - and leaves the experiment's status as it stands. That is a missing answer rather than an error,
        so a subclass should log the reason at DEBUG and return None rather than raise.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            ProfilingExperimentStatus | None: The state the run reached, or None if it cannot be determined.
        """

    @property
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how the application is parallelised.

        Returns:
            ParallelComponent: Root of the component tree, as the application being profiled states it. It
                holds the domains and the requirements that every valid layout of that application must
                satisfy; requirements specific to a particular study belong in the allocation strategy passed
                to the layout search instead.
        """
        return self.application.parallel_component

    def select_layouts(
        self,
        total_cores: int,
        allocations: RootAllocation | None = None,
        max_layouts: int | None = None,
    ) -> list[ComponentLayout]:
        """Returns the valid layouts of the application for a given number of cores, fewest idle cores first.

        Args:
            total_cores (int): Total number of cores the layouts must distribute among the application's
                components.
            allocations (RootAllocation | None): Allocation strategy deciding how many cores each component may
                receive, and any further constraints the layouts must satisfy. None (the default) leaves every
                component unconstrained, which is rarely what is wanted: the number of valid layouts grows very
                quickly with the number of cores.
            max_layouts (int | None): Maximum number of layouts to enumerate. None (the default) enumerates all of
                them. Note that this bounds the *enumeration*, so the returned layouts are the first ones found and
                not necessarily those with the fewest idle cores. Without lower bounds on the components or a limit
                on idle cores, the first ones found can leave most of the cores idle. Returning the best ones
                instead would mean ranking the whole result set, which is the cost this argument exists to avoid:
                the search is lazy and layout counts reach millions at production core counts. Sorting is applied
                to what was enumerated, so it orders the returned layouts without deciding which ones they are.

        Returns:
            list[ComponentLayout]: The layouts found, sorted by increasing number of idle cores.
        """
        layouts = iter_layouts(self.parallel_component, total_cores, allocations=allocations)
        if max_layouts is not None:
            layouts = itertools.islice(layouts, max_layouts + 1)
        found = list(layouts)
        if max_layouts is not None and len(found) > max_layouts:
            logger.warning(
                f"More than {max_layouts} layouts found for {total_cores} cores. Only the first {max_layouts} "
                "enumerated will be used, which are not necessarily the ones with the fewest idle cores: "
                "finding those would mean enumerating all of them. Tighten the allocation strategy to choose "
                "which layouts are found rather than how many."
            )
            found = found[:max_layouts]
        return sorted(found, key=lambda layout: layout.idle_cores)

    @staticmethod
    def _validate_sizing(num_nodes_list: list[float], cores_per_node: int) -> None:
        """Rejects the sizes no layout search could be made for.

        Everything is checked before the first search, so that a bad value late in the list costs nothing and
        plan_scaling_experiments either plans all the experiments it was asked for or none of them.

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

    def _plan_experiments(
        self,
        num_nodes_list: list[float],
        cores_per_node: int,
        walltime: float | Callable[[float], float],
        allocations: RootAllocation | Callable[[float], RootAllocation] | None,
        max_layouts: int | None,
        regenerate_failed: bool,
        choice: ConfigurationChoice,
    ) -> StudyPlan:
        """Returns the experiments a scaling study is to create, one per layout it has not covered yet.

        Args:
            num_nodes_list (list[float]): Numbers of nodes to generate experiments for.
            cores_per_node (int): Number of cores available on each node.
            walltime (float | Callable[[float], float]): Walltime in hours, or a function of the node count.
            allocations (RootAllocation | Callable[[float], RootAllocation] | None): Allocation strategy, or a
                function of the node count, or None to leave every component unconstrained.
            max_layouts (int | None): Maximum number of layouts to enumerate for each number of nodes.
            regenerate_failed (bool): Whether to plan an experiment this manager already holds again when its
                run failed.
            choice (ConfigurationChoice): The changes and the labels every experiment of the plan has, with no layout.

        Returns:
            StudyPlan: What to create, in the order the sizes were asked for.
        """
        plans: list[ExperimentPlan] = []
        rows: list[dict] = []
        held_now: list[str] = []
        # The layouts this call has already covered. self.experiments on its own does not say, the
        # registration being deferred until the engine has returned, and the same layout is regularly valid at
        # two different node counts.
        planned: set[str] = set()
        held = self._held_identities()
        stated = json.dumps(choice.changes, sort_keys=True)
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
                key = self.application.layout_key(layout)
                if key in planned:
                    continue
                planned.add(key)

                # An experiment of a scaling study is its layout and the changes the study stated.
                name = held.get((key, stated))
                existing = self.experiments[name] if name is not None else None
                if existing is not None and not (
                    regenerate_failed and existing.status == ProfilingExperimentStatus.FAILED
                ):
                    logger.info(f"Experiment {name} already exists for layout {key}. Skipping addition.")
                    held_now.append(name)
                    continue
                if existing is not None:
                    logger.info(f"Regenerating experiment {name}, whose run failed.")

                plans.append(
                    ExperimentPlan(
                        name=name,
                        layout=layout,
                        changes=_merge_changes(self.application.config_changes(layout), choice.changes),
                        walltime_hours=walltime_hrs,
                        regenerating=existing is not None,
                        stated_changes=choice.changes,
                        labels=choice.labels,
                    )
                )
                row = {"nodes": num_nodes, "cores": total_cores, "layout": key, "walltime (hours)": walltime_hrs}
                row |= choice.labels
                if existing is not None:
                    row["regenerates"] = name
                rows.append(row)

        return StudyPlan("plan_scaling_experiments", tuple(plans), pd.DataFrame(rows), tuple(held_now))

    def plan_scaling_experiments(
        self,
        num_nodes_list: list[float],
        cores_per_node: int,
        walltime: float | Callable[[float], float],
        allocations: RootAllocation | Callable[[float], RootAllocation] | None = None,
        max_layouts: int | None = None,
        regenerate_failed: bool = False,
        changes: dict | None = None,
        labels: dict | None = None,
    ) -> StudyPlan:
        """Plans the experiments of a scaling study, one per valid layout of the application being profiled.

        For each requested number of nodes, the valid layouts are enumerated and each one becomes an
        experiment. A scaling study asks for several numbers of nodes, and a study of the best layout of one
        budget for a single one. Layouts whose experiment the study already has, or that were already planned
        for another size of the same call, are left out, so the same layout found for two different numbers of
        nodes only makes one experiment. Nothing is written: the plan is created with generate().

        That happens whenever a layout fits both budgets, which is a matter of how many idle cores the
        allocation strategy tolerates: a layout spending 508 cores is valid on 520 and on 546, leaving 2.3% and
        7.0% of them idle. It is rarer for a strategy that gives every component a lower bound in fractions of
        the total, since its bounds move with the budget. Strategies stated in cores, strategies without lower
        bounds, and callables repeat themselves more often.

        Which experiment already runs a layout is told from the experiments' records, not from their names: by
        the layout and by the changes the study stated beyond it. A study that varies a setting across its sizes
        plans once for each value, with the changes that set it and the labels that say it, and each value makes
        experiments of its own.

        An experiment that failed may be planned again, which is what regenerate_failed asks for. The changes
        each experiment makes are worked out afresh from the arguments of this call rather than replayed from
        what was generated before, so correcting whatever produced the failure - the walltime, the allocation,
        the application's own config_changes - and generating the plan is what puts the correction on the
        experiment. Regenerating leaves the status alone, so an experiment that failed still reads as failed:
        running it again takes run_experiments(retry_failed=True), which is also what clears that record.

        Args:
            num_nodes_list (list[float]): Numbers of nodes to plan experiments for. Fractional values are
                allowed; the number of cores the layouts are searched for is the product with cores_per_node,
                truncated to an integer.
            cores_per_node (int): Number of cores available on each node. Must be a positive integer.
            walltime (float | Callable[[float], float]): Walltime in hours to request for each experiment,
                either as a fixed value or as a function of the number of nodes.
            allocations (RootAllocation | Callable[[float], RootAllocation] | None): Allocation strategy
                deciding how many cores each component may receive, either as a single strategy or as a
                function of the number of nodes. A single strategy is usually enough, since the bounds of an
                allocation may be written as fractions of the total core count, which resolve to a different
                number of cores at every size in the study. Pass a function only where a bound cannot be
                expressed that way; note that allocation bounds are in cores, so it typically multiplies by
                cores_per_node itself. None (the default) leaves every component unconstrained.
            max_layouts (int | None): Maximum number of layouts to enumerate for each number of nodes. None
                (the default) enumerates all of them.
            regenerate_failed (bool): Whether to plan an experiment this manager already holds again when its run
                failed, so that a correction reaches it. False (the default) leaves out every experiment already
                held, whatever became of it. Only failed ones are ever regenerated: a running one would be rewritten
                underneath a live job, and one that finished has results that its configuration should go on
                matching. The statuses are brought up to date first, so that a run that failed in an earlier session
                is found.
            changes (dict | None): Changes every experiment makes beyond its layout, keyed by file, as the changes
                of a `ConfigurationChoice` are, such as the time step of the ocean. They are merged over the changes
                that realise each layout, and win. They may not set the walltime, which `walltime` gives. None (the
                default) makes none.
            labels (dict | None): The study's own keys and values for every experiment of the plan, such as
                `{"dt": 900}`, as JSON values. None (the default) gives none.

        Returns:
            StudyPlan: The experiments to create, with a table of their sizes, layouts, walltimes and labels.

        Raises:
            ValueError: If cores_per_node is not a positive integer, if any of the node counts is not positive, if the
                changes or the labels are not JSON values, if a label is named as one of `RESERVED_LABELS`, or if the
                changes and a layout both give a list for the same key.
        """
        self._validate_sizing(num_nodes_list, cores_per_node)
        # A choice without a layout checks the changes and the labels, and copies them, as for plan_experiments.
        choice = ConfigurationChoice(
            changes if changes is not None else {}, labels=labels if labels is not None else {}
        )
        if regenerate_failed:
            # Which runs failed is the engine's to say, and a status does not outlive the session: an experiment
            # found again from its record is NEW until the engine has been asked.
            self.update_statuses()
        return self._plan_experiments(
            num_nodes_list, cores_per_node, walltime, allocations, max_layouts, regenerate_failed, choice
        )

    def plan_experiments(self, choices: list[ConfigurationChoice]) -> StudyPlan:
        """Plans one experiment for each configuration choice, whatever it is that the choices change.

        Where plan_scaling_experiments searches for the layouts of a study, this method searches nothing: a study
        states each configuration it wants an experiment of, as changes to the files of the control. A study of a
        setting on the layout of the control gives changes alone, and one that varies the layout as well gives each
        choice a layout, for example one that select_layouts returned. Nothing is written: the plan is created with
        generate().

        What an experiment changes is worked out from its choice:

        - without a layout, the changes are the choice's own, and the application is not consulted;
        - with a layout, they are the changes realising the layout, as the application states them, with the
          choice's merged over them. Mappings are merged key by key and the choice wins. Where both give a list
          for the same key, the plan is refused, since lists are the workflow engine's to merge.

        Nothing checks the changes of a choice against its layout. The application works out what a layout
        comes to from its own fields, so a choice that changes a setting those depend on does not change them.
        Nothing here states a walltime either: it is one of the changes, in the file the engine reads it from.

        A choice the study already has an experiment for is left out. That is told from the experiments' records,
        by the layout and the changes, so that planning again in a later session plans nothing twice, and an
        experiment of a scaling study is the experiment of its layout with the changes stated for it. The labels
        are not part of it: an experiment found this way keeps the labels it has, with a warning where the choice
        gives others.

        There is no regenerating a failed experiment here. A corrected change is another configuration, and so
        another experiment, and the failed one is deleted with delete_experiments.

        Args:
            choices (list[ConfigurationChoice]): The configurations to create an experiment for.

        Returns:
            StudyPlan: The experiments to create, with a table of their layouts and labels.

        Raises:
            ValueError: If two of the choices are the same configuration, or if a choice and its layout both give a
                list for the same key.
        """
        plans: list[ExperimentPlan] = []
        rows: list[dict] = []
        held_now: list[str] = []
        asked: set[tuple[str | None, str]] = set()
        held = self._held_identities()
        for choice in choices:
            key = None if choice.layout is None else self.application.layout_key(choice.layout)
            identity = (key, json.dumps(choice.changes, sort_keys=True))
            if identity in asked:
                raise ValueError(
                    f"Two of the configuration choices are the same configuration: the layout {key} with the "
                    f"changes {choice.changes}. A study has one experiment for each configuration, so give it once."
                )
            asked.add(identity)

            name = held.get(identity)
            if name is not None:
                logger.info(f"Experiment {name} already exists for this configuration choice. Skipping addition.")
                held_now.append(name)
                labels = self.experiments[name].labels
                if labels != choice.labels:
                    logger.warning(
                        f"Experiment {name} keeps the labels it has, {labels}, and not those of the configuration "
                        f"choice it was found for, {choice.labels}."
                    )
                continue

            # With a layout, the application says what the layout comes to and the choice has the last word.
            # Without one, the layout is the control's, and the application has nothing to say.
            changes = (
                choice.changes
                if choice.layout is None
                else _merge_changes(self.application.config_changes(choice.layout), choice.changes)
            )
            plans.append(
                ExperimentPlan(
                    name=None,
                    layout=choice.layout,
                    changes=changes,
                    walltime_hours=None,
                    stated_changes=choice.changes,
                    labels=choice.labels,
                )
            )
            rows.append(
                {"layout": key, "cores": None if choice.layout is None else choice.layout.n_cores} | choice.labels
            )

        return StudyPlan("plan_experiments", tuple(plans), pd.DataFrame(rows), tuple(held_now))

    def generate(self, plan: StudyPlan) -> pd.DataFrame:
        """Creates the experiments of a plan, numbering them, and returns them.

        The experiments are numbered, `exp-0001` onwards, in the order of the plan, on from the highest number the
        study has given. The plan is checked against what the study holds by then: an experiment the study has
        come to have since the plan was made, in this session or another, is left out. Generating a plan twice
        therefore creates nothing twice.

        Experiments become known to this manager only once the engine has created them, since an experiment
        it holds is one that can be run, archived and deleted. A generation that fails therefore registers
        nothing, not even what the engine managed to create before failing, and it is the whole call that is
        abandoned: correcting the cause and generating again creates the rest, under the numbers that follow,
        since the ones the failed call took are not given again. A call the engine refuses before it creates
        anything, as `_refuse_plans` lets it, takes no numbers. A regenerated experiment is already registered and
        so survives such a failure, whether or not the engine reached it.

        Deciding what to generate is the same question for every workflow engine and is answered by the plans;
        creating it is each engine's own and is left to _create_experiments.

        Args:
            plan (StudyPlan): What plan_scaling_experiments or plan_experiments returned.

        Returns:
            pd.DataFrame: The experiments created or regenerated, one row each, as status() shows them.

        Raises:
            ValueError: If this manager has no control, or if an experiment the plan regenerates is no longer held.
            Exception: Whatever the workflow engine raises, unchanged. No experiment is registered when it does.
        """
        self._refuse_no_control()
        held = self._held_identities()
        number = self._last_number()
        named: list[ExperimentPlan] = []
        for planned in plan.experiments:
            if planned.regenerating:
                if planned.name not in self.experiments:
                    raise ValueError(
                        f"The plan regenerates experiment {planned.name}, which this study no longer holds. Make the "
                        "plan again."
                    )
                named.append(planned)
                continue
            key = None if planned.layout is None else self.application.layout_key(planned.layout)
            name = held.get((key, json.dumps(planned.stated_changes, sort_keys=True)))
            if name is not None:
                logger.info(f"Experiment {name} has been created for this configuration since the plan was made.")
                continue
            number += 1
            named.append(dataclasses.replace(planned, name=f"exp-{number:04d}"))
        return self._table(self._generate(f"generate({plan.call})", named))

    def _refuse_no_control(self) -> None:
        """Refuses to generate experiments for a manager that was given no control.

        Raises:
            ValueError: If this manager has no control.
        """
        if self.control is None:
            raise ValueError(
                "Cannot generate experiments without a control: every experiment is a perturbation of one, so "
                "there is nothing to generate from. Pass a ControlSource to this manager."
            )

    def _generate(self, call: str, plans: list[ExperimentPlan]) -> list[str]:
        """Creates the planned experiments and starts holding them: what follows planning, whatever planned them.

        Args:
            call (str): The call that created them, for the history of the study.
            plans (list[ExperimentPlan]): What to create, each with its name.

        Returns:
            list[str]: The names of the experiments created or regenerated, in the order of the plans.

        Raises:
            Exception: Whatever the workflow engine raises, unchanged. No experiment is registered when it
                does.
        """
        if not plans:
            logger.warning("No new experiments to generate. Will skip generation.")
            return []

        # What the engine can tell it could not create is refused first: nothing is written yet, so a call
        # refused here takes no number.
        self._refuse_plans(plans)

        # The numbers are taken before the engine is called and stay taken if it fails, since how far it got is
        # unknown: an experiment it half created must not be created over.
        new_numbers = [int(str(plan.name).removeprefix("exp-")) for plan in plans if not plan.regenerating]
        if new_numbers:
            study = self._read_study()
            study["last_number"] = max(new_numbers)
            self._write_study(study)

        try:
            created = self._create_experiments(plans)
        except Exception:
            # How far the engine got is unknown, so nothing new is registered: an experiment this manager has
            # never heard of is simply generated again on the next call, whereas one registered without
            # anything on disk behind it would be submitted by run_experiments() and would never be
            # generated, the check above having claimed it.
            new_names = sorted(str(plan.name) for plan in plans if not plan.regenerating)
            logger.warning(
                f"Experiment generation failed, so none of {new_names} were added to this manager. Some may "
                "already exist in the working directory, so their numbers are not given again: correcting the "
                "cause and calling this method again generates the experiments under the numbers that follow."
            )
            regenerating = sorted(str(plan.name) for plan in plans if plan.regenerating)
            if regenerating:
                logger.warning(
                    f"The experiments being regenerated, {regenerating}, are still held by this manager, "
                    "since they were already. How far the engine got through them is unknown, so some may "
                    "carry the correction and others not."
                )
            raise

        # What the study stated goes into each record, since it is what tells the experiment from the others. The
        # experiment takes copies of its own: it is compared with later calls and written into its archive's record
        # long after the choice it came from, whose dictionaries a study may still change in place.
        planned = {plan.name: plan for plan in plans}
        for name, experiment in created.items():
            experiment.changes = copy.deepcopy(planned[name].stated_changes)
            experiment.labels = copy.deepcopy(planned[name].labels)
            self.experiments[name] = experiment
            self._write_record(name)
        names = [str(plan.name) for plan in plans]
        self._log(call, names, generated=True)
        return names

    def _refuse_plans(self, plans: list[ExperimentPlan]) -> None:
        """Refuses the planned experiments that this manager's workflow engine can tell it could not create.

        Called with what `_create_experiments` is about to be called with, before the study takes a number for any
        of the plans. A call refused here leaves the study as it was, whereas one that fails in
        `_create_experiments` keeps its numbers, how far the engine got being unknown. So a subclass checks here
        whatever it can check without writing anything. Nothing is refused by default.

        Args:
            plans (list[ExperimentPlan]): What is about to be created, at least one plan, each with its name.

        Raises:
            Exception: Whatever the subclass refuses the plans with.
        """

    @abstractmethod
    def _create_experiments(self, plans: list[ExperimentPlan]) -> dict[str, ProfilingExperiment]:
        """Creates the planned experiments with this manager's workflow engine.

        Called once with everything one generate call creates, so that an engine which sets up many experiments in
        one invocation is invoked once, and only with what `_refuse_plans` let through. The changes made to the
        control first are `_control_options()`.
        Raising abandons the whole call and registers nothing.

        Args:
            plans (list[ExperimentPlan]): What to create. A plan marked as regenerating corrects an experiment
                this manager already holds, so the engine is to apply its changes to what is already there
                rather than start it over. A plan without a layout keeps the layout of the control, and one
                without a walltime states none beyond what its changes say.

        Returns:
            dict[str, ProfilingExperiment]: The experiments this manager is to start holding, keyed by name,
                each with the layout of its plan. The ones being regenerated are left out, being held already.
                The stated changes and the labels of each plan are put on its experiment by the caller.
        """

    @abstractmethod
    def run_experiments(self, retry_failed: bool = False) -> pd.DataFrame:
        """Runs the experiments this manager holds that have not run, with its workflow engine.

        Every NEW experiment is submitted, and so is every FAILED one where retry_failed asks for it, and each is
        marked RUNNING once the engine has taken it. The statuses are brought up to date first, since a status does
        not outlive the session and an experiment held as NEW may have run in an earlier one. The method returns
        once the experiments are submitted, without waiting for them to finish.

        Args:
            retry_failed (bool): Whether to run the experiments whose runs failed as well as the new ones. False
                (the default) runs only the new ones.

        Returns:
            pd.DataFrame: The experiments submitted, one row each, as status() shows them. It has no rows when there
                was nothing to submit.
        """

    def archive_experiments(
        self,
        exclude_dirs: list[str] | None = None,
        exclude_files: list[str] | None = None,
        follow_symlinks: bool = False,
        overwrite: bool = False,
    ) -> pd.DataFrame:
        """Archives completed experiments to the specified archive path.

        This method will create a tar.gz archive containing relevant data from an experiment. No data will be deleted
        once an experiment is archived, but data will be parsed directly from the archive instead of the original
        experiment directory.

        Args:
            exclude_dirs (list[str] | None): Directory patterns to exclude when archiving experiments.
            exclude_files (list[str] | None): File patterns to exclude when archiving experiments.
            follow_symlinks (bool): Whether to follow symlinks when archiving experiments. Defaults to False.
            overwrite (bool): Whether to overwrite existing archives. Defaults to False.

        The record in each archive states the number of CPUs of the experiment. Where the count cannot be read, the
        record leaves it null, with a warning, and the experiment is archived all the same.

        Returns:
            pd.DataFrame: The experiments archived by this call, one row each, as status() shows them.
        """
        # Only DONE experiments are archived, so a run that finished since the last look should be counted
        # among them rather than skipped as still running.
        self.update_statuses()
        done = [name for name, exp in self.experiments.items() if exp.status == ProfilingExperimentStatus.DONE]

        self.archive_dir.mkdir(parents=True, exist_ok=True)
        for branch, exp in self.experiments.items():
            if exp.status == ProfilingExperimentStatus.DONE:
                # The run is over, so the count can no longer change. Kept in the archive's record, it is not
                # read out of the tarball again. A count that cannot be read now is left to be read from the
                # tarball when it is needed, rather than stopping the other experiments being archived.
                try:
                    self._ncpus(branch)
                except (OSError, ValueError) as error:
                    logger.warning(
                        f"The number of CPUs of experiment '{branch}' could not be read ({error}), so the record in "
                        "its archive leaves it null. It is read from the archive when a selection or a plot needs it."
                    )
            exp.archive(
                self.archive_dir / branch,
                exclude_dirs=exclude_dirs,
                exclude_files=exclude_files,
                follow_symlinks=follow_symlinks,
                overwrite=overwrite,
                record=exp.record(self.work_dir),
            )

        # The study goes with its archives, so that it can still be told once the working directory is gone.
        study_file = self.work_dir / STUDY_FILENAME
        if study_file.is_file():
            shutil.copyfile(study_file, self.archive_dir / STUDY_FILENAME)
        return self._table(
            [name for name in done if self.experiments[name].status == ProfilingExperimentStatus.ARCHIVED]
        )

    def add_experiment_from_directory(self, name: str, path: Path | str, labels: dict | None = None) -> None:
        """Adds an existing experiment from the specified directory.

        Note that the directory must already exist on disk and be inside the working directory. Also, the experiment
        will be marked as DONE, so any runs associated with the experiment must already be completed.

        The experiment keeps the name it is given, and gets a record in `<work_dir>/<name>`, from which a manager
        built later finds it again.

        Args:
            name (str): Name of the experiment: the name of a single directory, other than one of the form
                `exp-<digits>`, which is what generated experiments are named.
            path (Path | str): Path to the experiment directory, absolute or relative to the working directory.
            labels (dict | None): The study's own keys and values for this experiment, as JSON values. None (the
                default) gives it none.

        Raises:
            ValueError: If the name is not that of a single directory or is of the form generated experiments are
                given, if the specified path does not exist, is not a directory, or is not inside the working
                directory, or if the labels are not JSON values. Nothing is added when it is raised.
        """
        self._refuse_added_name(name)
        path = Path(path)
        if not path.is_absolute():
            path = self.work_dir / path
        if not path.is_dir():
            raise ValueError(f"Experiment path '{path}' does not exist or is not a directory.")
        if not path.resolve().is_relative_to(self.work_dir.resolve()):
            raise ValueError(f"Experiment path '{path}' is not inside the working directory '{self.work_dir}'.")
        self.experiments[name] = ProfilingExperiment(path=path, labels=labels, complete=True)
        self._write_record(name)
        self._log("add_experiment_from_directory", [name])

    def forget_experiment(self, name: str) -> None:
        """Forgets an experiment, without deleting any of its files.

        This removes the experiment from the manager's tracking, together with its record in the working directory,
        and adds the call to the history of the study, so that a manager built later does not find it there again.
        It does not delete any of the experiment's own files, nor its archive: a manager built later still finds an
        experiment that was archived, from its tarball in the archive directory. delete_experiments is the one that
        deletes the files.

        Where the working directory cannot be written, as a colleague's study may not be, the experiment is
        forgotten in this session only, with a warning.

        Args:
            name (str): Name of the experiment to forget.
        """
        if name in self.experiments:
            # The working directory first, so that the experiment is still held if writing it fails part way.
            try:
                self._remove_record(name)
                self._log("forget_experiment", [name])
            except OSError as error:
                logger.warning(
                    f"Could not update the working directory '{self.work_dir}' ({error}), so experiment '{name}' is "
                    "forgotten in this session only: a manager built later finds it again."
                )
            del self.experiments[name]
        else:
            logger.warning(f"Experiment '{name}' not found; cannot forget it.")

    @abstractmethod
    def _delete_experiments(self, names: list[str], dry_run: bool, **kwargs) -> None:
        """Deletes the on-disk artifacts of the experiments selected for deletion.

        This is the runner-specific counterpart to delete_experiments, which handles selection, validation and
        manager-state bookkeeping. The whole selection comes at once, and while this runs the manager still holds
        every experiment, so that what the selected ones share with the rest can be told apart. Implementations
        should only remove files and, when dry_run is True, log what would be removed without making any changes.

        Args:
            names (list[str]): Names of the experiments to delete, at least one. Guaranteed to be managed by this
                instance.
            dry_run (bool): If True, log what would be deleted without making any changes.
            **kwargs: Runner-specific options forwarded verbatim from delete_experiments.
        """

    def delete_experiments(
        self,
        experiments: list[str] | None = None,
        all_experiments: bool = False,
        dry_run: bool = False,
        **kwargs,
    ) -> None:
        """Deletes experiments and removes them from the manager.

        The selection, validation and manager-state bookkeeping are handled here, while the actual on-disk deletion is
        delegated to the runner-specific _delete_experiments method.

        Args:
            experiments (list[str] | None): List of experiment names to delete.
            all_experiments (bool): If True, deletes all experiments managed by this instance.
            dry_run (bool): If True, logs what would be deleted without making any changes. Defaults to False.
            **kwargs (Any): Runner-specific options forwarded to _delete_experiments.

        Raises:
            ValueError: If both experiments and all_experiments are specified, or neither is.
            KeyError: If any experiment name is not managed by this instance.
        """
        if all_experiments and experiments is not None:
            raise ValueError("Pass either experiments=[...] or all_experiments=True, not both.")
        if not all_experiments and not experiments:
            raise ValueError("No experiments specified. Pass either experiments=[...] or all_experiments=True.")
        existing = set(self.experiments.keys())
        names_to_delete = list(self.experiments) if all_experiments else list(dict.fromkeys(experiments or ()))
        unmanaged = set(names_to_delete) - existing
        if unmanaged:
            raise KeyError(
                f"Experiments {unmanaged} are not managed by this manager "
                f"(existing: {existing}). Please check the names and try again."
            )
        if not names_to_delete:
            return

        self._delete_experiments(names_to_delete, dry_run=dry_run, **kwargs)

        if dry_run:
            return

        for name in names_to_delete:
            self._remove_record(name)
            del self.experiments[name]
        self._log("delete_experiments", names_to_delete)

    def parse_profiling_data(self) -> ProfilingResults:
        """Parses the profiling logs of every finished experiment, and returns them as results.

        The statuses are brought up to date first, so that a run that finished since the last look is parsed. Every
        experiment that is DONE or ARCHIVED is parsed. Every other one is passed over with a warning, and the results
        say why in their `skipped`.

        Experiments that ran several times produce one set of profiling logs per run. In that case the parsed
        datasets are concatenated along a 'run' dimension, coordinated by the number the workflow engine gives each
        run. Experiments with a single run are stored without a 'run' dimension, so that they can be used
        directly. The results' select_best_run() or aggregate_runs() reduce the 'run' dimension before plotting.
        Note that if all but one run fail to produce a log, the result has no 'run' dimension.

        While the directory of an experiment is open, the number of CPUs its job occupied and the layout it ran are
        read too, since the selections and the plots need them, unless the experiment has them already, as the record
        of a generated or an archived experiment gives them. One that cannot be read is kept with the reason, which a
        selection or a plot that needs it raises with the name of the experiment. Both are kept on the experiment, so
        they are read once.

        Every call parses the logs again and returns new results. Those an earlier call returned are left as they
        were.

        Returns:
            ProfilingResults: The profiling data of every finished experiment, with its labels, its layout and its
                number of CPUs.

        Raises:
            Exception: Whatever the parser of a log that is not optional raises.
        """
        # A run that finished since the last look is one whose data is wanted now, so ask before deciding
        # which experiments have any.
        self.update_statuses()

        parsed: dict[str, ParsedExperiment] = {}
        skipped: dict[str, str] = {}
        for exp_name, exp in self.experiments.items():
            if exp.status not in (ProfilingExperimentStatus.DONE, ProfilingExperimentStatus.ARCHIVED):
                logger.warning(
                    f"Experiment '{exp_name}' is not completed (status: {exp.status.name}). Skipping parsing profiling "
                    "data."
                )
                skipped[exp_name] = f"its status is {exp.status.name}"
                continue
            logger.info(f"Parsing profiling data for experiment '{exp_name}'.")
            with exp.directory() as (exp_path, run_path):
                data = self._parse_logs(exp_path, run_path)
                ncpus_error = self._read_ncpus(exp, exp_path, run_path)
                layout_error = self._read_layout(exp, exp_path, run_path)
            parsed[exp_name] = ParsedExperiment(data, exp.labels, exp.layout, exp.ncpus, layout_error, ncpus_error)
        return ProfilingResults(self.application, parsed, skipped)

    def _parse_logs(self, exp_path: Path, run_path: Path | None) -> dict[str, xr.Dataset]:
        """Returns the profiling data of the logs of one experiment, keyed by log name.

        Args:
            exp_path (Path): Path to the experiment directory, extracted where it is archived.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            dict[str, xr.Dataset]: The data of each log, with a 'run' dimension where several runs produced it.

        Raises:
            Exception: Whatever the parser of a log that is not optional raises.
        """
        data = {}
        for log_name, run_logs in self.profiling_logs(exp_path, run_path).items():
            datasets: dict[int, xr.Dataset] = {}
            for run, log in run_logs.items():
                logger.info(f"Parsing {log_name} profiling log for run {run}: {log.filepath}. ")
                if log.optional:
                    try:
                        datasets[run] = log.parse()
                    except FileNotFoundError:
                        logger.info(f"Optional profiling log '{log.filepath}' not found. Skipping.")
                        continue
                    except Exception as e:
                        # A warning, unlike a log that is not there: the run wrote it, and a parser that cannot read
                        # it is one to look into before the log is missed from a plot.
                        logger.warning(
                            f"Failed to parse optional profiling log '{log.filepath}' with exception:\n"
                            f"    {e}\nSkipping."
                        )
                        continue
                else:
                    datasets[run] = log.parse()
                logger.info(" Done.")
            # A single run is stored as is; several runs are concatenated along a new 'run' dimension.
            if len(datasets) == 1:
                data[log_name] = next(iter(datasets.values()))
            elif datasets:
                data[log_name] = xr.concat(
                    [ds.expand_dims({"run": [run]}) for run, ds in sorted(datasets.items())],
                    dim="run",
                    join="outer",
                )
        return data

    def _read_ncpus(self, experiment: ProfilingExperiment, exp_path: Path, run_path: Path | None) -> str | None:
        """Reads the number of CPUs of an experiment being parsed, unless it has it already.

        Args:
            experiment (ProfilingExperiment): The experiment, which is given the count.
            exp_path (Path): Path to its directory, which is open.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            str | None: Why the count could not be read, or None where it was, or was known already.
        """
        if experiment.ncpus is None:
            try:
                experiment.ncpus = self.parse_ncpus(exp_path, run_path)
            except (OSError, ValueError) as error:
                return str(error)
        return None

    def _read_layout(self, experiment: ProfilingExperiment, exp_path: Path, run_path: Path | None) -> str | None:
        """Reads the layout of an experiment being parsed back out of its files, unless it has one already.

        An experiment generated from a layout search already carries the layout it was generated from, in this
        session and, through its record, in every later one. It is the whole of it, and nothing else on disk says
        more. Only an experiment that was given no layout is read back from its configuration.

        Args:
            experiment (ProfilingExperiment): The experiment, which is given the layout.
            exp_path (Path): Path to its directory, which is open.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            str | None: Why the layout could not be read, or None where it was, or was known already, or where its
                files do not state one.
        """
        if experiment.layout is None:
            try:
                experiment.layout = self._parse_layout(exp_path, run_path)
            except (OSError, ValueError) as error:
                return str(error)
        return None

    def _ncpus(self, exp_name: str) -> int:
        """Returns the number of CPUs occupied by an experiment, parsing it at most once.

        The count is kept on the experiment itself, so it lasts exactly as long as the experiment does: an
        experiment added under a name used by a deleted one is parsed afresh rather than inheriting what the
        old one used. An archived experiment has the count its record states, which archive_experiments put
        there, instead of having its tarball extracted to read the same answer back.

        Args:
            exp_name (str): Name of the experiment.

        Returns:
            int: Number of CPUs the experiment occupied, as reported by parse_ncpus.
        """
        experiment = self.experiments[exp_name]
        if experiment.ncpus is None:
            with experiment.directory() as (exp_path, run_path):
                experiment.ncpus = self.parse_ncpus(exp_path, run_path)
        return experiment.ncpus

    def _parse_layout(self, path: Path, run_path: Path | None) -> ComponentLayout | None:
        """Returns the layout an experiment ran, read back out of its configuration files.

        The application reads the files, and which files those are is the workflow engine's: those of the
        experiment directory, unless a subclass knows of a copy the run kept, as a Cylc run keeps the
        configuration it used.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            ComponentLayout | None: The layout, or None if the files do not say.
        """
        return self.application.parse_layout(path)

    def status(self) -> pd.DataFrame:
        """Returns where every experiment of the study is, asking the workflow engine afresh.

        A status is not stored, so this is how a notebook sees which experiments have run, finished or failed,
        whichever session it is in. The labels are columns of their own, since the names of the experiments say
        nothing about them.

        Returns:
            pd.DataFrame: One row for each experiment, indexed by its name, with its status, the CPUs its job
                occupied where they are known yet, the key of the layout it was given, and one column for each label.
        """
        self.update_statuses()
        return self._table(list(self.experiments))

    def _table(self, names: list[str]) -> pd.DataFrame:
        """Returns some of the experiments as status() shows them, with the statuses they have.

        Args:
            names (list[str]): The experiments, in order.

        Returns:
            pd.DataFrame: One row for each experiment.
        """
        rows = []
        for name in names:
            experiment = self.experiments[name]
            # A table is how the experiments are looked at, so a layout of another application is said to be one,
            # as the manager's repr says it, and nothing is raised.
            try:
                key = self.layout_key(name)
            except ValueError:
                key = f"not a layout of {self.application.name}"
            row = {"experiment": name, "status": experiment.status.name, "ncpus": experiment.ncpus, "layout": key}
            rows.append(row | experiment.labels)
        return pd.DataFrame(rows, columns=["experiment", "status", "ncpus", "layout"] if not rows else None).set_index(
            "experiment"
        )

    def update_statuses(self) -> None:
        """Brings the status of every experiment up to date with the state its run actually reached.

        Every experiment is consulted but the archived ones, and each of the others for its own reason. A
        status lives in memory and so does not outlive the session, which means NEW says only that this
        manager has not submitted the experiment, not that it has never run. A failed experiment may have been
        fixed and run again by hand. A finished one may have been run again deliberately - parse_status reads
        the newest run, so a run under way reads back as running, which is what keeps parse_profiling_data off
        its half-written output and archive_experiments from tarring it mid-run.

        Only an archived experiment is settled: its tarball does not change, the directory it was made from
        may be gone, and asking would mean extracting the archive to be told what is already known.

        The answer is never cached, unlike the CPU count in _ncpus - a count does not change once a run is
        over, whereas the whole point of a status is that it does.

        An experiment whose state cannot be determined is left as it stands, so an experiment not yet run and
        one whose records cannot be read both simply keep the status they had.
        """
        for name, experiment in self.experiments.items():
            if experiment.status == ProfilingExperimentStatus.ARCHIVED:
                continue
            with experiment.directory() as (exp_path, run_path):
                status = self.parse_status(exp_path, run_path)
            if status is None:
                logger.debug(f"Could not determine the state of the run of experiment '{name}'. Leaving it be.")
                continue
            if status != experiment.status:
                logger.info(f"Experiment '{name}' is now {status.name}.")
                experiment.status = status
