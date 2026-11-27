# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The experiments a profiling study is made of, and the profiling logs they produce.

A `ProfilingExperiment` is one experiment held by a manager: the directory it runs in, the status it has reached
and, once they are known, the cores it occupied and the layout it ran. Once finished it can be archived to a
tarball, and it is read from the tarball after that. A `ProfilingLog` pairs one log file with the parser that
reads it. A `ConfigurationChoice` is one configuration a study asks an experiment for, and an `ExperimentPlan` is
an experiment a manager has decided to create, before any workflow engine has touched it. An `ExperimentCoordinate`
is a quantity of an experiment's layout that a table or a plot puts the experiments side by side by, such as the
cores of one component, which `component_cores` reads with the help of `find_component`.

An experiment's name says nothing about it, so what it is has to be written down. Each experiment has a record, a
small JSON file named `RECORD_FILENAME`: in its directory under the working directory, and as the first member of
its archive. The record holds only what differs from one experiment to the next, which is what lets a new manager
find the experiment again and tell whether a study already has it. Its status is not part of it: a status changes,
and is the workflow engine's to say.

The record also states the version of its own format, `RECORD_VERSION`. It is written once and may be read from an
archive years later, by a later version of this package, so the file itself has to say how it is to be read.
"""

import copy
import io
import json
import logging
import os
import tarfile
import tempfile
import time
from collections.abc import Callable, Hashable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import xarray as xr
from access.config.parallel_component import ComponentLayout, CoreSharing
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid

from access.profiling.parser import ProfilingParser, flatten_hierarchical

logger = logging.getLogger(__name__)

RECORD_FILENAME: str = "access-profiling.json"
"""The name of an experiment's record, in its directory under the working directory and in its archive."""

RECORD_VERSION: int = 1
"""The version of the format of an experiment's record, which every record states."""

# What every record holds, which is checked when one is read, so that a record missing an entry is refused with its
# name rather than with a bare KeyError.
_RECORD_KEYS = ("version", "path", "run_path", "complete", "layout", "changes", "labels", "ncpus")


def _make_unique_region_names(regions: list[object]) -> list[object]:
    """Return region names with deterministic suffixes for duplicates."""

    counts: dict[object, int] = {}
    unique_regions: list[object] = []
    for region in regions:
        count = counts.get(region, 0) + 1
        counts[region] = count
        unique_regions.append(region if count == 1 else f"{region}_{count}")
    return unique_regions


class ProfilingLog:
    """Represents a profiling log file.

    Args:
        filepath (Path): Path to the log file.
        parser (ProfilingParser): Parser to use for this log file.
        optional (bool): Whether this log might be missing or does not contain parsable data. If True, no error should
            be raised if the log is missing or unparsable. Defaults to False.
    """

    filepath: Path  # Path to the log file
    parser: ProfilingParser  # Parser to use for this log file
    _optional: bool = False  # Whether this log might not be present

    def __init__(self, filepath: Path, parser: ProfilingParser, optional: bool = False) -> None:
        self.filepath = filepath
        self.parser = parser
        self._optional = optional

    @property
    def optional(self) -> bool:
        """Returns whether this log might not be present.

        Returns:
            bool: True if the log might be missing or might not contain parsable data, in which case a manager
                skips it rather than raising.
        """
        return self._optional

    def parse(self) -> xr.Dataset:
        """Parses the log file and returns the profiling data as an xarray Dataset.

        Accepts all three parser output formats (see parser.py module docstring):

        - **Flat**: standard 1D Dataset over the `region` dimension.
        - **Hierarchical nested dict**: automatically flattened via
          `flatten_hierarchical` before building the Dataset.
        - **Per-PE**: produces a 2D Dataset with both `region` and `pe` dimensions.
          Use `aggregate_pe_data` on the result to compute summary statistics.

        Returns:
           xr.Dataset: Parsed profiling data.
        """
        data = self.parser.parse(self.filepath)

        # Flatten hierarchical (nested dict) format if needed
        if "region" not in data:
            data = flatten_hierarchical(data, self.parser.metrics)

        has_pe = "pe" in data
        dims = ["region", "pe"] if has_pe else ["region"]
        coords: dict = {"region": _make_unique_region_names(list(data["region"]))}
        if has_pe:
            coords["pe"] = data["pe"]

        return xr.Dataset(
            data_vars=dict(
                zip(
                    self.parser.metrics,
                    [xr.DataArray(data[m], dims=dims).pint.quantify(m.units) for m in self.parser.metrics],
                    strict=True,
                )
            ),
            coords=coords,
        )


def _refuse_other_than_json(what: str, value: dict) -> None:
    """Refuses a value that an experiment's record could not hold and give back as it was.

    Args:
        what (str): What the value is and whose, for the message.
        value (dict): The value.

    Raises:
        ValueError: If the value is not made of JSON values, as NaN and infinity are not, or is not the same once
            written and read again, as a tuple or a key that is not a string is not.
    """
    try:
        unchanged = json.loads(json.dumps(value, allow_nan=False)) == value
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"The {what} must be JSON values: strings, finite numbers, booleans, null, and lists and mappings of them "
            f"with strings as keys. Got {value!r}."
        ) from error
    if not unchanged:
        raise ValueError(
            f"The {what} must be JSON values, which {value!r} is not: written to a record and read again, it is "
            "not what it was. Use lists rather than tuples, and strings as keys."
        )


@dataclass(frozen=True)
class ConfigurationChoice:
    """One configuration a study asks an experiment for: changes to the control's files, a layout, or both.

    The changes are stated the way the workflow engine of the manager takes them, keyed by the path of each file
    relative to the control directory, and are written as given. With a layout, they are merged over the changes
    that realise the layout, and nothing checks the one against the other: the application works out what a layout
    comes to from its own fields, not from what a choice changes.

    The changes and the labels are kept in the experiment's record, which is how a study tells whether it already
    has the experiment of a choice. They are therefore JSON values. A choice holds copies of its own, so changing
    the dictionaries it was made from afterwards changes neither the choice nor the experiment generated from it.

    Args:
        changes (dict): What the experiment changes in the control, keyed by file. Empty (the default) changes
            nothing beyond the layout.
        layout (ComponentLayout | None): The layout the experiment runs, for example one that
            `ProfilingManager.select_layouts` returned. None (the default) keeps the layout of the control.
        labels (dict): The study's own keys and values for the experiment, such as `{"distribution":
            "roundrobin"}`. They say what the choice is to whoever reads the study, and are not part of what
            identifies the experiment. Empty by default.

    Raises:
        ValueError: If the changes or the labels are not JSON values.
    """

    changes: dict = field(default_factory=dict)
    layout: ComponentLayout | None = None
    labels: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        _refuse_other_than_json("changes of a configuration choice", self.changes)
        _refuse_other_than_json("labels of a configuration choice", self.labels)
        # Copied, rather than written and read back as JSON, which would turn a YAML library's quoted string into
        # a plain one and lose its quotes. A study may build several choices from one dictionary and change it
        # between batches, so each choice holds what it was validated as.
        object.__setattr__(self, "changes", copy.deepcopy(self.changes))
        object.__setattr__(self, "labels", copy.deepcopy(self.labels))


@dataclass(frozen=True)
class ExperimentPlan:
    """One experiment a manager has decided to create, before any engine has touched it.

    Args:
        name (str): The experiment's name. It is the key this manager holds it under, the branch or directory it
            lives in, and the name written into the experiment's own configuration.
        layout (ComponentLayout | None): The layout the experiment runs, or None for an experiment that keeps the
            layout of the control.
        changes (dict): Every configuration file change the engine is to make, keyed by the path of each file
            relative to the control directory: those realising the layout, with the stated ones merged over them.
        walltime_hours (float | None): Walltime to request, or None where the plan states none of its own: the
            changes may state one, and otherwise the experiment keeps the control's.
        regenerating (bool): Whether this plan corrects an experiment the manager already holds, rather than
            describing a new one.
        stated_changes (dict): What the study stated beyond the layout, which the experiment's record keeps as its
            changes. Empty (the default) for an experiment that is its layout and nothing more.
        labels (dict): The study's own keys and values for the experiment. Empty by default.
    """

    name: str
    layout: ComponentLayout | None
    changes: dict
    walltime_hours: float | None
    regenerating: bool = False
    stated_changes: dict = field(default_factory=dict)
    labels: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ExperimentCoordinate:
    """A quantity of the layout an experiment runs, which a table or a plot puts the experiments side by side by.

    The experiments of a study differ in two kinds of things. What the study says about an experiment is in its
    labels, and a label is named by its key, as a plain string. What the layout of an experiment says is read with
    a coordinate: a name and a function that works the quantity out from a layout, such as the number of cores the
    layout gives one component. Nothing is read out of the name of an experiment.

    Args:
        name (str): What the quantity is called. It names the coordinate in the data
            `ProfilingManager.gather` returns, and is the default label of an axis.
        value (Callable[[ComponentLayout], Hashable]): Returns the quantity for a layout. It raises `ValueError`
            for a layout that does not have the quantity.
    """

    name: str
    value: Callable[[ComponentLayout], Hashable]


def find_component(layout: ComponentLayout, name: str) -> ComponentLayout | None:
    """Returns the part of a layout belonging to the named component, wherever it sits in it.

    A component is looked for at any depth, since a tree groups its components as the application runs them
    rather than as a reader asks about them: a component that takes its turn on a range of cores shared with
    others sits under that range, not beside the components running concurrently with it.

    Args:
        layout (ComponentLayout): Layout to look in, itself included.
        name (str): Name of the component to look for.

    Returns:
        ComponentLayout | None: The layout of that component, or None if it holds no such component.
    """
    if layout.name == name:
        return layout
    for sub_layout in layout.sub_layouts:
        found = find_component(sub_layout, name)
        if found is not None:
            return found
    return None


def component_cores(name: str) -> ExperimentCoordinate:
    """Returns the coordinate that is the number of cores a layout gives the named component.

    The component is looked for at any depth of the layout, as `find_component` looks for it, and the coordinate is
    named `"<name> cores"`.

    Args:
        name (str): Name of the component, as the layout names it.

    Returns:
        ExperimentCoordinate: The coordinate. Its value raises `ValueError` for a layout that holds no such
            component.
    """

    def cores(layout: ComponentLayout) -> int:
        found = find_component(layout, name)
        if found is None:
            raise ValueError(f"The layout holds no component '{name}'. It holds {sorted(_component_names(layout))}.")
        return found.n_cores

    return ExperimentCoordinate(f"{name} cores", cores)


def _component_names(layout: ComponentLayout) -> list[str]:
    """Returns the name of every component in a layout, itself included, for saying what it does hold."""

    names = [layout.name]
    for sub_layout in layout.sub_layouts:
        names.extend(_component_names(sub_layout))
    return names


class ProfilingExperimentStatus(Enum):
    """Enumeration representing the status of a profiling experiment.

    A status is kept in memory, on the experiment, so it does not outlive the session. A manager brings it up to
    date with what the workflow engine recorded in `ProfilingManager.update_statuses`, which archiving, parsing and
    running experiments all call first. `CylcRoseManager.add_rose_experiment` starts a suite from those records
    too.

    Attributes:
        NEW: Experiment has been created but not started. Every experiment a manager generates starts here, as
            does a suite added with no run recorded, but since a status does not outlive the session, this only
            says that this manager has not submitted it: `update_statuses` moves it on if the engine has records
            of a run.
        RUNNING: Experiment is running or is queued. Set by `run_experiments` once it has submitted the
            experiment, and by `update_statuses` while the engine's records say a run is under way.
        DONE: Experiment has finished successfully. Set by `update_statuses` from the engine's records, and by
            `add_experiment_from_directory`, which takes the experiment it adds to be complete. Only a DONE
            experiment is archived, and only a DONE or an ARCHIVED one is parsed.
        FAILED: Experiment ran but did not finish successfully. Set by `update_statuses` from the engine's
            records. `run_experiments(retry_failed=True)` runs it again, and
            `generate_scaling_experiments(regenerate_failed=True)` regenerates it first.
        ARCHIVED: Experiment has been archived. Set by `ProfilingExperiment.archive`, and from the start for an
            experiment created from a tarball. It is final: `update_statuses` leaves an archived experiment alone.
    """

    NEW = 1
    RUNNING = 2
    DONE = 3
    FAILED = 4
    ARCHIVED = 5


def _target_inside(link: Path, root: Path) -> Path | None:
    """Returns where a symlink points relative to the directory being archived, if it points inside it.

    Args:
        link (Path): The symlink.
        root (Path): The directory being archived.

    Returns:
        Path | None: The target of the link relative to root, or None if the target is outside root.
    """
    target = link.resolve()
    real_root = root.resolve()
    return target.relative_to(real_root) if target.is_relative_to(real_root) else None


def _excluded(file: Path, name: Path, exclude_dirs: list[str], exclude_files: list[str]) -> bool:
    """Returns whether an entry of the walk is left out of the archive by the exclusion patterns.

    The patterns are matched against the entry's path in the archive, so that the directories above the one being
    archived never match, and a file is matched by the name it is archived under.

    Args:
        file (Path): The entry on disk, as the walk yields it.
        name (Path): Its path in the archive, relative to the directory being archived.
        exclude_dirs (list[str]): Directory patterns: an entry in a matching directory is left out, and so is a
            symlink to a directory that is kept as a link and matches one itself.
        exclude_files (list[str]): File patterns, matched against the entry itself.

    Returns:
        bool: True if the entry is left out.
    """
    directories = list(name.parents)[:-1]  # the last is the directory being archived itself
    if file.is_symlink() and file.is_dir():
        directories.append(name)
    if any(directory.match(pattern) for directory in directories for pattern in exclude_dirs):
        return True
    return any(name.match(pattern) for pattern in exclude_files)


def experiment_directory_walker(path: Path, arcname: Path, root: Path, follow_symlinks: bool = False):
    """Walks through the experiment directory, yielding files and corresponding names in the archive.

    Symlinks are treated in a special manner.

    - if the target is inside the experiment directory, the symlink itself is always returned, whether the target
      is a file or a directory, so that what it points to is archived once, where it is
    - if follow_symlinks is True and the target is a directory, then all the contents of the target directory are
      recursively iterated
    - if follow_symlinks is True and the target is a file, then the target file name is returned, not the symlink
    - if follow_symlinks is False and the target is outside the experiment directory, then the symlink is left out,
      and logged at INFO: kept as a link, it could not be extracted, since the data filter ProfilingExperiment
      directory extracts archives with refuses links that point outside the archive

    Args:
        path (Path): Path to walk through.
        arcname (Path): Archive name for the current path.
        root (Path): The directory being archived, which the walk started from. A symlink whose target is inside it
            counts as being inside the experiment directory.
        follow_symlinks (bool): Whether to follow symlinks. Defaults to False.

    Yields:
        Tuple[Path, Path]: A tuple containing the file path and its archive name.
    """
    if path.is_symlink():
        if _target_inside(path, root) is not None:
            # Add symlink itself without following: what a symlink inside the experiment points to is archived
            # where it is
            yield path, arcname
        elif not follow_symlinks:
            logger.info(
                f"Leaving {path} out of the archive: it links outside the experiment, to {path.resolve()}, and "
                "symlinks are not followed."
            )
        else:
            target = path.resolve()
            if target.is_dir():
                # Recursively add target contents
                for child in target.iterdir():
                    yield from experiment_directory_walker(
                        child, Path(arcname) / child.name, root, follow_symlinks=follow_symlinks
                    )
            else:
                # Target is outside the experiment directory, add the target file instead
                yield target, arcname

    elif path.is_dir():
        # Recursively add directory contents
        for child in path.iterdir():
            yield from experiment_directory_walker(
                child, Path(arcname) / child.name, root, follow_symlinks=follow_symlinks
            )
    else:
        yield path, arcname


def _layout_record(layout: ComponentLayout) -> dict:
    """Returns a layout as JSON values, field by field, its sub-layouts included.

    Args:
        layout (ComponentLayout): The layout.

    Returns:
        dict: What `_layout_from_record` builds the same layout from.
    """
    decomposition = layout.decomposition
    return {
        "name": layout.name,
        "n_cores": layout.n_cores,
        "n_ranks": layout.n_ranks,
        "threads_per_rank": layout.threads_per_rank,
        "decomposition": None
        if decomposition is None
        else {"domain": list(decomposition.domain.shape), "grid": list(decomposition.grid.shape)},
        "sub_layouts": [_layout_record(sub_layout) for sub_layout in layout.sub_layouts],
        "core_sharing": layout.core_sharing.name,
        "core_offset": layout.core_offset,
    }


def _layout_from_record(record: dict) -> ComponentLayout:
    """Returns the layout a record holds, the inverse of `_layout_record`.

    The layout is built again rather than trusted, so one a record states wrongly is refused as any other is.

    Args:
        record (dict): The layout, as `_layout_record` wrote it.

    Returns:
        ComponentLayout: The layout.

    Raises:
        ValueError: If what the record states is not a layout.
    """
    decomposition = record["decomposition"]
    return ComponentLayout(
        name=record["name"],
        n_cores=record["n_cores"],
        n_ranks=record["n_ranks"],
        threads_per_rank=record["threads_per_rank"],
        decomposition=None
        if decomposition is None
        else DomainDecompositionSpec(
            Domain(shape=tuple(decomposition["domain"])), MPICartesianGrid(shape=tuple(decomposition["grid"]))
        ),
        sub_layouts=tuple(_layout_from_record(sub_layout) for sub_layout in record["sub_layouts"]),
        core_sharing=CoreSharing[record["core_sharing"]],
        core_offset=record["core_offset"],
    )


def read_versioned(text: str | bytes, source: Path, what: str, version: int, keys: Iterable[str]) -> dict:
    """Returns what a file written by this package holds, refusing one in a format this version does not read.

    Args:
        text (str | bytes): The contents of the file.
        source (Path): The file, or the archive it is in, for the message.
        what (str): What the file is, for the message.
        version (int): The version of the format that this version of the package reads.
        keys (Iterable[str]): The entries the file holds in that version.

    Returns:
        dict: What the file holds.

    Raises:
        ValueError: If the contents are not a JSON object, if they state no format version or another one than
            `version`, or if they lack one of the keys.
    """
    try:
        content = json.loads(text)
    except ValueError as error:
        raise ValueError(f"The {what} '{source}' cannot be read, since it is not JSON: {error}.") from error
    if not isinstance(content, dict):
        raise ValueError(
            f"The {what} '{source}' cannot be read, since it holds a JSON {type(content).__name__} and not an object."
        )
    found = content.get("version")
    if found != version:
        stated = "no format version" if found is None else f"the format version {found}"
        raise ValueError(
            f"The {what} '{source}' states {stated}, and this version of access-profiling reads version {version}. "
            "Update access-profiling if a later version of it wrote the file."
        )
    missing = [key for key in keys if key not in content]
    if missing:
        raise ValueError(f"The {what} '{source}' cannot be read, since it lacks the entries {missing}.")
    return content


def read_record(record_file: Path) -> dict:
    """Returns the record of an experiment, from its directory under the working directory.

    Args:
        record_file (Path): The record.

    Returns:
        dict: The record.

    Raises:
        ValueError: If the record is not a JSON object, if it states no format version or another one than
            `RECORD_VERSION`, or if it lacks one of its entries.
    """
    return read_versioned(record_file.read_text(), record_file, "record", RECORD_VERSION, _RECORD_KEYS)


def read_archived_record(archive_file: Path) -> dict | None:
    """Returns the record an archive holds, read without extracting the archive.

    `ProfilingExperiment.archive` writes the record first, so only the start of the tarball is read.

    Args:
        archive_file (Path): The archive.

    Returns:
        dict | None: The record, or None for an archive that holds none, as one made before experiments had
            records does.

    Raises:
        ValueError: If the record is not a JSON object, if it states no format version or another one than
            `RECORD_VERSION`, or if it lacks one of its entries.
    """
    with tarfile.open(archive_file) as tar:
        first = tar.next()
        if first is None or first.name != RECORD_FILENAME:
            return None
        member = tar.extractfile(first)
        assert member is not None, "the record is added as a regular file"
        return read_versioned(member.read(), archive_file, "record in the archive", RECORD_VERSION, _RECORD_KEYS)


class ProfilingExperiment:
    """Represents a profiling experiment.

    The number of CPUs the experiment occupied is kept here rather than in the manager, so that it lives
    and dies with the experiment it describes: deleting an experiment takes its count with it, and one
    added later under the same name starts without one. Only a manager can work the count out, since how
    to read it depends on the workflow engine, so ncpus is filled in on demand by
    ProfilingManager._ncpus() rather than at construction.

    The layout is kept here for the same reason, and arrives one of two ways. An experiment generated
    from a layout search, or from a configuration choice that has a layout, is given the layout it was
    generated from, which is the whole of it, grids included, and its record keeps it, so that the
    experiment has it again in a later session and once it is archived. One that was given no layout -
    generated from a choice without one, added from a directory, or archived before experiments had records -
    is read back from its configuration on demand by ProfilingManager._layout(), which recovers what each
    component was given rather than how it divided its domain.

    Args:
        path (Path): Path to the experiment directory.
        run_path (Path | None): Path to a separate runs directory. If None, runs are assumed to be
            inside path. When provided, the runs directory is also traversed during archival.
            path contents are stored under experiment/ and run_path contents under runs/.
        layout (ComponentLayout | None): Layout the experiment was given, where it was given one. None (the
            default) leaves it to be read back from the configuration when it is asked for.
        changes (dict | None): What the study changed in the control beyond the layout, keyed by file. None (the
            default) says they are unknown, as they are for an experiment this manager did not generate.
        labels (dict | None): The study's own keys and values for this experiment, as JSON values, since the
            record keeps them. None (the default) gives it none.
        complete (bool): Whether the experiment was added as one that had finished. Defaults to False.

    Raises:
        ValueError: If the labels are not JSON values.

    Attributes:
        path (Path): Path to the experiment directory, or to its tarball once it has been archived.
        run_path (Path | None): Path to a separate runs directory, or None. Always None once the experiment has
            been archived, its runs being in the tarball.
        status (ProfilingExperimentStatus): Status of the experiment. ARCHIVED where path is a tarball, DONE
            where the experiment was added as complete, and NEW otherwise.
        ncpus (int | None): CPUs the experiment occupied, or None while that is still unknown.
        layout (ComponentLayout | None): Layout the experiment runs, or None while that is unknown.
        layout_given (bool): Whether the layout is the one the experiment was given, rather than one read back
            from its configuration. Only a given layout is part of what identifies the experiment, and only a
            given one is written into its record.
        changes (dict | None): What the study changed in the control beyond the layout, or None if unknown.
        labels (dict): The study's own keys and values for this experiment.
        complete (bool): Whether the experiment was added as one that had finished.
    """

    path: Path
    run_path: Path | None
    status: ProfilingExperimentStatus = ProfilingExperimentStatus.NEW
    ncpus: int | None = None
    layout: ComponentLayout | None = None

    def __init__(
        self,
        path: Path,
        run_path: Path | None = None,
        layout: ComponentLayout | None = None,
        changes: dict | None = None,
        labels: dict | None = None,
        complete: bool = False,
    ) -> None:
        self.path = path
        self.run_path = run_path
        self.layout = layout
        self.layout_given = layout is not None
        self.changes = changes
        self.labels = copy.deepcopy(labels) if labels is not None else {}
        _refuse_other_than_json("labels of an experiment", self.labels)
        self.complete = complete
        if self.path.name.endswith(".tar.gz"):
            self.status = ProfilingExperimentStatus.ARCHIVED
        elif complete:
            self.status = ProfilingExperimentStatus.DONE

    @classmethod
    def from_record(cls, record: dict, path: Path, run_path: Path | None = None) -> "ProfilingExperiment":
        """Returns the experiment a record describes.

        Args:
            record (dict): The record, as `record` returned it.
            path (Path): Where the experiment is now: its directory, or its tarball.
            run_path (Path | None): Where its runs are, if they are kept elsewhere.

        Returns:
            ProfilingExperiment: The experiment, with the layout, the changes, the labels and the CPU count the
                record states.

        Raises:
            ValueError: If the layout the record states is not a layout.
        """
        layout = None if record["layout"] is None else _layout_from_record(record["layout"])
        experiment = cls(
            path, run_path, layout, changes=record["changes"], labels=record["labels"], complete=record["complete"]
        )
        experiment.ncpus = record["ncpus"]
        return experiment

    def record(self, work_dir: Path) -> dict:
        """Returns this experiment's record: the version of its format, and what differs from one experiment of a
        study to the next.

        Args:
            work_dir (Path): The working directory of the study, which the experiment's path is stated
                relative to where it is inside it.

        Returns:
            dict: The record, as JSON values.
        """
        path = self.path.relative_to(work_dir) if self.path.is_relative_to(work_dir) else self.path
        return {
            "version": RECORD_VERSION,
            "path": str(path),
            "run_path": None if self.run_path is None else str(self.run_path),
            "complete": self.complete,
            # A layout read back from the configuration is left out: it says less than the one the experiment
            # was given, and is not what identifies it.
            "layout": _layout_record(self.layout) if self.layout is not None and self.layout_given else None,
            "changes": self.changes,
            "labels": self.labels,
            "ncpus": self.ncpus,
        }

    def __repr__(self) -> str:
        """Returns a string representation of the ProfilingExperiment.

        The fields that may be unset are reported only once they are set, so that the representation of an
        experiment states what is known about it and nothing else. The layout is left out altogether: a
        whole component tree on this line would bury everything else on it, and what is wanted here is
        what an experiment is, not how it divides its cores.
        """
        fields = [f"path={self.path!r}"]
        if self.run_path is not None:
            fields.append(f"run_path={self.run_path!r}")
        fields.append(f"status={self.status.name}")
        if self.ncpus is not None:
            fields.append(f"ncpus={self.ncpus}")
        if self.labels:
            fields.append(f"labels={self.labels}")
        return f"{type(self).__name__}({', '.join(fields)})"

    @contextmanager
    def directory(self):
        """Context manager returning the experiment and runs directories.

        If the experiment has been archived, it will be extracted to a temporary directory. Otherwise, the original
        directory paths will be used. Note that after exiting the context, the temporary directory is removed.

        Returns:
            tuple[Path, Path | None]: The experiment directory path and optional runs directory path.
        """
        if self.path.name.endswith(".tar.gz"):
            with tempfile.TemporaryDirectory(prefix="access-profiling_", suffix="_data") as tmpdir:
                with tarfile.open(self.path) as tar:
                    tar.extractall(path=Path(tmpdir), filter="data")
                path = Path(tmpdir) / "experiment"
                run_path = Path(tmpdir) / "runs"
                yield path, run_path if run_path.exists() else None
        else:
            yield self.path, self.run_path

    def _ready_to_archive(self, archive_file: Path) -> bool:
        """Returns whether this experiment may be archived, saying why in the log when it may not.

        Only a finished experiment has anything worth keeping: one still to start or still running has not
        produced it yet, one that failed has nothing worth keeping, and one already archived has had it kept. The
        last is said at INFO, the others as a warning: archiving a study again passes over every experiment it
        archived before, which is no cause for concern.

        Args:
            archive_file (Path): Where the archive would be written, named in the log message.

        Returns:
            bool: True if archiving should go ahead.

        Raises:
            ValueError: If the status is not one this knows how to judge.
        """
        if self.status == ProfilingExperimentStatus.DONE:
            logger.info(f"Archiving experiment at {self.path} to {archive_file}")
            return True

        reasons = {
            ProfilingExperimentStatus.NEW: "is not yet started",
            ProfilingExperimentStatus.RUNNING: "is still running",
            ProfilingExperimentStatus.FAILED: "did not run successfully",
            ProfilingExperimentStatus.ARCHIVED: "is already archived",
        }
        if self.status not in reasons:
            # Every member of the enum is accounted for above, so this is one added without deciding whether
            # it may be archived - which would otherwise fall through and archive it.
            raise ValueError(f"Experiment at {self.path} has an unknown status {self.status}.")

        level = logging.INFO if self.status == ProfilingExperimentStatus.ARCHIVED else logging.WARNING
        logger.log(level, f"Experiment at {self.path} {reasons[self.status]}. Skipping archiving.", stacklevel=3)
        return False

    def archive(
        self,
        archive_path: Path,
        exclude_dirs: list[str] | None = None,
        exclude_files: list[str] | None = None,
        follow_symlinks: bool = False,
        overwrite: bool = False,
        record: dict | None = None,
    ):
        """Archives the experiment to the specified archive path.

        Only experiments with status DONE will be archived. No error will be raised if the experiment is not DONE.

        A record is written first, as `RECORD_FILENAME`, so that it can be read back without extracting the rest.
        The archive is written whole or not at all: it is written to a temporary file beside it, which then takes
        its name. Otherwise an archiving stopped part way would leave a tarball whose record is readable ahead of
        the rest, and a manager built later would read the experiment from it.

        Symlinks to files and directories inside the experiment directory will be included as symlinks, pointing
        to their targets relative to where they are in the archive, so that they still point at them once the archive
        is extracted. Symlinks to files and directories outside the experiment directory will be followed if
        follow_symlinks is True, otherwise they will be included as symlinks. path contents are stored under
        experiment/ in the archive. If run_path is set, its contents are stored under runs/ in the archive.

        Args:
            archive_path (Path): Path to the archive destination. This should include the file name, but without
                the .tar.gz suffix.
            exclude_dirs (list[str] | None): Directory patterns to exclude when archiving.
            exclude_files (list[str] | None): File patterns to exclude when archiving.
            follow_symlinks (bool): Whether to follow symlinks when archiving. Defaults to False.
            overwrite (bool): Whether to overwrite existing archives. Defaults to False.
            record (dict | None): The experiment's record, to keep in the archive. None (the default) keeps none.

        Raises:
            FileExistsError: If the archive destination already exists and overwrite is False.
            ValueError: If the experiment status is unknown.
        """

        archive_file = archive_path.parent / (archive_path.name + ".tar.gz")

        if not self._ready_to_archive(archive_file):
            return

        if not overwrite and archive_file.exists():
            raise FileExistsError(f"Archive destination {archive_file} already exists.")

        exclude_dirs = exclude_dirs or []
        exclude_files = exclude_files or []

        paths_to_walk = (
            [(self.path, Path("experiment"))]
            if self.run_path is None
            else [(self.path, Path("experiment")), (self.run_path, Path("runs"))]
        )

        partial = archive_file.with_name(f".{archive_file.name}.{os.getpid()}.tmp")
        try:
            with tarfile.open(partial, "w:gz") as tar:
                if record is not None:
                    data = json.dumps(record, indent=2).encode()
                    info = tarfile.TarInfo(RECORD_FILENAME)
                    info.size = len(data)
                    info.mtime = time.time()
                    tar.addfile(info, io.BytesIO(data))
                for path, prefix in paths_to_walk:
                    # A directory reached through a symlink, as a Cylc run directory often is, is archived as what it
                    # points to, rather than as the one link.
                    root = path.resolve()
                    for file, arcname in experiment_directory_walker(
                        root, prefix, root, follow_symlinks=follow_symlinks
                    ):
                        if _excluded(file, arcname.relative_to(prefix), exclude_dirs, exclude_files):
                            continue
                        logger.debug(f"Archiving file: {file} as {arcname}")
                        inside = _target_inside(file, root) if file.is_symlink() else None
                        if inside is None:
                            tar.add(file, arcname=arcname)
                            continue
                        # Rewritten relative to the link's place in the archive, which also keeps it extractable at all:
                        # the data filter directory() extracts with refuses a link to an absolute path.
                        link = tar.gettarinfo(file, arcname=str(arcname))
                        link.linkname = os.path.relpath(prefix / inside, arcname.parent)
                        tar.addfile(link)
            partial.replace(archive_file)
        except BaseException:
            partial.unlink(missing_ok=True)
            raise

        self.status = ProfilingExperimentStatus.ARCHIVED
        self.path = archive_file
        self.run_path = None
