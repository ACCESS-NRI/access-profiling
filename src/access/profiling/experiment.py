# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The experiments a profiling study is made of, and the profiling logs they produce.

A `ProfilingExperiment` is one experiment held by a manager: the directory it runs in, the status it has reached
and, once they are known, the cores it occupied and the layout it ran. Once finished it can be archived to a
tarball, and it is read from the tarball after that. A `ProfilingLog` pairs one log file with the parser that
reads it, and an `ExperimentPlan` is an experiment a manager has decided to create, before any workflow engine
has touched it.
"""

import logging
import os
import tarfile
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import xarray as xr
from access.config.parallel_component import ComponentLayout

from access.profiling.parser import ProfilingParser, flatten_hierarchical

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class ExperimentPlan:
    """One experiment a manager has decided to create, before any engine has touched it.

    Args:
        name (str): What identifies the experiment. It is the key this manager holds it under, the branch or
            directory it lives in, and the name written into the experiment's own configuration, so it has to
            be distinct for every distinct layout and stable from one session to the next.
        layout (ComponentLayout): The layout the experiment runs.
        changes (dict): The configuration file changes realising that layout, keyed by the path of each file
            relative to the control directory.
        walltime_hours (float): Walltime to request.
        regenerating (bool): Whether this plan corrects an experiment the manager already holds, rather than
            describing a new one.
    """

    name: str
    layout: ComponentLayout
    changes: dict
    walltime_hours: float
    regenerating: bool = False


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


def experiment_directory_walker(path: Path, arcname: Path, root: Path, follow_symlinks: bool = False):
    """Walks through the experiment directory, yielding files and corresponding names in the archive.

    Symlinks are treated in a special manner.

    - if the target is inside the experiment directory, the symlink itself is always returned, whether the target
      is a file or a directory, so that what it points to is archived once, where it is
    - if follow_symlinks is True and the target is a directory, then all the contents of the target directory are
      recursively iterated
    - if follow_symlinks is True and the target is a file, then the target file name is returned, not the symlink
    - if follow_symlinks is False, then the symlink itself is returned for both files and directories

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
        if not follow_symlinks or _target_inside(path, root) is not None:
            # Add symlink itself without following: what a symlink inside the experiment points to is archived
            # where it is
            yield path, arcname
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


class ProfilingExperiment:
    """Represents a profiling experiment.

    The number of CPUs the experiment occupied is kept here rather than in the manager, so that it lives
    and dies with the experiment it describes: deleting an experiment takes its count with it, and one
    added later under the same name starts without one. Only a manager can work the count out, since how
    to read it depends on the workflow engine, so ncpus is filled in on demand by
    ProfilingManager._ncpus() rather than at construction.

    The layout is kept here for the same reason, and arrives one of two ways. An experiment generated
    from a layout search is given the layout it was generated from, which is the whole of it, grids
    included. One this manager did not generate - archived, or added from a directory - is read back from
    its configuration on demand by ProfilingManager._layout(), which recovers what each component was
    given rather than how it divided its domain.

    Args:
        path (Path): Path to the experiment directory.
        run_path (Path | None): Path to a separate runs directory. If None, runs are assumed to be
            inside path. When provided, the runs directory is also traversed during archival.
            path contents are stored under experiment/ and run_path contents under runs/.
        layout (ComponentLayout | None): Layout the experiment runs, where it is known at construction.
            None (the default) leaves it to be read back from the configuration when it is asked for.

    Attributes:
        path (Path): Path to the experiment directory, or to its tarball once it has been archived.
        run_path (Path | None): Path to a separate runs directory, or None. Always None once the experiment has
            been archived, its runs being in the tarball.
        status (ProfilingExperimentStatus): Status of the experiment. NEW at construction, or ARCHIVED where path
            is a tarball.
        ncpus (int | None): CPUs the experiment occupied, or None while that is still unknown.
        layout (ComponentLayout | None): Layout the experiment runs, or None while that is unknown.
    """

    path: Path
    run_path: Path | None
    status: ProfilingExperimentStatus = ProfilingExperimentStatus.NEW
    ncpus: int | None = None
    layout: ComponentLayout | None = None

    def __init__(self, path: Path, run_path: Path | None = None, layout: ComponentLayout | None = None) -> None:
        self.path = path
        self.run_path = run_path
        self.layout = layout
        if self.path.name.endswith(".tar.gz"):
            self.status = ProfilingExperimentStatus.ARCHIVED

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
        produced it yet, one that failed has nothing worth keeping, and one already archived has had it kept.

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

        logger.warning(f"Experiment at {self.path} {reasons[self.status]}. Skipping archiving.", stacklevel=3)
        return False

    def archive(
        self,
        archive_path: Path,
        exclude_dirs: list[str] | None = None,
        exclude_files: list[str] | None = None,
        follow_symlinks: bool = False,
        overwrite: bool = False,
    ):
        """Archives the experiment to the specified archive path.

        Only experiments with status DONE will be archived. No error will be raised if the experiment is not DONE.

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

        Raises:
            FileExistsError: If the archive destination already exists and overwrite is False.
            ValueError: If the experiment status is unknown.
        """

        archive_file = archive_path.parent / (archive_path.name + ".tar.gz")

        if not self._ready_to_archive(archive_file):
            return

        mode = "w:gz" if overwrite else "x:gz"
        if not overwrite and archive_file.exists():
            raise FileExistsError(f"Archive destination {archive_file} already exists.")

        exclude_dirs = exclude_dirs or []
        exclude_files = exclude_files or []

        paths_to_walk = (
            [(self.path, Path("experiment"))]
            if self.run_path is None
            else [(self.path, Path("experiment")), (self.run_path, Path("runs"))]
        )

        with tarfile.open(archive_file, mode) as tar:
            for root, prefix in paths_to_walk:
                for file, arcname in experiment_directory_walker(root, prefix, root, follow_symlinks=follow_symlinks):
                    # Skip if file is inside an excluded directory pattern
                    if any(any(parent.match(pat) for pat in exclude_dirs) for parent in file.parents):
                        continue
                    # Skip if the file itself matches an excluded filename pattern
                    if any(file.match(pat) for pat in exclude_files):
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

        self.status = ProfilingExperimentStatus.ARCHIVED
        self.path = archive_file
        self.run_path = None
