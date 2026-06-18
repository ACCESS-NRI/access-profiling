# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import tarfile
import tempfile
from contextlib import contextmanager
from enum import Enum
from pathlib import Path

import xarray as xr

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
        """bool: Whether this log might not be present."""
        return self._optional

    def parse(self) -> xr.Dataset:
        """Parses the log file and returns the profiling data as an xarray Dataset.

        Accepts all three parser output formats (see parser.py module docstring):

        - **Flat**: standard 1D Dataset over the ``region`` dimension.
        - **Hierarchical nested dict**: automatically flattened via
          :func:`flatten_hierarchical` before building the Dataset.
        - **Per-PE**: produces a 2D Dataset with both ``region`` and ``pe`` dimensions.
          Use :func:`aggregate_pe_data` on the result to compute summary statistics.

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


class ProfilingExperimentStatus(Enum):
    """Enumeration representing the status of a profiling experiment."""

    NEW = 1  # Experiment has been created but not started
    RUNNING = 2  # Experiment is running or is queued
    DONE = 3  # Experiment has finished
    ARCHIVED = 4  # Experiment has been archived


def experiment_directory_walker(path: Path, arcname: Path, root: Path, follow_symlinks: bool = False):
    """Walks through the experiment directory, yielding files and corresponding names in the archive.

    Symlinks are treated in a special manner.
        - if the target is inside the experiment directory, the symlink itself is always returned
        - if follow_symlinks is True and the target is a directory, then the all contents in the target directory are
            recursively iterated
        - if follow_symlinks is True and the target is a file, then the target file name is returned, not the symlink
        - if follow_symlinks is False, then the symlink itself is returned for both files and directories

    Args:
        path (Path): Path to walk through.
        arcname (Path): Archive name for the current path.
        follow_symlinks (bool): Whether to follow symlinks. Defaults to False.

    Yields:
        Tuple[Path, Path]: A tuple containing the file path and its archive name.
    """
    if path.is_symlink():
        if not follow_symlinks:
            # Add symlink itself without following
            yield path, arcname
        else:
            target = path.resolve()
            if target.is_dir():
                # Recursively add target contents
                for child in target.iterdir():
                    yield from experiment_directory_walker(
                        child, Path(arcname) / child.name, root, follow_symlinks=follow_symlinks
                    )
            elif target.absolute().is_relative_to(root.absolute()):
                # Target is within the experiment directory, so add symlink as is
                yield path, arcname
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

    Args:
        path (Path): Path to the experiment directory.
        run_path (Path | None): Path to a separate runs directory. If None, runs are assumed to be
            inside path. When provided, the runs directory is also traversed during archival.
            path contents are stored under experiment/ and run_path contents under runs/.
    """

    path: Path  # Path to the experiment directory
    run_path: Path | None  # Path to a separate runs directory, or None
    status: ProfilingExperimentStatus = ProfilingExperimentStatus.NEW  # Status of the experiment

    def __init__(self, path: Path, run_path: Path | None = None) -> None:
        self.path = path
        self.run_path = run_path
        if self.path.suffixes == [".tar", ".gz"]:
            self.status = ProfilingExperimentStatus.ARCHIVED

    def __repr__(self) -> str:
        """Returns a string representation of the ProfilingExperiment."""
        if self.run_path is not None:
            return f"{type(self).__name__}(path={self.path!r}, run_path={self.run_path!r}, status={self.status.name})"
        return f"{type(self).__name__}(path={self.path!r}, status={self.status.name})"

    @contextmanager
    def directory(self):
        """Context manager returning the experiment and runs directories.

        If the experiment has been archived, it will be extracted to a temporary directory. Otherwise, the original
        directory paths will be used. Note that after exiting the context, the temporary directory is removed.

        Returns:
            tuple[Path, Path | None]: The experiment directory path and optional runs directory path.
        """
        if self.path.suffixes == [".tar", ".gz"]:
            with tempfile.TemporaryDirectory(prefix="access-profiling_", suffix="_data") as tmpdir:
                with tarfile.open(self.path) as tar:
                    tar.extractall(path=Path(tmpdir), filter="data")
                path = Path(tmpdir) / "experiment"
                run_path = Path(tmpdir) / "runs"
                yield path, run_path if run_path.exists() else None
        else:
            yield self.path, self.run_path

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

        Symlinks to files and directories inside the experiment directory will be include as symlinks. Symlinks to files
        and directories outside the experiment directory will be followed if follow_symlinks is True, otherwise they
        will be included as symlinks. path contents are stored under experiment/ in the archive. If
        run_path is set, its contents are stored under runs/ in the archive.

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
        if self.status == ProfilingExperimentStatus.NEW:
            logger.warning(f"Experiment at {self.path} is not yet started. Skipping archiving.", stacklevel=2)
            return
        elif self.status == ProfilingExperimentStatus.RUNNING:
            logger.warning(f"Experiment at {self.path} is still running. Skipping archiving.", stacklevel=2)
            return
        elif self.status == ProfilingExperimentStatus.DONE:
            logger.info(f"Archiving experiment at {self.path} to {archive_path.with_suffix('.tar.gz')}")
        elif self.status == ProfilingExperimentStatus.ARCHIVED:
            logger.warning(f"Experiment at {self.path} is already archived. Skipping archiving.", stacklevel=2)
            return

        archive_file = archive_path.with_suffix(".tar.gz")
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
                    tar.add(file, arcname=arcname)

        self.status = ProfilingExperimentStatus.ARCHIVED
        self.path = archive_file
        self.run_path = None
