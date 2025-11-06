# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import tarfile
import tempfile
import warnings
from contextlib import contextmanager
from enum import Enum
from pathlib import Path

import xarray as xr

from access.profiling.parser import ProfilingParser

logger = logging.getLogger(__name__)


class ProfilingLog:
    """Represents a profiling log file.

    Args:
        filepath (Path): Path to the log file.
        parser (ProfilingParser): Parser to use for this log file.
    """

    filepath: Path  # Path to the log file
    parser: ProfilingParser  # Parser to use for this log file

    def __init__(self, filepath: Path, parser: ProfilingParser):
        self.filepath = filepath
        self.parser = parser

    def parse(self) -> xr.Dataset:
        """Parses the log file and returns the profiling data as an xarray Dataset.

        Returns:
           xr.Dataset: Parsed profiling data."""
        path = self.filepath
        data = self.parser.parse(path)
        return xr.Dataset(
            data_vars=dict(
                zip(
                    self.parser.metrics,
                    [
                        xr.DataArray(data[metric], dims=["region"]).pint.quantify(metric.units)
                        for metric in self.parser.metrics
                    ],
                    strict=True,
                )
            ),
            coords={"region": data["region"]},
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
    """

    path: Path  # Path to the experiment directory
    status: ProfilingExperimentStatus = ProfilingExperimentStatus.NEW  # Status of the experiment

    def __init__(self, path: Path) -> None:
        self.path = path
        if self.path.suffixes == [".tar", ".gz"]:
            self.status = ProfilingExperimentStatus.ARCHIVED

    @contextmanager
    def directory(self):
        """Context manager returning the experiment directory, handling archived experiments appropriately.

        If the experiment has been archived, it will be extracted to a temporary directory. Otherwise, the original
        directory path will be used. Note that after exiting the context, the temporary directory is removed.

        Returns:
            Path: The path to the directory.
        """
        if self.path.suffixes == [".tar", ".gz"]:
            with tempfile.TemporaryDirectory(prefix="access-profiling_", suffix="_data") as tmpdir:
                with tarfile.open(self.path) as tar:
                    tar.extractall(path=Path(tmpdir))
                yield Path(tmpdir)
        else:
            yield self.path

    def archive(
        self,
        archive_path: Path,
        exclude_dirs: list[str] | None = None,
        exclude_files: list[str] | None = None,
        follow_symlinks: bool = False,
    ):
        """Archives the experiment to the specified archive path.

        Only experiments with status DONE will be archived. No error will be raised if the experiment is not DONE.

        Symlinks to files and directories inside the experiment directory will be include as symlinks. Symlinks to files
        and directories outside the experiment directory will be followed if follow_symlinks is True, otherwise they
        will be included as symlinks.

        Args:
            archive_path (Path): Path to the archive destination. This should include the file name, but without
            the .tar.gz suffix.
            exclude_dirs (list[str] | None): Directory patterns to exclude when archiving.
            exclude_files (list[str] | None): File patterns to exclude when archiving.
            follow_symlinks (bool): Whether to follow symlinks when archiving. Defaults to False.

        Raises:
            FileExistsError: If the archive destination already exists.
            ValueError: If the experiment status is unknown.
        """
        if self.status == ProfilingExperimentStatus.NEW:
            warnings.warn(f"Experiment at {self.path} is not yet started. Skipping archiving.", stacklevel=2)
            return
        elif self.status == ProfilingExperimentStatus.RUNNING:
            warnings.warn(f"Experiment at {self.path} is still running. Skipping archiving.", stacklevel=2)
            return
        elif self.status == ProfilingExperimentStatus.DONE:
            print(f"Archiving experiment at {self.path} to {archive_path.with_suffix('.tar.gz')}")
        elif self.status == ProfilingExperimentStatus.ARCHIVED:
            warnings.warn(f"Experiment at {self.path} is already archived. Skipping archiving.", stacklevel=2)
            return

        archive_file = archive_path.with_suffix(".tar.gz")
        if archive_file.exists():
            raise FileExistsError(f"Archive destination {archive_file} already exists.")

        exclude_dirs = exclude_dirs or []
        exclude_files = exclude_files or []

        with tarfile.open(archive_file, "w:gz") as tar:
            for file, arcname in experiment_directory_walker(
                self.path, self.path.relative_to(self.path), self.path, follow_symlinks=follow_symlinks
            ):
                # Skip if file is inside an excluded directory pattern
                if any(any(parent.match(pat) for pat in exclude_dirs) for parent in file.parents):
                    continue
                # Skip if the file itself matches an excluded filename pattern
                if any(file.match(pat) for pat in exclude_files):
                    continue
                print(f"Archiving file: {file} as {arcname}")
                logging.debug(f"Archiving file: {file} as {arcname}")
                tar.add(file, arcname=arcname)

        self.status = ProfilingExperimentStatus.ARCHIVED
        self.path = archive_file
