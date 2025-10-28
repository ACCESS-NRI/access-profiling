# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import tarfile
import tempfile
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


def experiment_directory_walker(path: Path, arcname: Path):
    """Walks through the experiment directory, yielding files and archive names, handling symlinks appropriately.

    Args:
        path (Path): Path to walk through.
        arcname (Path): Archive name for the current path.

    Yields:
        Tuple[Path, Path]: A tuple containing the file path and its archive name.
    """
    if path.is_symlink():
        target = path.resolve()
        if target.is_dir():
            # Recursively add target contents
            for child in target.iterdir():
                yield from experiment_directory_walker(child, arcname=Path(arcname) / child.name)
        else:
            yield target, arcname
    elif path.is_dir():
        # Recursively add directory contents
        for child in path.iterdir():
            yield from experiment_directory_walker(child, arcname=Path(arcname) / child.name)
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
        self, archive_path: Path, exclude_dirs: list[str] | None = None, exclude_files: list[str] | None = None
    ):
        """Archives the experiment to the specified archive path.

        Only experiments with status DONE will be archived. No error will be raised if the experiment is not DONE.

        Args:
            archive_path (Path): Path to the archive destination. This should include the file name, but without
            the .tar.gz suffix.
            exclude_dirs (list[str] | None): Directory patterns to exclude when archiving.
            exclude_files (list[str] | None): File patterns to exclude when archiving.

        Raises:
            FileExistsError: If the archive destination already exists.
        """
        if self.status != ProfilingExperimentStatus.DONE:
            return  # Only archive completed experiments

        archive_file = archive_path.with_suffix(".tar.gz")
        if archive_file.exists():
            raise FileExistsError(f"Archive destination {archive_file} already exists.")

        exclude_dirs = exclude_dirs or []
        exclude_files = exclude_files or []

        with tarfile.open(archive_file, "w:gz") as tar:
            for file, arcname in experiment_directory_walker(self.path, arcname=self.path.relative_to(self.path)):
                # Skip if file is inside an excluded directory pattern
                if any(any(parent.match(pat) for pat in exclude_dirs) for parent in file.parents):
                    continue
                # Skip if the file itself matches an excluded filename pattern
                if any(file.match(pat) for pat in exclude_files):
                    continue
                logging.debug(f"Archiving file: {file} as {arcname}")
                tar.add(file, arcname=arcname)

        self.status = ProfilingExperimentStatus.ARCHIVED
        self.path = archive_file
