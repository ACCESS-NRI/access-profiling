# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from access.profiling.experiment import ProfilingExperiment, ProfilingExperimentStatus, ProfilingLog
from access.profiling.metrics import tavg, tmax


def test_profiling_log():
    """Test the ProfilingLog class."""

    # Mock parser and path
    mock_parser = mock.MagicMock(autospec=True)
    mock_parser.metrics = [tavg, tmax]
    mock_parser.parse.return_value = {
        "region": ["Region 1", "Region 2"],
        tavg: [1.0, 2.0],
        tmax: [3.0, 4.0],
    }

    mock_path = mock.MagicMock()

    # Instantiate ProfilingLog and check attributes
    profiling_log = ProfilingLog(filepath=mock_path, parser=mock_parser)
    assert not profiling_log.optional

    # Parse the log
    dataset = profiling_log.parse()

    # Check dataset contents
    assert set(dataset.dims) == {"region"}
    assert set(dataset.data_vars) == {tavg, tmax}
    assert list(dataset["region"].values) == ["Region 1", "Region 2"]
    assert list(dataset[tavg].values) == [1.0, 2.0]
    assert list(dataset[tmax].values) == [3.0, 4.0]

    # Check parser and path calls
    mock_parser.parse.assert_called_once_with(mock_path)


def test_profiling_experiment():
    """Test the ProfilingExperiment class constructor, status and directory context manager."""

    # Instantiate ProfilingExperiment
    path = Path("/fake/work_dir")
    experiment = ProfilingExperiment(path=path)

    # Check representation
    assert repr(experiment) == "ProfilingExperiment(path=PosixPath('/fake/work_dir'), status=NEW)"

    # Assert path and status
    assert experiment.path == path
    assert experiment.status == ProfilingExperimentStatus.NEW

    # Change status and assert
    experiment.status = ProfilingExperimentStatus.RUNNING
    assert experiment.status == ProfilingExperimentStatus.RUNNING

    # Check directory context manager
    with experiment.directory() as temp_dir:
        assert temp_dir == path


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archived(mock_tarfile_open):
    """Test instantiation of ProfilingExperiment class from an archived tar.gz file."""

    # Mock the tarfile module
    mock_tarfile = mock.MagicMock()
    mock_tarfile_open.return_value = mock.MagicMock(__enter__=lambda s: mock_tarfile, __exit__=lambda *a: None)

    # Instantiate ProfilingExperiment with a .tar.gz path
    path = Path("/fake/path.tar.gz")
    experiment = ProfilingExperiment(path=path)

    # Assert path and status
    assert experiment.path == path
    assert experiment.status == ProfilingExperimentStatus.ARCHIVED

    # Check directory context manager
    with experiment.directory() as temp_dir:
        assert temp_dir.name.startswith("access-profiling_")
        assert temp_dir.name.endswith("_data")
        assert temp_dir.parent == Path(tempfile.gettempdir())
        mock_tarfile_open.assert_called_once_with(path)
        mock_tarfile.extractall.assert_called_once_with(path=Path(temp_dir), filter="data")


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_not_done(mock_open, caplog):
    """Test the archive method of ProfilingExperiment for non-DONE statuses."""

    exp = ProfilingExperiment(Path("/fake/work_dir/exp1"))
    for status in ProfilingExperimentStatus:
        if status == ProfilingExperimentStatus.DONE:
            continue
        exp.status = status
        with caplog.at_level(logging.WARNING):
            exp.archive(Path("/fake/archive"))
        assert mock_open.call_count == 0, "Tarfile should only be created for DONE experiments."
        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelname == "WARNING"
        caplog.clear()


def test_profiling_experiment_archive_file_exists():
    """Test the archive method of ProfilingExperiment when the archive file already exists."""

    exp = ProfilingExperiment(Path("/fake/work_dir/exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    with mock.patch.object(Path, "exists", return_value=True) as mock_exists, pytest.raises(FileExistsError):
        exp.archive(Path("/fake/archive"))
        mock_exists.assert_called_once()


@mock.patch("access.profiling.experiment.tarfile.open")
@mock.patch("access.profiling.experiment.experiment_directory_walker", return_value=[])
def test_profiling_experiment_archive_file_overwrite(mock_walker, mock_open):
    """Test the archive method of ProfilingExperiment when the archive file already exists and overwrite is True."""

    exp = ProfilingExperiment(Path("/fake/work_dir/exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    exp.archive(Path("/fake/archive"), overwrite=True)
    mock_open.assert_called_with(Path("/fake/archive").with_suffix(".tar.gz"), "w:gz")


@pytest.fixture()
def setup_experiment_directory():
    """Fixture to setup a mock experiment directory structure."""

    def _setup_experiment_directory(root: Path, follow_symlinks: bool):
        """Sets up a mock experiment directory structure.

        Args:
            root (Path): Root temporary directory.
            follow_symlinks (bool): Whether archiving will follow symlinks.
        Returns:
            tuple: (list of files expected to be archived, list of all files in the experiment)
        """
        # Create directories
        for dir in [
            Path("scratch/archive/output001"),
            Path("scratch/archive/restart001"),
            Path("exp1/.git"),
            Path("exp1/.git/logs"),
            Path("exp1/latest/logs"),
            Path("exp1/logs/profiling"),
        ]:
            full_dir = root / dir
            full_dir.mkdir(parents=True, exist_ok=True)

        # Setup files
        files_in = [
            Path("exp1/logs/profiling/log_01.txt"),
            Path("exp1/data.nc"),
            Path("exp1/.git/config"),
            Path("exp1/.git/logs/HEAD"),
        ]
        files_out = [
            Path("another.log"),
            Path("scratch/archive/output001/log.txt"),
            Path("scratch/archive/output001/data.nc"),
            Path("scratch/archive/restart001/data.nc"),
        ]
        for file in files_in + files_out:
            (root / file).touch()

        # Link to directory outside experiment directory
        dir_link = root / Path("exp1/archive")
        dir_link.symlink_to(root / Path("scratch/archive"))

        # Link to files inside experiment directory
        file_link_in = root / Path("exp1/latest/logs/log.txt")
        file_link_in.symlink_to(root / Path("exp1/profiling/log_01.txt"))

        # Link to files outside experiment directory
        file_link_out = root / Path("exp1/another.log")
        file_link_out.symlink_to(root / Path("another.log"))

        # Lists of symlinks inside and outside experiment directory
        links_in = [Path("exp1/latest/logs/log.txt")]
        links_out = [Path("exp1/another.log"), Path("exp1/archive")]

        if follow_symlinks:
            # Return files inside and outside experiment directory, plus links pointing to files inside
            return files_in + files_out + links_in
        else:
            # Return files inside the experiment directory, plus links pointing to files inside and outside
            return files_in + links_in + links_out

    return _setup_experiment_directory


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive(mock_open, tmp_path, setup_experiment_directory):
    """Test the archive method of ProfilingExperiment whithout following symlinks."""

    files = setup_experiment_directory(tmp_path, follow_symlinks=False)

    # Setup mock TarFile with context manager
    mock_tarfile = mock.MagicMock()
    mock_open.return_value = mock.MagicMock(__enter__=lambda s: mock_tarfile, __exit__=lambda *a: None)

    # Instantiate ProfilingExperiment
    exp = ProfilingExperiment(tmp_path / Path("exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    # Archive experiment with no exclude patterns, not following symlinks (default)
    exp.archive(Path("/fake/archive"))

    # Check calls
    assert exp.status == ProfilingExperimentStatus.ARCHIVED  # Status should be updated to ARCHIVED
    mock_open.assert_called_with(Path("/fake/archive").with_suffix(".tar.gz"), "x:gz")  # Check tarfile opening
    assert mock_tarfile.add.call_count == len(files), "All files should be added to the archive."
    for file in files:
        if "scratch" in file.parts:
            arcname = file.relative_to(Path("scratch"))
        elif "exp1" in file.parts:
            arcname = file.relative_to(Path("exp1"))
        else:
            arcname = file
        mock_tarfile.add.assert_any_call(tmp_path / file, arcname=arcname)


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_follow_symlinks(mock_open, tmp_path, setup_experiment_directory):
    """Test the archive method of ProfilingExperiment when following symlinks."""

    files = setup_experiment_directory(tmp_path, follow_symlinks=True)

    # Setup mock TarFile with context manager
    mock_tarfile = mock.MagicMock()
    mock_open.return_value = mock.MagicMock(__enter__=lambda s: mock_tarfile, __exit__=lambda *a: None)

    # Instantiate ProfilingExperiment
    exp = ProfilingExperiment(tmp_path / Path("exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    # Archive experiment with no exclude patterns, following symlinks
    exp.archive(Path("/fake/archive"), follow_symlinks=True)

    # Check calls
    assert exp.status == ProfilingExperimentStatus.ARCHIVED  # Status should be updated to ARCHIVED
    mock_open.assert_called_with(Path("/fake/archive").with_suffix(".tar.gz"), "x:gz")  # Check tarfile opening
    assert mock_tarfile.add.call_count == len(files), "All files should be added to the archive."
    for file in files:
        if "scratch" in file.parts:
            arcname = file.relative_to(Path("scratch"))
        elif "exp1" in file.parts:
            arcname = file.relative_to(Path("exp1"))
        else:
            arcname = file
        mock_tarfile.add.assert_any_call(tmp_path / file, arcname=arcname)


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_with_filters(mock_open, tmp_path, setup_experiment_directory):
    """Test the archive method of ProfilingExperiment with exclude patterns."""

    files = setup_experiment_directory(tmp_path, follow_symlinks=True)
    files_to_archive = []
    for file in files:
        # Exclude files that match the exclude patterns
        if not (
            file.match("*.nc")
            or file.match("restart*")
            or file.match(".git")
            or file.match(".git/*/*")
            or file.match(".git/*")
        ):
            files_to_archive.append(file)

    # Setup mock TarFile with context manager
    mock_tarfile = mock.MagicMock()
    mock_open.return_value = mock.MagicMock(__enter__=lambda s: mock_tarfile, __exit__=lambda *a: None)

    # Instantiate ProfilingExperiment
    exp = ProfilingExperiment(tmp_path / Path("exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    # Archive experiment with exclude patterns
    exp.archive(Path("/fake/archive"), exclude_files=["*.nc"], exclude_dirs=["restart*", ".git"], follow_symlinks=True)

    # Check calls
    mock_open.assert_called_with(Path("/fake/archive").with_suffix(".tar.gz"), "x:gz")  # Check tarfile opening
    assert mock_tarfile.add.call_count == len(files_to_archive), (
        "Only non-excluded files should be added to the archive."
    )
    for file in files_to_archive:
        if "scratch" in file.parts:
            arcname = file.relative_to(Path("scratch"))
        elif "exp1" in file.parts:
            arcname = file.relative_to(Path("exp1"))
        else:
            arcname = file
        mock_tarfile.add.assert_any_call(tmp_path / file, arcname=arcname)
