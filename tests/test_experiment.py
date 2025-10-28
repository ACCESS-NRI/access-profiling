# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

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

    # Instantiate ProfilingLog and parse
    profiling_log = ProfilingLog(filepath=mock_path, parser=mock_parser)
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
    """Test the ProfilingExperiment class."""

    # Instantiate ProfilingExperiment
    path = Path("/fake/work_dir")
    experiment = ProfilingExperiment(path=path)

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
    """Test the ProfilingExperiment class."""

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
        mock_tarfile.extractall.assert_called_once_with(path=Path(temp_dir))


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_not_done(mock_open):
    """Test the archive method of ProfilingExperiment for non-DONE statuses."""

    exp = ProfilingExperiment(Path("/fake/work_dir/exp1"))
    for status in ProfilingExperimentStatus:
        if status == ProfilingExperimentStatus.DONE:
            continue
        exp.status = status
        exp.archive(Path("/fake/archive"))
        assert mock_open.call_count == 0, "Tarfile should only be created for DONE experiments."


def test_profiling_experiment_archive_file_exists():
    """Test the archive method of ProfilingExperiment when the archive file already exists."""

    exp = ProfilingExperiment(Path("/fake/work_dir/exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    with mock.patch.object(Path, "exists", return_value=True) as mock_exists, pytest.raises(FileExistsError):
        exp.archive(Path("/fake/archive"))
        mock_exists.assert_called_once()


@pytest.fixture()
def setup_experiment_directory():
    """Fixture to setup a mock experiment directory structure."""

    def _setup_experiment_directory(root: Path):
        """Sets up a mock experiment directory structure.

        Returns:
            tuple: (list of files expected to be archived, list of all files in the experiment)
        """
        # Setup directories
        dirs = [
            Path("scratch/archive/output001"),
            Path("scratch/archive/restart001"),
            Path("exp1/.git"),
            Path("exp1/.git/logs"),
        ]
        for dir in dirs:
            full_dir = root / dir
            full_dir.mkdir(parents=True, exist_ok=True)

        # Setup files
        files_to_archive = [Path("scratch/archive/output001/log.txt"), Path("exp1/log.txt"), Path("another.log")]
        other_files = [
            Path("scratch/archive/output001/data.nc"),
            Path("scratch/archive/restart001/data.nc"),
            Path("exp1/data.nc"),
            Path("exp1/.git/config"),
            Path("exp1/.git/logs/HEAD"),
        ]
        for file in files_to_archive + other_files:
            full_file_path = root / file
            full_file_path.touch()

        # Setup symlinks
        dir_link = root / Path("exp1/archive")
        dir_link.symlink_to(root / Path("scratch/archive"))

        file_link = root / Path("exp1/another.log")
        file_link.symlink_to(root / Path("another.log"))

        return files_to_archive, files_to_archive + other_files

    return _setup_experiment_directory


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive(mock_open, tmp_path, setup_experiment_directory):
    """Test the archive method of ProfilingExperiment."""

    files_to_archive, all_files = setup_experiment_directory(tmp_path)

    # Setup mock TarFile with context manager
    mock_tarfile = mock.MagicMock()
    mock_open.return_value = mock.MagicMock(__enter__=lambda s: mock_tarfile, __exit__=lambda *a: None)

    # Instantiate ProfilingExperiment
    exp = ProfilingExperiment(tmp_path / Path("exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    # Archive experiment with no exclude patterns
    exp.archive(Path("/fake/archive"))
    print(mock_tarfile.add.call_args_list)
    # Check calls
    assert exp.status == ProfilingExperimentStatus.ARCHIVED  # Status should be updated to ARCHIVED
    mock_open.assert_called_with(Path("/fake/archive").with_suffix(".tar.gz"), "w:gz")  # Check tarfile opening
    assert mock_tarfile.add.call_count == len(all_files), "All files should be added to the archive."
    for file in all_files:
        if "scratch" in file.parts:
            arcname = file.relative_to(Path("scratch"))
        elif "exp1" in file.parts:
            arcname = file.relative_to(Path("exp1"))
        else:
            arcname = file
        mock_tarfile.add.assert_any_call(tmp_path / file, arcname=arcname)


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_with_filters(mock_open, tmp_path, setup_experiment_directory):
    """Test the archive method of ProfilingExperiment."""

    files_to_archive, all_files = setup_experiment_directory(tmp_path)

    # Setup mock TarFile with context manager
    mock_tarfile = mock.MagicMock()
    mock_open.return_value = mock.MagicMock(__enter__=lambda s: mock_tarfile, __exit__=lambda *a: None)

    # Instantiate ProfilingExperiment
    exp = ProfilingExperiment(tmp_path / Path("exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    # Archive experiment with exclude patterns
    exp.archive(Path("/fake/archive"), exclude_files=["*.nc"], exclude_dirs=["restart*", ".git"])

    # Check calls
    mock_open.assert_called_with(Path("/fake/archive").with_suffix(".tar.gz"), "w:gz")  # Check tarfile opening
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
