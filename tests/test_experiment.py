# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import dataclasses
import json
import logging
import os
import tarfile
import tempfile
from pathlib import Path
from unittest import mock

import pytest
from access.config.parallel_component import ComponentLayout, CoreSharing
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid
from conftest import component_of, layout_with, leaf_layout

from access.profiling.experiment import (
    RECORD_FILENAME,
    ConfigurationChoice,
    ExperimentCoordinate,
    ProfilingExperiment,
    ProfilingExperimentStatus,
    ProfilingLog,
    _layout_from_record,
    _layout_record,
    component_cores,
    experiment_directory_walker,
    find_component,
    read_archived_record,
    read_record,
    read_versioned,
)
from access.profiling.metrics import tavg, tmax
from access.profiling.parser import ProfilingParser


def test_profiling_log():
    """Test the ProfilingLog class."""

    # Mock parser and path
    mock_parser = mock.Mock(spec=ProfilingParser)
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


def test_profiling_log_duplicate_regions():
    """Test duplicate region names are renamed correctly during parsing."""

    mock_parser = mock.Mock(spec=ProfilingParser)
    mock_parser.metrics = [tavg]
    mock_parser.parse.return_value = {
        "region": ["Region 1", "Region 1", "Region 2", "Region 1"],
        tavg: [1.0, 2.0, 3.0, 4.0],
    }

    mock_path = mock.MagicMock()
    profiling_log = ProfilingLog(filepath=mock_path, parser=mock_parser)

    dataset = profiling_log.parse()

    expected_unique_regions = ["Region 1", "Region 1_2", "Region 2", "Region 1_3"]
    assert list(dataset["region"].values) == expected_unique_regions
    assert list(dataset[tavg].values) == [1.0, 2.0, 3.0, 4.0]
    assert mock_parser.parse.call_count == 1
    assert mock_parser.parse.return_value["region"] == ["Region 1", "Region 1", "Region 2", "Region 1"]


def test_profiling_log_hierarchical():
    """Test ProfilingLog.parse() with hierarchical (nested dict) parser output."""
    mock_parser = mock.Mock(spec=ProfilingParser)
    mock_parser.metrics = [tavg, tmax]
    # Nested dict: no 'region' key — string keys are children, metric keys are values
    mock_parser.parse.return_value = {
        "Root": {
            tavg: 10.0,
            tmax: 12.0,
            "Child1": {tavg: 4.0, tmax: 5.0},
            "Child2": {tavg: 6.0, tmax: 7.0},
        }
    }

    mock_path = mock.MagicMock()
    dataset = ProfilingLog(filepath=mock_path, parser=mock_parser).parse()

    # DFS pre-order: Root, Child1, Child2
    assert list(dataset["region"].values) == ["Root", "Child1", "Child2"]
    assert list(dataset[tavg].values) == [10.0, 4.0, 6.0]
    assert list(dataset[tmax].values) == [12.0, 5.0, 7.0]
    assert set(dataset.dims) == {"region"}


def test_profiling_log_with_pe():
    """Test ProfilingLog.parse() with per-PE parser output."""
    mock_parser = mock.Mock(spec=ProfilingParser)
    mock_parser.metrics = [tavg]
    mock_parser.parse.return_value = {
        "region": ["Region 1", "Region 2"],
        "pe": [0, 1, 2],
        tavg: [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
    }

    mock_path = mock.MagicMock()
    dataset = ProfilingLog(filepath=mock_path, parser=mock_parser).parse()

    assert set(dataset.dims) == {"region", "pe"}
    assert dataset[tavg].shape == (2, 3)
    assert list(dataset.coords["pe"].values) == [0, 1, 2]
    assert list(dataset[tavg].isel(region=0).pint.dequantify().values) == [1.0, 2.0, 3.0]
    assert list(dataset[tavg].isel(region=1).pint.dequantify().values) == [4.0, 5.0, 6.0]


def test_profiling_experiment():
    """Test the ProfilingExperiment class constructor, status and directory context manager."""

    # Instantiate ProfilingExperiment
    path = Path("/fake/work_dir")
    experiment = ProfilingExperiment(path=path)

    # Check representation without run_path
    assert repr(experiment) == "ProfilingExperiment(path=PosixPath('/fake/work_dir'), status=NEW)"

    # Check representation with run_path
    experiment_with_result = ProfilingExperiment(path=path, run_path=Path("/fake/run_dir"))
    assert repr(experiment_with_result) == (
        "ProfilingExperiment(path=PosixPath('/fake/work_dir'), run_path=PosixPath('/fake/run_dir'), status=NEW)"
    )

    # The number of CPUs is reported only once it is known
    assert experiment.ncpus is None
    experiment.ncpus = 416
    assert repr(experiment) == "ProfilingExperiment(path=PosixPath('/fake/work_dir'), status=NEW, ncpus=416)"
    experiment.ncpus = None

    # Assert path and status
    assert experiment.path == path
    assert experiment.status == ProfilingExperimentStatus.NEW

    # Change status and assert
    experiment.status = ProfilingExperimentStatus.RUNNING
    assert experiment.status == ProfilingExperimentStatus.RUNNING

    # Check directory context manager
    with experiment.directory() as (experiment_dir, run_dir):
        assert experiment_dir == path
        assert run_dir is None

    with experiment_with_result.directory() as (experiment_dir, run_dir):
        assert experiment_dir == path
        assert run_dir == Path("/fake/run_dir")


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
    with experiment.directory() as (experiment_dir, run_dir):
        assert experiment_dir.name == "experiment"
        assert experiment_dir.parent.name.startswith("access-profiling_")
        assert experiment_dir.parent.name.endswith("_data")
        assert experiment_dir.parent.parent == Path(tempfile.gettempdir())
        assert run_dir is None
        mock_tarfile_open.assert_called_once_with(path)
        mock_tarfile.extractall.assert_called_once_with(path=experiment_dir.parent, filter="data")


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archived_with_runs(mock_tarfile_open):
    """Archived experiments expose an extracted runs directory when one is present."""

    def extractall_side_effect(*, path, filter):
        assert filter == "data"
        (path / "experiment").mkdir()
        (path / "runs").mkdir()

    mock_tarfile = mock.MagicMock()
    mock_tarfile.extractall.side_effect = extractall_side_effect
    mock_tarfile_open.return_value = mock.MagicMock(__enter__=lambda s: mock_tarfile, __exit__=lambda *a: None)

    experiment = ProfilingExperiment(path=Path("/fake/path.tar.gz"))

    with experiment.directory() as (experiment_dir, run_dir):
        assert experiment_dir.name == "experiment"
        assert run_dir is not None
        assert run_dir.name == "runs"
        assert run_dir.parent == experiment_dir.parent


def test_profiling_experiment_archive_unknown_status():
    """Test that a status archive() cannot judge is refused rather than archived.

    Every member of the enum is accounted for, so this guards a member added later without deciding
    whether it may be archived - which would otherwise fall through and archive it.
    """

    exp = ProfilingExperiment(path=Path("/fake/path"))
    exp.status = "not a status"  # pyright: ignore[reportAttributeAccessIssue]
    with pytest.raises(ValueError, match="unknown status"):
        exp.archive(Path("/fake/archive"))


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_not_done(mock_open, caplog):
    """Test the archive method of ProfilingExperiment for non-DONE statuses.

    An experiment already archived is passed over at INFO, since archiving a study again passes over every
    experiment it archived before. The others are a warning.
    """

    exp = ProfilingExperiment(path=Path("/fake/work_dir/exp1"))
    for status in ProfilingExperimentStatus:
        if status == ProfilingExperimentStatus.DONE:
            continue
        exp.status = status
        with caplog.at_level(logging.INFO):
            exp.archive(Path("/fake/archive"))
        assert mock_open.call_count == 0, "Tarfile should only be created for DONE experiments."
        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelname == ("INFO" if status == ProfilingExperimentStatus.ARCHIVED else "WARNING"), status
        caplog.clear()


def test_profiling_experiment_archive_file_exists():
    """Test the archive method of ProfilingExperiment when the archive file already exists."""

    exp = ProfilingExperiment(path=Path("/fake/work_dir/exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    with mock.patch.object(Path, "exists", return_value=True) as mock_exists:
        with pytest.raises(FileExistsError):
            exp.archive(Path("/fake/archive"))
        mock_exists.assert_called_once()


@mock.patch.object(Path, "replace")
@mock.patch("access.profiling.experiment.tarfile.open")
@mock.patch("access.profiling.experiment.experiment_directory_walker", return_value=[])
def test_profiling_experiment_archive_file_overwrite(mock_walker, mock_open, mock_replace):
    """Test the archive method of ProfilingExperiment when the archive file already exists and overwrite is True."""

    exp = ProfilingExperiment(path=Path("/fake/work_dir/exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    exp.archive(Path("/fake/archive"), overwrite=True)
    mock_open.assert_called_with(Path(f"/fake/.archive.tar.gz.{os.getpid()}.tmp"), "w:gz")
    mock_replace.assert_called_once_with(Path("/fake/archive").with_suffix(".tar.gz"))


@mock.patch.object(Path, "replace")
@mock.patch("access.profiling.experiment.tarfile.open")
@mock.patch("access.profiling.experiment.experiment_directory_walker", return_value=[])
def test_profiling_experiment_archive_name_with_dots(mock_walker, mock_open, mock_replace):
    """Archive names containing dots (e.g. version numbers) must not be truncated."""

    exp = ProfilingExperiment(path=Path("/fake/work_dir/exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    archive_path = Path("/fake/archive/am3-release-n96e-1.0-24")
    exp.archive(archive_path)

    expected_archive_file = Path("/fake/archive/am3-release-n96e-1.0-24.tar.gz")
    mock_open.assert_called_with(Path(f"/fake/archive/.am3-release-n96e-1.0-24.tar.gz.{os.getpid()}.tmp"), "w:gz")
    mock_replace.assert_called_once_with(expected_archive_file)
    assert exp.path == expected_archive_file


@pytest.fixture()
def setup_experiment_directory():
    """Fixture to setup a mock experiment directory structure."""

    def _setup_experiment_directory(root: Path, follow_symlinks: bool):
        """Sets up a mock experiment directory structure.

        Args:
            root (Path): Root temporary directory.
            follow_symlinks (bool): Whether archiving will follow symlinks.
        Returns:
            tuple: (list of files expected to be added to the archive, list of the symlinks inside the experiment,
                which are kept as links relative to where they are)
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

        # Symlinks inside the experiment directory, kept as links whether or not symlinks are followed
        links_in = [Path("exp1/latest/logs/log.txt")]

        if follow_symlinks:
            # Return files inside and outside experiment directory, and links pointing to files inside
            return files_in + files_out, links_in
        else:
            # Return files inside the experiment directory, and links pointing inside: the links pointing outside,
            # exp1/another.log and exp1/archive, are left out, as they could not be extracted
            return files_in, links_in

    return _setup_experiment_directory


def assert_links_kept(mock_tarfile, root, links):
    """Asserts that every symlink inside the experiment was added as a link relative to where it is."""
    assert mock_tarfile.addfile.call_count == len(links)
    for link in links:
        mock_tarfile.gettarinfo.assert_any_call(root / link, arcname=str(Path("experiment") / link.relative_to("exp1")))


@mock.patch.object(Path, "replace")
@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive(mock_open, mock_replace, tmp_path, setup_experiment_directory, caplog):
    """Test the archive method of ProfilingExperiment whithout following symlinks."""

    files, links = setup_experiment_directory(tmp_path, follow_symlinks=False)

    # Setup mock TarFile with context manager
    mock_tarfile = mock.MagicMock()
    mock_open.return_value = mock.MagicMock(__enter__=lambda s: mock_tarfile, __exit__=lambda *a: None)

    # Instantiate ProfilingExperiment
    exp = ProfilingExperiment(path=tmp_path / Path("exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    # Archive experiment with no exclude patterns, not following symlinks (default)
    with caplog.at_level(logging.INFO, logger="access.profiling.experiment"):
        exp.archive(Path("/fake/archive"))

    # The links pointing outside the experiment directory are left out, and said to be
    for link in ["exp1/another.log", "exp1/archive"]:
        assert f"Leaving {tmp_path / link} out of the archive" in caplog.text

    # Check calls
    assert exp.status == ProfilingExperimentStatus.ARCHIVED  # Status should be updated to ARCHIVED
    mock_open.assert_called_with(Path(f"/fake/.archive.tar.gz.{os.getpid()}.tmp"), "w:gz")  # Check tarfile opening
    mock_replace.assert_called_once_with(Path("/fake/archive").with_suffix(".tar.gz"))
    assert mock_tarfile.add.call_count == len(files), "All files should be added to the archive."
    for file in files:
        if "exp1" in file.parts:
            arcname = Path("experiment") / file.relative_to(Path("exp1"))
        elif "scratch" in file.parts:
            arcname = Path("experiment") / file.relative_to(Path("scratch"))
        else:
            arcname = Path("experiment") / file
        mock_tarfile.add.assert_any_call(tmp_path / file, arcname=arcname)
    assert_links_kept(mock_tarfile, tmp_path, links)


@mock.patch.object(Path, "replace")
@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_follow_symlinks(mock_open, mock_replace, tmp_path, setup_experiment_directory):
    """Test the archive method of ProfilingExperiment when following symlinks."""

    files, links = setup_experiment_directory(tmp_path, follow_symlinks=True)

    # Setup mock TarFile with context manager
    mock_tarfile = mock.MagicMock()
    mock_open.return_value = mock.MagicMock(__enter__=lambda s: mock_tarfile, __exit__=lambda *a: None)

    # Instantiate ProfilingExperiment
    exp = ProfilingExperiment(path=tmp_path / Path("exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    # Archive experiment with no exclude patterns, following symlinks
    exp.archive(Path("/fake/archive"), follow_symlinks=True)

    # Check calls
    assert exp.status == ProfilingExperimentStatus.ARCHIVED  # Status should be updated to ARCHIVED
    mock_open.assert_called_with(Path(f"/fake/.archive.tar.gz.{os.getpid()}.tmp"), "w:gz")  # Check tarfile opening
    mock_replace.assert_called_once_with(Path("/fake/archive").with_suffix(".tar.gz"))
    assert mock_tarfile.add.call_count == len(files), "All files should be added to the archive."
    for file in files:
        if "exp1" in file.parts:
            arcname = Path("experiment") / file.relative_to(Path("exp1"))
        elif "scratch" in file.parts:
            arcname = Path("experiment") / file.relative_to(Path("scratch"))
        else:
            arcname = Path("experiment") / file
        mock_tarfile.add.assert_any_call(tmp_path / file, arcname=arcname)
    assert_links_kept(mock_tarfile, tmp_path, links)


@mock.patch.object(Path, "replace")
@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_with_filters(mock_open, mock_replace, tmp_path, setup_experiment_directory):
    """Test the archive method of ProfilingExperiment with exclude patterns."""

    files, links = setup_experiment_directory(tmp_path, follow_symlinks=True)
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
    exp = ProfilingExperiment(path=tmp_path / Path("exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    # Archive experiment with exclude patterns
    exp.archive(Path("/fake/archive"), exclude_files=["*.nc"], exclude_dirs=["restart*", ".git"], follow_symlinks=True)

    # Check calls
    mock_open.assert_called_with(Path(f"/fake/.archive.tar.gz.{os.getpid()}.tmp"), "w:gz")  # Check tarfile opening
    mock_replace.assert_called_once_with(Path("/fake/archive").with_suffix(".tar.gz"))
    assert mock_tarfile.add.call_count == len(files_to_archive), (
        "Only non-excluded files should be added to the archive."
    )
    for file in files_to_archive:
        if "exp1" in file.parts:
            arcname = Path("experiment") / file.relative_to(Path("exp1"))
        elif "scratch" in file.parts:
            arcname = Path("experiment") / file.relative_to(Path("scratch"))
        else:
            arcname = Path("experiment") / file
        mock_tarfile.add.assert_any_call(tmp_path / file, arcname=arcname)
    assert_links_kept(mock_tarfile, tmp_path, links)


@mock.patch.object(Path, "replace")
@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_with_run_path(mock_open, mock_replace, tmp_path):
    """Test that archive() traverses both path and run_path, storing under experiment/ and runs/."""

    # Create experiment directory with one file
    exp_dir = tmp_path / "exp1"
    exp_dir.mkdir()
    exp_file = exp_dir / "config.yaml"
    exp_file.touch()

    # Create separate run directory with two files
    run_dir = tmp_path / "scratch" / "runs"
    run_dir.mkdir(parents=True)
    run_file1 = run_dir / "output.log"
    run_file2 = run_dir / "timing.txt"
    run_file1.touch()
    run_file2.touch()

    mock_tarfile = mock.MagicMock()
    mock_open.return_value = mock.MagicMock(__enter__=lambda s: mock_tarfile, __exit__=lambda *a: None)

    exp = ProfilingExperiment(path=exp_dir, run_path=run_dir)
    exp.status = ProfilingExperimentStatus.DONE
    exp.archive(Path("/fake/archive"))

    # path and run_path files should both be added under their respective prefixes
    assert mock_tarfile.add.call_count == 3
    mock_tarfile.add.assert_any_call(exp_file, arcname=Path("experiment/config.yaml"))
    mock_tarfile.add.assert_any_call(run_file1, arcname=Path("runs/output.log"))
    mock_tarfile.add.assert_any_call(run_file2, arcname=Path("runs/timing.txt"))

    # run_path cleared after archiving
    mock_replace.assert_called_once_with(Path("/fake/archive.tar.gz"))
    assert exp.run_path is None
    assert exp.path == Path("/fake/archive.tar.gz")


class TestWholeArchives:
    """An archive is written whole or not at all, so that an archiving stopped part way leaves no tarball whose record
    reads as if the rest were there."""

    @staticmethod
    def done(tmp_path):
        path = tmp_path / "exp1"
        path.mkdir()
        (path / "config.yaml").write_text("ncpus: 4\n")
        experiment = ProfilingExperiment(path)
        experiment.status = ProfilingExperimentStatus.DONE
        return experiment

    @mock.patch("access.profiling.experiment.experiment_directory_walker", side_effect=OSError("disk full"))
    def test_one_stopped_part_way_leaves_nothing(self, mock_walker, tmp_path):
        experiment = self.done(tmp_path)

        with pytest.raises(OSError, match="disk full"):
            experiment.archive(tmp_path / "archive", record={"version": 1})

        assert list(tmp_path.iterdir()) == [tmp_path / "exp1"], "neither the archive nor the partial file is left"
        assert experiment.status == ProfilingExperimentStatus.DONE
        assert experiment.path == tmp_path / "exp1"

    @mock.patch("access.profiling.experiment.experiment_directory_walker", side_effect=OSError("disk full"))
    def test_one_stopped_part_way_leaves_the_archive_it_was_to_overwrite(self, mock_walker, tmp_path):
        experiment = self.done(tmp_path)
        (tmp_path / "archive.tar.gz").write_bytes(b"the archive made before")

        with pytest.raises(OSError, match="disk full"):
            experiment.archive(tmp_path / "archive", overwrite=True)

        assert (tmp_path / "archive.tar.gz").read_bytes() == b"the archive made before"
        assert not list(tmp_path.glob(".*.tmp"))

    def test_a_finished_one_takes_the_name_and_leaves_no_partial_file(self, tmp_path):
        experiment = self.done(tmp_path)

        experiment.archive(tmp_path / "archive")

        assert sorted(path.name for path in tmp_path.iterdir()) == ["archive.tar.gz", "exp1"]
        with tarfile.open(tmp_path / "archive.tar.gz") as tar:
            assert "experiment/config.yaml" in tar.getnames()


class TestSymlinksInsideTheExperiment:
    """A symlink to something inside the experiment is archived as a link, whatever it points to and whether symlinks
    are followed, so that what it points to is archived once, where it is, and the link still points at it once the
    archive is extracted."""

    @staticmethod
    def make_experiment(root: Path) -> Path:
        """An experiment with an absolute symlink to a directory in it, and a relative one to a file."""
        exp = root / "exp1"
        (exp / "archive" / "output001").mkdir(parents=True)
        (exp / "archive" / "output001" / "timings.txt").write_text("timings")
        (exp / "latest").symlink_to(exp / "archive" / "output001")
        (exp / "archive" / "timings.txt").symlink_to("output001/timings.txt")
        return exp

    @staticmethod
    def done(path: Path) -> ProfilingExperiment:
        experiment = ProfilingExperiment(path=path)
        experiment.status = ProfilingExperimentStatus.DONE
        return experiment

    @pytest.mark.parametrize("follow_symlinks", [True, False])
    def test_the_walk_keeps_them(self, tmp_path, follow_symlinks):
        exp = self.make_experiment(tmp_path)

        walked = set(experiment_directory_walker(exp, Path("experiment"), exp, follow_symlinks=follow_symlinks))

        assert walked == {
            (exp / "archive/output001/timings.txt", Path("experiment/archive/output001/timings.txt")),
            (exp / "latest", Path("experiment/latest")),
            (exp / "archive/timings.txt", Path("experiment/archive/timings.txt")),
        }

    @pytest.mark.parametrize("follow_symlinks", [True, False])
    def test_the_archive_extracts_with_them(self, tmp_path, follow_symlinks):
        """The links are made relative, which is also what lets the archive be extracted at all, the data filter
        refusing links to absolute paths."""
        experiment = self.done(self.make_experiment(tmp_path))

        experiment.archive(tmp_path / "archived", follow_symlinks=follow_symlinks)

        with experiment.directory() as (path, _):
            assert (path / "latest").readlink() == Path("archive/output001")
            assert (path / "archive/timings.txt").readlink() == Path("output001/timings.txt")
            assert (path / "latest/timings.txt").read_text() == "timings"
            assert (path / "archive/timings.txt").read_text() == "timings"
            assert sorted(p.name for p in path.rglob("*") if p.is_file() and not p.is_symlink()) == ["timings.txt"]

    def test_a_link_met_outside_that_points_back_inside(self, tmp_path):
        """Payu keeps an experiment's outputs outside it, in its laboratory, behind a symlink that is followed. A
        link there to a file of the experiment is kept relative to where it lands in the archive."""
        exp = tmp_path / "exp1"
        exp.mkdir()
        (exp / "config.yaml").write_text("model: access-esm1.6\n")
        lab_output = tmp_path / "lab" / "output001"
        lab_output.mkdir(parents=True)
        (lab_output / "config.yaml").symlink_to(exp / "config.yaml")
        (exp / "archive").symlink_to(tmp_path / "lab")
        experiment = self.done(exp)

        experiment.archive(tmp_path / "archived", follow_symlinks=True)

        with experiment.directory() as (path, _):
            assert (path / "archive/output001/config.yaml").readlink() == Path("../../config.yaml")
            assert (path / "archive/output001/config.yaml").read_text() == "model: access-esm1.6\n"


class TestWhatIsLeftOut:
    """What archiving leaves out: links it could not extract, and what the exclusion patterns name, matched in the
    archive rather than on disk."""

    @staticmethod
    def done(path: Path, run_path: Path | None = None) -> ProfilingExperiment:
        experiment = ProfilingExperiment(path=path, run_path=run_path)
        experiment.status = ProfilingExperimentStatus.DONE
        return experiment

    @staticmethod
    def archived(path: Path) -> list[Path]:
        """Everything an extracted directory holds, files and links alike, relative to it."""
        return sorted(entry.relative_to(path) for entry in path.rglob("*") if entry.is_file() or entry.is_symlink())

    def test_a_link_outside_is_left_out_when_links_are_not_followed(self, tmp_path, caplog):
        """Kept as a link, it could not be extracted: the data filter refuses links pointing outside the archive."""
        exp = tmp_path / "exp1"
        exp.mkdir()
        (exp / "timings.txt").write_text("timings")
        (tmp_path / "shared").mkdir()
        (tmp_path / "shared" / "forcing.nc").write_text("forcing")
        (exp / "shared").symlink_to(tmp_path / "shared")
        experiment = self.done(exp)

        with caplog.at_level(logging.INFO, logger="access.profiling.experiment"):
            experiment.archive(tmp_path / "archived")

        with experiment.directory() as (path, _):
            assert self.archived(path) == [Path("timings.txt")]
        assert f"Leaving {exp / 'shared'} out of the archive" in caplog.text

    def test_a_root_that_is_a_symlink_is_archived_as_what_it_points_to(self, tmp_path):
        """As a Cylc run directory is, when the runs are kept on another file system."""
        exp = tmp_path / "u-aa123"
        exp.mkdir()
        (exp / "rose-suite.conf").write_text("um_layout = 2,3\n")
        runs = tmp_path / "scratch" / "cylc-run" / "u-aa123"
        (runs / "log").mkdir(parents=True)
        (runs / "log" / "db").write_text("tasks")
        (tmp_path / "cylc-run").mkdir()
        (tmp_path / "cylc-run" / "u-aa123").symlink_to(runs)
        experiment = self.done(exp, run_path=tmp_path / "cylc-run" / "u-aa123")

        experiment.archive(tmp_path / "archived")

        with experiment.directory() as (_, run_path):
            assert run_path is not None
            assert self.archived(run_path) == [Path("log/db")]

    def test_the_patterns_are_matched_in_the_archive(self, tmp_path):
        """A directory above the experiment named like an excluded one excludes nothing, and a link to a directory,
        kept as a link, is excluded by its own name."""
        exp = tmp_path / "share" / "exp1"
        (exp / "restart000").mkdir(parents=True)
        (exp / "restart000" / "ocean.res").write_text("restart")
        (exp / "restart_latest").symlink_to(exp / "restart000")
        (exp / "timings.txt").write_text("timings")
        (exp / "output.nc").write_text("output")
        experiment = self.done(exp)

        experiment.archive(
            tmp_path / "archived", exclude_dirs=["share", "restart*"], exclude_files=["*.nc"], follow_symlinks=True
        )

        with experiment.directory() as (path, _):
            assert self.archived(path) == [Path("timings.txt")]


def recorded_layout():
    """A layout with everything a record has to keep: a process grid, a shared range, an offset and idle cores."""

    atm = ComponentLayout(
        name="atm",
        n_cores=4,
        n_ranks=4,
        threads_per_rank=1,
        decomposition=DomainDecompositionSpec(Domain(shape=(8, 6)), MPICartesianGrid(shape=(2, 2))),
    )
    cpl = ComponentLayout(name="cpl", n_cores=4, n_ranks=4, threads_per_rank=1, decomposition=None)
    ice = ComponentLayout(name="ice", n_cores=2, n_ranks=2, threads_per_rank=1, decomposition=None, core_offset=2)
    shared = ComponentLayout(
        name="shared",
        n_cores=4,
        n_ranks=4,
        threads_per_rank=None,
        decomposition=None,
        sub_layouts=(cpl, ice),
        core_sharing=CoreSharing.SHARED,
    )
    return ComponentLayout(
        name="model", n_cores=10, n_ranks=8, threads_per_rank=None, decomposition=None, sub_layouts=(atm, shared)
    )


class TestLayoutRecord:
    """A layout written into a record, and built again from it."""

    def test_it_comes_back_as_it_was_through_json(self):
        layout = recorded_layout()

        record = json.loads(json.dumps(_layout_record(layout)))

        assert _layout_from_record(record) == layout

    def test_one_stated_wrongly_is_refused(self):
        """The layout is built again, so a record edited into something that is no layout does not get through."""
        record = _layout_record(recorded_layout())
        record["sub_layouts"][0]["n_ranks"] = 3

        with pytest.raises(ValueError, match="n_ranks"):
            _layout_from_record(record)


class TestRecord:
    """What an experiment writes down about itself, since its name says nothing."""

    def test_it_holds_what_differs_from_one_experiment_to_the_next(self, tmp_path):
        layout = recorded_layout()
        experiment = ProfilingExperiment(
            tmp_path / "exp-0001" / "config",
            run_path=Path("/runs/exp-0001"),
            layout=layout,
            changes={"ice_in": {"domain_nml": {"block_size_x": "15"}}},
            labels={"blocks per rank": "5-8"},
        )
        experiment.ncpus = 104

        assert experiment.record(tmp_path) == {
            "version": 1,
            "path": "exp-0001/config",
            "run_path": "/runs/exp-0001",
            "complete": False,
            "layout": _layout_record(layout),
            "changes": {"ice_in": {"domain_nml": {"block_size_x": "15"}}},
            "labels": {"blocks per rank": "5-8"},
            "ncpus": 104,
        }

    @pytest.mark.parametrize(
        "labels, message",
        [
            ({"where": Path("/some/input")}, r"The labels of an experiment must be JSON values: strings, .* Got"),
            ({"speedup": float("nan")}, r"must be JSON values: strings, finite numbers, .* Got \{'speedup': nan\}"),
            ({"speedup": float("inf")}, r"must be JSON values: strings, finite numbers, .* Got \{'speedup': inf\}"),
            ({"grid": (4, 4)}, "The labels of an experiment must be JSON values, which .* is not what it was"),
        ],
    )
    def test_labels_it_could_not_hold_are_refused(self, tmp_path, labels, message):
        """Whatever gives an experiment its labels, they are what a later session reads back from the record.

        NaN and infinity are not JSON, and NaN is not even equal to itself, so they are refused as not JSON
        values rather than as values that change when written and read again."""
        with pytest.raises(ValueError, match=message):
            ProfilingExperiment(tmp_path / "release", labels=labels)

    def test_what_is_unknown_is_null(self, tmp_path):
        """As for an experiment added by hand, which was given no layout and whose changes nothing states."""
        assert ProfilingExperiment(tmp_path / "release", complete=True).record(tmp_path) == {
            "version": 1,
            "path": "release",
            "run_path": None,
            "complete": True,
            "layout": None,
            "changes": None,
            "labels": {},
            "ncpus": None,
        }

    def test_a_path_outside_the_working_directory_is_kept_whole(self, tmp_path):
        experiment = ProfilingExperiment(tmp_path / "elsewhere" / "expt")

        assert experiment.record(tmp_path / "work")["path"] == str(tmp_path / "elsewhere" / "expt")

    def test_a_layout_read_back_is_not_recorded(self, tmp_path):
        """It is not what identifies the experiment, and says less than a layout the experiment was given."""
        experiment = ProfilingExperiment(tmp_path / "expt")
        experiment.layout = recorded_layout()  # as ProfilingManager._layout() keeps what it reads back

        assert not experiment.layout_given
        assert experiment.record(tmp_path)["layout"] is None

    def test_the_labels_are_the_experiments_own(self, tmp_path):
        """Down to a value that is a list, as a process grid is."""
        labels = {"version": "1.0", "grid": [12, 16]}
        experiment = ProfilingExperiment(tmp_path / "expt", labels=labels)

        labels["version"] = "2.0"
        labels["grid"].append(1)

        assert experiment.labels == {"version": "1.0", "grid": [12, 16]}

    def test_an_experiment_comes_back_from_its_record(self, tmp_path):
        layout = recorded_layout()
        original = ProfilingExperiment(
            tmp_path / "expt", layout=layout, changes={"config.yaml": {"walltime": "1:00:00"}}, labels={"a": 1}
        )
        original.ncpus = 48
        record = json.loads(json.dumps(original.record(tmp_path)))

        experiment = ProfilingExperiment.from_record(record, tmp_path / "expt", run_path=tmp_path / "runs")

        assert (experiment.path, experiment.run_path) == (tmp_path / "expt", tmp_path / "runs")
        assert experiment.layout == layout and experiment.layout_given
        assert experiment.changes == {"config.yaml": {"walltime": "1:00:00"}}
        assert experiment.labels == {"a": 1}
        assert experiment.ncpus == 48
        assert experiment.status == ProfilingExperimentStatus.NEW

    def test_a_label_written_before_its_key_was_reserved_is_read_under_another(self, tmp_path, caplog):
        """So that a study made before `run` named a coordinate can still be opened, its label read."""
        record = ProfilingExperiment(tmp_path / "expt", labels={"a": 1}).record(tmp_path)
        record["labels"] = {"run": 1, "a": 1}

        with caplog.at_level(logging.WARNING):
            experiment = ProfilingExperiment.from_record(record, tmp_path / "expt")

        assert experiment.labels == {"run (label)": 1, "a": 1}
        assert f"The record of the experiment at '{tmp_path / 'expt'}' has a label 'run'" in caplog.text

    def test_one_added_as_complete_comes_back_done(self, tmp_path):
        """No engine record says so, which is why the record does."""
        record = ProfilingExperiment(tmp_path / "expt", complete=True).record(tmp_path)

        assert ProfilingExperiment.from_record(record, tmp_path / "expt").status == ProfilingExperimentStatus.DONE

    def test_an_archived_one_stays_archived(self, tmp_path):
        record = ProfilingExperiment(tmp_path / "expt", complete=True).record(tmp_path)

        experiment = ProfilingExperiment.from_record(record, tmp_path / "expt.tar.gz")

        assert experiment.status == ProfilingExperimentStatus.ARCHIVED

    def test_the_labels_are_shown(self, tmp_path):
        experiment = ProfilingExperiment(tmp_path / "expt", labels={"distribution": "roundrobin"})

        assert repr(experiment).endswith("status=NEW, labels={'distribution': 'roundrobin'})")


class TestArchivedRecord:
    """The record an archive holds, written first so that it is read without extracting the rest."""

    @staticmethod
    def record(tmp_path, **entries):
        """A record with every entry, as an experiment writes one, with the given entries changed."""
        return {**ProfilingExperiment(tmp_path / "expt").record(tmp_path), **entries}

    @staticmethod
    def archive(tmp_path, record):
        path = tmp_path / "expt"
        path.mkdir()
        (path / "config.yaml").write_text("ncpus: 4\n")
        experiment = ProfilingExperiment(path)
        experiment.status = ProfilingExperimentStatus.DONE
        (tmp_path / "archive").mkdir()
        experiment.archive(tmp_path / "archive" / "expt", record=record)
        return experiment

    def test_it_is_the_first_member(self, tmp_path):
        experiment = self.archive(tmp_path, self.record(tmp_path, labels={"a": 1}))

        with tarfile.open(experiment.path) as tar:
            names = tar.getnames()
        assert names[0] == RECORD_FILENAME
        assert "experiment/config.yaml" in names

    def test_it_is_read_back(self, tmp_path):
        record = self.record(tmp_path, labels={"a": 1})
        experiment = self.archive(tmp_path, record)

        assert read_archived_record(experiment.path) == record

    def test_one_lacking_an_entry_is_refused_with_the_archive_it_is_in(self, tmp_path):
        experiment = self.archive(tmp_path, {"version": 1, "labels": {"a": 1}})

        with pytest.raises(
            ValueError, match=r"The record in the archive '.*expt\.tar\.gz' cannot be read, since it lacks the entries"
        ):
            read_archived_record(experiment.path)

    def test_one_in_another_format_is_refused_with_the_archive_it_is_in(self, tmp_path):
        """An archive is read years after it was made, so it may come from a later version of the package."""
        experiment = self.archive(tmp_path, self.record(tmp_path, version=2))

        with pytest.raises(
            ValueError, match=r"The record in the archive '.*expt\.tar\.gz' states the format version 2"
        ):
            read_archived_record(experiment.path)

    def test_the_format_it_is_held_to_is_the_records(self, tmp_path):
        experiment = self.archive(tmp_path, self.record(tmp_path))

        with (
            mock.patch("access.profiling.experiment.RECORD_VERSION", 2),
            pytest.raises(ValueError, match="states the format version 1, and .* reads version 2"),
        ):
            read_archived_record(experiment.path)

    def test_the_experiment_is_still_extracted_beside_it(self, tmp_path):
        experiment = self.archive(tmp_path, self.record(tmp_path))

        with experiment.directory() as (path, run_path):
            assert (path / "config.yaml").read_text() == "ncpus: 4\n"
            assert run_path is None

    def test_an_archive_given_none_holds_none(self, tmp_path):
        """As the archives made before experiments had records."""
        experiment = self.archive(tmp_path, None)

        assert read_archived_record(experiment.path) is None

    def test_an_empty_archive_holds_none(self, tmp_path):
        tarfile.open(tmp_path / "empty.tar.gz", "w:gz").close()

        assert read_archived_record(tmp_path / "empty.tar.gz") is None


class TestReadVersioned:
    """A file states the version of its own format, and one in another format is refused with its name."""

    def test_a_file_in_the_format_that_is_read_is_returned_whole(self, tmp_path):
        text = json.dumps({"version": 3, "labels": {"a": 1}})

        assert read_versioned(text, tmp_path / "f.json", "record", 3, ["labels"]) == {"version": 3, "labels": {"a": 1}}

    def test_the_record_an_experiment_writes_is_in_the_format_that_is_read(self, tmp_path):
        record = ProfilingExperiment(tmp_path / "expt").record(tmp_path)
        (tmp_path / RECORD_FILENAME).write_text(json.dumps(record))

        assert read_record(tmp_path / RECORD_FILENAME) == record

    def test_a_file_lacking_an_entry_is_refused_with_its_name(self, tmp_path):
        """Rather than with a bare KeyError from whatever reads the entry first."""
        with pytest.raises(
            ValueError, match=r"The record '.*f\.json' cannot be read, since it lacks the entries \['b'\]"
        ):
            read_versioned(json.dumps({"version": 1, "a": 1}), tmp_path / "f.json", "record", 1, ["a", "b"])

    @pytest.mark.parametrize("content, kind", [([], "list"), ("text", "str"), (3, "int"), (None, "NoneType")])
    def test_json_that_is_not_an_object_is_refused_with_its_name(self, tmp_path, content, kind):
        with pytest.raises(
            ValueError, match=rf"The record '.*f\.json' cannot be read, since it holds a JSON {kind} and"
        ):
            read_versioned(json.dumps(content), tmp_path / "f.json", "record", 1, [])

    def test_another_version_is_refused(self, tmp_path):
        message = (
            rf"The record '{tmp_path}/file\.json' states the format version 2, and this version of access-profiling "
            r"reads version 1\. Update access-profiling"
        )
        with pytest.raises(ValueError, match=message):
            read_versioned(json.dumps({"version": 2}), tmp_path / "file.json", "record", 1, [])

    def test_a_file_stating_no_version_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match=r"The study file '.*file\.json' states no format version, and"):
            read_versioned(json.dumps({"last_number": 3}), tmp_path / "file.json", "study file", 1, ["last_number"])

    def test_a_file_that_is_not_json_is_refused_with_its_name(self, tmp_path):
        """As a file cut short when the disk filled up is, which the JSON error alone would not say which it is."""
        with pytest.raises(
            ValueError, match=r"The record '.*file\.json' cannot be read, since it is not JSON: Expecting"
        ):
            read_versioned('{"version": 1, "path"', tmp_path / "file.json", "record", 1, [])


class TestConfigurationChoice:
    """A configuration choice states what an experiment's record can hold and give back as it was."""

    def test_it_changes_nothing_and_keeps_the_layout_of_the_control_by_default(self):
        choice = ConfigurationChoice()

        assert choice.changes == {} and choice.layout is None and choice.labels == {}

    def test_it_holds_what_it_is_given(self):
        layout = recorded_layout()
        choice = ConfigurationChoice({"ice_in": {"domain_nml": {"block_size_x": 30}}}, layout, {"blocks": "30x54"})

        assert choice.changes == {"ice_in": {"domain_nml": {"block_size_x": 30}}}
        assert choice.layout is layout
        assert choice.labels == {"blocks": "30x54"}

    def test_it_holds_copies_of_its_own(self):
        """As a study that states the walltime of one batch of choices, and then of the next, needs."""
        walltime = {"config.yaml": {"walltime": "2:00:00"}}
        labels = {"grid": [12, 16]}
        choice = ConfigurationChoice(walltime, labels=labels)

        walltime["config.yaml"]["walltime"] = "4:00:00"
        labels["grid"].append(1)

        assert choice.changes == {"config.yaml": {"walltime": "2:00:00"}}
        assert choice.labels == {"grid": [12, 16]}

    @pytest.mark.parametrize("changes", [{"config.yaml": {"input": Path("/some/input")}}, {"config.yaml": {1, 2}}])
    def test_changes_that_are_not_json_values_are_refused(self, changes):
        with pytest.raises(
            ValueError, match="The changes of a configuration choice must be JSON values: strings, .* Got"
        ):
            ConfigurationChoice(changes)

    @pytest.mark.parametrize("changes", [{"config.yaml": {"input": ("a", "b")}}, {"config.yaml": {1: "a"}}])
    def test_changes_a_record_would_not_give_back_are_refused(self, changes):
        """A tuple is read back as a list and a number as a string, so the experiment would not be recognised."""
        with pytest.raises(ValueError, match="changes of a configuration choice .* is not what it was"):
            ConfigurationChoice(changes)

    @pytest.mark.parametrize("labels", [{"where": Path("/some/input")}, {"blocks": (30, 54)}])
    def test_labels_are_held_to_the_same(self, labels):
        with pytest.raises(ValueError, match="The labels of a configuration choice must be JSON values"):
            ConfigurationChoice(labels=labels)

    def test_a_string_of_another_kind_is_kept_as_it_is(self):
        """A YAML library's quoted string is a string, and the quotes are in its type: it must not be rebuilt."""

        class Quoted(str):
            pass

        choice = ConfigurationChoice({"config.yaml": {"env": {"ESMF_RUNTIME_PROFILE": Quoted("on")}}})

        assert type(choice.changes["config.yaml"]["env"]["ESMF_RUNTIME_PROFILE"]) is Quoted


class TestExperimentCoordinate:
    """A quantity of a layout, with the name it goes by."""

    def test_it_holds_a_name_and_how_the_quantity_is_read(self):
        coordinate = ExperimentCoordinate("cores", lambda layout: layout.n_cores)

        assert coordinate.name == "cores"
        assert coordinate.value(recorded_layout()) == recorded_layout().n_cores

    def test_it_cannot_be_changed(self):
        coordinate = ExperimentCoordinate("cores", lambda layout: layout.n_cores)

        with pytest.raises(dataclasses.FrozenInstanceError):
            coordinate.name = "ranks"


class TestFindComponent:
    """Looking a component up in a layout, which access.config offers no way to do."""

    def test_it_finds_a_component_beside_the_others(self):
        layout = layout_with(ocn=240, ice=120)
        assert component_of(layout, "ice").n_cores == 120

    def test_it_finds_the_layout_itself(self):
        layout = layout_with(ocn=240)
        assert find_component(layout, "model") is layout

    def test_it_looks_at_any_depth(self):
        """A tree groups components as the model runs them, not as a reader asks about them."""

        shared = ComponentLayout(
            name="shared",
            n_cores=120,
            n_ranks=120,
            threads_per_rank=None,
            decomposition=None,
            sub_layouts=(leaf_layout("ice", 120),),
        )
        layout = ComponentLayout(
            name="model",
            n_cores=360,
            n_ranks=360,
            threads_per_rank=None,
            decomposition=None,
            sub_layouts=(shared, leaf_layout("ocn", 240)),
        )
        assert component_of(layout, "ice").n_cores == 120

    def test_a_component_that_is_not_there(self):
        assert find_component(layout_with(ocn=240), "atm") is None


class TestComponentCores:
    """The cores a layout gives one component, as a coordinate of the experiments."""

    def test_it_is_named_after_the_component(self):
        assert component_cores("ice").name == "ice cores"

    def test_its_value_is_the_cores_the_layout_gives_the_component(self):
        assert component_cores("ice").value(layout_with(ocn=240, ice=120)) == 120

    def test_cores_are_not_ranks(self):
        """A component with two threads on each rank occupies twice as many cores as it has ranks."""

        threaded = ComponentLayout(name="atm", n_cores=96, n_ranks=48, threads_per_rank=2, decomposition=None)

        assert component_cores("atm").value(threaded) == 96

    def test_a_layout_without_the_component(self):
        with pytest.raises(ValueError, match="The layout holds no component 'atm'. It holds \\['model', 'ocn'\\]"):
            component_cores("atm").value(layout_with(ocn=240))


class TestReservedLabels:
    """A label may not be named as a coordinate every experiment has, or as the label of a run."""

    @pytest.mark.parametrize("key", ["experiment", "ncpus", "run"])
    def test_a_choice_refuses_one(self, key):
        with pytest.raises(ValueError, match=f"The labels of a configuration choice \\['{key}'\\] are named as"):
            ConfigurationChoice(labels={key: 1})

    def test_an_experiment_refuses_one(self):
        with pytest.raises(ValueError, match="The labels of an experiment \\['ncpus'\\] are named as"):
            ProfilingExperiment(path=Path("/fake/exp"), labels={"ncpus": 4})
