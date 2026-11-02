# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from access.profiling.experiment import (
    ProfilingExperiment,
    ProfilingExperimentStatus,
    ProfilingLog,
    experiment_directory_walker,
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
    """Test the archive method of ProfilingExperiment for non-DONE statuses."""

    exp = ProfilingExperiment(path=Path("/fake/work_dir/exp1"))
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

    exp = ProfilingExperiment(path=Path("/fake/work_dir/exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    with mock.patch.object(Path, "exists", return_value=True) as mock_exists:
        with pytest.raises(FileExistsError):
            exp.archive(Path("/fake/archive"))
        mock_exists.assert_called_once()


@mock.patch("access.profiling.experiment.tarfile.open")
@mock.patch("access.profiling.experiment.experiment_directory_walker", return_value=[])
def test_profiling_experiment_archive_file_overwrite(mock_walker, mock_open):
    """Test the archive method of ProfilingExperiment when the archive file already exists and overwrite is True."""

    exp = ProfilingExperiment(path=Path("/fake/work_dir/exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    exp.archive(Path("/fake/archive"), overwrite=True)
    mock_open.assert_called_with(Path("/fake/archive").with_suffix(".tar.gz"), "w:gz")


@mock.patch("access.profiling.experiment.tarfile.open")
@mock.patch("access.profiling.experiment.experiment_directory_walker", return_value=[])
def test_profiling_experiment_archive_name_with_dots(mock_walker, mock_open):
    """Archive names containing dots (e.g. version numbers) must not be truncated."""

    exp = ProfilingExperiment(path=Path("/fake/work_dir/exp1"))
    exp.status = ProfilingExperimentStatus.DONE

    archive_path = Path("/fake/archive/am3-release-n96e-1.0-24")
    exp.archive(archive_path)

    expected_archive_file = Path("/fake/archive/am3-release-n96e-1.0-24.tar.gz")
    mock_open.assert_called_with(expected_archive_file, "x:gz")
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


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive(mock_open, tmp_path, setup_experiment_directory, caplog):
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
    mock_open.assert_called_with(Path("/fake/archive").with_suffix(".tar.gz"), "x:gz")  # Check tarfile opening
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


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_follow_symlinks(mock_open, tmp_path, setup_experiment_directory):
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
    mock_open.assert_called_with(Path("/fake/archive").with_suffix(".tar.gz"), "x:gz")  # Check tarfile opening
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


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_with_filters(mock_open, tmp_path, setup_experiment_directory):
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
    mock_open.assert_called_with(Path("/fake/archive").with_suffix(".tar.gz"), "x:gz")  # Check tarfile opening
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


@mock.patch("access.profiling.experiment.tarfile.open")
def test_profiling_experiment_archive_with_run_path(mock_open, tmp_path):
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
    assert exp.run_path is None
    assert exp.path == Path("/fake/archive.tar.gz")


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
