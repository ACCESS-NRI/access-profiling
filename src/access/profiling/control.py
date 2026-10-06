# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Where a profiling study's control configuration comes from.

A study perturbs a control: one configuration of one model, from which every experiment is derived. Which
control that is belongs neither to the model being profiled nor to the engine running it. The same
ACCESS-ESM1.6 configuration can be profiled from the released tag, from a fork, or from a directory already on
disk, and none of those change what the model is or how it is run.

So a control is its own thing, and the axis it varies on is how it is *retrieved* rather than which runner
uses it. ACCESS-AM3 is driven by Cylc and takes its control from git, exactly as the Payu-driven models do,
while ACCESS-rAM3 is driven by the same runner and takes its control from a directory checked out beforehand.
Both runners accept both sources.

A control source records coordinates; it does not fetch anything. For Payu the fetching is the experiment
generator's, which clones the repository itself, and for Cylc the directory is expected to exist already. That
keeps credentials - ssh keys, MOSRS passphrases - outside this package, where a prompt cannot hang a notebook
kernel waiting for input nobody can give it.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


class ControlSource(ABC):
    """Abstract base class for the control configuration a profiling study perturbs."""

    @property
    @abstractmethod
    def directory(self) -> str:
        """Returns the name the control takes under the working directory.

        Returns:
            str: Directory name, relative to the working directory.
        """

    @property
    @abstractmethod
    def label(self) -> str:
        """Returns a short human-readable identifier for this control.

        Used where a study has to say what it was run against: in a plot, in an archive path, or in a record
        written beside the results.

        Returns:
            str: The identifier.
        """

    @property
    @abstractmethod
    def origin(self) -> str:
        """Returns where the control is obtained from.

        A repository URL or a path on disk, depending on how the control is retrieved. A runner that fetches
        the control itself - the Payu experiment generator does - is given this; one that expects it to be
        there already ignores it and keeps it only as provenance.

        Returns:
            str: The origin.
        """

    @property
    @abstractmethod
    def start_point(self) -> str | None:
        """Returns the revision of the control to start the experiments from.

        None means the control is taken as it stands, which is what an already checked-out directory of
        unknown revision amounts to. A runner that has to start branches from somewhere needs a value here
        and should say so rather than guess one.

        Returns:
            str | None: A tag, branch, commit or revision, or None if the control states none.
        """

    @abstractmethod
    def provenance(self) -> dict[str, str]:
        """Returns what this control was, as a record to keep beside the results.

        Returns:
            dict[str, str]: The coordinates, keyed by name. What the keys are depends on how the control is
                retrieved, so a reader should treat this as a record rather than something to act on.
        """


@dataclass(frozen=True)
class GitControlSource(ControlSource):
    """A control configuration held in a git repository.

    This is what every released ACCESS configuration uses, whichever runner drives it: the configurations of
    ACCESS-ESM1.6, ACCESS-OM3 and ACCESS-AM3 are all git repositories whose releases are tags.

    Args:
        repository (str): Repository URL, or the path of one on disk.
        start_point (str): What to start the experiments from: a tag, a branch or a commit. Released
            configurations are tagged, so this is usually a tag.
        directory (str): Name the clone takes under the working directory. Defaults to "config", which is
            what Payu-driven studies use.
        label (str | None): Short identifier for this control. Defaults to the start point, which for a
            released configuration is the release.

    Raises:
        ValueError: If the repository or the start point is empty.
    """

    repository: str
    start_point: str
    _directory: str = "config"
    _label: str | None = None

    def __post_init__(self) -> None:
        if not self.repository:
            raise ValueError("GitControlSource.repository must be non-empty.")
        if not self.start_point:
            raise ValueError(
                "GitControlSource.start_point must be non-empty: a control is a particular state of a "
                "repository, so a tag, branch or commit is needed to say which."
            )

    @property
    def directory(self) -> str:
        return self._directory

    @property
    def label(self) -> str:
        return self._label if self._label is not None else self.start_point

    @property
    def origin(self) -> str:
        return self.repository

    def provenance(self) -> dict[str, str]:
        return {"repository": self.repository, "start_point": self.start_point}


@dataclass(frozen=True)
class ExistingDirectoryControlSource(ControlSource):
    """A control configuration already checked out on disk.

    The control is wherever the caller put it, and this package does not put it there. ACCESS-rAM3 works this
    way today: `rosie checkout` runs in a terminal, after `mosrs-auth`, and the suite is on disk before any
    manager is built. The revision is recorded rather than acted on, so that a study can still say which
    control it used.

    Args:
        path (Path): The control directory, which must already exist when a study runs.
        label (str | None): Short identifier for this control. Defaults to the directory's own name.
        revision (str | None): The revision the directory holds, if it is known. Nothing here checks it out -
            the directory is taken as it stands - but it is what a runner that has to start branches from
            somewhere is given, and it is recorded as provenance either way.

    Raises:
        ValueError: If the path is not absolute, since a control outlives the working directory a study runs
            in and a relative path would mean different things from different places.
    """

    path: Path
    _label: str | None = None
    revision: str | None = None

    def __post_init__(self) -> None:
        if not self.path.is_absolute():
            raise ValueError(
                f"ExistingDirectoryControlSource.path must be absolute, got {str(self.path)!r}. The control is "
                "found again from wherever a study runs, so a relative path would name different directories "
                "at different times."
            )

    @property
    def directory(self) -> str:
        return self.path.name

    @property
    def label(self) -> str:
        return self._label if self._label is not None else self.path.name

    @property
    def origin(self) -> str:
        return str(self.path)

    @property
    def start_point(self) -> str | None:
        return self.revision

    def provenance(self) -> dict[str, str]:
        record = {"path": str(self.path)}
        if self.revision is not None:
            record["revision"] = self.revision
        return record
