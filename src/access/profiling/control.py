# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Where a profiling study's control comes from.

A study perturbs a control: one setup of one application, from which every experiment is derived. Which
control that is belongs neither to the application being profiled nor to the engine running it. The same
setup can be profiled from a released tag, from a fork, or from a directory already on disk, and none of
those change what the application is or how it is run.

So a control is its own thing, and the axis it varies on is how it is *retrieved* rather than which runner
uses it. Two studies of the same application may take their controls from different places, and one runner
may drive both; retrieval and runner vary independently, so every runner accepts every source here.

A control source records coordinates; it does not fetch anything. Some runners fetch for themselves, and
others expect the directory to be there already. Keeping the fetch out of this package keeps credentials -
ssh keys, repository passphrases - out of it too, where a prompt cannot hang a notebook kernel waiting for
input nobody can give it.
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
        the control itself is given this; one that expects it to be there already ignores it and keeps it
        only as provenance.

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
    """A control held in a git repository.

    This is what a published setup normally uses, whichever runner drives it: its releases are tags, and a
    study names the tag it was run against.

    Args:
        repository (str): Repository URL, or the path of one on disk.
        start_point (str): What to start the experiments from: a tag, a branch or a commit. Published setups
            are tagged, so this is usually a tag.
        directory (str): Name the clone takes under the working directory. Defaults to "config", which is
            the name studies in this package conventionally use.
        label (str | None): Short identifier for this control. Defaults to the start point, which for a
            published setup is the release.

    Raises:
        ValueError: If the repository or the start point is empty.
    """

    repository: str
    # Defaulted only so that this satisfies the abstract start_point of the base class: a dataclass field
    # with no default leaves nothing in the class namespace, and the member would stay abstract. Omitting it
    # is still refused, by __post_init__ below.
    start_point: str = ""
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
    """A control already checked out on disk.

    The control is wherever the caller put it, and this package does not put it there. That is what a
    credential-protected repository needs: the checkout runs in a terminal, where whatever it asks for can be
    answered, and the directory is on disk before any manager is built. The revision is recorded rather than
    acted on, so that a study can still say which control it used.

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
