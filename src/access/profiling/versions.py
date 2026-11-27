# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The versions of the packages a study runs with.

A study is run over days and read back long after, and what it shows can depend on the version of this package and
of the packages it requires: a layout is written by access-config-utils, an experiment created by
experiment-generator. A notebook therefore states them first. They are read from the requirements this package
declares, so that a package it comes to require is listed without anything here changing.
"""

import platform
import re
from importlib.metadata import PackageNotFoundError, requires, version

_DISTRIBUTION = "access-profiling"

# The name a requirement starts with, before any version, extra or marker.
_REQUIREMENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def _installed_version(distribution: str) -> str:
    """Returns the installed version of a distribution, or "not installed"."""
    try:
        return version(distribution)
    except PackageNotFoundError:
        return "not installed"


def show_versions() -> None:
    """Prints the versions of Python, of this package and of each package it requires.

    The packages are those this package requires to run, in the order it declares them. Those only needed to develop
    or test it are left out. This package run without being installed declares nothing, and only the version of
    Python is known. The versions and the requirements are those of the installed metadata, which an editable install
    writes when it is installed: install it again after its requirements change, or they are not all listed.

    For example, in the first cell of a notebook:

    ```python
    from access.profiling import show_versions

    show_versions()
    ```
    """
    lines = [f"Python: {platform.python_version()}", f"{_DISTRIBUTION}: {_installed_version(_DISTRIBUTION)}"]
    try:
        requirements = requires(_DISTRIBUTION) or []
    except PackageNotFoundError:
        requirements = []
    for requirement in requirements:
        if "extra ==" in requirement:
            continue
        match = _REQUIREMENT_NAME.match(requirement)
        assert match is not None, f"a requirement starts with the name of a distribution: {requirement!r}"
        lines.append(f"{match.group(0)}: {_installed_version(match.group(0))}")
    print("\n".join(lines))
