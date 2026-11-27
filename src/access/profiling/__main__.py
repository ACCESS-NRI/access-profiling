# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Prints the guide to writing a study notebook, as `python -m access.profiling guide`.

The guide ships inside the package, so that whoever writes a notebook, a coding agent included, reads the version
that matches the package installed rather than whatever the documentation site shows.
"""

import sys
from importlib.resources import files

_USAGE = "Usage: python -m access.profiling guide"


def main(arguments: list[str]) -> int:
    """Prints the guide, given the one command there is.

    Args:
        arguments (list[str]): The command line arguments, without the program name.

    Returns:
        int: The exit status: 0 once the guide is printed, and 2 for any other command, after the usage.
    """
    if arguments != ["guide"]:
        print(_USAGE, file=sys.stderr)
        return 2
    print(files("access.profiling").joinpath("guide.md").read_text(encoding="utf-8"), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
