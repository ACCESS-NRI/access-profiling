# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import inspect

import access.profiling


def test_all_lists_every_name_the_package_imports():
    """A name imported into the package but left out of __all__, or listed there but not imported, is a mistake in
    one of the two lists."""
    imported = {
        name
        for name, value in vars(access.profiling).items()
        if not name.startswith("_")
        and not inspect.ismodule(value)
        and getattr(value, "__module__", "").startswith("access.profiling.")
    }

    assert imported == set(access.profiling.__all__)
