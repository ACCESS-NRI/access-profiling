# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import platform
from importlib.metadata import PackageNotFoundError
from unittest import mock

from access.profiling.versions import show_versions

INSTALLED = {"access-profiling": "1.2.3", "xarray": "2026.1.0", "access-config-utils": "0.6.0"}

REQUIREMENTS = ["xarray", "access-config-utils>=0.6", "payu; python_version >= '3.11'", 'pytest; extra == "test"']


def fake_version(distribution):
    if distribution not in INSTALLED:
        raise PackageNotFoundError(distribution)
    return INSTALLED[distribution]


class TestShowVersions:
    """The versions a study runs with, read from what the package requires, so that none is listed by hand."""

    @mock.patch("access.profiling.versions.version", side_effect=fake_version)
    @mock.patch("access.profiling.versions.requires", return_value=REQUIREMENTS)
    def test_it_prints_python_this_package_and_each_requirement(self, mock_requires, mock_version, capsys):
        """In the order they are declared, under their names alone, and without what only testing needs."""
        show_versions()

        assert capsys.readouterr().out.splitlines() == [
            f"Python: {platform.python_version()}",
            "access-profiling: 1.2.3",
            "xarray: 2026.1.0",
            "access-config-utils: 0.6.0",
            "payu: not installed",
        ]
        mock_requires.assert_called_once_with("access-profiling")

    @mock.patch("access.profiling.versions.version", side_effect=PackageNotFoundError("access-profiling"))
    @mock.patch("access.profiling.versions.requires", side_effect=PackageNotFoundError("access-profiling"))
    def test_without_this_package_installed(self, mock_requires, mock_version, capsys):
        """Run from the sources, it declares nothing, so only Python is known."""
        show_versions()

        assert capsys.readouterr().out.splitlines() == [
            f"Python: {platform.python_version()}",
            "access-profiling: not installed",
        ]

    def test_the_installed_package_lists_what_it_requires(self, capsys):
        """Read from the metadata of the installed package, which lists xarray among the rest."""
        show_versions()

        names = [line.split(":")[0] for line in capsys.readouterr().out.splitlines()]
        assert names[:3] == ["Python", "access-profiling", "xarray"]
        assert "pytest" not in names
