# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Classes and utilities to build profiling parsers for reading profiling data."""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

# Next import is required to register pint with xarray
import pint_xarray  # noqa: F401

from access.profiling.metrics import ProfilingMetric


class ProfilingParser(ABC):
    """Abstract parser of profiling data.

    The main purpose of a parser of profiling data is to read said data from a file or directory and return it in a
    standard format.

    Once parsed, the profiling data should be stored in a dict in the following way:

    {
        'region': ['region1', 'region2', ...],
        'metric a': [val1a, val2a, ...],
        'metric b': [val1b, val2b, ...],
        ...
    }

    The 'region' values correspond to the labels of the profile regions. Then, for each metric, there is a list of
    values, one for each profiling region. Therefore, 'val1a', is the value for metric a of region 1.
    """

    _metrics: list[ProfilingMetric]

    @property
    def metrics(self) -> list[ProfilingMetric]:
        """list: Metrics available when using this parser."""
        return self._metrics

    @abstractmethod
    def parse(self, file_path: str | Path | os.PathLike) -> dict:
        """Parse the given file.

        Args:
            file_path (str | Path | os.PathLike): file to parse.

        Returns:
            dict: profiling data.

        Raises:
            ValueError: If no parsable text is found in file_path.
            TypeError: If file_path cannot be converted to a valid Path object.
            FileNotFoundError: If file_path doesn't exist or isn't a file.
        """


def _convert_from_string(value: str) -> Any:
    """Tries to convert a string to the most appropriate numeric type. Leaves it unchanged if conversion does not
    succeed.

    Args:
        value (str): string to convert.

    Returns:
        Any: the converted string or the original string.
    """
    for type_conversion in (int, float):
        try:
            return type_conversion(value)
        except Exception:
            continue
    return value


def _test_file(file_path: str | Path | os.PathLike) -> Path:
    """Checks whether file_path is a valid path.

    Args:
        file_path (str | Path | os.PathLike): the path to check.

    Returns
        Path: file_path as a Path object.

    Raises:
        TypeError: if file_path cannot be turned into a Path object.
        FileNotFoundError: if file_path can be converted into a Path, but the file dosen't exist.
    """

    try:
        path = Path(file_path)
    except TypeError as e:
        raise TypeError(f"{file_path} is not a valid path.") from e

    if not path.is_file():
        raise FileNotFoundError(f"{file_path} is not a file or doesn't exist.")

    return path


def _read_text_file(file_path: str | Path | os.PathLike) -> str:
    """Checks whether file_path is a valid path to a text file and tries to read it.

    Args:
        file_path (str | Path | os.PathLike): the path to check/read

    Returns:
        str: The text within the file.

    Raises:
        TypeError: if file_path is not a valid path
        FileNotFoundError: if file_path is a path, but is not a file or doesn't exist.
        ValueError: if file_path is a file, but cannot be read as a text file.
    """

    path = _test_file(file_path)

    try:
        return path.read_text()
    except UnicodeDecodeError as e:
        raise ValueError(f"{file_path} is not a text file.") from e
