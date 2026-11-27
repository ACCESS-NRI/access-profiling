# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Narrowing helpers shared by the tests.

Several things a layout search returns are optional in general and present in every case these tests build:
a component a configuration declares, and the decomposition of a component that carries a domain. Reaching
through them directly works, but when the assumption is ever wrong the test dies on an AttributeError that
names neither the layout nor the component.

These say what was assumed, so a broken assumption reads as a failed assertion rather than as a crash. They
are for test code only: production code states the same thing with an explicit ValueError, since there the
reader is a user rather than whoever is holding the test output.

It also holds what more than one test module needs: a stand-in parser, the layout search of a Payu application,
the variables of a Rose suite, and the closing of the figures a test draws.
"""

from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import matplotlib.pyplot as plt
import pytest
from access.config.parallel_component import ComponentLayout
from access.config.parallel_domain import DomainDecompositionSpec

from access.profiling.experiment import find_component
from access.profiling.parser import ProfilingParser
from access.profiling.payu_configuration import PayuConfiguration
from access.profiling.payu_manager import PayuManager


@pytest.fixture(autouse=True)
def close_figures() -> Iterator[None]:
    """Closes the figures a test drew once it is done.

    The plotting functions leave their figure open for the caller to show or save, so without this they pile up
    over the session, and matplotlib warns about the memory they hold.
    """
    yield
    plt.close("all")


def fake_parser() -> ProfilingParser:
    """Returns a stand-in parser, for the tests that carry a parser to where a log is declared without running it.

    What the parser reads never matters there, and a real one would only tie those tests to a log format.

    Returns:
        ProfilingParser: The stand-in.
    """
    return mock.Mock(spec=ProfilingParser)


def select_layouts(application: PayuConfiguration, total_cores: int, **kwargs) -> list[ComponentLayout]:
    """Returns the layouts of a Payu application at a size, through the manager that searches for them.

    Args:
        application (PayuConfiguration): The application to search over.
        total_cores (int): Cores the layouts must distribute among the components.
        **kwargs: As for ProfilingManager.select_layouts.

    Returns:
        list[ComponentLayout]: The layouts found, fewest idle cores first.
    """
    manager = PayuManager(Path("/fake/test_path"), Path("/fake/archive_path"), application)
    return manager.select_layouts(total_cores, **kwargs)


def write_suite_conf(directory: Path, variables: dict[str, str], file: str = "rose-suite.conf") -> Path:
    """Writes the variables of a Rose suite into the `[jinja2:suite.rc]` section of its configuration.

    Args:
        directory (Path): The suite directory, or its run directory. Created if need be.
        variables (dict[str, str]): The values, as the file is to state them, keyed by variable.
        file (str): The file, relative to the directory: `rose-suite.conf` (the default), or the copy of it a run
            records, `log/rose-suite-run.conf`.

    Returns:
        Path: The file.
    """
    path = directory / file
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[jinja2:suite.rc]\n" + "".join(f"{name}={value}\n" for name, value in variables.items()))
    return path


def leaf_layout(name: str, n_cores: int) -> ComponentLayout:
    """Returns a single component of a layout, one thread per rank as the ACCESS models have it.

    Args:
        name (str): Name of the component.
        n_cores (int): Its cores, which are also its ranks.

    Returns:
        ComponentLayout: The component, with no decomposition.
    """
    return ComponentLayout(name=name, n_cores=n_cores, n_ranks=n_cores, threads_per_rank=1, decomposition=None)


def layout_with(**cores: int) -> ComponentLayout:
    """Returns a layout named "model" whose components run side by side.

    Args:
        **cores (int): The cores of each component, keyed by its name.

    Returns:
        ComponentLayout: The layout.
    """
    sub_layouts = tuple(leaf_layout(name, n) for name, n in cores.items())
    total = sum(sub_layout.n_cores for sub_layout in sub_layouts)
    return ComponentLayout(
        name="model",
        n_cores=total,
        n_ranks=total,
        threads_per_rank=None,
        decomposition=None,
        sub_layouts=sub_layouts,
    )


def component_of(layout: ComponentLayout, name: str) -> ComponentLayout:
    """Returns the named component of a layout, at whatever depth it sits.

    Args:
        layout (ComponentLayout): The layout to look in.
        name (str): Name of the component.

    Returns:
        ComponentLayout: What that component was given.
    """
    found = find_component(layout, name)
    assert found is not None, f"the layout {layout.name!r} holds no component named {name!r}"
    return found


def layout_of(layout: ComponentLayout | None) -> ComponentLayout:
    """Returns a layout that was read back, asserting it could be told.

    Args:
        layout (ComponentLayout | None): What was read back.

    Returns:
        ComponentLayout: The layout.
    """
    assert layout is not None, "the layout could not be told"
    return layout


def decomposition_of(sub_layout: ComponentLayout) -> DomainDecompositionSpec:
    """Returns the decomposition a component was given.

    Args:
        sub_layout (ComponentLayout): What that component was given.

    Returns:
        DomainDecompositionSpec: Its domain and the process grid it is split over.
    """
    assert sub_layout.decomposition is not None, (
        f"{sub_layout.name!r} states no decomposition, so the search chose no process grid for it"
    )
    return sub_layout.decomposition


def grid_of(sub_layout: ComponentLayout) -> tuple[int, ...]:
    """Returns the process grid a component was given, as its extents.

    Args:
        sub_layout (ComponentLayout): What that component was given.

    Returns:
        tuple[int, ...]: The extents of its process grid.
    """
    return tuple(decomposition_of(sub_layout).grid)


def legend_labels(ax) -> list[str]:
    """Returns the labels of an axes' legend, asserting there is one.

    Args:
        ax (Axes): The axes to read.

    Returns:
        list[str]: The labels, in the order the legend lists them.
    """
    legend = ax.get_legend()
    assert legend is not None, "the axes carries no legend"
    return [text.get_text() for text in legend.get_texts()]
