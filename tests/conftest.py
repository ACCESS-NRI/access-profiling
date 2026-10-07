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
"""

from access.config.parallel_component import ComponentLayout
from access.config.parallel_domain import DomainDecompositionSpec

from access.profiling.manager import find_component


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
