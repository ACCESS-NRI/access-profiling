# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Draws the example figures of the documentation, from synthetic data.

The figures show what each of the package's plots looks like and how to read it, so the data behind them is
made up: a coupled model whose ocean and sea ice run concurrently, timed by a simple model of strong scaling. The
plots themselves are drawn by the package's own plotting functions, with the regions labelled the way the
manager labels them, so regenerating the figures after a change to the plotting code shows what that change did.

Run it from the repository root, in an environment where access-profiling is installed:

    python documentation/scripts/generate_figures.py

The figures are written to documentation/docs/assets/figures/.
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import xarray as xr  # noqa: E402

from access.profiling import RegionGroup  # noqa: E402
from access.profiling.metrics import tmax  # noqa: E402
from access.profiling.plotting_utils import plot_bar_metrics  # noqa: E402
from access.profiling.scaling import plot_component_scaling, plot_scaling_metrics  # noqa: E402

FIGURES = Path(__file__).resolve().parents[1] / "docs" / "assets" / "figures"
CORES_PER_NODE = 104
NODES = [2, 4, 8, 16, 26, 32]

# How the made-up model shares out its cores: most go to the ocean, a tenth to the pool the sea ice runs on,
# and the rest is left idle by the layout.
OCEAN_FRACTION = 0.88
SEA_ICE_FRACTION = 0.10


def strong_scaling(cores: np.ndarray, serial_seconds: float, parallel_seconds: float, comms: float) -> np.ndarray:
    """Returns the time a component takes on the given cores: a serial part, a part divided among the cores,
    and a communication cost growing with the logarithm of their number."""
    return serial_seconds + parallel_seconds / cores + comms * np.log2(cores)


def timings(total_cores: np.ndarray) -> dict[str, np.ndarray]:
    """Returns the time spent in each region of the made-up model, keyed by the region's name in its log."""
    ocean_cores = np.floor(OCEAN_FRACTION * total_cores)
    sea_ice_cores = np.floor(SEA_ICE_FRACTION * total_cores)
    ocean = strong_scaling(ocean_cores, serial_seconds=40.0, parallel_seconds=2.4e6, comms=6.0)
    sea_ice = strong_scaling(sea_ice_cores, serial_seconds=150.0, parallel_seconds=1.6e5, comms=12.0)
    # The two run concurrently, so the whole model waits for the slower of them, plus the coupling.
    whole = np.maximum(ocean, sea_ice) + 25.0 + 2.0 * np.log2(total_cores)
    # The ocean finishes its own work sooner as it gets more cores, and then waits longer for the sea ice.
    ocean_wait = 5.0 + np.maximum(0.0, sea_ice - ocean) + 0.004 * ocean_cores
    return {
        "[ensemble] RunPhase1": whole,
        "[OCN] RunPhase1": ocean,
        "[ICE] RunPhase1": sea_ice,
        "[OCN] wait for coupler": ocean_wait,
    }


def dataset(cores: np.ndarray, values: dict[str, np.ndarray], regions: list[str]) -> xr.Dataset:
    """Returns the given regions as the manager hands them to the plotting functions: one value per core count
    and region, in seconds, keyed by the metric."""
    data = np.stack([values[region] for region in regions], axis=1)
    array = xr.DataArray(data, dims=["ncpus", "region"], coords={"ncpus": cores, "region": regions})
    return xr.Dataset({tmax: array.pint.quantify("second")})


def labelled(ds: xr.Dataset, group: RegionGroup) -> xr.Dataset:
    """Returns the regions of a group with the labels the manager would plot them under."""
    ds = ds.sel(region=list(group.regions))
    return ds.assign_coords(region=[group.label(region) for region in ds.region.values])


def scaling_figure() -> plt.Figure:
    """The figure ProfilingManager.plot_scaling_data draws: every region against the cores of the whole job."""
    total_cores = np.array(NODES) * CORES_PER_NODE
    values = timings(total_cores)
    whole = RegionGroup("ESMF", ["[ensemble] RunPhase1"], relabel={"[ensemble] RunPhase1": "Whole model"})
    components = RegionGroup(
        "ESMF",
        ["[OCN] RunPhase1", "[ICE] RunPhase1"],
        relabel={"[OCN] RunPhase1": "Ocean", "[ICE] RunPhase1": "Sea ice"},
    )
    stats = [labelled(dataset(total_cores, values, list(group.regions)), group) for group in (whole, components)]
    return plot_scaling_metrics(stats, tmax, xlabel="Cores occupied by the job", show=False)


def component_scaling_figure() -> plt.Figure:
    """The figure ProfilingManager.plot_component_scaling_data draws: every region against its own component's
    cores."""
    total_cores = np.array(NODES) * CORES_PER_NODE
    values = timings(total_cores)
    ocean = RegionGroup(
        "ESMF",
        ["[OCN] RunPhase1", "[OCN] wait for coupler"],
        component="ocn",
        relabel={"[OCN] RunPhase1": "Ocean", "[OCN] wait for coupler": "Ocean waiting for the coupler"},
    )
    sea_ice = RegionGroup("ESMF", ["[ICE] RunPhase1"], component="ice", relabel={"[ICE] RunPhase1": "Sea ice"})
    # Each group is read against the cores of its own component.
    stats = [
        labelled(dataset(np.floor(OCEAN_FRACTION * total_cores).astype(int), values, list(ocean.regions)), ocean),
        labelled(dataset(np.floor(SEA_ICE_FRACTION * total_cores).astype(int), values, list(sea_ice.regions)), sea_ice),
    ]
    return plot_component_scaling(stats, tmax, ylabel="Maximum time (s)", show=False)


def bar_chart_figure() -> plt.Figure:
    """The figure ProfilingManager.plot_bar_chart draws: chosen regions of a few experiments side by side."""
    regions = ["[ensemble] RunPhase1", "[OCN] RunPhase1", "[ICE] RunPhase1"]
    group = RegionGroup(
        "ESMF",
        regions,
        relabel={"[ensemble] RunPhase1": "Whole model", "[OCN] RunPhase1": "Ocean", "[ICE] RunPhase1": "Sea ice"},
    )
    # Three layouts of the same 832 cores, sharing them out differently between the ocean and the sea ice.
    total_cores = 832
    data = {}
    for ocean_cores, sea_ice_cores in [(728, 78), (702, 104), (676, 130)]:
        ocean = strong_scaling(np.array(ocean_cores), serial_seconds=40.0, parallel_seconds=2.4e6, comms=6.0)
        sea_ice = strong_scaling(np.array(sea_ice_cores), serial_seconds=150.0, parallel_seconds=1.6e5, comms=12.0)
        whole = np.maximum(ocean, sea_ice) + 25.0 + 2.0 * np.log2(total_cores)
        data[f"ocn {ocean_cores}, ice {sea_ice_cores}"] = [float(whole), float(ocean), float(sea_ice)]
    return plot_bar_metrics(data, [group.label(region) for region in regions], tmax, show=False)


def main() -> None:
    FIGURES.mkdir(parents=True, exist_ok=True)
    for name, draw in [
        ("scaling", scaling_figure),
        ("component-scaling", component_scaling_figure),
        ("bar-chart", bar_chart_figure),
    ]:
        figure = draw()
        figure.savefig(FIGURES / f"{name}.png", dpi=100, bbox_inches="tight")
        plt.close(figure)
        print(f"Wrote {FIGURES / f'{name}.png'}")


if __name__ == "__main__":
    main()
