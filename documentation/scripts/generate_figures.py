# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Draws the example figures of the documentation, from synthetic data.

The figures show what each of the package's plots looks like and how to read it, so the data behind them is
made up: a coupled model whose ocean and sea ice run concurrently, timed by a simple model of strong scaling. The
plots themselves are drawn by the plotting methods of the results, from results made of the synthetic data, so
regenerating the figures after a change to the plotting code shows what that change did.

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
from access.config.parallel_component import ComponentLayout  # noqa: E402

from access.profiling import ParsedExperiment, ProfilingResults, Region  # noqa: E402
from access.profiling.metrics import tmax  # noqa: E402
from access.profiling.models.om3 import OM3_MC_25KM  # noqa: E402

FIGURES = Path(__file__).resolve().parents[1] / "docs" / "assets" / "figures"
CORES_PER_NODE = 104
NODES = [2, 4, 8, 16, 26, 32]

# How the made-up model shares out its cores: most go to the ocean, a tenth to the pool the sea ice runs on,
# and the rest is left idle by the layout.
OCEAN_FRACTION = 0.88
SEA_ICE_FRACTION = 0.10


def strong_scaling(cores: int, serial_seconds: float, parallel_seconds: float, comms: float) -> float:
    """Returns the time a component takes on the given cores: a serial part, a part divided among the cores,
    and a communication cost growing with the logarithm of their number."""
    return serial_seconds + parallel_seconds / cores + comms * float(np.log2(cores))


def experiment(
    total_cores: int,
    ocean_cores: int,
    sea_ice_cores: int,
    labels: dict | None = None,
    sea_ice_seconds: float | None = None,
) -> ParsedExperiment:
    """Returns one made-up experiment as parsing gives it: its ESMF summary, its layout and its CPUs.

    The time of each region comes from the made-up model, unless the time of the sea ice is given.
    """
    ocean = strong_scaling(ocean_cores, serial_seconds=40.0, parallel_seconds=2.4e6, comms=6.0)
    if sea_ice_seconds is None:
        sea_ice_seconds = strong_scaling(sea_ice_cores, serial_seconds=150.0, parallel_seconds=1.6e5, comms=12.0)
    times = {
        # The two run concurrently, so the whole model waits for the slower of them, plus the coupling.
        "[ensemble] RunPhase1": max(ocean, sea_ice_seconds) + 25.0 + 2.0 * float(np.log2(total_cores)),
        "[OCN] RunPhase1": ocean,
        "[ICE] RunPhase1": sea_ice_seconds,
        # The ocean finishes its own work sooner as it gets more cores, and then waits longer for the sea ice.
        "[OCN] wait for coupler": 5.0 + max(0.0, sea_ice_seconds - ocean) + 0.004 * ocean_cores,
    }
    summary = xr.Dataset(
        {tmax: xr.DataArray(list(times.values()), dims=["region"]).pint.quantify("second")},
        coords={"region": list(times)},
    )
    components = tuple(
        ComponentLayout(name=name, n_cores=cores, n_ranks=cores, threads_per_rank=1, decomposition=None)
        for name, cores in (("ocn", ocean_cores), ("ice", sea_ice_cores))
    )
    layout = ComponentLayout(
        name=OM3_MC_25KM.name,
        n_cores=total_cores,
        n_ranks=ocean_cores + sea_ice_cores,
        threads_per_rank=None,
        decomposition=None,
        sub_layouts=components,
    )
    return ParsedExperiment({"ESMF": summary}, labels or {}, layout, total_cores)


def scaling_study() -> ProfilingResults:
    """Returns the results of a scaling study of the made-up model, one experiment for each size."""
    experiments = {}
    for number, nodes in enumerate(NODES, start=1):
        total = nodes * CORES_PER_NODE
        experiments[f"exp-{number:04d}"] = experiment(total, int(OCEAN_FRACTION * total), int(SEA_ICE_FRACTION * total))
    return ProfilingResults(OM3_MC_25KM, experiments)


def scaling_figure() -> plt.Figure:
    """The figure ProfilingResults.plot_scaling_data draws: every region against the cores of the whole job."""
    regions = [
        Region("ESMF", "[ensemble] RunPhase1", label="Whole model"),
        Region("ESMF", "[OCN] RunPhase1", label="Ocean"),
        Region("ESMF", "[ICE] RunPhase1", label="Sea ice"),
    ]
    speedup, _ = scaling_study().plot_scaling_data(regions, xlabel="Cores occupied by the job")
    return speedup.figure


def component_scaling_figure() -> plt.Figure:
    """The figure ProfilingResults.plot_component_scaling_data draws: every region against its own component's
    cores."""
    regions = [
        Region("ESMF", "[OCN] RunPhase1", label="Ocean", component="ocn"),
        Region("ESMF", "[OCN] wait for coupler", label="Ocean waiting for the coupler", component="ocn"),
        Region("ESMF", "[ICE] RunPhase1", label="Sea ice", component="ice"),
    ]
    return scaling_study().plot_component_scaling_data(regions, ylabel="Maximum time (s)").figure


def bar_chart_figure() -> plt.Figure:
    """The figure ProfilingResults.plot_bar_chart draws: chosen regions of a few experiments side by side."""
    regions = [
        Region("ESMF", "[ensemble] RunPhase1", label="Whole model"),
        Region("ESMF", "[OCN] RunPhase1", label="Ocean"),
        Region("ESMF", "[ICE] RunPhase1", label="Sea ice"),
    ]
    # Three layouts of the same 832 cores, sharing them out differently between the ocean and the sea ice.
    results = ProfilingResults(
        OM3_MC_25KM,
        {
            f"ocn {ocean}, ice {sea_ice}": experiment(832, ocean, sea_ice)
            for ocean, sea_ice in [(728, 78), (702, 104), (676, 130)]
        },
    )
    return results.plot_bar_chart(regions).figure


def line_plot_figure() -> plt.Figure:
    """The figure ProfilingResults.plot_metric draws: a region against a coordinate of the experiments."""
    # Four block sizes on one layout. With few blocks on each rank the work is shared out unevenly, and with many
    # the ranks spend longer on the boundaries between the blocks.
    results = ProfilingResults(
        OM3_MC_25KM,
        {
            f"exp-{number:04d}": experiment(
                312, 288, 24, {"blocks per rank": blocks}, sea_ice_seconds=70.0 + 30.0 / blocks + 1.2 * blocks
            )
            for number, blocks in enumerate([1, 4, 6, 12], start=1)
        },
    )
    region = Region("ESMF", "[ICE] RunPhase1", label="CICE6")
    return results.plot_metric(region, "blocks per rank", xlabel="Blocks per rank").figure


def main() -> None:
    FIGURES.mkdir(parents=True, exist_ok=True)
    for name, draw in [
        ("scaling", scaling_figure),
        ("component-scaling", component_scaling_figure),
        ("bar-chart", bar_chart_figure),
        ("line-plot", line_plot_figure),
    ]:
        figure = draw()
        figure.savefig(FIGURES / f"{name}.png", dpi=100, bbox_inches="tight")
        plt.close(figure)
        print(f"Wrote {FIGURES / f'{name}.png'}")


if __name__ == "__main__":
    main()
