# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from unittest import mock

import numpy as np
import pint
import pytest
import xarray as xr
from matplotlib.figure import Figure
from matplotlib.ticker import LogLocator

from access.profiling.metrics import count, tavg
from access.profiling.scaling import (
    parallel_efficiency,
    parallel_speedup,
    plot_component_scaling,
    plot_scaling_metrics,
)


@pytest.fixture()
def simple_scaling_data():
    """Fixture instantiating a dataset containing scaling data.

    The mock data contains two regions, "Region 1" and "Region 2", and two metrics, hits and tavg.
    Hits are always [1, 2] while tavg depends on the number of CPUs:
        - For 1 CPU: [600365 s, 2.345388 s]
        - For 2 CPUs: [300182.5 s, 1.172694 s]
        - For 4 CPUs: [300182.5 s, 1.172694 s]
    """
    ncpus = [1, 2, 4]
    datasets = []
    for n in ncpus:
        datasets.append(
            xr.Dataset(
                data_vars={
                    count: xr.DataArray([[1, 2]], dims=["ncpus", "region"]),
                    tavg: xr.DataArray(
                        [[value / min(n, 2) for value in [600365, 2.345388]]], dims=["ncpus", "region"]
                    ).pint.quantify("seconds"),
                },
                coords={"region": ["Region 1", "Region 2"], "ncpus": [n]},
            )
        )
    return xr.concat(datasets, dim="ncpus")


def test_parallel_speedup(simple_scaling_data):
    """Test parallel speedup calculation."""
    speedup = parallel_speedup(simple_scaling_data, tavg)

    assert speedup.shape == (2, 3)
    assert speedup.name == "speedup"
    assert str(speedup.pint.units) == "dimensionless"
    assert speedup.attrs == {}
    assert list(speedup.coords) == ["region", "ncpus"]
    assert list(speedup["ncpus"].values) == [1, 2, 4]
    assert list(speedup["region"].values) == ["Region 1", "Region 2"]
    speedup = speedup.pint.dequantify()  # Dequantify to remove warnings when getting values
    assert speedup.sel(ncpus=1, region="Region 1").values == pytest.approx(1.0)
    assert speedup.sel(ncpus=2, region="Region 1").values == pytest.approx(2.0)
    assert speedup.sel(ncpus=4, region="Region 1").values == pytest.approx(2.0)
    assert speedup.sel(ncpus=1, region="Region 2").values == pytest.approx(1.0)
    assert speedup.sel(ncpus=2, region="Region 2").values == pytest.approx(2.0)
    assert speedup.sel(ncpus=4, region="Region 2").values == pytest.approx(2.0)


def test_parallel_efficiency(simple_scaling_data):
    """Test parallel efficiency calculation."""
    ureg = pint.UnitRegistry()

    eff = parallel_efficiency(simple_scaling_data, tavg)

    assert eff.shape == (2, 3)
    assert eff.name == "parallel efficiency"
    assert str(eff.pint.units) == "percent"
    assert eff.attrs == {}
    assert list(eff.coords) == ["region", "ncpus"]
    assert list(eff["ncpus"].values) == [1, 2, 4]
    assert list(eff["region"].values) == ["Region 1", "Region 2"]
    eff = eff.pint.dequantify()  # Dequantify to remove warnings when getting values
    assert eff.sel(ncpus=1, region="Region 1").values == pytest.approx(100 * ureg.percent)
    assert eff.sel(ncpus=2, region="Region 1").values == pytest.approx(100 * ureg.percent)
    assert eff.sel(ncpus=4, region="Region 1").values == pytest.approx(50 * ureg.percent)
    assert eff.sel(ncpus=1, region="Region 2").values == pytest.approx(100 * ureg.percent)
    assert eff.sel(ncpus=2, region="Region 2").values == pytest.approx(100 * ureg.percent)
    assert eff.sel(ncpus=4, region="Region 2").values == pytest.approx(50 * ureg.percent)


def test_incorrect_units(simple_scaling_data):
    """Test calculation with incorrect units."""
    with pytest.raises(ValueError):
        parallel_speedup(simple_scaling_data, count)


@mock.patch("matplotlib.pyplot.show", autospec=True)
def test_plot_scaling_metrics(mock_plt, simple_scaling_data):
    """Test plotting scaling metrics. Currently only checks that the function runs without errors."""

    plot_scaling_metrics(
        stats=[simple_scaling_data],
        metric=tavg,
        xcoordinate="ncpus",
    )
    mock_plt.assert_called_once()


@mock.patch("matplotlib.pyplot.show", autospec=True)
def test_plot_scaling_metrics_default_xlabel(mock_plt, simple_scaling_data):
    """Without an explicit xlabel, the default label derived from xcoordinate is kept."""

    fig = plot_scaling_metrics(
        stats=[simple_scaling_data],
        metric=tavg,
        xcoordinate="ncpus",
    )
    assert fig.axes[0].get_xlabel() == "ncpus"
    assert fig.axes[1].get_xlabel() == "ncpus"
    tbl_chart = fig.axes[2].tables[0]
    assert tbl_chart.get_celld()[(0, 0)].get_text().get_text() == "ncpus"


@mock.patch("matplotlib.pyplot.show", autospec=True)
def test_plot_scaling_metrics_custom_xlabel(mock_plt, simple_scaling_data):
    """An explicit xlabel overrides the default label on both subplots."""

    fig = plot_scaling_metrics(
        stats=[simple_scaling_data],
        metric=tavg,
        xcoordinate="ncpus",
        xlabel="Number of CPUs",
    )
    assert fig.axes[0].get_xlabel() == "Number of CPUs"
    assert fig.axes[1].get_xlabel() == "Number of CPUs"


@mock.patch("matplotlib.pyplot.show", autospec=True)
def test_plot_scaling_metrics_custom_xlabel_updates_table_header(mock_plt, simple_scaling_data):
    """The table's header cell must reflect a custom xlabel, not just the subplot axes."""

    fig = plot_scaling_metrics(
        stats=[simple_scaling_data],
        metric=tavg,
        xcoordinate="ncpus",
        xlabel="Number of CPUs",
    )
    tbl_chart = fig.axes[2].tables[0]
    assert tbl_chart.get_celld()[(0, 0)].get_text().get_text() == "Number of CPUs"


@mock.patch("matplotlib.pyplot.show", autospec=True)
def test_plot_scaling_metrics_efficiency_ylim_covers_superlinear_efficiency(mock_plt):
    """The efficiency ylim must auto-adjust to cover efficiency above 100%, not silently clip it.

    Regression test: comparing a bare int against a pint percent Quantity via max() compares against the
    Quantity's fractional (base-unit) magnitude rather than its percent magnitude, so values above 100% were
    previously dropped when computing the default axis limit.
    """
    ncpus = [1, 2]
    datasets = []
    for n, val in zip(ncpus, [1000, 400], strict=True):  # superlinear: 2 cpus is >2x faster than 1 cpu
        datasets.append(
            xr.Dataset(
                data_vars={tavg: xr.DataArray([[val]], dims=["ncpus", "region"]).pint.quantify("seconds")},
                coords={"region": ["R1"], "ncpus": [n]},
            )
        )
    stat = xr.concat(datasets, dim="ncpus")

    fig = plot_scaling_metrics(stats=[stat], metric=tavg, xcoordinate="ncpus")
    ax2 = fig.axes[1]

    assert ax2.get_ylim()[1] >= 1.1 * 125  # true max efficiency is 125%; default ylim must cover it


@pytest.fixture()
def component_scaling_data():
    """Two components, each with its own core counts, as the manager hands them over.

    The ocean is given 60, 120 and 240 cores and the sea ice 30, 60 and 120, so the two sit at different
    points of one axis - which is the whole difference between this plot and plot_scaling_metrics. The
    region coordinate carries the label each curve is to be plotted under, the manager having settled it.

    The ocean's regions halve as its cores double, so they scale perfectly and sit on their ideal. The sea
    ice's scales too, but only as the square root of its cores, so it falls away from its ideal - and does
    so by bending, which is what the axis is laid out to show.
    """
    stats = []
    for component, cores, regions, seconds in [
        ("MOM6", [60, 120, 240], ["MOM6: Ocean dynamics", "MOM6: Ocean"], lambda n: 600.0 * 60 / n),
        ("CICE6", [30, 60, 120], ["CICE6: Total"], lambda n: 100.0 * (30 / n) ** 0.5),
    ]:
        datasets = [
            xr.Dataset(
                data_vars={
                    tavg: xr.DataArray([[seconds(n) for _ in regions]], dims=["ncpus", "region"]).pint.quantify(
                        "seconds"
                    )
                },
                coords={"region": regions, "ncpus": [n]},
            )
            for n in cores
        ]
        stats.append((component, xr.concat(datasets, dim="ncpus")))
    return stats


def _measured(fig) -> dict[str, list]:
    """The x data of each measured curve, keyed by its label. The ideals are black; these are not."""

    return {line.get_label(): list(line.get_xdata()) for line in fig.axes[0].lines if line.get_color() != "k"}


def _ideals(fig) -> list:
    """The ideal curves, which are the black ones."""

    return [line for line in fig.axes[0].lines if line.get_color() == "k"]


class TestPlotComponentScaling:
    """The walltime of a component's regions against the cores that component was given."""

    def test_one_line_per_region_of_every_component(self, component_scaling_data):
        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        assert isinstance(fig, Figure)
        assert len(_measured(fig)) == 3

    def test_the_lines_carry_the_labels_they_were_given(self, component_scaling_data):
        """The manager settles those, so that a relabelled region can stand on its own name."""

        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        assert list(_measured(fig)) == ["MOM6: Ocean dynamics", "MOM6: Ocean", "CICE6: Total"]

    def test_each_component_carries_its_own_cores(self, component_scaling_data):
        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        by_label = _measured(fig)
        assert by_label["MOM6: Ocean"] == [60, 120, 240]
        assert by_label["CICE6: Total"] == [30, 60, 120]

    def test_the_y_axis_is_labelled_from_the_metric(self, component_scaling_data):
        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        ax = fig.axes[0]
        assert tavg.name in ax.get_ylabel()
        assert str(tavg.units) in ax.get_ylabel()

    def test_a_custom_y_label(self, component_scaling_data):
        fig = plot_component_scaling(component_scaling_data, tavg, ylabel="Ocean walltime", show=False)
        assert fig.axes[0].get_ylabel() == "Ocean walltime"

    def test_there_is_no_title(self, component_scaling_data):
        """A figure going into a document is captioned there rather than in the image."""

        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        assert fig.axes[0].get_title() == ""

    def test_the_default_x_label_says_whose_cores_they_are(self, component_scaling_data):
        """ "ncpus" on its own would read as the whole job's, which is the other plot's question."""

        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        assert fig.axes[0].get_xlabel() == "Cores assigned to the component"

    def test_a_custom_x_label(self, component_scaling_data):
        fig = plot_component_scaling(component_scaling_data, tavg, xlabel="Ocean cores", show=False)
        assert fig.axes[0].get_xlabel() == "Ocean cores"

    @mock.patch("matplotlib.pyplot.show", autospec=True)
    def test_it_is_shown_when_asked(self, mock_show, component_scaling_data):
        plot_component_scaling(component_scaling_data, tavg)
        mock_show.assert_called_once()

    @mock.patch("matplotlib.pyplot.show", autospec=True)
    def test_it_is_not_shown_otherwise(self, mock_show, component_scaling_data):
        plot_component_scaling(component_scaling_data, tavg, show=False)
        mock_show.assert_not_called()

    def test_one_log_read_twice_keeps_both(self, component_scaling_data):
        """A log holding two components' regions is read once per component, and neither is dropped."""

        (mom6, ocean), _ = component_scaling_data
        repeated = [(mom6, ocean), (mom6, ocean.isel(region=[0]))]

        fig = plot_component_scaling(repeated, tavg, show=False)

        measured = [line for line in fig.axes[0].lines if line.get_color() != "k"]
        assert len(measured) == 3, "two regions from the first, one from the second"
        assert [line.get_label() for line in measured] == [
            "MOM6: Ocean dynamics",
            "MOM6: Ocean",
            "MOM6: Ocean dynamics",
        ]


class TestIdealScaling:
    """The curve each measured one would be if every core it was given were bought in full."""

    def test_one_ideal_per_measured_curve(self, component_scaling_data):
        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        assert len(_ideals(fig)) == len(_measured(fig)) == 3

    def test_they_are_all_one_solid_black_line(self, component_scaling_data):
        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        assert all(line.get_linestyle() == "-" for line in _ideals(fig))

    def test_the_legend_names_them_once(self, component_scaling_data):
        """They say the same thing about every curve, so saying it three times would only crowd it."""

        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        labels = [text.get_text() for text in fig.axes[0].get_legend().get_texts()]
        assert labels == ["MOM6: Ocean dynamics", "MOM6: Ocean", "CICE6: Total", "Ideal"]

    def test_an_ideal_meets_its_curve_at_the_fewest_cores(self, component_scaling_data):
        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        for measured, ideal in zip(
            [line for line in fig.axes[0].lines if line.get_color() != "k"], _ideals(fig), strict=True
        ):
            assert list(ideal.get_xdata()) == list(measured.get_xdata())
            assert ideal.get_ydata()[0] == pytest.approx(measured.get_ydata()[0])

    def test_it_halves_as_the_cores_double(self, component_scaling_data):
        """The sea ice only scales as the square root of its cores, so its ideal is what it did not manage."""

        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        (ice,) = [line for line in _ideals(fig) if list(line.get_xdata()) == [30, 60, 120]]
        assert list(ice.get_ydata()) == pytest.approx([100.0, 50.0, 25.0])

    def test_a_curve_that_scales_perfectly_sits_on_its_ideal(self, component_scaling_data):
        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        measured = [line for line in fig.axes[0].lines if line.get_color() != "k"][0]
        ideal = _ideals(fig)[0]
        assert list(ideal.get_ydata()) == pytest.approx(list(measured.get_ydata()))

    def test_a_curve_of_a_single_point(self):
        """One core count is one point, and its ideal is that same point rather than an error."""

        dataset = xr.Dataset(
            data_vars={tavg: xr.DataArray([[10.0]], dims=["ncpus", "region"]).pint.quantify("seconds")},
            coords={"region": ["Only"], "ncpus": [48]},
        )

        fig = plot_component_scaling([("log", dataset)], tavg, show=False)

        (ideal,) = _ideals(fig)
        assert list(ideal.get_xdata()) == [48]
        assert list(ideal.get_ydata()) == pytest.approx([10.0])


def _is_straight_on_screen(line) -> bool:
    """Whether a line is drawn straight, which is a question about the axis and not about the data."""

    points = line.axes.transData.transform(list(zip(line.get_xdata(), line.get_ydata(), strict=True)))
    first, last = points[0], points[-1]
    # The area of the triangle each point makes with the ends, which is zero exactly when it is on the line
    # between them. Scaled by the length of that line, so the tolerance is a distance in pixels.
    span = np.hypot(*(last - first))
    return all(abs(np.cross(last - first, point - first)) / span < 0.5 for point in points[1:-1])


def _screen_slope(line) -> float:
    """How steeply a line falls across the figure, which on logarithmic axes is the power it scales by."""

    points = line.axes.transData.transform(list(zip(line.get_xdata(), line.get_ydata(), strict=True)))
    return float((points[-1][1] - points[0][1]) / (points[-1][0] - points[0][0]))


class TestTheAxesStraightenTheIdeal:
    """Both axes are logarithmic, which is what makes perfect scaling a straight line."""

    def test_an_ideal_is_drawn_straight(self, component_scaling_data):
        """It is a hyperbola in the data, so this holds only because of how the axes are laid out."""

        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        assert all(_is_straight_on_screen(line) for line in _ideals(fig))

    def test_it_would_not_be_on_plain_axes(self, component_scaling_data):
        """The same points, laid out evenly, bend - which is what was worth changing."""

        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        fig.axes[0].set_xscale("linear")
        fig.axes[0].set_yscale("linear")
        fig.canvas.draw()

        assert not any(_is_straight_on_screen(line) for line in _ideals(fig))

    def test_a_curve_keeping_up_with_its_cores_runs_parallel_to_its_ideal(self, component_scaling_data):
        """What tells the two apart on these axes is the slope, every power of the cores being straight."""

        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        measured = [line for line in fig.axes[0].lines if line.get_color() != "k"]
        ideals = _ideals(fig)

        assert _screen_slope(measured[0]) == pytest.approx(_screen_slope(ideals[0]), rel=1e-6), (
            "the ocean halves as its cores double, so it falls exactly as its ideal does"
        )
        assert _screen_slope(measured[-1]) == pytest.approx(0.5 * _screen_slope(ideals[-1]), rel=1e-6), (
            "the sea ice only scales as their square root, so it falls at half the slope"
        )

    def test_the_ticks_are_matplotlib_s_own(self, component_scaling_data):
        """Pinning them to the sizes that were run is what made them collide on a wide sweep."""

        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        assert isinstance(fig.axes[0].xaxis.get_major_locator(), LogLocator)

    def test_both_axes_are_logarithmic(self, component_scaling_data):
        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        assert (fig.axes[0].get_xscale(), fig.axes[0].get_yscale()) == ("log", "log")


def _axis_fractions(fig) -> list[float]:
    """Where every plotted point sits across the axis, 0 being its left edge and 1 its right."""

    ax = fig.axes[0]
    fig.canvas.draw()
    fractions = []
    for line in ax.lines:
        for x, y in zip(line.get_xdata(), line.get_ydata(), strict=True):
            pixels = ax.transData.transform((x, y))
            fractions.append(float(ax.transAxes.inverted().transform(pixels)[0]))
    return fractions


def _one_group(cores: list[int]):
    """A single region timed at each of the given core counts."""

    datasets = [
        xr.Dataset(
            data_vars={tavg: xr.DataArray([[1000.0 / n]], dims=["ncpus", "region"]).pint.quantify("seconds")},
            coords={"region": ["Region"], "ncpus": [n]},
        )
        for n in cores
    ]
    return [("log", xr.concat(datasets, dim="ncpus"))]


class TestTheAxesHoldThePoints:
    """However wide the sweep, every point it measured has to be on the figure that reports it."""

    @pytest.mark.parametrize(
        "cores",
        [
            pytest.param([48, 96, 192, 384, 768, 1536], id="a thirty-two-fold sweep"),
            pytest.param([24, 12288], id="a five-hundred-fold sweep"),
            pytest.param([48, 96, 1000, 2000], id="sizes in two clusters"),
        ],
    )
    def test_every_point_is_on_the_canvas_however_wide_the_sweep(self, cores):
        """A sweep of hundreds of cores to thousands is what a real scaling study looks like."""

        fig = plot_component_scaling(_one_group(cores), tavg, show=False)
        assert all(0.0 <= fraction <= 1.0 for fraction in _axis_fractions(fig))

    @pytest.mark.parametrize("cores", [[48, 1536], [60, 120, 240], [240], [24, 12288]])
    def test_the_axis_runs_between_core_counts(self, cores):
        """A count of cores is a positive thing, and the larger runs belong to the right of the smaller."""

        fig = plot_component_scaling(_one_group(cores), tavg, show=False)
        low, high = fig.axes[0].get_xlim()
        assert 0 < low < high

    def test_a_narrow_sweep_keeps_its_margins(self, component_scaling_data):
        """The fix is not paying for itself by cramping the studies that were never broken."""

        fig = plot_component_scaling(component_scaling_data, tavg, show=False)
        fractions = _axis_fractions(fig)
        assert min(fractions) == pytest.approx(0.05, abs=0.01)
        assert max(fractions) == pytest.approx(0.95, abs=0.01)

    def test_a_single_core_count_is_on_the_figure(self):
        """One size is a study of one point, which is still a point that has to be shown."""

        fig = plot_component_scaling(_one_group([240]), tavg, show=False)
        assert all(0.0 <= fraction <= 1.0 for fraction in _axis_fractions(fig))
