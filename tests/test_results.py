# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
import math
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
import xarray as xr
from conftest import MockApplication, layout_with

from access.profiling.experiment import ExperimentCoordinate, component_cores
from access.profiling.metrics import count, tavg
from access.profiling.plotting_utils import RegionGroup
from access.profiling.results import ParsedExperiment, ProfilingResults


class ParsedStudy:
    """Parsed experiments that a test describes by changing them, read through results made afresh for every call.

    Results cannot be changed, so the parts of each experiment are kept here instead, where a test can give an
    experiment a layout, a label or a log of its own. Any other attribute is read off results made from them at that
    moment, so `study.gather(...)` is `study.results().gather(...)`.

    Args:
        paths (list[Path]): The experiments, named after the last part of each path.
        ncpus (list[int] | None): The CPUs each experiment occupied, or None to leave them unknown.
        datasets (list[dict[str, xr.Dataset]] | None): The data of each experiment, keyed by log, or None for none.
    """

    def __init__(self, paths, ncpus=None, datasets=None):
        names = [path.name for path in paths]
        self.application = MockApplication()
        self.data = {name: dict(data) for name, data in zip(names, datasets or [{}] * len(names), strict=True)}
        counts = ncpus if ncpus is not None else [None] * len(names)
        self.experiments = {
            name: SimpleNamespace(labels={}, layout=None, ncpus=n, layout_error=None, ncpus_error=None)
            for name, n in zip(names, counts, strict=True)
        }

    def results(self):
        return ProfilingResults(
            self.application,
            {
                name: ParsedExperiment(
                    self.data[name], part.labels, part.layout, part.ncpus, part.layout_error, part.ncpus_error
                )
                for name, part in self.experiments.items()
            },
        )

    def __getattr__(self, name):
        return getattr(self.results(), name)


def make_component_dataset(tavg_values: list[float]) -> dict[str, xr.Dataset]:
    """Builds mock parsed profiling data for a single component with two regions.

    Args:
        tavg_values (list[float]): Values of the tavg metric for "Region 1" and "Region 2".

    Returns:
        dict[str, xr.Dataset]: Mock data for a single component, named "component".
    """
    regions = ["Region 1", "Region 2"]
    count_array = xr.DataArray([1, 2], dims=["region"]).pint.quantify(count.units)
    tavg_array = xr.DataArray(list(tavg_values), dims=["region"]).pint.quantify(tavg.units)
    return {"component": xr.Dataset(data_vars={count: count_array, tavg: tavg_array}, coords={"region": regions})}


@pytest.fixture()
def scaling_data():
    """Fixture instantiating fake parsed profiling data for different CPU configurations, as one would get from
    a scaling study.

    The mock data contains two regions, "Region 1" and "Region 2", and two metrics, count and tavg.
    Counts are always [1, 2] while tavg depends on the number of CPUs:
    - For 1 CPU: [600365 s, 2.345388 s]
    - For 2 CPUs: [300182.5 s, 1.172694 s]
    - For 4 CPUs: [300182.5 s, 1.172694 s]
    """
    paths = [Path("1cpu"), Path("4cpu"), Path("2cpu")]
    ncpus = [1, 4, 2]  # Intentionally unordered to test sorting in the study
    datasets = []
    for n in ncpus:
        regions = ["Region 1", "Region 2"]
        count_array = xr.DataArray([1, 2], dims=["region"]).pint.quantify(count.units)
        tavg_array = xr.DataArray([value / min(n, 2) for value in [600365, 2.345388]], dims=["region"]).pint.quantify(
            tavg.units
        )
        datasets.append(
            {"component": xr.Dataset(data_vars={count: count_array, tavg: tavg_array}, coords={"region": regions})}
        )

    return paths, ncpus, datasets


@mock.patch("access.profiling.results.plot_scaling_metrics")
def test_scaling_data(mock_plot, scaling_data):
    """Test the plot_scaling_data method of ProfilingResults.

    This test will check that datasets are correctly concatenated across different numbers of CPUs
    and that the plotting function is called correctly.
    """
    paths, ncpus, datasets = scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    # Test plotting scaling data for non-existing component
    with pytest.raises(ValueError):
        study.plot_scaling_data(
            [RegionGroup("non_existing_component", ["Region 1"])],
            metric=tavg,
        )

    # Test plotting scaling data
    study.plot_scaling_data(
        [RegionGroup("component", ["Region 1"], relabel={"Region 1": "Total"})],
        metric=tavg,
        experiments=["1cpu", "4cpu"],
        xlabel="Number of CPUs",
    )
    assert mock_plot.call_count == 1
    assert mock_plot.call_args.kwargs["xlabel"] == "Number of CPUs"
    scaling_data = mock_plot.call_args.args[0]
    assert isinstance(scaling_data, list)
    assert len(scaling_data) == 1  # One component
    component_data = scaling_data[0]
    assert isinstance(component_data, xr.Dataset)
    assert set(component_data.coords["ncpus"].values) == {1, 4}  # Only 1cpu and 4cpu experiments included
    assert set(component_data.coords["region"].values) == {"Total"}  # Region selection and relabelling
    assert set(component_data.data_vars.keys()) == {count, tavg}
    assert component_data[count].sel(region="Total").values.tolist() == [1, 1]
    assert component_data[tavg].sel(region="Total").values.tolist() == [600365.0, 300182.5]
    assert mock_plot.call_args.args[1] == tavg


def test_scaling_data_missing_region_raises_value_error(scaling_data):
    """Test plot_scaling_data raises a clear error when a requested region is missing."""

    paths, ncpus, datasets = scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    with pytest.raises(ValueError, match="not found in") as exc_info:
        study.plot_scaling_data(
            [RegionGroup("component", ["Missing Region"])],
            metric=tavg,
            experiments=["1cpu"],
        )

    message = str(exc_info.value)
    assert "Missing Region" in message
    assert "'component'" in message
    assert "experiment '1cpu'" in message


def test_scaling_data_no_selected_experiments_raises_value_error(scaling_data):
    """Test plot_scaling_data raises when no experiments are selected."""

    paths, ncpus, datasets = scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    with pytest.raises(ValueError, match="No experiments selected"):
        study.plot_scaling_data(
            [RegionGroup("component", ["Region 1"])],
            metric=tavg,
            experiments=[],
        )


def test_scaling_data_missing_experiment_data_raises_value_error(scaling_data):
    """Test plot_scaling_data raises when selected experiments have no parsed data."""

    paths, ncpus, datasets = scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    with pytest.raises(ValueError, match=r"No parsed profiling data found for experiment\(s\)") as exc_info:
        study.plot_scaling_data(
            [RegionGroup("component", ["Region 1"])],
            metric=tavg,
            experiments=["missing_exp"],
        )

    message = str(exc_info.value)
    assert "missing_exp" in message
    assert "Available experiments" in message


@mock.patch("access.profiling.results.plot_bar_metrics")
def test_bar_chart_data(mock_plot, scaling_data):
    """Test the plot_bar_chart method of ProfilingResults.

    This test checks that bar chart data is correctly extracted from the datasets and that the
    plotting function is called with the right arguments.
    """
    paths, ncpus, datasets = scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    # Test plotting bar chart for non-existing component
    with pytest.raises(ValueError):
        study.plot_bar_chart(
            [RegionGroup("non_existing_component", ["Region 1"])],
            metric=tavg,
        )

    # Test plotting bar chart with region selection, relabelling, and experiment filtering
    study.plot_bar_chart(
        [RegionGroup("component", ["Region 1", "Region 2"], relabel={"Region 1": "Total"})],
        metric=tavg,
        experiments=["1cpu", "4cpu"],
    )
    assert mock_plot.call_count == 1

    # Verify the data dict passed to plot_bar_metrics
    bar_data = mock_plot.call_args.args[0]
    assert isinstance(bar_data, dict)
    assert set(bar_data.keys()) == {"1cpu", "4cpu"}
    assert bar_data["1cpu"] == pytest.approx([600365.0, 2.345388])
    assert bar_data["4cpu"] == pytest.approx([300182.5, 1.172694])

    # Verify region labels
    region_labels = mock_plot.call_args.args[1]
    assert region_labels == ["Total", "component: Region 2"]

    # Verify metric
    assert mock_plot.call_args.args[2] == tavg

    # Verify show kwarg is passed through
    assert mock_plot.call_args.kwargs["show"] is True

    # Test experiment relabelling
    mock_plot.reset_mock()
    study.plot_bar_chart(
        [RegionGroup("component", ["Region 1"])],
        metric=tavg,
        experiment_relabel_map={"1cpu": "1 CPU", "4cpu": "4 CPUs"},
        experiments=["1cpu", "4cpu"],
    )
    bar_data = mock_plot.call_args.args[0]
    assert set(bar_data.keys()) == {"1 CPU", "4 CPUs"}
    assert bar_data["1 CPU"] == pytest.approx([600365.0])
    assert bar_data["4 CPUs"] == pytest.approx([300182.5])


@mock.patch("access.profiling.results.plot_bar_metrics")
def test_bar_chart_all_experiments(mock_plot, scaling_data):
    """Test plot_bar_chart includes all experiments when none are specified."""
    paths, ncpus, datasets = scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    study.plot_bar_chart(
        [RegionGroup("component", ["Region 1"])],
        metric=tavg,
        show=False,
    )
    bar_data = mock_plot.call_args.args[0]
    assert set(bar_data.keys()) == {"1cpu", "4cpu", "2cpu"}
    assert mock_plot.call_args.kwargs["show"] is False


@pytest.fixture()
def layout_scaling_data():
    """Fixture instantiating fake parsed profiling data where two experiments use the same number of CPUs.

    This is what a layout study looks like: '2cpu_slow' and '2cpu_fast' both use 2 CPUs, but '2cpu_fast' is the
    better performing decomposition. The tavg values on "Region 1" are 400 s, 800 s and 300 s respectively.
    """
    paths = [Path("2cpu_slow"), Path("1cpu"), Path("2cpu_fast")]
    ncpus = [2, 1, 2]  # Intentionally unordered, with a repeated number of CPUs
    datasets = [
        make_component_dataset([400.0, 4.0]),
        make_component_dataset([800.0, 8.0]),
        make_component_dataset([300.0, 3.0]),
    ]

    return paths, ncpus, datasets


def test_select_best_experiments(layout_scaling_data):
    """Test select_best_experiments keeps the fastest experiment for each number of CPUs."""

    paths, ncpus, datasets = layout_scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    # Only one experiment per CPU count, ordered by increasing number of CPUs
    assert study.select_best_experiments("component", "Region 1", tavg) == ["1cpu", "2cpu_fast"]


def test_select_best_experiments_subset(layout_scaling_data):
    """Test select_best_experiments only selects among the requested experiments."""

    paths, ncpus, datasets = layout_scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    # The faster 2 CPU experiment is not a candidate, so the slower one survives
    selected = study.select_best_experiments("component", "Region 1", tavg, experiments=["2cpu_slow", "1cpu"])
    assert selected == ["1cpu", "2cpu_slow"]


def test_select_best_experiments_unique_ncpus(scaling_data):
    """Test select_best_experiments is a no-op, apart from sorting, when all CPU counts are distinct."""

    paths, ncpus, datasets = scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    assert study.select_best_experiments("component", "Region 1", tavg) == ["1cpu", "2cpu", "4cpu"]


def test_select_best_experiments_tie_keeps_first(caplog):
    """Test select_best_experiments keeps the first experiment and warns when two experiments tie."""

    paths = [Path("2cpu_a"), Path("2cpu_b")]
    datasets = [make_component_dataset([400.0, 4.0]), make_component_dataset([400.0, 4.0])]
    study = ParsedStudy(paths, ncpus=[2, 2], datasets=datasets)

    with caplog.at_level(logging.WARNING):
        selected = study.select_best_experiments("component", "Region 1", tavg)

    assert selected == ["2cpu_a"]
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert "2cpu_a" in caplog.records[0].message and "2cpu_b" in caplog.records[0].message


def test_select_best_experiments_keeps_the_incumbent_when_a_later_experiment_is_slower(caplog):
    """A layout that is merely slower than the one already held must lose quietly, neither winning nor warning.

    The warning exists to flag two layouts that cannot be told apart, which is a result worth a second look. A
    simply slower layout is the ordinary outcome of a layout study, and reporting it would bury the real ties in
    noise - while promoting it would hand the scaling plot the wrong point for that CPU count.
    """

    paths = [Path("2cpu_fast"), Path("2cpu_slow")]
    datasets = [make_component_dataset([300.0, 3.0]), make_component_dataset([400.0, 4.0])]
    study = ParsedStudy(paths, ncpus=[2, 2], datasets=datasets)

    with caplog.at_level(logging.WARNING):
        selected = study.select_best_experiments("component", "Region 1", tavg)

    assert selected == ["2cpu_fast"]
    assert caplog.records == [], "A slower layout is not a tie, so nothing should be reported about it."


@mock.patch("access.profiling.results.plot_scaling_metrics")
def test_scaling_data_with_best_experiments(mock_plot, layout_scaling_data):
    """Test that feeding select_best_experiments into plot_scaling_data drops the slower layouts."""

    paths, ncpus, datasets = layout_scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    study.plot_scaling_data(
        [RegionGroup("component", ["Region 1"])],
        metric=tavg,
        experiments=study.select_best_experiments("component", "Region 1", tavg),
    )

    component_data = mock_plot.call_args.args[0][0]
    assert component_data.coords["ncpus"].values.tolist() == [1, 2]  # No duplicated CPU counts
    assert component_data[tavg].sel(region="component: Region 1").values.tolist() == [
        800.0,
        300.0,
    ]  # Fastest 2 CPU layout


def test_scaling_data_duplicate_ncpus_raises_value_error(layout_scaling_data):
    """Test plot_scaling_data refuses to plot when several experiments share a number of CPUs."""

    paths, ncpus, datasets = layout_scaling_data
    study = ParsedStudy(paths, ncpus, datasets)

    with pytest.raises(ValueError, match="same number of CPUs") as exc_info:
        study.plot_scaling_data(
            [RegionGroup("component", ["Region 1"])],
            metric=tavg,
        )

    message = str(exc_info.value)
    assert "[2]" in message
    assert "select_best_experiments" in message


def runs_of(values: list[list[float]]) -> xr.Dataset:
    """Returns the data of a log that ran several times, numbered 0, 3, 7, ..., as parsing concatenates it.

    The run numbers are not positions, so that a test tells a run number from its place along the run dimension.

    Args:
        values (list[list[float]]): The tavg of "Region 1" and "Region 2" in each run.

    Returns:
        xr.Dataset: The data, with a `run` dimension.
    """
    runs = [make_component_dataset(region_values)["component"] for region_values in values]
    return xr.concat(
        [ds.expand_dims({"run": [run]}) for run, ds in zip([0, 3, 7, 11], runs, strict=False)], dim="run", join="outer"
    )


@pytest.fixture()
def run_data():
    """Results of one experiment that ran three times.

    Both logs hold the same three runs, numbered 0, 3 and 7. Run 7 is the fastest on "Region 1" while run 0 is the
    fastest on "Region 2", so tests can tell a whole-experiment selection from a per-region reduction.
    """
    data = {
        "component": runs_of([[400.0, 1.0], [600.0, 5.0], [200.0, 9.0]]),
        "other": runs_of([[40.0, 10.0], [60.0, 50.0], [20.0, 90.0]]),
    }
    return ParsedStudy([Path("exp1")], ncpus=[4], datasets=[data]).results()


class TestSelectBestRun:
    """One run of each experiment, the same in every log, chosen by one region."""

    def test_the_fastest_run_is_kept_in_every_log(self, run_data):
        best = run_data.select_best_run("component", "Region 1", tavg)

        # Run 7 is the fastest on the ranking region, and is selected in both logs
        for ds in best.data["exp1"].values():
            assert "run" not in ds.dims
            # The run coordinate is dropped too, so a reduced dataset looks exactly like a single-run one
            assert "run" not in ds.coords
        assert best.data["exp1"]["component"][tavg].sel(region="Region 1").pint.dequantify().item() == 200.0
        assert best.data["exp1"]["other"][tavg].sel(region="Region 1").pint.dequantify().item() == 20.0

    def test_other_regions_are_not_chosen_on_their_own(self, run_data):
        """The chosen run is a whole-experiment choice, not the best value of every region."""

        best = run_data.select_best_run("component", "Region 1", tavg)

        # Run 0 is faster on "Region 2" (1.0 s), but run 7 won on the ranking region so its value is kept
        assert best.data["exp1"]["component"][tavg].sel(region="Region 2").pint.dequantify().item() == 9.0

    def test_the_results_it_is_called_on_keep_every_run(self, run_data):
        """A notebook can keep the runs and their best side by side, whichever cell ran first."""

        run_data.select_best_run("component", "Region 1", tavg)

        assert run_data.data["exp1"]["component"].sizes["run"] == 3

    def test_what_the_results_say_of_each_experiment_is_kept(self):
        study = ParsedStudy([Path("exp1")], ncpus=[4], datasets=[{"component": runs_of([[4.0, 1.0], [2.0, 1.0]])}])
        study.experiments["exp1"].labels = {"blocks": 4}
        study.experiments["exp1"].layout = layout_with(component=4)

        best = study.select_best_run("component", "Region 1", tavg)

        assert best.experiments["exp1"].labels == {"blocks": 4}
        assert best.experiments["exp1"].layout == layout_with(component=4)
        assert best.experiments["exp1"].ncpus == 4

    def test_data_without_runs_is_kept_as_it_is(self, layout_scaling_data):
        paths, ncpus, datasets = layout_scaling_data
        results = ParsedStudy(paths, ncpus, datasets).results()

        best = results.select_best_run("component", "Region 1", tavg)

        xr.testing.assert_identical(best.data["1cpu"]["component"], results.data["1cpu"]["component"])

    def test_a_log_an_experiment_does_not_have(self, run_data):
        with pytest.raises(ValueError, match="No profiling data found for 'absent' in experiment 'exp1'. Its logs are"):
            run_data.select_best_run("absent", "Region 1", tavg)

    def test_a_region_the_log_does_not_have(self, run_data):
        with pytest.raises(ValueError, match="Regions \\['absent'\\] not found .* Available regions"):
            run_data.select_best_run("component", "absent", tavg)

    def test_results_holding_no_experiments(self):
        with pytest.raises(ValueError, match="These results hold no experiments"):
            ParsedStudy([]).select_best_run("component", "Region 1", tavg)


class TestAggregateRuns:
    """Each region and metric of every experiment reduced over the runs on its own."""

    def test_the_minimum_can_mix_runs(self, run_data):
        ds = run_data.aggregate_runs(how="min").data["exp1"]["component"]

        assert "run" not in ds.dims
        assert ds[tavg].sel(region="Region 1").pint.dequantify().item() == 200.0  # From run 7
        assert ds[tavg].sel(region="Region 2").pint.dequantify().item() == 1.0  # From run 0

    def test_the_mean_keeps_the_metrics_and_their_units(self, run_data):
        ds = run_data.aggregate_runs(how="mean").data["exp1"]["component"]

        assert tavg in ds.data_vars  # Metric objects survive the reduction, unlike aggregate_pe_data
        assert ds[tavg].pint.units == tavg.units
        assert ds[tavg].sel(region="Region 1").pint.dequantify().item() == pytest.approx(400.0)

    def test_the_median(self, run_data):
        ds = run_data.aggregate_runs(how="median").data["exp1"]["component"]

        assert ds[tavg].sel(region="Region 1").pint.dequantify().item() == 400.0

    def test_the_maximum(self, run_data):
        ds = run_data.aggregate_runs(how="max").data["exp1"]["component"]

        assert ds[tavg].sel(region="Region 1").pint.dequantify().item() == 600.0  # From run 3
        assert ds[tavg].sel(region="Region 2").pint.dequantify().item() == 9.0  # From run 7

    def test_the_results_it_is_called_on_keep_every_run(self, run_data):
        """So that the fastest and the slowest of the same runs measure their spread."""

        fastest = run_data.aggregate_runs("min")
        slowest = run_data.aggregate_runs("max")

        assert run_data.data["exp1"]["component"].sizes["run"] == 3
        spread = slowest.data["exp1"]["component"][tavg] - fastest.data["exp1"]["component"][tavg]
        assert spread.pint.dequantify().values.tolist() == [400.0, 8.0]

    def test_an_unsupported_statistic(self, run_data):
        with pytest.raises(ValueError, match="Unknown reduction"):
            run_data.aggregate_runs(how="bogus")


class TestAReductionHasDatasetsOfItsOwn:
    """A log that ran once is carried over to the reduced results, as a dataset of their own."""

    @pytest.mark.parametrize(
        "reduce",
        [
            lambda results: results.aggregate_runs("min"),
            lambda results: results.select_best_run("runs", "Region 1", tavg),
        ],
        ids=["aggregate_runs", "select_best_run"],
    )
    def test_a_variable_added_to_one_is_not_added_to_the_other(self, reduce):
        """Both in an experiment that ran several times, and in one that ran once, whose logs are all carried over."""
        several = {
            "runs": runs_of([[400.0, 1.0], [200.0, 9.0]]),
            "once": make_component_dataset([1.0, 2.0])["component"],
        }
        once = {"runs": make_component_dataset([3.0, 4.0])["component"]}
        results = ParsedStudy([Path("exp1"), Path("exp2")], ncpus=[4, 8], datasets=[several, once]).results()

        reduced = reduce(results)
        for name, log in [("exp1", "once"), ("exp2", "runs")]:
            reduced.data[name][log]["per_call"] = reduced.data[name][log][tavg]

        assert "per_call" not in results.data["exp1"]["once"]
        assert "per_call" not in results.data["exp2"]["runs"]


class TestRunsAreRefusedUntilReduced:
    """A selection, a table or a plot needs one value for each experiment, and says how to get it."""

    @mock.patch("access.profiling.results.plot_bar_metrics")
    def test_once_reduced_the_plots_work_unchanged(self, mock_plot, run_data):
        best = run_data.select_best_run("component", "Region 1", tavg)
        best.plot_bar_chart([RegionGroup("component", ["Region 1"])], metric=tavg, show=False)

        assert mock_plot.call_args.args[0] == {"exp1": pytest.approx([200.0])}

    @pytest.mark.parametrize(
        "read",
        [
            lambda results: results.select_best_experiments("component", "Region 1", tavg),
            lambda results: results.plot_scaling_data([RegionGroup("component", ["Region 1"])], metric=tavg),
            lambda results: results.plot_bar_chart([RegionGroup("component", ["Region 1"])], metric=tavg),
            lambda results: results.gather(RegionGroup("component", ["Region 1"])),
        ],
        ids=["select_best_experiments", "plot_scaling_data", "plot_bar_chart", "gather"],
    )
    def test_unreduced_data_is_refused_with_the_experiments_and_the_way_out(self, run_data, read):
        with pytest.raises(ValueError, match="experiments \\['exp1'\\] still has a 'run' dimension.*select_best_run"):
            read(run_data)


def test_bar_chart_of_a_metric_with_more_than_one_value_for_each_region(block_study):
    """As data for each PE has, which a bar cannot show, rather than a TypeError from turning it into one number."""

    add_per_pe_log(block_study)

    with pytest.raises(ValueError, match="has the dimensions \\['pe'\\] .* more values than a chart has bars"):
        block_study.plot_bar_chart([RegionGroup("per_pe", ["Region 1"])], tavg, show=False)


@pytest.fixture()
def component_scaling_data():
    """Parsed data for three experiments whose components were given different numbers of cores.

    The log is named "component", as make_component_dataset has it, and is looked for in the layout under
    that name since no mapping says otherwise. The layout holds a second component, "other", given half as
    many cores, so that a group read against one is telling apart from a group read against the other.
    """
    paths = [Path("small"), Path("large"), Path("medium")]
    cores = [60, 240, 120]  # Unordered, so that the plot has to sort them
    datasets = [make_component_dataset([100.0 * 60 / n, 10.0]) for n in cores]
    study = ParsedStudy(paths, ncpus=[n * 2 for n in cores], datasets=datasets)
    for path, n in zip(paths, cores, strict=True):
        study.experiments[path.name].layout = layout_with(component=n, other=n // 2)
    return study, cores


class TestPlotComponentScalingData:
    """Plotting a metric against the cores the component itself was given."""

    @staticmethod
    def _plotted(mock_plot) -> list[list]:
        """The x values of each group handed to the plot, in the order the groups were given."""

        return [list(stat["ncpus"].values) for stat in mock_plot.call_args.args[0]]

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_the_x_axis_is_the_components_cores(self, mock_plot, component_scaling_data):
        """Not the whole job's: each experiment occupied twice as many CPUs."""

        study, cores = component_scaling_data
        study.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)

        assert self._plotted(mock_plot) == [sorted(cores)]

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_every_region_asked_for_is_plotted(self, mock_plot, component_scaling_data):
        study, _ = component_scaling_data
        study.plot_component_scaling_data([RegionGroup("component", ["Region 1", "Region 2"])], tavg)

        (stat,) = mock_plot.call_args.args[0]
        assert list(stat["region"].values) == ["component: Region 1", "component: Region 2"]

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_a_region_is_labelled_with_the_log_it_came_from(self, mock_plot, component_scaling_data):
        """Region names are only unique within one log, so two components may each have a "Total"."""

        study, _ = component_scaling_data
        study.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)

        (stat,) = mock_plot.call_args.args[0]
        assert list(stat["region"].values) == ["component: Region 1"]

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_a_relabelled_region_stands_on_its_own_name(self, mock_plot, component_scaling_data):
        """Naming one is the caller saying what it should be called, so the log is not put in front of it."""

        study, _ = component_scaling_data
        study.plot_component_scaling_data(
            [RegionGroup("component", ["Region 1", "Region 2"], relabel={"Region 1": "Dynamics"})],
            tavg,
        )

        (stat,) = mock_plot.call_args.args[0]
        assert list(stat["region"].values) == ["Dynamics", "component: Region 2"], "one named, one not"

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_one_log_holding_several_components(self, mock_plot, component_scaling_data):
        """The ACCESS-OM3 ESMF summary, in miniature: one log, and each group read against its own cores."""

        study, _ = component_scaling_data
        study.plot_component_scaling_data(
            [
                RegionGroup("component", ["Region 1"], component="component"),
                RegionGroup("component", ["Region 2"], component="other"),
            ],
            tavg,
        )

        groups = mock_plot.call_args.args[0]
        assert len(groups) == 2, "both survive, one log or two"
        assert list(groups[0]["ncpus"].values) == [60, 120, 240]
        assert list(groups[1]["ncpus"].values) == [30, 60, 120], "the other component's cores"

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_one_component_spread_over_several_logs(self, mock_plot, component_scaling_data):
        """As the ACCESS-ESM1.6 atmosphere is over its two logs: same cores, different regions."""

        study, cores = component_scaling_data
        for exp_name in study.data:
            study.data[exp_name]["another_log"] = study.data[exp_name]["component"]

        study.plot_component_scaling_data(
            [
                RegionGroup("component", ["Region 1"], component="component"),
                RegionGroup("another_log", ["Region 2"], component="component"),
            ],
            tavg,
        )

        groups = mock_plot.call_args.args[0]
        assert len(groups) == 2
        assert all(list(stat["ncpus"].values) == sorted(cores) for stat in groups), "one component, one axis"

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_a_group_naming_no_component_falls_back(self, mock_plot, component_scaling_data):
        """First to what the application associates with the log, and failing that to the log's own name."""

        study, cores = component_scaling_data
        study.application.log_components = {"a_log": "other"}
        for exp_name in study.data:
            study.data[exp_name]["a_log"] = study.data[exp_name]["component"]

        study.plot_component_scaling_data(
            [RegionGroup("a_log", ["Region 1"]), RegionGroup("component", ["Region 1"])], tavg
        )

        groups = mock_plot.call_args.args[0]
        assert list(groups[0]["ncpus"].values) == [30, 60, 120], "the mapping named 'other'"
        assert list(groups[1]["ncpus"].values) == sorted(cores), "nothing named it, so the log's own name"

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_the_axis_labels_are_passed_on(self, mock_plot, component_scaling_data):
        """Both of them, so a caller can name the axes of a plot this method builds for them."""

        study, _ = component_scaling_data
        study.plot_component_scaling_data(
            [RegionGroup("component", ["Region 1"])], tavg, xlabel="Ocean cores", ylabel="Ocean walltime"
        )

        assert mock_plot.call_args.kwargs["xlabel"] == "Ocean cores"
        assert mock_plot.call_args.kwargs["ylabel"] == "Ocean walltime"

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_no_axis_labels_are_given_by_default(self, mock_plot, component_scaling_data):
        study, _ = component_scaling_data
        study.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)

        assert mock_plot.call_args.kwargs["xlabel"] is None
        assert mock_plot.call_args.kwargs["ylabel"] is None

    def test_an_experiment_whose_layout_cannot_be_told(self, component_scaling_data):
        study, _ = component_scaling_data
        study.experiments["small"].layout = None

        with pytest.raises(ValueError, match="layout of experiment 'small' could not be read"):
            study.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)

    def test_a_layout_naming_no_such_component(self, component_scaling_data):
        study, _ = component_scaling_data

        with pytest.raises(ValueError, match="holds no component 'absent'"):
            study.plot_component_scaling_data([RegionGroup("component", ["Region 1"], component="absent")], tavg)

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_two_experiments_giving_the_component_the_same_cores(self, mock_plot, component_scaling_data):
        """A layout says what share a component had, not how big the job around it was, so this happens."""

        study, _ = component_scaling_data
        study.experiments["large"].layout = layout_with(component=60, other=240)

        study.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)

        (stat,) = mock_plot.call_args.args[0]
        assert list(stat["ncpus"].values) == [60, 120], "the two at 60 cores become one point"
        assert stat[tavg].pint.dequantify().values.tolist() == [[25.0], [50.0]], "and it is the faster of them"

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_the_fastest_is_taken_region_by_region(self, mock_plot, component_scaling_data):
        """So a point can take its value for one region from one run and for another from another."""

        study, _ = component_scaling_data
        study.experiments["large"].layout = layout_with(component=60, other=240)
        # 'large' is the faster of the two on Region 1 and the slower on Region 2.
        study.data["large"] = make_component_dataset([25.0, 99.0])

        study.plot_component_scaling_data([RegionGroup("component", ["Region 1", "Region 2"])], tavg)

        (stat,) = mock_plot.call_args.args[0]
        at_sixty = stat[tavg].sel(ncpus=60).pint.dequantify().values.tolist()
        assert at_sixty == [25.0, 10.0], "Region 1 from 'large', Region 2 from 'small'"

    def test_the_experiments_sharing_a_core_count_are_reported(self, component_scaling_data, caplog):
        """A point that is the best of several is not the only measurement there was, and says so."""

        study, _ = component_scaling_data
        study.experiments["large"].layout = layout_with(component=60, other=240)

        with caplog.at_level(logging.INFO):
            study.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg, show=False)

        assert "['large', 'small']" in caplog.text
        assert "60 cores" in caplog.text

    @mock.patch("access.profiling.results.plot_component_scaling")
    def test_a_colliding_experiment_can_be_left_out(self, mock_plot, component_scaling_data):
        study, _ = component_scaling_data
        study.experiments["large"].layout = layout_with(component=60, other=240)

        study.plot_component_scaling_data(
            [RegionGroup("component", ["Region 1"])], tavg, experiments=["small", "medium"]
        )
        assert self._plotted(mock_plot) == [[60, 120]]

    def test_no_experiments_selected(self, component_scaling_data):
        study, _ = component_scaling_data
        with pytest.raises(ValueError, match="No experiments selected"):
            study.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg, experiments=[])

    def test_an_experiment_with_no_parsed_data(self, component_scaling_data):
        study, _ = component_scaling_data
        with pytest.raises(ValueError, match="No parsed profiling data"):
            study.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg, experiments=["nothing"])

    def test_a_log_with_no_parsed_data(self, component_scaling_data):
        study, _ = component_scaling_data
        with pytest.raises(ValueError, match="No profiling data found for 'absent'"):
            study.plot_component_scaling_data([RegionGroup("absent", ["Region 1"])], tavg)

    def test_a_region_that_is_not_there(self, component_scaling_data):
        study, _ = component_scaling_data
        with pytest.raises(ValueError, match="Regions \\['Region 3'\\] not found"):
            study.plot_component_scaling_data([RegionGroup("component", ["Region 3"])], tavg)

    def test_data_still_holding_several_runs(self, component_scaling_data):
        study, _ = component_scaling_data
        study.data["small"]["component"] = study.data["small"]["component"].expand_dims({"run": [1]})

        with pytest.raises(ValueError, match="'run' dimension"):
            study.plot_component_scaling_data([RegionGroup("component", ["Region 1"])], tavg)


class TestOneListDrivesEveryPlot:
    """The point of the type: a study is described once and handed to all four plotting methods."""

    @staticmethod
    def _groups() -> list[RegionGroup]:
        return [RegionGroup("component", ["Region 1", "Region 2"], component="component", relabel={"Region 1": "Dyn"})]

    @mock.patch("access.profiling.results.plot_line_metrics")
    @mock.patch("access.profiling.results.plot_bar_metrics")
    @mock.patch("access.profiling.results.plot_component_scaling")
    @mock.patch("access.profiling.results.plot_scaling_metrics")
    def test_the_same_groups_label_every_figure_alike(
        self, mock_scaling, mock_component, mock_bar, mock_line, scaling_data
    ):
        """A reader moving between the four figures reads one set of names."""

        paths, ncpus, datasets = scaling_data
        study = ParsedStudy(paths, ncpus, datasets)
        for path, n in zip(paths, ncpus, strict=True):
            study.experiments[path.name].layout = layout_with(component=n)
        groups = self._groups()

        study.plot_scaling_data(groups, tavg)
        study.plot_component_scaling_data(groups, tavg)
        study.plot_bar_chart(groups, tavg)
        study.plot_metric(groups, tavg, component_cores("component"))

        expected = ["Dyn", "component: Region 2"]
        assert list(mock_scaling.call_args.args[0][0]["region"].values) == expected
        assert list(mock_component.call_args.args[0][0]["region"].values) == expected
        assert mock_bar.call_args.args[1] == expected
        assert [label for label, _, _ in mock_line.call_args.args[0]] == expected

    @mock.patch("access.profiling.results.plot_scaling_metrics")
    def test_the_component_is_ignored_where_it_cannot_be_honoured(self, mock_plot, scaling_data):
        """plot_scaling_data reads the whole job, so a group naming a component it could not find still plots."""

        paths, ncpus, datasets = scaling_data
        study = ParsedStudy(paths, ncpus, datasets)

        study.plot_scaling_data([RegionGroup("component", ["Region 1"], component="no such component")], tavg)

        assert list(mock_plot.call_args.args[0][0]["ncpus"].values) == sorted(ncpus)


BLOCK_STUDY_GROUP = RegionGroup("component", ["Region 1", "Region 2"], relabel={"Region 1": "Dynamics"})


@pytest.fixture()
def block_study():
    """Parsed data for four experiments that differ in the cores of a component and in a label.

    | Experiment | `component` cores | `other` cores | `blocks` | Region 1 | Region 2 |
    |---|---|---|---|---|---|
    | exp-0001 | 120 | 60 | 4 | 50 s | 9 s |
    | exp-0002 | 60 | 30 | 4 | 100 s | 10 s |
    | exp-0003 | 120 | 60 | 8 | 40 s | 11 s |
    | exp-0004 | 60 | 30 | 8 | 80 s | 12 s |

    The cores are out of order, so that a plot has to sort them. Of two experiments on the same cores, neither is
    the faster on both regions.
    """
    cores = [120, 60, 120, 60]
    blocks = [4, 4, 8, 8]
    times = [[50.0, 9.0], [100.0, 10.0], [40.0, 11.0], [80.0, 12.0]]
    paths = [Path(f"exp-000{number}") for number in (1, 2, 3, 4)]
    study = ParsedStudy(
        paths, ncpus=[2 * n for n in cores], datasets=[make_component_dataset(values) for values in times]
    )
    for path, n, size in zip(paths, cores, blocks, strict=True):
        study.experiments[path.name].layout = layout_with(component=n, other=n // 2)
        study.experiments[path.name].labels = {"blocks": size}
    return study


def add_per_pe_log(study):
    """Gives every experiment a log with data for each of three PEs, which has more than one value for each region."""

    per_pe = xr.DataArray([[1.0, 2.0, 3.0]], dims=["region", "pe"]).pint.quantify(tavg.units)
    for data in study.data.values():
        data["per_pe"] = xr.Dataset(data_vars={tavg: per_pe}, coords={"region": ["Region 1"], "pe": [0, 1, 2]})


class TestGather:
    """One dataset for several experiments, with what tells them apart as coordinates."""

    def test_the_experiments_are_put_along_a_dimension_of_their_names(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP)

        assert dict(gathered.sizes) == {"experiment": 4, "region": 2}
        assert list(gathered["experiment"].values) == ["exp-0001", "exp-0002", "exp-0003", "exp-0004"]
        assert set(gathered.coords) == {"experiment", "region"}, "nothing more unless it is asked for"

    def test_the_regions_carry_the_labels_of_the_group(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP)

        assert list(gathered["region"].values) == ["Dynamics", "component: Region 2"]

    def test_the_metrics_keep_their_values_and_their_units(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP)

        times = gathered[tavg].sel(region="Dynamics")
        assert times.pint.units == tavg.units
        assert times.pint.dequantify().values.tolist() == [50.0, 100.0, 40.0, 80.0]
        assert gathered[count].sel(experiment="exp-0003").pint.dequantify().values.tolist() == [1, 2]

    def test_a_label_is_a_coordinate_named_by_its_key(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP, ["blocks"])

        assert gathered["blocks"].dims == ("experiment",)
        assert gathered["blocks"].values.tolist() == [4, 4, 8, 8]

    def test_a_quantity_of_the_layout_is_a_coordinate_with_its_own_name(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP, [component_cores("other"), "blocks"])

        assert gathered["other cores"].dims == ("experiment",)
        assert gathered["other cores"].values.tolist() == [60, 30, 60, 30]
        assert gathered["blocks"].values.tolist() == [4, 4, 8, 8]

    def test_the_experiments_can_be_looked_up_by_a_coordinate(self, block_study):
        """Which is what the coordinates are for: the names say nothing."""

        gathered = block_study.gather(BLOCK_STUDY_GROUP, [component_cores("component"), "blocks"])

        wide = gathered.where((gathered["component cores"] == 120) & (gathered["blocks"] == 8), drop=True)
        assert list(wide["experiment"].values) == ["exp-0003"]

    def test_only_the_experiments_asked_for_in_the_order_they_are_given(self, block_study):
        gathered = block_study.gather(BLOCK_STUDY_GROUP, ["blocks"], experiments=["exp-0004", "exp-0001"])

        assert list(gathered["experiment"].values) == ["exp-0004", "exp-0001"]
        assert gathered["blocks"].values.tolist() == [8, 4]
        assert gathered[tavg].sel(region="Dynamics").pint.dequantify().values.tolist() == [80.0, 50.0]

    def test_a_name_given_twice_is_gathered_once(self, block_study):
        """As two selections put together give it: an experiment is one entry along the dimension."""

        gathered = block_study.gather(BLOCK_STUDY_GROUP, ["blocks"], experiments=["exp-0003", "exp-0001", "exp-0003"])

        assert list(gathered["experiment"].values) == ["exp-0003", "exp-0001"]
        assert gathered["blocks"].values.tolist() == [8, 4]

    def test_a_value_that_is_a_sequence_stays_one_value(self, block_study):
        """A process grid is one value of an experiment, not a row of a table of its own."""

        shape = ExperimentCoordinate("shape", lambda layout: (layout.n_cores, 1))
        gathered = block_study.gather(BLOCK_STUDY_GROUP, [shape])

        assert gathered["shape"].dims == ("experiment",)
        assert gathered["shape"].values.tolist() == [(180, 1), (90, 1), (180, 1), (90, 1)]

    def test_values_of_different_kinds_are_kept_as_they_are(self, block_study):
        """Not all turned into strings, which is what putting them in one array does by default."""

        block_study.experiments["exp-0001"].labels = {"blocks": "5-8"}
        gathered = block_study.gather(BLOCK_STUDY_GROUP, ["blocks"])

        assert gathered["blocks"].values.tolist() == ["5-8", 4, 8, 8]

    def test_the_component_of_the_group_is_ignored(self, block_study):
        group = RegionGroup("component", ["Region 1"], component="no such component")

        assert dict(block_study.gather(group).sizes) == {"experiment": 4, "region": 1}

    def test_experiments_of_different_sizes_keep_all_their_pes(self, block_study):
        """Data for each PE has as many PEs as the experiment has ranks, and none of them is dropped."""

        for name, n_pes in (("exp-0001", 3), ("exp-0002", 2)):
            per_pe = xr.DataArray([[1.0] * n_pes], dims=["region", "pe"]).pint.quantify(tavg.units)
            block_study.data[name]["per_pe"] = xr.Dataset(
                data_vars={tavg: per_pe}, coords={"region": ["Region 1"], "pe": list(range(n_pes))}
            )

        gathered = block_study.gather(RegionGroup("per_pe", ["Region 1"]), experiments=["exp-0001", "exp-0002"])

        assert dict(gathered.sizes) == {"experiment": 2, "region": 1, "pe": 3}
        missing = gathered[tavg].pint.dequantify().isnull()
        assert missing.sel(region="per_pe: Region 1").values.tolist() == [[False, False, False], [False, False, True]]

    def test_an_experiment_without_the_label(self, block_study):
        block_study.experiments["exp-0002"].labels = {"distribution": "roundrobin"}

        with pytest.raises(
            ValueError, match="Experiment 'exp-0002' has no label 'blocks'. Its labels are \\['distribution'\\]"
        ):
            block_study.gather(BLOCK_STUDY_GROUP, ["blocks"])

    @pytest.mark.parametrize("value", [[5, 8], {"from": 5, "to": 8}])
    def test_a_label_that_is_a_list_or_a_mapping(self, block_study, value):
        """Experiments are grouped by the values of a coordinate, and neither of these can be grouped by."""

        block_study.experiments["exp-0003"].labels = {"blocks": value}

        with pytest.raises(ValueError, match="The label 'blocks' of experiment 'exp-0003' is .* cannot be a coord"):
            block_study.gather(BLOCK_STUDY_GROUP, ["blocks"])

    def test_an_experiment_whose_layout_cannot_be_told(self, block_study):
        block_study.experiments["exp-0003"].layout = None

        with pytest.raises(
            ValueError, match="layout of experiment 'exp-0003' could not be read, .* read 'component cores' from"
        ):
            block_study.gather(BLOCK_STUDY_GROUP, [component_cores("component")])

    def test_a_layout_without_the_quantity_names_the_experiment(self, block_study):
        """The coordinate is handed a layout and nothing else, so the results say whose layout it was."""

        with pytest.raises(
            ValueError, match="Experiment 'exp-0001' has no 'absent cores'. The layout holds no component 'absent'"
        ):
            block_study.gather(BLOCK_STUDY_GROUP, [component_cores("absent")])

    def test_two_coordinates_with_the_same_name(self, block_study):
        again = ExperimentCoordinate("blocks", lambda layout: layout.n_cores)

        with pytest.raises(ValueError, match="The coordinates \\['blocks'\\] are each named more than once"):
            block_study.gather(BLOCK_STUDY_GROUP, ["blocks", again])

    @pytest.mark.parametrize("name", ["experiment", "region"])
    def test_a_coordinate_named_as_one_the_data_has_of_its_own(self, block_study, name):
        with pytest.raises(ValueError, match=f"The coordinates \\['{name}'\\] are each named more than once"):
            block_study.gather(BLOCK_STUDY_GROUP, [ExperimentCoordinate(name, lambda layout: layout.n_cores)])

    def test_no_experiments_selected(self, block_study):
        with pytest.raises(ValueError, match="No experiments selected"):
            block_study.gather(BLOCK_STUDY_GROUP, experiments=[])

    def test_a_region_that_is_not_there(self, block_study):
        with pytest.raises(ValueError, match="Regions \\['Region 3'\\] not found"):
            block_study.gather(RegionGroup("component", ["Region 3"]))

    def test_data_still_holding_several_runs(self, block_study):
        """The message names the experiment, and the methods that reduce its runs."""

        data = block_study.data["exp-0001"]
        data["component"] = data["component"].expand_dims({"run": [1]})

        with pytest.raises(ValueError, match="\\['exp-0001'\\] still has a 'run' dimension.*select_best_run"):
            block_study.gather(BLOCK_STUDY_GROUP)


class TestGatherAfterAReduction:
    """The runs of an experiment are reduced by the results, which a table or a dataset is then gathered from."""

    def test_each_region_is_reduced_on_its_own(self, run_data):
        gathered = run_data.aggregate_runs("max").gather(RegionGroup("component", ["Region 1", "Region 2"]))

        assert "run" not in gathered.dims
        # The slowest run differs between the regions: run 3 for the first, run 7 for the second.
        assert gathered[tavg].sel(experiment="exp1").pint.dequantify().values.tolist() == [600.0, 9.0]

    def test_an_experiment_that_ran_once_is_taken_as_it_is(self, block_study):
        reduced = block_study.aggregate_runs("mean").gather(BLOCK_STUDY_GROUP, ["blocks"])

        xr.testing.assert_identical(reduced, block_study.gather(BLOCK_STUDY_GROUP, ["blocks"]))


class TestTable:
    """A metric of the experiments as a table, with a row for each value of one coordinate and a column for each
    value of another."""

    # The regions of BLOCK_STUDY_GROUP the other way round, so that their labels are not in sorted order.
    REVERSED_GROUP = RegionGroup("component", ["Region 2", "Region 1"], relabel={"Region 1": "Dynamics"})

    def test_the_rows_are_sorted_and_the_columns_follow_the_regions(self, block_study):
        table = block_study.table(self.REVERSED_GROUP, tavg, component_cores("component"), "blocks")

        # The experiments give 120 cores first, but the rows are sorted, as plot_metric sorts its x-axis.
        assert table.index.tolist() == [60, 120]
        assert table.index.name == "component cores"
        # The regions are in the order of the group, which is not the order of their labels.
        assert table.columns.tolist() == [
            ("component: Region 2", 4),
            ("component: Region 2", 8),
            ("Dynamics", 4),
            ("Dynamics", 8),
        ]
        assert table.columns.names == ["region", "blocks"]
        assert table["Dynamics"].values.tolist() == [[100.0, 80.0], [50.0, 40.0]]

    def test_the_values_of_the_columns_are_in_the_order_the_experiments_give_them(self, block_study):
        """As plot_metric orders its series: sorting would put a label such as "17" before "9-16"."""

        for name, label in (("exp-0001", "9-16"), ("exp-0002", "9-16"), ("exp-0003", "17"), ("exp-0004", "17")):
            block_study.experiments[name].labels = {"blocks": label}

        table = block_study.table(BLOCK_STUDY_GROUP, tavg, component_cores("component"), "blocks")

        assert table["Dynamics"].columns.tolist() == ["9-16", "17"]

    def test_without_a_second_coordinate_the_columns_are_the_regions(self, block_study):
        table = block_study.table(
            self.REVERSED_GROUP, tavg, component_cores("component"), experiments=["exp-0001", "exp-0002"]
        )

        assert table.columns.tolist() == ["component: Region 2", "Dynamics"]
        assert table.columns.name == "region"
        assert table.values.tolist() == [[10.0, 100.0], [9.0, 50.0]]

    def test_the_values_are_in_the_units_of_the_metric(self, block_study):
        """A table holds numbers, not quantities, so it says what the numbers are in."""

        table = block_study.table(BLOCK_STUDY_GROUP, tavg, component_cores("component"), "blocks")

        assert table.attrs["units"] == str(tavg.units)

    def test_a_pair_of_values_no_experiment_has_is_left_empty(self, block_study):
        table = block_study.table(
            BLOCK_STUDY_GROUP,
            tavg,
            component_cores("component"),
            "blocks",
            experiments=["exp-0001", "exp-0002", "exp-0003"],
        )

        assert math.isnan(table.loc[60, ("Dynamics", 8)])
        assert table.loc[120, ("Dynamics", 8)] == 40.0

    def test_a_table_of_reduced_runs(self):
        study = ParsedStudy([Path("exp1")], ncpus=[4], datasets=[{"component": runs_of([[4.0, 1.0], [6.0, 1.0]])}])
        study.experiments["exp1"].labels = {"blocks": 4}

        table = study.aggregate_runs("max").table(RegionGroup("component", ["Region 1"]), tavg, "blocks")

        assert table.values.tolist() == [[6.0]]

    def test_experiments_sharing_the_values_of_both_coordinates(self, block_study):
        """Two experiments in one cell of the table: rather than pick one, the table names them."""

        with pytest.raises(
            ValueError, match="The experiments \\['exp-0001', 'exp-0002'\\] and \\['exp-0003', 'exp-0004'\\] share"
        ):
            block_study.table(BLOCK_STUDY_GROUP, tavg, "blocks")

    def test_experiments_sharing_a_null_value(self, block_study):
        """A null label is a value like any other, which pandas leaves out of its groups by default."""

        for name in ("exp-0001", "exp-0003"):
            block_study.experiments[name].labels = {"blocks": None}

        with pytest.raises(ValueError, match="The experiments \\['exp-0001', 'exp-0003'\\] share their values"):
            block_study.table(BLOCK_STUDY_GROUP, tavg, "blocks")

    def test_two_regions_labelled_alike(self, block_study):
        """The plots draw them as two lines, but a table would give them one column, and blame the experiments."""

        group = RegionGroup("component", ["Region 1", "Region 2"], relabel={"Region 1": "X", "Region 2": "X"})

        with pytest.raises(ValueError, match="The group gives the same label to several of its regions: \\['X'\\]"):
            block_study.table(group, tavg, "blocks", component_cores("component"))

    def test_a_coordinate_named_value(self, block_study):
        """The name a table would give its column of values, were that column not keyed by the metric."""

        for experiment in block_study.experiments.values():
            experiment.labels = {"value": experiment.labels["blocks"]}

        table = block_study.table(BLOCK_STUDY_GROUP, tavg, "value", component_cores("component"))

        assert table.index.name == "value"
        assert table["Dynamics"].values.tolist() == [[50.0, 100.0], [40.0, 80.0]]

    def test_a_metric_with_more_than_one_value_for_each_region(self, block_study):
        for name, n_pes in (("exp-0001", 3), ("exp-0002", 2)):
            per_pe = xr.DataArray([[1.0] * n_pes], dims=["region", "pe"]).pint.quantify(tavg.units)
            block_study.data[name]["per_pe"] = xr.Dataset(
                data_vars={tavg: per_pe}, coords={"region": ["Region 1"], "pe": list(range(n_pes))}
            )

        with pytest.raises(ValueError, match="has the dimensions \\['pe'\\] besides the experiments and the regions"):
            block_study.table(
                RegionGroup("per_pe", ["Region 1"]),
                tavg,
                component_cores("component"),
                experiments=["exp-0001", "exp-0002"],
            )


class TestSelectExperiments:
    """The experiments with one value of a coordinate, picked out by it rather than by their names."""

    def test_by_a_label(self, block_study):
        assert block_study.select_experiments("blocks", 8) == ["exp-0003", "exp-0004"]

    def test_by_a_quantity_of_the_layout(self, block_study):
        assert block_study.select_experiments(component_cores("component"), 60) == ["exp-0002", "exp-0004"]

    def test_among_the_experiments_given_in_their_order(self, block_study):
        selected = block_study.select_experiments("blocks", 4, experiments=["exp-0004", "exp-0002", "exp-0001"])

        assert selected == ["exp-0002", "exp-0001"]

    def test_a_value_no_experiment_has(self, block_study):
        assert block_study.select_experiments("blocks", 16) == []

    def test_an_experiment_without_the_label_is_not_one_of_them(self, block_study):
        """As in a study of two kinds of choice, each labelled by keys of its own."""

        block_study.experiments["exp-0002"].labels = {"distribution": "roundrobin"}

        assert block_study.select_experiments("blocks", 4) == ["exp-0001"]

    def test_a_label_none_of_them_has(self, block_study):
        """Which is a misspelt key, rather than a value none of them has."""

        with pytest.raises(
            ValueError, match="None of the experiments \\[.*'exp-0004'\\] has a label 'block'. Their labels are"
        ):
            block_study.select_experiments("block", 4)

    def test_nothing_to_select_from(self, block_study):
        assert block_study.select_experiments("blocks", 4, experiments=[]) == []

    def test_an_experiment_the_results_do_not_hold(self, block_study):
        with pytest.raises(ValueError, match="Experiments \\['nope'\\] are not in these results. Available"):
            block_study.select_experiments("blocks", 4, experiments=["exp-0001", "nope"])


class TestBarChartNamedByACoordinate:
    """The bars of each experiment named by a coordinate, as plot_metric names its series."""

    @mock.patch("access.profiling.results.plot_bar_metrics")
    def test_each_experiment_is_named_by_its_value(self, mock_plot, block_study):
        experiments = block_study.select_experiments(component_cores("component"), 120)

        block_study.plot_bar_chart([BLOCK_STUDY_GROUP], tavg, experiments=experiments, label="blocks", show=False)

        assert mock_plot.call_args.args[0] == {"blocks 4": [50.0, 9.0], "blocks 8": [40.0, 11.0]}

    def test_two_experiments_with_the_same_value(self, block_study):
        """Their bars would have one name, and one experiment would replace the other in the chart."""

        with pytest.raises(
            ValueError,
            match="the experiments \\['exp-0001', 'exp-0003'\\] would all be 'component cores 120'; "
            "\\['exp-0002', 'exp-0004'\\] would all be 'component cores 60'",
        ):
            block_study.plot_bar_chart([BLOCK_STUDY_GROUP], tavg, label=component_cores("component"), show=False)

    def test_a_relabel_map_as_well(self, block_study):
        with pytest.raises(ValueError, match="Give experiment_relabel_map or label, not both"):
            block_study.plot_bar_chart(
                [BLOCK_STUDY_GROUP], tavg, experiment_relabel_map={"exp-0001": "one"}, label="blocks", show=False
            )


class TestPlotMetric:
    """Plotting a metric against whatever the study varied, one line for each region and each series."""

    @staticmethod
    def _lines(mock_plot):
        """The lines handed to the plot: the label, the values along x and the values of the metric of each."""

        return [tuple(line) for line in mock_plot.call_args.args[0]]

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_each_region_is_a_line_against_the_coordinate(self, mock_plot, block_study):
        """In the order of the coordinate, whatever the order of the experiments."""

        block_study.plot_metric(
            [BLOCK_STUDY_GROUP], tavg, component_cores("component"), experiments=["exp-0001", "exp-0002"]
        )

        assert self._lines(mock_plot) == [
            ("Dynamics", [60, 120], [100.0, 50.0]),
            ("component: Region 2", [60, 120], [10.0, 9.0]),
        ]

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_a_label_can_go_along_the_x_axis(self, mock_plot, block_study):
        block_study.plot_metric(
            [RegionGroup("component", ["Region 1"])], tavg, "blocks", experiments=["exp-0004", "exp-0002"]
        )

        assert self._lines(mock_plot) == [("component: Region 1", [4, 8], [100.0, 80.0])]

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_every_group_is_plotted(self, mock_plot, block_study):
        groups = [RegionGroup("component", ["Region 2"]), RegionGroup("component", ["Region 1"])]
        block_study.plot_metric(groups, tavg, "blocks", experiments=["exp-0002", "exp-0004"])

        assert self._lines(mock_plot) == [
            ("component: Region 2", [4, 8], [10.0, 12.0]),
            ("component: Region 1", [4, 8], [100.0, 80.0]),
        ]

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_a_series_divides_each_region_into_lines(self, mock_plot, block_study):
        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks")

        assert self._lines(mock_plot) == [
            ("Dynamics, blocks 4", [60, 120], [100.0, 50.0]),
            ("Dynamics, blocks 8", [60, 120], [80.0, 40.0]),
            ("component: Region 2, blocks 4", [60, 120], [10.0, 9.0]),
            ("component: Region 2, blocks 8", [60, 120], [12.0, 11.0]),
        ]

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_the_series_come_in_the_order_the_experiments_give_them(self, mock_plot, block_study):
        block_study.plot_metric(
            [RegionGroup("component", ["Region 1"], relabel={"Region 1": "Dynamics"})],
            tavg,
            component_cores("component"),
            series="blocks",
            experiments=["exp-0003", "exp-0001", "exp-0002", "exp-0004"],
        )

        assert [label for label, _, _ in self._lines(mock_plot)] == ["Dynamics, blocks 8", "Dynamics, blocks 4"]

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_a_quantity_of_the_layout_can_be_the_series(self, mock_plot, block_study):
        block_study.plot_metric(
            [RegionGroup("component", ["Region 1"])], tavg, "blocks", series=component_cores("component")
        )

        assert self._lines(mock_plot) == [
            ("component: Region 1, component cores 120", [4, 8], [50.0, 40.0]),
            ("component: Region 1, component cores 60", [4, 8], [100.0, 80.0]),
        ]

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_experiments_sharing_a_point_become_the_fastest_of_them_region_by_region(self, mock_plot, block_study):
        """Without a series, two experiments are on each number of cores, and neither wins both regions."""

        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"))

        assert self._lines(mock_plot) == [
            ("Dynamics", [60, 120], [80.0, 40.0]),
            ("component: Region 2", [60, 120], [10.0, 9.0]),
        ]

    def test_the_experiments_sharing_a_point_are_reported(self, block_study, caplog):
        """A point that is the best of several is not the only measurement there was, and says so."""

        with caplog.at_level(logging.INFO):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"), show=False)

        assert "Experiments ['exp-0002', 'exp-0004'] all have 'component cores' 60. Plotting the fastest" in caplog.text
        assert "Experiments ['exp-0001', 'exp-0003'] all have 'component cores' 120. Plotting" in caplog.text

    def test_with_a_series_the_report_says_which_line_the_point_is_on(self, block_study, caplog):
        block_study.experiments["exp-0003"].labels = {"blocks": 4}

        with caplog.at_level(logging.INFO):
            block_study.plot_metric(
                [BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks", show=False
            )

        assert "Experiments ['exp-0001', 'exp-0003'] all have 'component cores' 120 and 'blocks' 4." in caplog.text
        assert caplog.text.count("Plotting the fastest of them") == 1, "no other point is shared"

    @pytest.mark.parametrize("order", [["exp-0001", "exp-0003"], ["exp-0003", "exp-0001"]])
    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_an_experiment_with_no_value_for_a_region_is_passed_over(self, mock_plot, block_study, order):
        """Whichever experiment comes first, as plot_component_scaling_data passes over it.

        A region that the run select_best_run kept does not have is NaN, the runs being put together with an outer
        join.
        """
        block_study.data["exp-0001"] = make_component_dataset([math.nan, 9.0])

        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"), experiments=order)

        assert self._lines(mock_plot) == [("Dynamics", [120], [40.0]), ("component: Region 2", [120], [9.0])]

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_a_point_where_no_experiment_has_a_value_is_nan(self, mock_plot, block_study):
        for name in ("exp-0001", "exp-0003"):
            block_study.data[name] = make_component_dataset([math.nan, 9.0])

        block_study.plot_metric([RegionGroup("component", ["Region 1"])], tavg, component_cores("component"))

        ((_, x_values, fastest),) = self._lines(mock_plot)
        assert x_values == [60, 120]
        assert fastest[0] == 80.0 and math.isnan(fastest[1])

    def test_a_metric_with_more_than_one_value_for_each_region(self, block_study):
        """As data for each PE has, rather than a TypeError from turning it into one number."""

        add_per_pe_log(block_study)

        with pytest.raises(ValueError, match="has the dimensions \\['pe'\\] .* more values than a line has points"):
            block_study.plot_metric([RegionGroup("per_pe", ["Region 1"])], tavg, "blocks")

    def test_nothing_is_reported_where_no_point_is_shared(self, block_study, caplog):
        with caplog.at_level(logging.INFO):
            block_study.plot_metric(
                [BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks", show=False
            )

        assert "Plotting the fastest of them" not in caplog.text

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_the_x_axis_is_labelled_with_the_name_of_the_coordinate(self, mock_plot, block_study):
        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks")
        assert mock_plot.call_args.kwargs["xlabel"] == "component cores"
        assert mock_plot.call_args.kwargs["ylabel"] is None

        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks", series=component_cores("component"))
        assert mock_plot.call_args.kwargs["xlabel"] == "blocks"

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_the_axis_labels_the_metric_and_show_are_passed_on(self, mock_plot, block_study):
        block_study.plot_metric(
            [BLOCK_STUDY_GROUP], tavg, "blocks", xlabel="Blocks per rank", ylabel="Walltime (s)", show=False
        )

        assert mock_plot.call_args.args[1] is tavg
        assert mock_plot.call_args.kwargs == {"xlabel": "Blocks per rank", "ylabel": "Walltime (s)", "show": False}

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_it_is_shown_by_default(self, mock_plot, block_study):
        block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks")

        assert mock_plot.call_args.kwargs["show"] is True

    def test_it_returns_the_figure_it_drew(self, block_study):
        fig = block_study.plot_metric(
            [BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks", show=False
        )

        (ax,) = fig.axes
        assert len(ax.lines) == 4
        assert ax.get_xlabel() == "component cores"

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_the_component_of_a_group_is_ignored(self, mock_plot, block_study):
        group = RegionGroup("component", ["Region 1"], component="no such component")
        block_study.plot_metric([group], tavg, "blocks", experiments=["exp-0001"])

        assert self._lines(mock_plot) == [("component: Region 1", [4], [50.0])]

    def test_a_value_of_x_that_is_not_a_number(self, block_study):
        """Lines join points in the order of their numbers, and a string has no place among them."""

        block_study.experiments["exp-0002"].labels = {"blocks": "5-8"}

        with pytest.raises(
            ValueError, match="Experiment 'exp-0002' has the value '5-8' for 'blocks', which is not a number"
        ):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks")

    def test_a_boolean_is_not_a_number_along_the_x_axis(self, block_study):
        """Python counts it as one, but true and false have no place along an axis."""

        block_study.experiments["exp-0002"].labels = {"blocks": True}

        with pytest.raises(ValueError, match="Experiment 'exp-0002' has the value True for 'blocks', which is not a"):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks")

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_the_series_are_told_apart_as_they_are_shown(self, mock_plot, block_study):
        """1, True and 1.0 are equal in Python, so keyed by their values they would be one line named after one."""

        for name, value in (("exp-0001", 1), ("exp-0002", True), ("exp-0003", 1.0)):
            block_study.experiments[name].labels = {"threads": value}

        block_study.plot_metric(
            [RegionGroup("component", ["Region 1"])],
            tavg,
            component_cores("component"),
            series="threads",
            experiments=["exp-0001", "exp-0002", "exp-0003"],
        )

        assert self._lines(mock_plot) == [
            ("component: Region 1, threads 1", [120], [50.0]),
            ("component: Region 1, threads True", [60], [100.0]),
            ("component: Region 1, threads 1.0", [120], [40.0]),
        ]

    def test_two_values_of_the_series_shown_alike(self, block_study):
        """Their lines would carry the same label."""

        block_study.experiments["exp-0001"].labels = {"threads": 1}
        block_study.experiments["exp-0002"].labels = {"threads": "1"}

        with pytest.raises(ValueError, match="The values 1 and '1' of 'threads' are both shown as '1'"):
            block_study.plot_metric(
                [BLOCK_STUDY_GROUP],
                tavg,
                component_cores("component"),
                series="threads",
                experiments=["exp-0001", "exp-0002"],
            )

    @mock.patch("access.profiling.results.plot_line_metrics")
    def test_a_series_need_not_be_a_number(self, mock_plot, block_study):
        for experiment in block_study.experiments.values():
            experiment.labels = {"distribution": "roundrobin"}
        block_study.plot_metric(
            [RegionGroup("component", ["Region 1"])],
            tavg,
            component_cores("component"),
            series="distribution",
            experiments=["exp-0001", "exp-0002"],
        )

        assert self._lines(mock_plot) == [("component: Region 1, distribution roundrobin", [60, 120], [100.0, 50.0])]

    def test_an_experiment_without_the_label_of_the_series(self, block_study):
        block_study.experiments["exp-0004"].labels = {}

        with pytest.raises(ValueError, match="Experiment 'exp-0004' has no label 'blocks'"):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, component_cores("component"), series="blocks")

    def test_a_log_with_no_parsed_data_is_said_before_anything_about_the_coordinates(self, block_study):
        with pytest.raises(ValueError, match="No profiling data found for 'absent'"):
            block_study.plot_metric([RegionGroup("absent", ["Region 1"])], tavg, "no such label")

    def test_no_experiments_selected(self, block_study):
        with pytest.raises(ValueError, match="No experiments selected"):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks", experiments=[])

    def test_data_still_holding_several_runs(self, block_study):
        """A plot takes no runs, so the message offers the methods that reduce them."""

        data = block_study.data["exp-0001"]
        data["component"] = data["component"].expand_dims({"run": [1]})

        with pytest.raises(ValueError, match="'run' dimension.*select_best_run\\(\\)"):
            block_study.plot_metric([BLOCK_STUDY_GROUP], tavg, "blocks")


class TestResults:
    """What parsing returns: the data of every experiment, with what tells it from the others, never changed."""

    def test_the_data_is_keyed_by_experiment_and_then_by_log(self, block_study):
        results = block_study.results()

        assert list(results.data) == ["exp-0001", "exp-0002", "exp-0003", "exp-0004"]
        assert list(results.data["exp-0001"]) == ["component"]

    def test_the_data_cannot_be_changed(self, block_study):
        results = block_study.results()

        with pytest.raises(TypeError, match="does not support item assignment"):
            results.data["exp-0001"]["other"] = results.data["exp-0001"]["component"]
        with pytest.raises(TypeError, match="does not support item assignment"):
            results.experiments["exp-0005"] = results.experiments["exp-0001"]

    def test_what_they_were_made_from_can_change_without_them(self, block_study):
        """A test, or a notebook, that goes on editing the dictionaries it passed changes nothing in them."""

        labels = {"blocks": [4, 8]}
        data = {"component": block_study.data["exp-0001"]["component"]}
        experiment = ParsedExperiment(data, labels)

        labels["blocks"].append(16)
        data["other"] = data["component"]

        assert experiment.labels == {"blocks": [4, 8]}
        assert list(experiment.data) == ["component"]

    def test_the_repr_names_each_experiment_with_its_logs_and_labels(self, block_study):
        results = ProfilingResults(
            block_study.application,
            {"exp-0001": ParsedExperiment({"component": block_study.data["exp-0001"]["component"]}, {"blocks": 4})},
            skipped={"exp-0002": "its status is RUNNING"},
        )

        assert repr(results) == (
            "<ProfilingResults of mock: 1 experiments>\n"
            "    'exp-0001': component, labels {'blocks': 4}\n"
            "    Not parsed:\n"
            "        'exp-0002': its status is RUNNING"
        )

    def test_the_repr_of_an_experiment_with_no_logs(self, block_study):
        results = ProfilingResults(block_study.application, {"exp-0001": ParsedExperiment({})})

        assert repr(results) == "<ProfilingResults of mock: 1 experiments>\n    'exp-0001': no logs"

    def test_results_holding_no_experiments_say_why(self, block_study):
        results = ProfilingResults(block_study.application, {}, skipped={"exp-0001": "its status is RUNNING"})

        with pytest.raises(
            ValueError,
            match="These results hold no experiments: .* found none. Parsing passed over "
            "\\{'exp-0001': 'its status is RUNNING'\\}",
        ):
            results.gather(BLOCK_STUDY_GROUP)

    def test_results_of_a_study_with_nothing_to_parse(self, block_study):
        with pytest.raises(ValueError, match="These results hold no experiments: .* found none\\.$"):
            ProfilingResults(block_study.application, {}).gather(BLOCK_STUDY_GROUP)

    def test_a_count_that_could_not_be_read_is_raised_with_the_experiment(self, layout_scaling_data):
        paths, ncpus, datasets = layout_scaling_data
        study = ParsedStudy(paths, ncpus, datasets)
        study.experiments["1cpu"].ncpus = None
        study.experiments["1cpu"].ncpus_error = "no job file"

        with pytest.raises(ValueError, match="The number of CPUs of experiment '1cpu' is not known: no job file"):
            study.select_best_experiments("component", "Region 1", tavg)

    def test_a_count_never_given_is_not_known(self, layout_scaling_data):
        paths, _, datasets = layout_scaling_data

        with pytest.raises(ValueError, match="The number of CPUs of experiment '2cpu_slow' is not known\\.$"):
            ParsedStudy(paths, datasets=datasets).select_best_experiments("component", "Region 1", tavg)

    def test_a_layout_that_could_not_be_read_is_raised_with_the_experiment(self, block_study):
        block_study.experiments["exp-0002"].layout = None
        block_study.experiments["exp-0002"].layout_error = "no rose-suite.conf"

        with pytest.raises(ValueError, match="The layout of experiment 'exp-0002' could not be read: no rose-suite"):
            block_study.gather(BLOCK_STUDY_GROUP, [component_cores("component")])


class TestSelectionsAreChecked:
    """A selection checks what it reads as the plots do, saying what there is instead of raising a bare KeyError."""

    def test_a_log_an_experiment_does_not_have(self, layout_scaling_data):
        paths, ncpus, datasets = layout_scaling_data

        with pytest.raises(ValueError, match="No profiling data found for 'absent' .* Its logs are \\['component'\\]"):
            ParsedStudy(paths, ncpus, datasets).select_best_experiments("absent", "Region 1", tavg)

    def test_a_region_the_log_does_not_have(self, layout_scaling_data):
        paths, ncpus, datasets = layout_scaling_data

        with pytest.raises(ValueError, match="Regions \\['absent'\\] not found .* Available regions"):
            ParsedStudy(paths, ncpus, datasets).select_best_experiments("component", "absent", tavg)

    def test_results_holding_no_experiments(self):
        with pytest.raises(ValueError, match="These results hold no experiments"):
            ParsedStudy([]).select_best_experiments("component", "Region 1", tavg)

    def test_an_experiment_the_results_do_not_hold(self, layout_scaling_data):
        paths, ncpus, datasets = layout_scaling_data

        with pytest.raises(ValueError, match="No parsed profiling data found for experiment\\(s\\): \\['nope'\\]"):
            ParsedStudy(paths, ncpus, datasets).select_best_experiments(
                "component", "Region 1", tavg, experiments=["nope"]
            )
