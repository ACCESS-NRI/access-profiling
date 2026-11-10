# Analysing profiling data

Once the experiments have finished, it is time to look at the results. In this tutorial we go through the steps that are
the same for every kind of study: reading the logs, choosing the regions to show, making the plots, and working with the
data directly. Each study guide then uses these steps for its own purpose: a [comparison of
experiments](comparing-experiments.md), a measurement of [run-to-run variability](run-to-run-variability.md), the search
for the [best parallelisation layout of a budget](best-layout.md), or a [scaling study](scaling-studies.md).

We use the `manager` of [Generating and running experiments](experiments.md), and the log and region names of the
ACCESS-ESM1.6 and ACCESS-OM3 presets, which [ACCESS models](../access/index.md) and
[Log formats](../access/log-formats.md) list.

## Parsing the logs

The first step is to read the logs. This is done with
[`parse_profiling_data`][access.profiling.manager.ProfilingManager.parse_profiling_data], which first updates the
statuses and then reads the logs of every DONE and ARCHIVED experiment:

```python
manager.parse_profiling_data()
```

The results are now in `manager.data`, with one dataset for each log of each experiment. Take a look at the logs
of one experiment, and at the regions of one of them:

```python
name = "esm1p6-layout_piControl_atm_18x14_mom_16x16_ice_12"
print(list(manager.data[name]))
print(manager.data[name]["MOM5"].region.values)
```

This prints the names of the logs, and then the regions of the MOM5 timers. An ACCESS-ESM1.6 experiment has five
logs: `payu`, Payu's timings of the whole job, and `UM`, `UM_Total_Walltime`, `MOM5` and `CICE5`, the timers of its
components. An ACCESS-OM3 experiment has `payu`, `MOM6`, `CICE6` and `ESMF`, each of the last three only if the run
wrote it. Note that an optional log that is missing or that cannot be read is skipped, and the reason is given in
an INFO message.

## Repeated runs

If an experiment ran several times, each of its datasets has a `run` dimension. The plots and the selection of
the best experiments do not accept such data, so you first have to choose what to do with the runs.
[Run-to-run variability](run-to-run-variability.md#reducing-the-runs) explains how to look at the differences
between the runs, and how to get one value per experiment.

## Choosing the regions

Before plotting, we need to say which regions we want to see. Each plotting method takes a list of
[`RegionGroup`][access.profiling.plotting_utils.RegionGroup]s. A group names a log and the regions to take from
it, and you can also give the regions nicer labels. These are the groups we use for the two models:

=== "ACCESS-ESM1.6"

    ```python
    from access.profiling import RegionGroup

    groups = [
        RegionGroup(
            "payu",
            ["payu_model_run_duration_seconds"],
            component="ACCESS-ESM1.6 piControl",
            relabel={"payu_model_run_duration_seconds": "Whole model"},
        ),
        RegionGroup("UM_Total_Walltime", ["um_total_walltime"], relabel={"um_total_walltime": "UM7"}),
        RegionGroup("MOM5", ["Total runtime"], relabel={"Total runtime": "MOM5"}),
        RegionGroup("CICE5", ["Total"], relabel={"Total": "CICE5"}),
    ]
    ```

=== "ACCESS-OM3"

    ```python
    from access.profiling import RegionGroup

    groups = [
        RegionGroup(
            "ESMF",
            ["[ensemble] RunPhase1"],
            component="ACCESS-OM3 MC-25km",
            relabel={"[ensemble] RunPhase1": "Whole model"},
        ),
        RegionGroup("ESMF", ["[OCN] RunPhase1"], component="ocn", relabel={"[OCN] RunPhase1": "MOM6"}),
        RegionGroup("ESMF", ["[ICE] RunPhase1"], component="ice", relabel={"[ICE] RunPhase1": "CICE6"}),
    ]
    ```

If you do not give a region a label, it is labelled `"<log>: <region>"`. The `component` is only used by the
component scaling plot, which plots each region against the cores of its component. Most logs belong to one
component, and the application already knows which one. You only need to give a `component` for the logs that
cover the whole model, such as Payu's timings and the ESMF summary here. For those, give either one of the
components, or the name of the whole model, which is `ACCESS-ESM1.6 piControl` or `ACCESS-OM3 MC-25km` here. In the
second case the regions are plotted against the cores of the whole job.

## The plots

The manager can make three kinds of plot. Each of them takes the groups, a metric and, optionally, the
experiments to include, and returns the Matplotlib figure:

| Method | What it plots | What it needs | Shown in |
|---|---|---|---|
| [`plot_scaling_data`][access.profiling.manager.ProfilingManager.plot_scaling_data] | The speedup and parallel efficiency of every region, against the cores the whole job occupied | A single experiment for every number of CPUs | [Scaling studies](scaling-studies.md) |
| [`plot_component_scaling_data`][access.profiling.manager.ProfilingManager.plot_component_scaling_data] | Every region against the cores of its own component | The layout of every experiment | [Scaling studies](scaling-studies.md) |
| [`plot_bar_chart`][access.profiling.manager.ProfilingManager.plot_bar_chart] | The regions side by side, with one bar per experiment | | [Best layout for a budget](best-layout.md), [Comparing experiments](comparing-experiments.md) |

Note that none of them accepts data that still has a `run` dimension.

## Saving the figures

Every plotting method returns its Matplotlib figure, and also shows it unless you pass `show=False`. In a script,
or to save a figure, use:

```python
from access.profiling.metrics import tmax

fig = manager.plot_bar_chart(groups, tmax, experiments=["<experiment>", "<another experiment>"], show=False)
fig.savefig("bar-chart.png", dpi=150, bbox_inches="tight")
```

The figure is saved as `bar-chart.png` in the current directory. Adapt the path to where you want to keep your
figures.

## Choosing a metric

Which metric should you plot? In the examples we use `tmax`, which is the time of the PE that spent longest in a region.
This is the time the rest of the run had to wait for. `tavg`, the mean over the PEs, tells you how much work a region
does instead. The difference between the two shows how unevenly the work is shared among the PEs. Logs that only give
one time per region, such as Payu's timings and the Cylc logs, store it as `tmax`. [Log
formats](../access/log-formats.md) lists which metrics each log of the ACCESS models provides.

## Working with the data directly

The plots do not cover everything, but the datasets in `manager.data` are ordinary xarray datasets, with units,
so you can compute anything else yourself. For example, this is how much longer the slowest PE spent in each
region of MOM5, compared with the average PE:

```python
from access.profiling.metrics import tavg, tmax

mom5 = manager.data[name]["MOM5"]
imbalance = (mom5[tmax] - mom5[tavg]) / mom5[tavg]
```

The functions that the scaling plots use are also available, in `access.profiling.scaling`. See
[Scaling studies](scaling-studies.md#computing-speedup-and-efficiency) for an example.
