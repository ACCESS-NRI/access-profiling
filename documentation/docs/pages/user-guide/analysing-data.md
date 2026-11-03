# Analysing profiling data

Once experiments have finished, the manager parses their profiling logs, and selects, plots and compares what the
experiments measured. In this tutorial we go through what every kind of study shares: parsing the logs, choosing
the regions to show, the plots, and working with the data directly. Each study guide then puts these to work: a
[comparison of experiments](comparing-experiments.md), a measurement of
[run-to-run variability](run-to-run-variability.md), the search for the [best layout of a budget](best-layout.md),
or a [scaling study](scaling-studies.md).

We use the `manager` of [Generating and running experiments](experiments.md), and the log and region names of the
ACCESS-ESM1.6 and ACCESS-OM3 presets, which [ACCESS models](../access/index.md) and
[Log formats](../access/log-formats.md) list.

## Parsing the logs

[`parse_profiling_data`][access.profiling.manager.ProfilingManager.parse_profiling_data] first brings the statuses
up to date, then reads the logs of every DONE and ARCHIVED experiment. Call it once the experiments have finished:

```python
manager.parse_profiling_data()
```

The results are in `manager.data`, one dataset per experiment and log. Take a look at the logs of one experiment,
and at the regions of one of them:

```python
name = "esm1p6-layout_piControl_atm_18x14_mom_16x16_ice_12"
print(list(manager.data[name]))
print(manager.data[name]["MOM5"].region.values)
```

This prints the names of the logs, and then the regions of the MOM5 timers. An ACCESS-ESM1.6 experiment has five
logs: `payu`, Payu's timings of the whole job, and `UM`, `UM_Total_Walltime`, `MOM5` and `CICE5`, the timers of its
components. An ACCESS-OM3 experiment has `payu`, `MOM6`, `CICE6` and `ESMF`, each of the last three only if the run
wrote it. Note that a log that is optional and missing or unreadable is skipped, and the reason is logged at INFO.

## Repeated runs

An experiment run several times has a `run` dimension in each of its datasets, which the plots and the selection of
the best experiments refuse. [Run-to-run variability](run-to-run-variability.md#reducing-the-runs) explains how to
look at the spread of the runs, and how to reduce them to one value per experiment.

## Choosing the regions

Each plotting method takes a list of [`RegionGroup`][access.profiling.plotting_utils.RegionGroup]s, naming a log
and the regions to take from it, and optionally the labels to give them. These are the groups we use for the two
models:

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

A region not given a label is labelled `"<log>: <region>"`. `component` is only read by the component scaling
plot, which plots each region against the cores of its component. An application already says which component
each of its logs times, so only the logs it declares without one need it, such as Payu's timings and the ESMF
summary here. Give those either one of the components, or the name of the root of the application's component
tree, which is `ACCESS-ESM1.6 piControl` or `ACCESS-OM3 MC-25km` here, to plot them against the cores of the whole
job.

## The plots

The manager draws three plots. Each takes the groups, a metric and, optionally, the experiments to include, and
returns the Matplotlib figure:

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

## Choosing a metric

We use `tmax` in the examples, the time of the PE that spent longest in a region, which is the time the rest of the
run waited for. `tavg`, the mean over the PEs, measures the work a region does instead, and the difference between
the two measures how unevenly it is shared out. Logs that record a single time per region, such as Payu's timings
and the Cylc logs, store it as `tmax`. [Log formats](../access/log-formats.md) lists which metrics each log of the
ACCESS models provides.

## Working with the data directly

The datasets in `manager.data` are ordinary xarray datasets whose values carry their units, so you can compute
anything the plots do not cover from them. For example, this is how much longer than the average PE the slowest
one spent in each region of MOM5:

```python
from access.profiling.metrics import tavg, tmax

mom5 = manager.data[name]["MOM5"]
imbalance = (mom5[tmax] - mom5[tavg]) / mom5[tavg]
```

The functions the scaling plots are built on are in `access.profiling.scaling`, and
[Scaling studies](scaling-studies.md#computing-speedup-and-efficiency) shows how to use them on the data of a
study.
