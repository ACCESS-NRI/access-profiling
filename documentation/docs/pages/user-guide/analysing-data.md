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
statuses and then reads the logs of every DONE and ARCHIVED experiment. It returns the results, a
[`ProfilingResults`][access.profiling.results.ProfilingResults], which we keep as `results`:

```python
results = manager.parse_profiling_data()
```

Printing `results` lists the experiments it holds, with their logs and labels. It also lists the experiments that were
not parsed, with the reason, such as a status of RUNNING. The results do not change once they are made. A method that
selects experiments or reduces their runs returns new results, and leaves `results` as it was. To read the experiments
that have finished since, parse them again. The datasets in `results.data` belong to the results, so copy a dataset
before you change it, for example with `ds.copy()`.

The data is in `results.data`, with one dataset for each log of each experiment. Take a look at the logs of one
experiment, and at the regions of one of them:

```python
name = "exp-0001"
print(list(results.data[name]))
print(results.data[name]["MOM5"].region.values)
```

This prints the names of the logs, and then the regions of the MOM5 timers. An ACCESS-ESM1.6 experiment has five
logs: `payu`, Payu's timings of the whole job, and `UM`, `UM_Total_Walltime`, `MOM5` and `CICE5`, the timers of its
components. An ACCESS-OM3 experiment has `payu`, `MOM6`, `CICE6` and `ESMF`, each of the last three only if the run
wrote it. Note that an optional log that is missing, or that is not in the format of its parser, is skipped with an
INFO message. An optional log that its parser fails on in any other way is skipped too, with a warning that gives the
reason.

## Repeated runs

If an experiment ran several times, each of its datasets has a `run` dimension. The plots and the selection of
the best experiments do not accept such data, so you first have to choose what to do with the runs.
[Run-to-run variability](run-to-run-variability.md#reducing-the-runs) explains how to look at the differences
between the runs, and how to get one value per experiment.

## Choosing the regions

Before plotting, we need to say which regions we want to see. Each table and each plot takes the regions to show,
each as a [`Region`][access.profiling.results.Region]. A region names a log and a region of that log, and you can
also give it a nicer label. To find the names, list the regions that a log holds, with the number of experiments
that have each:

```python
print(results.regions("MOM5"))
```

This prints a table with a row for each region of the MOM5 timers. These are the regions we use for the two models:

=== "ACCESS-ESM1.6"

    ```python
    from access.profiling import Region

    regions = [
        Region("payu", "payu_model_run_duration_seconds", label="Whole model", component="ACCESS-ESM1.6 piControl"),
        Region("UM_Total_Walltime", "um_total_walltime", label="UM7"),
        Region("MOM5", "Total runtime", label="MOM5"),
        Region("CICE5", "Total", label="CICE5"),
    ]
    ```

=== "ACCESS-OM3"

    ```python
    from access.profiling import Region

    regions = [
        Region("ESMF", "[ensemble] RunPhase1", label="Whole model", component="ACCESS-OM3 MC-25km"),
        Region("ESMF", "[OCN] RunPhase1", label="MOM6", component="ocn"),
        Region("ESMF", "[ICE] RunPhase1", label="CICE6", component="ice"),
    ]
    ```

If you do not give a region a label, it is labelled with its name. The regions of one table or one plot must have
labels of their own, so give a label to two regions of two logs that have the same name, such as two `Total`s.
The `component` is only used by the component scaling plot, which plots each region against the cores of its
component. Most logs belong to one component, and the application already knows which one. You only need to give a
`component` for the logs that cover the whole model, such as Payu's timings and the ESMF summary here. For those,
give either one of the components, or the name of the whole model, which is `ACCESS-ESM1.6 piControl` or
`ACCESS-OM3 MC-25km` here. In the second case the regions are plotted against the cores of the whole job.

## The plots

The results can make four kinds of plot. Each of them takes the regions, or a single region, and returns the
Matplotlib axes it drew on:

| Method | What it plots | What it needs | Shown in |
|---|---|---|---|
| [`plot_scaling_data`][access.profiling.results.ProfilingResults.plot_scaling_data] | The speedup and parallel efficiency of every region, against the cores the whole job occupied | A single experiment for every number of CPUs | [Scaling studies](scaling-studies.md) |
| [`plot_component_scaling_data`][access.profiling.results.ProfilingResults.plot_component_scaling_data] | Every region against the cores of its own component | The layout of every experiment | [Scaling studies](scaling-studies.md) |
| [`plot_bar_chart`][access.profiling.results.ProfilingResults.plot_bar_chart] | The regions side by side, with one bar per experiment | | [Best layout for a budget](best-layout.md), [Comparing experiments](comparing-experiments.md) |
| [`plot_metric`][access.profiling.results.ProfilingResults.plot_metric] | Every region against a coordinate of the experiments: one of their labels, a quantity of their layouts, or their number of CPUs | A number for every experiment, in its labels or its layout | [Configuration choices](configuration-choices.md#plotting-against-a-setting) |

Each plot shows the metric `tmax` unless you pass another as `metric`, as [Choosing a metric](#choosing-a-metric)
explains. Note that none of them accepts data that still has a `run` dimension.

Each plot shows all the experiments of the results. To show only some of them, select them first. The names of the
experiments are a coordinate, `"experiment"`, so this selects two of them by name:

```python
two = results.select_experiments("experiment", "exp-0001", "exp-0002")
```

[`select_experiments`][access.profiling.results.ProfilingResults.select_experiments] returns new results, which
hold only the experiments with one of the values you give. It selects by a label or by a quantity of the layout in
the same way, as [Configuration choices](configuration-choices.md#putting-the-experiments-in-a-table) shows.

## Customising and saving the figures

Every plotting method returns the Matplotlib axes it drew on, and shows nothing itself. In a notebook, the figure is
shown at the end of the cell. If the call is the last line of the cell, assign what it returns, or end the line with
a semicolon, so that the axes are not printed as well. The axes are what you change the figure with. For example,
this puts the times on a logarithmic scale and gives the chart a title:

```python
ax = two.plot_bar_chart(regions)
ax.set(yscale="log", title="Two experiments")
```

In a script, show the figure with `matplotlib.pyplot.show()`, or save it:

```python
ax.figure.savefig("bar-chart.png", dpi=150, bbox_inches="tight")
```

The figure is saved as `bar-chart.png` in the current directory. Adapt the path to where you want to keep your
figures.

A plot can also draw on axes of your own, given as `ax`. This is how two plots share a figure, here the first
region and the others side by side:

```python
import matplotlib.pyplot as plt

fig, (left, right) = plt.subplots(1, 2, figsize=(16, 6))
two.plot_bar_chart(regions[0], ax=left)
two.plot_bar_chart(regions[1:], ax=right);
```

The scaling plot is the exception. It draws the speedup, the efficiency and a table of the timings on a figure of
its own, and returns the axes of the speedup and of the efficiency.

## Choosing a metric

Which metric should you plot? The tables and the plots use `tmax` unless you pass another as `metric`. It is the time of
the PE that spent longest in a region, which is the time the rest of the run had to wait for. `tavg`, the mean over the
PEs, tells you how much work a region does instead. For example, `results.plot_bar_chart(regions, metric=tavg)` draws
the mean time. You import the metrics from `access.profiling.metrics`. The difference between the two shows how unevenly
the work is shared among the PEs. Logs that only give one time per region, such as Payu's timings and the Cylc logs,
store it as `tmax`. [Log formats](../access/log-formats.md) lists which metrics each log of the ACCESS models provides.

## Working with the data directly

The plots do not cover everything, but the datasets in `results.data` are ordinary xarray datasets, with units,
so you can compute anything else yourself. For example, this is how much longer the slowest PE spent in each
region of MOM5, compared with the average PE:

```python
from access.profiling.metrics import tavg, tmax

mom5 = results.data[name]["MOM5"]
imbalance = (mom5[tmax] - mom5[tavg]) / mom5[tavg]
```

To work with several experiments at once, use [`gather`][access.profiling.results.ProfilingResults.gather]. It
collects the regions you give, for every experiment, into a single dataset. Here we also ask for the number of
cores that each experiment gives MOM5, as a coordinate:

```python
from access.profiling import Region, component_cores

ocean = Region("MOM5", "Total runtime", label="MOM5")
gathered = results.gather(ocean, [component_cores("MOM5")])
print(gathered["MOM5 cores"].values)
print(gathered[tmax].sel(region="MOM5").pint.dequantify().values)
```

This prints the cores of MOM5 in every experiment, and then the time that MOM5 took in each of them. The
experiments are along a dimension named `experiment`.
[Configuration choices](configuration-choices.md#putting-the-experiments-in-a-table) shows the coordinates in more
detail.

The functions that the scaling plots use are also available, in `access.profiling.scaling`. See
[Scaling studies](scaling-studies.md#computing-speedup-and-efficiency) for an example.
