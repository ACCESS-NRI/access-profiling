# Scaling studies

A scaling study runs an application on several numbers of cores, and measures how its run time falls as cores are
added, for the application as a whole and for each of its components. Its experiments are the layouts of several
sizes, generated as [Generating and running experiments](experiments.md) shows, whose example is a scaling study,
or Rose suites prepared by hand, as in [Cylc Rose suites](../access/cylc-rose-suites.md). In this tutorial we use
the `manager` of that page and the `groups` of [Choosing the regions](analysing-data.md#choosing-the-regions). The
figures are drawn from synthetic data, to show what each plot looks like and how to read it.

## The experiments of a scaling study

The sizes of the study are the `num_nodes_list` it is generated with: 1 to 8 nodes for ACCESS-ESM1.6 and 1 to 32
for ACCESS-OM3, in the example. Every size holds the layouts its allocation strategy admits, and a strategy whose
bounds are fractions of the total serves every size at once. Be aware that the layouts add up quickly, so count
them before generating the study, as [Counting the layouts](experiments.md#counting-the-layouts) shows.

## Selecting the best layouts

A study usually holds several layouts of the same size, which compete with each other, while the scaling plot
needs a single experiment for every number of cores.
[`select_best_experiments`][access.profiling.manager.ProfilingManager.select_best_experiments] keeps, for every
number of CPUs the experiments occupied, the experiment in which the given region of the given log took the least
time. We rank them by the time the whole model took:

=== "ACCESS-ESM1.6"

    ```python
    from access.profiling.metrics import tmax

    best = manager.select_best_experiments("payu", "payu_model_run_duration_seconds", tmax)
    ```

=== "ACCESS-OM3"

    ```python
    from access.profiling.metrics import tmax

    best = manager.select_best_experiments("ESMF", "[ensemble] RunPhase1", tmax)
    ```

`best` is a list of experiment names, ordered by the number of CPUs, which we pass to the plotting methods.
Ranking the layouts by a region that covers the whole run, as these do, keeps the layouts that made the
application as a whole fastest.

## Scaling against the whole job

[`plot_scaling_data`][access.profiling.manager.ProfilingManager.plot_scaling_data] plots the parallel speedup and
efficiency of every region against the number of CPUs the whole job occupied, the cores the scheduler charged for:

```python
fig = manager.plot_scaling_data(groups, tmax, experiments=best, xlabel="Cores occupied by the job")
```

<figure markdown="span">
  ![Two panels of speedup and parallel efficiency against cores occupied, with a table of timings](../../assets/figures/scaling.png)
  <figcaption>The scaling plot of a coupled model whose ocean and sea ice run concurrently. Synthetic data.</figcaption>
</figure>

The speedup at $n$ cores is $S(n) = T(n_0) / T(n)$, where $T$ is the value of the metric and $n_0$ the smallest
number of cores in the plot, and the dotted line is the ideal speedup $n / n_0$. The efficiency is
$E(n) = S(n)\,n_0 / n$, in percent, so the ideal is 100%. The table underneath gives the metric itself. In the
example, the sea ice keeps 70% efficiency at 16 times the cores, and the ocean, which the whole model waits for,
almost 90%.

Since every region is read against the whole job, a component's curve shows what the study pays for it rather than
how well the component itself scales. Note that speedup is only defined against a single experiment for every
number of cores, so the method refuses a selection holding two experiments of the same size: pass the output of
`select_best_experiments`.

## Scaling against each component's cores

[`plot_component_scaling_data`][access.profiling.manager.ProfilingManager.plot_component_scaling_data] plots every
region against the number of cores its own component was given:

```python
fig = manager.plot_component_scaling_data(groups, tmax, ylabel="Maximum time (s)")
```

<figure markdown="span">
  ![Log-log plot of time against the cores of each component, with straight ideal lines](../../assets/figures/component-scaling.png)
  <figcaption>The component scaling plot of the same model, with a region in which the ocean waits for the
  coupler. Synthetic data.</figcaption>
</figure>

Both axes are logarithmic, so perfect scaling is a straight line: each black line is the time a region would take
if every extra core were fully used, starting from its first point. How far a curve rises above its line is what
the extra cores did not buy. A region in which a component waits rather than works climbs instead. Why does the
ocean's wait for the coupler climb here? Given more cores, the ocean finishes its own work sooner, and waits longer
for the components that have not.

The cores of each component come from the layout of every experiment. Several experiments can give a component the
same number of cores, so this plot can use every experiment of a study, and keeps the fastest value at each core
count.

## Computing speedup and efficiency

The functions the scaling plots are built on, in `access.profiling.scaling`, work on any dataset with an `ncpus`
coordinate. `select_best_experiments` records the number of CPUs of every experiment it considers, so you can build
one from the best experiments like this:

```python
import xarray as xr

from access.profiling.scaling import parallel_efficiency

stats = xr.concat(
    [manager.data[name]["MOM5"].expand_dims(ncpus=[manager.experiments[name].ncpus]) for name in best],
    dim="ncpus",
)
efficiency = parallel_efficiency(stats, tmax)
print(efficiency.sel(region="Total runtime"))
```

This prints the parallel efficiency of MOM5's total runtime at every number of CPUs.

## Rose suites

The scaling plot works the same way for [Cylc Rose suites](../access/cylc-rose-suites.md), whose numbers of CPUs are
also those their jobs asked PBS for, as [Parsing and plotting](../access/cylc-rose-suites.md#parsing-and-plotting)
explains. The component scaling plot needs the layout of every experiment, which the package cannot read back from a
Rose suite.
