# Run-to-run variability

If you run the same experiment twice, it rarely takes exactly the same time. The job may land on different
nodes, and the load on the file systems and on the network changes all the time. So how much can you trust a
timing? In this tutorial we run every experiment of a study several times, look at how much the timings vary, and
then get one value per experiment. We use the
`manager` of [Generating and running experiments](experiments.md).

## Running every experiment several times

How to run an experiment several times depends on the workflow engine. With Payu, the manager runs every
experiment [`nruns`][access.profiling.payu_manager.PayuManager.nruns] times. The runs can either follow one
another, each continuing the simulation where the previous one stopped, or be repeats of the same run.
[Running an experiment several times](../access/payu.md#running-an-experiment-several-times) explains the
difference, and how to choose between the two. Here we ask for three runs:

```python
manager.nruns = 3
manager.run_experiments()
```

Every experiment then runs three times. Keep in mind that this also makes the study three times more expensive!
Be aware that you have to set `nruns` before the experiments run for the first time. The reason is that
`run_experiments` only submits the experiments that have not run and, with `retry_failed=True`, the ones that
failed.

## The runs in the data

When an experiment ran several times, each of its datasets has a `run` dimension. The runs are numbered as the
workflow engine numbers them. For Payu, this is 0 for `output000`, 1 for `output001`, and so on. Read the logs and
look at the runs of one experiment:

```python
from access.profiling.metrics import tmax

results = manager.parse_profiling_data()
name = "exp-0001"
runtime = results.data[name]["payu"][tmax].sel(region="payu_model_run_duration_seconds")
print(runtime.run.values)
print(runtime.pint.to("minute").data)
```

This prints the run numbers, and then the run time of each run in minutes. Note that if only one of the runs
wrote a given log, that log has no `run` dimension. We can now see how much the timings of each experiment vary.
Here we take the difference between the slowest and the fastest run, relative to the fastest:

```python
for name, logs in results.data.items():
    runtime = logs["payu"][tmax].sel(region="payu_model_run_duration_seconds")
    if "run" in runtime.dims:
        spread = (runtime.max("run") - runtime.min("run")) / runtime.min("run")
        print(f"{float(spread.pint.dequantify()):6.1%}  {name}")
```

This prints the spread of each experiment as a percentage, next to its name.

Be aware that the plots, the tables and `select_best_experiments` do not accept data with a `run` dimension. If you
try, you get a message like this one, which names the experiments that ran several times:

```text
The profiling data of experiments ['exp-0001', 'exp-0002'] still has a 'run' dimension, since they ran several times. Reduce it first: select_best_run() returns results with the best run of each experiment, aggregate_runs() results with each region reduced over the runs, and split_runs() results with each run an experiment of its own.
```

## Reducing the runs

As the message says, there are two ways of getting one value for each experiment, and a third, described in
[Comparing the runs](#comparing-the-runs), of making each run an experiment of its own:

- [`select_best_run`][access.profiling.results.ProfilingResults.select_best_run] keeps the best run of every
  experiment, which is the run where a region you choose was fastest. The same run is kept for all the logs of
  the experiment, so the result is a run that really happened.
- [`aggregate_runs`][access.profiling.results.ProfilingResults.aggregate_runs] computes the minimum, the maximum,
  the mean or the median over the runs. This is done for each region separately, so the values of different
  regions may come from different runs. Use the minimum to estimate the best timings you can get, the maximum for
  the worst ones, and the mean or the median for the typical ones. Note that integer metrics, such as the number of
  calls, become floats.

For example, to keep the run in which the whole model was fastest:

```python
from access.profiling import Region

runtime = Region("payu", "payu_model_run_duration_seconds", label="Whole model")
best = results.select_best_run(by=runtime)
```

The runs are ranked by `tmax`, unless you pass another `metric`.

Both methods return new results, for all the experiments, and keep the datasets that have no `run` dimension as
they are. They leave `results` as it was, so it still holds every run. This means that you can keep the runs, their
best and their mean side by side, and read each of them as often as you like.

## Measuring the spread of the runs

Since a reduction leaves the results it is called on as they were, the fastest and the slowest run of every
experiment come from the same results. For example, to collect both with
[`gather`][access.profiling.results.ProfilingResults.gather]:

```python
fastest = results.aggregate_runs("min").gather(runtime)[tmax]
slowest = results.aggregate_runs("max").gather(runtime)[tmax]
spread = (slowest - fastest) / fastest
```

Each region is reduced on its own, as `aggregate_runs` always does.
[Putting the experiments in a table](configuration-choices.md#putting-the-experiments-in-a-table) shows what
`gather` collects.

[`table`][access.profiling.results.ProfilingResults.table] gives the spread of every experiment as a table instead,
arranged by what tells the experiments apart. For example, by the cores of the sea ice of ACCESS-OM3:

```python
from access.profiling import component_cores

fastest = results.aggregate_runs("min").table(runtime, component_cores("ice"))
slowest = results.aggregate_runs("max").table(runtime, component_cores("ice"))
print(100 * (slowest - fastest) / fastest)
```

This prints the spread in percent, with a row for each number of cores of the sea ice.

## Comparing the runs

To compare the runs of an experiment with each other, make each run an experiment of its own, with
[`split_runs`][access.profiling.results.ProfilingResults.split_runs]:

```python
runs = results.split_runs()
print(list(runs.experiments))
```

This prints the names of the runs, such as `exp-0001 run 0`, `exp-0001 run 1` and so on. Each run keeps the labels,
the layout and the number of CPUs of its experiment, and gets a label of its own, `"run"`, with its number. A table
or a plot can then be arranged by the run, as by any other label. For example, this is a table with a row for each
run of the experiment `exp-0001`, and the mean, the spread and the extremes of its run time. We select the
experiment before splitting its runs:

```python
one = results.select_experiments("experiment", "exp-0001").split_runs()
table = one.table(runtime, "run")
print(table.describe())
```

The bar chart of a few runs puts them side by side, region by region:

```python
ax = one.select_experiments("run", 0, 1, 2).plot_bar_chart(runtime, label="run")
```

There are a few things to keep in mind:

- A log that only one of the runs wrote has no `run` dimension, so it is not known which run it belongs to. It is
  left out of every run.
- An experiment that ran once is kept as it is, without a label `"run"`.
- Like the reductions, `split_runs` returns new results, and leaves `results` as it was.

## Rose suites

A Rose suite always counts as a single run. If you want to see how much the timings of a suite vary, make copies
of the suite, run them, register each copy as an experiment of its own, and compare them as in
[Comparing experiments](comparing-experiments.md). The copies are made by hand, as in
[Suites prepared by hand](../access/cylc-rose-suites.md#suites-prepared-by-hand), since a study generates one
experiment for each configuration.
