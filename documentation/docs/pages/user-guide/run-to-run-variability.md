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

manager.parse_profiling_data()
name = "esm1p6-layout_piControl_atm_16x16_mom_16x15_ice_12"
runtime = manager.data[name]["payu"][tmax].sel(region="payu_model_run_duration_seconds")
print(runtime.run.values)
print(runtime.pint.to("minute").data)
```

This prints the run numbers, and then the run time of each run in minutes. Note that if only one of the runs
wrote a given log, that log has no `run` dimension. We can now see how much the timings of each experiment vary.
Here we take the difference between the slowest and the fastest run, relative to the fastest:

```python
for name, logs in manager.data.items():
    runtime = logs["payu"][tmax].sel(region="payu_model_run_duration_seconds")
    if "run" in runtime.dims:
        spread = (runtime.max("run") - runtime.min("run")) / runtime.min("run")
        print(f"{float(spread.pint.dequantify()):6.1%}  {name}")
```

This prints the spread of each experiment as a percentage, next to its name.

Be aware that the plots and `select_best_experiments` do not accept data with a `run` dimension. If you try, you
get this message:

```text
Profiling data still has a 'run' dimension. Use select_best_run() to keep the best run of each experiment, or aggregate_runs() to reduce over runs.
```

## Reducing the runs

As the message says, there are two ways of getting one value for each experiment:

- [`select_best_run`][access.profiling.manager.ProfilingManager.select_best_run] keeps the best run of every
  experiment, which is the run where a region you choose was fastest. The same run is kept for all the logs of
  the experiment, so the result is a run that really happened.
- [`aggregate_runs`][access.profiling.manager.ProfilingManager.aggregate_runs] computes the minimum, the mean or
  the median over the runs. This is done for each region separately, so the values of different regions may come
  from different runs. Use the minimum to estimate the best timings you can get, and the mean or the median for
  the typical ones. Note that integer metrics, such as the number of calls, become floats.

For example, to keep the run in which the whole model was fastest:

```python
manager.select_best_run("payu", "payu_model_run_duration_seconds", tmax)
```

Both methods are applied to all the experiments, and do not touch the datasets that have no `run` dimension.
Note that they replace the parsed data. If you want the individual runs back, call `parse_profiling_data` again.

## Rose suites

A Rose suite always counts as a single run. If you want to see how much the timings of a suite vary, make copies
of the suite, run them, register each copy as an experiment of its own, and compare them as in
[Comparing experiments](comparing-experiments.md).
