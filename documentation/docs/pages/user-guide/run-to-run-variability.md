# Run-to-run variability

Two runs of the same experiment rarely take exactly the same time: the nodes a job lands on, and the load on the
file systems and the network, vary from one run to the next. In this tutorial we run every experiment of a study
several times, look at how much the timings vary, and reduce the runs to one value per experiment. We use the
`manager` of [Generating and running experiments](experiments.md).

## Running every experiment several times

How an experiment is run several times is up to its workflow engine. The Payu manager runs every experiment
[`nruns`][access.profiling.payu_manager.PayuManager.nruns] times, either as successive runs, each continuing the
simulation from the one before, or as repeats of the same run.
[Running an experiment several times](../access/payu.md#running-an-experiment-several-times) explains the
difference, and how to choose between the two. Here we ask for three runs:

```python
manager.nruns = 3
manager.run_experiments()
```

Every experiment then runs three times, which triples the cost of the study. Be aware that you have to set `nruns`
before the experiments first run: `run_experiments` only submits the experiments that have not run, and, with
`retry_failed=True`, those that failed.

## The runs in the data

Once parsed, every dataset of an experiment that ran several times has a `run` dimension, whose coordinate is the
number the workflow engine gives each run: for Payu, 0 for `output000`, 1 for `output001`, and so on. Parse the
logs and look at the runs of one experiment:

```python
from access.profiling.metrics import tmax

manager.parse_profiling_data()
name = "esm1p6-layout_piControl_atm_16x16_mom_16x15_ice_12"
runtime = manager.data[name]["payu"][tmax].sel(region="payu_model_run_duration_seconds")
print(runtime.run.values)
print(runtime.pint.to("minute").data)
```

This prints the run numbers, and then the run time of each run in minutes. Note that a log which only one of the
runs wrote has no `run` dimension. The spread of every experiment, as the difference between its slowest and its
fastest run relative to the fastest, is then:

```python
for name, logs in manager.data.items():
    runtime = logs["payu"][tmax].sel(region="payu_model_run_duration_seconds")
    if "run" in runtime.dims:
        spread = (runtime.max("run") - runtime.min("run")) / runtime.min("run")
        print(f"{float(spread.pint.dequantify()):6.1%}  {name}")
```

This prints the spread of each experiment as a percentage, next to its name.

Be aware that the plots and `select_best_experiments` refuse data with a `run` dimension, with this message:

```text
Profiling data still has a 'run' dimension. Use select_best_run() to keep the best run of each experiment, or aggregate_runs() to reduce over runs.
```

## Reducing the runs

The runs of every experiment are reduced to one value in one of two ways:

- [`select_best_run`][access.profiling.manager.ProfilingManager.select_best_run] keeps, for every experiment, the
  run in which the given region of the given log was fastest. The same run is kept in every log of the
  experiment, so the result describes a run that actually happened.
- [`aggregate_runs`][access.profiling.manager.ProfilingManager.aggregate_runs] computes the minimum, the mean or
  the median over the runs, region by region, so different regions may come from different runs. The minimum
  estimates the best achievable timings, and the mean or the median the typical ones. Note that integer metrics,
  such as call counts, become floats.

For example, to keep the run in which the whole model was fastest:

```python
manager.select_best_run("payu", "payu_model_run_duration_seconds", tmax)
```

Both apply to every experiment, leave the datasets without a `run` dimension alone, and replace the parsed data.
To get the individual runs back, call `parse_profiling_data` again.

## Rose suites

A Rose suite is parsed as a single run. To see how much the timings of a suite vary, run copies of it, register each
as an experiment of its own, and compare them as in [Comparing experiments](comparing-experiments.md).
