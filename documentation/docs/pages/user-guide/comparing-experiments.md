# Comparing experiments

Did a new compiler flag make the model faster? Is one configuration slower than another, and where? To answer
this kind of question, you can put experiments side by side, region by region, and see what changed in each part
of the application. The experiments can be ones that the manager created, or existing experiments that were run in
some other way. In this tutorial we compare experiments of ACCESS-ESM1.6 and ACCESS-OM3, run with Payu, using the
`regions` of
[Choosing the regions](analysing-data.md#choosing-the-regions).

## What can be compared

- The experiments must all be runs of the same application, or of applications that write the same logs. This is
  because a manager works with one application.
- A scaling study has one experiment for each parallelisation layout. To compare settings of the application
  instead, create the experiments from [configuration choices](configuration-choices.md). To compare experiments
  that were made in some other way, such as two builds, register them yourself, each with its own name.
- The runs should simulate the same period, with the same start and the same length. Otherwise their times cannot
  be compared.

## Registering experiments

To work with existing experiments, the manager does not need a control. Without one it cannot create
experiments, but it can still read, plot and archive them. We create one for each model:

=== "ACCESS-ESM1.6"

    ```python
    from pathlib import Path

    from access.profiling import PayuManager
    from access.profiling.models.esm16 import ESM16_PI_CONTROL

    manager = PayuManager(
        work_dir=Path("/scratch/tm70/comparisons/ACCESS-ESM1.6"),
        archive_dir=Path("/g/data/tm70/comparisons/ACCESS-ESM1.6/archive"),
        application=ESM16_PI_CONTROL,
    )
    ```

=== "ACCESS-OM3"

    ```python
    from pathlib import Path

    from access.profiling import PayuManager
    from access.profiling.models.om3 import OM3_MC_25KM

    manager = PayuManager(
        work_dir=Path("/scratch/tm70/comparisons/ACCESS-OM3"),
        archive_dir=Path("/g/data/tm70/comparisons/ACCESS-OM3/archive"),
        application=OM3_MC_25KM,
    )
    ```

The two directories are only examples, so adapt them to your own setup, replacing `tm70` with your own project.
Then register each experiment with
[`add_experiment_from_directory`][access.profiling.manager.ProfilingManager.add_experiment_from_directory], under
a name of your choice:

```python
manager.add_experiment_from_directory("release", Path("release"))
manager.add_experiment_from_directory("modified", Path("modified"))
```

Each path is the directory that the experiment was run from. For Payu, this is the directory with the
`config.yaml` of the experiment and the `archive` link that Payu creates. Replace `release` and `modified` with
the directories of your own experiments. A relative path starts from the working directory. Note that every
experiment must be inside the working directory, and a symbolic link to an experiment somewhere else is not
accepted. So choose a working directory that contains all the experiments you want to compare.

The manager assumes that a registered experiment has finished. Note, however, that `parse_profiling_data` checks
this again. If the latest run of an experiment failed or is still running, the experiment is skipped, with a
warning. If you want to keep the comparison, archive the experiments. A manager created later with the same
archive directory then finds them again.

## Comparing region by region

The easiest way to compare experiments is a bar chart.
[`plot_bar_chart`][access.profiling.results.ProfilingResults.plot_bar_chart] draws one group of bars for each
region, with one bar for each experiment. Read the logs and draw it:

```python
results = manager.parse_profiling_data()
two = results.select_experiments("experiment", "release", "modified")
ax = two.plot_bar_chart(regions, experiment_relabel_map={"modified": "Modified build"})
```

The regions appear along the x axis in the order you give them, and the experiments in the order of the results. By
default, all the experiments of the results are included, so here we select the two we compare. The bars are labelled
with the names of the experiments, and you can use `experiment_relabel_map` to give them other labels. Experiments with
labels can also be named by one of them, with `label`, as [Configuration
choices](configuration-choices.md#running-and-comparing) shows. The title of the chart is the description of the metric.
Note that the number of cores does not appear anywhere in the chart, so you can also compare experiments of different
sizes. [Best layout for a budget](best-layout.md#comparing-the-fastest-layouts) shows an example.

If you want to see what changed inside one component, use more regions of its log. For example,
`[Region("UM", "AP2 Boundary Layer"), Region("UM", "AP2 Convection")]` compares two of the timers of
ACCESS-ESM1.6's UM.

## Comparing the numbers

A chart is not always enough. You can also compare the numbers directly, for example to get the relative change
in every region of Payu's timings:

```python
from access.profiling.metrics import tmax

release = results.data["release"]["payu"][tmax]
modified = results.data["modified"]["payu"][tmax]
change = (modified - release) / release
for region in change.region.values:
    print(f"{float(change.sel(region=region).pint.dequantify()):+7.1%}  {region}")
```

This prints the change of each region as a percentage, next to its name.

## Rose suites

Rose suites are generated with `generate_experiments` too, as
[Cylc Rose suites](../access/cylc-rose-suites.md#the-changes-of-an-experiment) shows, and compared in the same way.
A suite that you prepared yourself is registered with
[`add_rose_experiment`][access.profiling.cylc_manager.CylcRoseManager.add_rose_experiment] instead. Their regions
are in logs such as `cylc_tasks`.

Be aware that two runs of the same experiment never take exactly the same time. If the difference between two
experiments is not larger than that, it does not tell you anything.
[Run-to-run variability](run-to-run-variability.md) shows how to measure it.
