# Comparing experiments

Experiments that differ in their layout, in how the application was built, or in how it was configured can be set
side by side, region by region, to see what the difference did to each part of the application. The experiments
can be ones the manager generated, or runs made in some other way and registered with it. In this tutorial we
compare experiments of the ACCESS-ESM1.6 and ACCESS-OM3 presets, run with Payu, using the `groups` of
[Choosing the regions](analysing-data.md#choosing-the-regions).

## What can be compared

- A manager reads every experiment as a run of its application, so the experiments must be runs of that
  application, or of one that writes the same logs.
- The experiments the manager generates are named after their layouts, so it generates one experiment for each
  layout. Experiments of the same layout that are built or configured differently are registered under names of
  their own.
- For the times to be comparable, the runs should simulate the same period, from the same start and for the same
  length.

## Registering experiments

A manager created without a control reads, plots and archives experiments, although it cannot generate any. We
create one for each model:

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

Then register each experiment with
[`add_experiment_from_directory`][access.profiling.manager.ProfilingManager.add_experiment_from_directory], under
a name of your choice:

```python
manager.add_experiment_from_directory("release", Path("release"))
manager.add_experiment_from_directory("modified", Path("modified"))
```

Each path is the directory an experiment was run from, as its workflow engine left it. For Payu, that is the
directory holding the experiment's `config.yaml` and the `archive` link Payu creates. A relative path is taken
from the working directory, and every path must be inside it: a symbolic link to an experiment elsewhere is
refused. So choose a working directory that holds every experiment you want to compare.

A registered experiment is taken to have finished. Note that `parse_profiling_data` reads the workflow engine's
records again, so an experiment whose latest run failed or is still running is skipped, with a warning. Archiving
the experiments keeps the comparison: a manager created later with the same archive directory finds them there.

## Comparing region by region

[`plot_bar_chart`][access.profiling.manager.ProfilingManager.plot_bar_chart] draws one group of bars for every
region of the groups, with one bar for each experiment. Parse the logs and draw it:

```python
from access.profiling.metrics import tmax

manager.parse_profiling_data()
fig = manager.plot_bar_chart(
    groups,
    tmax,
    experiments=["release", "modified"],
    experiment_relabel_map={"modified": "Modified build"},
)
```

The regions are placed along the x axis in the order of the groups, and the experiments in the order they are
given, every experiment with parsed data by default. The bars are labelled with the names of the experiments,
which `experiment_relabel_map` can replace, and the chart is titled with the description of the metric. Note that
no cores appear on either axis, so experiments of different sizes can be compared as well.
[Best layout for a budget](best-layout.md#comparing-the-fastest-layouts) shows an example.

Groups naming more regions of a component's own log show what changed inside that component. For example,
`RegionGroup("UM", ["AP2 Boundary Layer", "AP2 Convection"])` compares two of the timers of ACCESS-ESM1.6's UM.

## Comparing the numbers

You can also compare the data of the experiments directly, for example as the relative change in every region of
Payu's timings:

```python
release = manager.data["release"]["payu"][tmax]
modified = manager.data["modified"]["payu"][tmax]
change = (modified - release) / release
for region in change.region.values:
    print(f"{float(change.sel(region=region).pint.dequantify()):+7.1%}  {region}")
```

This prints the change of each region as a percentage, next to its name.

## Rose suites

Rose suites are registered with
[`add_rose_experiment`][access.profiling.cylc_manager.CylcRoseManager.add_rose_experiment], as
[Cylc Rose suites](../access/cylc-rose-suites.md#registering-the-suites) shows, and compared in the same way.
Their groups name logs such as `cylc_tasks`.

Be aware that a difference no larger than the spread of the timings of identical runs says nothing about the
experiments. [Run-to-run variability](run-to-run-variability.md) shows how to measure that spread.
