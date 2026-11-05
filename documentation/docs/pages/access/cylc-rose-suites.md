# Cylc Rose suites

ACCESS-AM3 and ACCESS-rAM3 run as Cylc suites configured with Rose, and
[`CylcRoseManager`][access.profiling.cylc_manager.CylcRoseManager] is the manager for such suites. The package
runs, parses and plots the experiments of a study of them, but does not generate them: each experiment is a copy of
the suite with its own layout, prepared by hand. In this tutorial we go through the steps for ACCESS-AM3, and note
where ACCESS-rAM3 differs. [Other suites](#other-suites) shows how another suite is described.

## What is supported

[`CylcRoseManager`][access.profiling.cylc_manager.CylcRoseManager] registers suites that are already in its working
directory, runs them, parses their logs, plots and compares them, and archives and deletes them. It cannot generate
experiments, since the package cannot search over the parallelism a `rose-suite.conf` states, and it cannot draw the
component scaling plot, which needs the layout of every experiment.

The manager expects the layout of a Cylc 7 suite, run with `rose suite-run`: a `cylc-suite.db` database, the suite
log in `log/suite/log`, and the job logs of every task in `log/job/<cycle>/<task>/NN/job.out`.

## Preparing the suites

On Gadi, Cylc runs suites from a persistent session, and is loaded from the `hr22` project's modules. Start one,
and load Cylc:

```bash
persistent-sessions start -p $PROJECT am3-scaling
echo "am3-scaling.${USER}.${PROJECT}.ps.gadi.nci.org.au" > ~/.persistent-sessions/cylc-session
module use /g/data/hr22/modulefiles
module load cylc7
```

Then get the suite into the working directory of the study:

=== "ACCESS-AM3"

    ```bash
    mkdir -p /scratch/$PROJECT/model_scaling/roses
    cd /scratch/$PROJECT/model_scaling/roses
    git clone git@github.com:ACCESS-NRI/ACCESS-AM3-configs.git -b release-n96e-3.0 am3-release-n96e-3.0
    ```

    Note that access to the ACCESS-AM3 configurations repository is restricted.

=== "ACCESS-rAM3"

    ```bash
    mosrs-auth
    rosie checkout u-by395/nci_access_ram3
    ```

    `rosie checkout` puts the suite in `~/roses/u-by395`.

Each experiment is a copy of the suite whose `rose-suite.conf` states a different layout. ACCESS-AM3 states the two
extents of its atmosphere's process grid as `MAIN_ATM_PROCX` and `MAIN_ATM_PROCY`, 24 each in
`release-n96e-3.0`, and ACCESS-rAM3 states its grid as one comma-separated pair, `rg01_rs01_m01_nproc`. We make
three ACCESS-AM3 experiments, one for each of three process grids:

```python
import shutil
from pathlib import Path

work_dir = Path("/scratch/tm70/model_scaling/roses")
release = work_dir / "am3-release-n96e-3.0"

for nx, ny in [(12, 12), (16, 16), (24, 24)]:
    suite = work_dir / f"am3-n96e-{nx}x{ny}"
    shutil.copytree(release, suite)
    conf = suite / "rose-suite.conf"
    text = conf.read_text()
    text = text.replace("MAIN_ATM_PROCX=24", f"MAIN_ATM_PROCX={nx}")
    text = text.replace("MAIN_ATM_PROCY=24", f"MAIN_ATM_PROCY={ny}")
    conf.write_text(text)
```

Change whatever else the study needs in the same way, such as the project the jobs are charged to or the length of
the run.

## Registering the suites

A manager is created as any other, with the suite's configuration and, optionally, its control:

```python
from access.profiling import CylcRoseManager
from access.profiling.models.um_suites import AM3_N96E, AM3_N96E_SOURCE

manager = CylcRoseManager(
    work_dir=Path("/scratch/tm70/model_scaling/roses"),
    archive_dir=Path("/g/data/tm70/model_scaling/ACCESS-AM3/release-n96e-3.0"),
    application=AM3_N96E,
    control=AM3_N96E_SOURCE,
)
```

The control is only recorded, as what the study was run against: nothing is cloned. ACCESS-rAM3 uses
[`RAM3`][access.profiling.models.um_suites.RAM3], which has no preset control. Pass an
[`ExistingDirectoryControlSource`][access.profiling.control.ExistingDirectoryControlSource] for the checked-out
suite, or no control at all.

Then register each suite with
[`add_rose_experiment`][access.profiling.cylc_manager.CylcRoseManager.add_rose_experiment], giving its directory
name under the working directory and the Cylc run directory it runs in. That is `~/cylc-run/<suite>`, unless the
suite moves its run directories elsewhere, and it does not have to exist yet:

```python
for suite in ["am3-n96e-12x12", "am3-n96e-16x16", "am3-n96e-24x24"]:
    manager.add_rose_experiment(suite, run_path=Path.home() / "cylc-run" / suite)
```

A suite is registered with the status its run directory records: DONE once every task has succeeded, FAILED once
one has not, RUNNING while some are still going, and NEW if it has not run. Note that a suite registered without
its run directory stays NEW, since nothing it records can be read.

## Running the suites

[`run_experiments`][access.profiling.cylc_manager.CylcRoseManager.run_experiments] runs every NEW suite with
`rose suite-run`, from the suite's directory. We run the three suites:

```python
manager.run_experiments()
```

The manager then follows each suite through the database in its run directory, as
[Experiments and their statuses](../user-guide/index.md#experiments-and-their-statuses) describes. A suite you run
by hand, with `rose suite-run` in its directory, is followed in the same way. `run_experiments(retry_failed=True)`
runs the failed suites as well, with the same `rose suite-run` and not a restart, which some suites refuse once
they have run.

## Parsing and plotting

[`parse_profiling_data`](../user-guide/analysing-data.md#parsing-the-logs) reads three kinds of log from every suite's
run directory:

| Log | Read from | Regions |
|---|---|---|
| `cylc_suite_log` | `log/suite/log` | `pipeline_elapsed_time`, the time from the first line of the suite log to the last |
| `cylc_tasks` | `cylc-suite.db` | `<task>_cycle<cycle>`, the run time of every task of every cycle that succeeded |
| `<task>_cycle<cycle>_UM_regions` and `<task>_cycle<cycle>_UM_total` | the task's `job.out` | The UM's timer regions, and its total walltime, for the tasks that run the UM |

The number of CPUs of each experiment is what the job running its layout asked PBS for, as for Payu, so it counts
the cores left idle to fill whole nodes. The layout comes from the `rose-suite.conf` its run used: the product of
the layout variables, plus the I/O server's ranks, times the OpenMP threads of each rank. The request comes from the
job scripts Cylc submitted, `log/job/<cycle>/<task>/NN/job`, as the smallest `#PBS -l ncpus` that covers the
layout. Where no job asked for that many, the layout's own count is used.

The scaling plot then works as for any other manager, as [Scaling studies](../user-guide/scaling-studies.md)
describes. For example, we plot the time ACCESS-AM3's `atmos_main` task took for the first cycle of the run:

```python
from access.profiling import RegionGroup
from access.profiling.metrics import tmax

manager.parse_profiling_data()
groups = [
    RegionGroup(
        "cylc_tasks",
        ["atmos_main_cycle19820101T0000Z"],
        relabel={"atmos_main_cycle19820101T0000Z": "UM"},
    )
]
fig = manager.plot_scaling_data(groups, tmax, xlabel="Cores")
```

Where several suites occupy the same number of CPUs, select the best of them first with
[`select_best_experiments`](../user-guide/scaling-studies.md#selecting-the-best-layouts), as for any other manager.
Suites that differ in something other than their layout, such as their OpenMP threads or their I/O server, are
registered in the same way and compared region by region, as in
[Comparing experiments](../user-guide/comparing-experiments.md).

## Archiving

[`archive_experiments`][access.profiling.cylc_manager.CylcRoseManager.archive_experiments] stores each suite and
its run directory in one archive, under `experiment/` and `runs/`. It leaves out `.svn` and `share` directories and
`*.nc` files by default, and does not follow symlinks, so a symlink leading out of the suite's directory or out of
its run directory is left out.

## Other suites

A suite is described by the names of the variables its `rose-suite.conf` states its parallelism in, and the parsers
its task logs are read with. Profiling another one takes a
[`RoseSuiteConfiguration`][access.profiling.rose_configuration.RoseSuiteConfiguration] value and no code. For
example, for a suite that states its process grid as `ATM_PROCX` and `ATM_PROCY`, and its OpenMP threads as
`ATM_OMPTHR`:

```python
from access.profiling import RoseSuiteConfiguration
from access.profiling.models.um_suites import UM_SUITE_PARSERS

MY_SUITE = RoseSuiteConfiguration(
    name="my-suite",
    layout_variable=("ATM_PROCX", "ATM_PROCY"),
    parsers=UM_SUITE_PARSERS,
    cpus_per_proc_variable="ATM_OMPTHR",
)
```

`layout_variable` names either the two variables stating the extents of the process grid, or the one variable
stating both as a comma-separated pair. `cpus_per_proc_variable` and `io_server_variable` name the variables for
OpenMP threads and I/O server ranks, where the suite has them.
