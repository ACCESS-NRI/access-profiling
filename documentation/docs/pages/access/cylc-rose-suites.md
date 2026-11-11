# Cylc Rose suites

ACCESS-AM3 and ACCESS-rAM3 are run as Cylc suites configured with Rose. The manager for such suites is
[`CylcRoseManager`][access.profiling.cylc_manager.CylcRoseManager]. There is one important difference from Payu: the
package does not create the experiments for you. Each experiment is a copy of the suite with its own parallelisation
layout, which you prepare by hand. The package then runs the suites, reads their logs and plots the results. In this
tutorial we go through all the steps for ACCESS-AM3, and point out where ACCESS-rAM3 is different. If you want to
profile another suite, see [Other suites](#other-suites).

## What is supported

With [`CylcRoseManager`][access.profiling.cylc_manager.CylcRoseManager] you can register suites that are already
in the working directory, run them, read their logs, plot and compare them, and archive and delete them. Two
things are not possible. The manager cannot create experiments, because the package cannot search for layouts in
a `rose-suite.conf`. It also cannot make the component scaling plot, which needs the layout of every experiment.

The manager works with Cylc 7 suites that are run with `rose suite-run`. It expects to find a `cylc-suite.db`
database, the suite log in `log/suite/log`, and the job logs of every task in
`log/job/<cycle>/<task>/NN/job.out`.

## Preparing the suites

On Gadi, Cylc suites are run from a persistent session, and Cylc is available as a module of the `hr22` project.
Start a session and load Cylc:

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

    The working directory used here is only an example, so adapt it to your own setup. Note that access to the
    ACCESS-AM3 configurations repository is restricted.

=== "ACCESS-rAM3"

    ```bash
    mosrs-auth
    rosie checkout u-by395/nci_access_ram3
    ```

    `rosie checkout` puts the suite in `~/roses/u-by395`.

Each experiment is a copy of the suite, with a different layout in its `rose-suite.conf`. In ACCESS-AM3, the
process grid of the atmosphere is set with two variables, `MAIN_ATM_PROCX` and `MAIN_ATM_PROCY`, which are both 24
in `release-n96e-3.0`. In ACCESS-rAM3, it is set with one variable, `rg01_rs01_m01_nproc`, as two numbers
separated by a comma. We make three ACCESS-AM3 experiments, each with a different process grid:

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

Remember to set `work_dir` to your own working directory. If your study needs other changes, such as the project
that the jobs are charged to or the length of the run, make them in the same way.

## Registering the suites

The manager is created like any other manager, with the configuration of the suite and, optionally, its control:

```python
from access.profiling import AM3_N96E, AM3_N96E_SOURCE, CylcRoseManager

manager = CylcRoseManager(
    work_dir=Path("/scratch/tm70/model_scaling/roses"),
    archive_dir=Path("/g/data/tm70/model_scaling/ACCESS-AM3/release-n96e-3.0"),
    application=AM3_N96E,
    control=AM3_N96E_SOURCE,
)
```

As before, adapt the working directory and the archive directory to your own setup. Note that here the control is
only kept for reference, to remember which release the study was based on. Nothing is cloned. For ACCESS-rAM3, use
[`RAM3`][access.profiling.models.um_suites.RAM3] as the application. It has no preset control, so pass an
[`ExistingDirectoryControlSource`][access.profiling.control.ExistingDirectoryControlSource] for the suite you
checked out, or no control at all.

Then register each suite with
[`add_rose_experiment`][access.profiling.cylc_manager.CylcRoseManager.add_rose_experiment]. It needs the name of
the directory of the suite in the working directory, and the directory where Cylc runs the suite. This is usually
`~/cylc-run/<suite>`, as in the example, so adapt it if Cylc runs your suites somewhere else. The directory does
not have to exist yet:

```python
for suite in ["am3-n96e-12x12", "am3-n96e-16x16", "am3-n96e-24x24"]:
    manager.add_rose_experiment(suite, run_path=Path.home() / "cylc-run" / suite)
```

The status of each suite is read from its run directory. A suite is DONE if all its tasks succeeded, FAILED if
one of them failed, RUNNING if some tasks are still going, and NEW if it has not run. Note that if you register a
suite without its run directory, it stays NEW, because the manager has nowhere to read its status from.

## Running the suites

[`run_experiments`][access.profiling.cylc_manager.CylcRoseManager.run_experiments] runs every NEW suite with
`rose suite-run`, from the directory of the suite. We run our three suites:

```python
manager.run_experiments()
```

The manager then checks how each suite is going by reading the database in its run directory, as described in
[Experiments and their statuses](../user-guide/index.md#experiments-and-their-statuses). You can also run a suite
by hand, with `rose suite-run` in its directory, and the manager follows it in the same way. To run the failed
suites again, use `run_experiments(retry_failed=True)`. Note that this runs `rose suite-run` again, and not a
restart, and some suites do not accept this once they have run.

## Parsing and plotting

[`parse_profiling_data`](../user-guide/analysing-data.md#parsing-the-logs) reads three kinds of log from every suite's
run directory:

| Log | Read from | Regions |
|---|---|---|
| `cylc_suite_log` | `log/suite/log` | `pipeline_elapsed_time`, the time from the first line of the suite log to the last |
| `cylc_tasks` | `cylc-suite.db` | `<task>_cycle<cycle>`, the run time of every task of every cycle that succeeded |
| `<task>_cycle<cycle>_UM_regions` and `<task>_cycle<cycle>_UM_total` | the task's `job.out` | The UM's timer regions, and its total walltime, for the tasks that run the UM |

How many CPUs did each experiment use? As for Payu, the answer is the number of CPUs that the job requested from
PBS. This includes the cores that are left idle to fill whole nodes. To find it, the manager first works out how
many CPUs the layout needs, from the `rose-suite.conf` that the run used. This is the product of the layout
variables, plus the ranks of the I/O server, times the OpenMP threads of each rank. It then looks at the job
scripts that Cylc submitted, `log/job/<cycle>/<task>/NN/job`, and takes the smallest `#PBS -l ncpus` request that
is large enough for the layout. If no job requested that many CPUs, the number of CPUs of the layout is used.

The scaling plot then works as for any other manager, as described in
[Scaling studies](../user-guide/scaling-studies.md). For example, we plot the time that the `atmos_main` task of
ACCESS-AM3 took for the first cycle of the run:

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

If several suites use the same number of CPUs, select the best one first with
[`select_best_experiments`](../user-guide/scaling-studies.md#selecting-the-best-layouts), as for any other manager.
You can also compare suites that differ in something other than their layout, such as their OpenMP threads or
their I/O server. Register them in the same way and compare them region by region, as in
[Comparing experiments](../user-guide/comparing-experiments.md).

## Archiving

[`archive_experiments`][access.profiling.cylc_manager.CylcRoseManager.archive_experiments] puts each suite and its
run directory in the same archive, under `experiment/` and `runs/`. By default, `.svn` and `share` directories and
`*.nc` files are not archived. Symlinks are not followed either. This means that a symlink that points outside the
directory of the suite, or outside its run directory, is left out.

## Other suites

You can profile other suites without writing any code. All the package needs to know is which variables of the
`rose-suite.conf` set the layout, and which parsers read the logs of the tasks. This is given as a
[`RoseSuiteConfiguration`][access.profiling.rose_configuration.RoseSuiteConfiguration]. For example, for a suite
that sets its process grid with `ATM_PROCX` and `ATM_PROCY`, and its OpenMP threads with `ATM_OMPTHR`:

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

`layout_variable` is either the names of the two variables that set the process grid, or the name of the single
variable that sets both numbers, separated by a comma. If the suite has variables for the OpenMP threads and for
the ranks of the I/O server, give their names as `cpus_per_proc_variable` and `io_server_variable`.
