# Cylc Rose suites

ACCESS-AM3, ACCESS-rAM3 and ACCESS-CM3 are run as Cylc suites configured with Rose. The manager for such suites is
[`CylcRoseManager`][access.profiling.cylc_manager.CylcRoseManager]. There is one important difference from Payu: the
package does not create the experiments for you. Each experiment is a copy of the suite with its own parallelisation
layout, which you prepare by hand. The package then runs the suites, reads their logs and plots the results. In this
tutorial we go through all the steps for ACCESS-AM3, and point out where ACCESS-rAM3 is different. If you want to
profile another suite, see [Other suites](#other-suites).

## What is supported

With [`CylcRoseManager`][access.profiling.cylc_manager.CylcRoseManager] you can register suites that are already
in the working directory, run them, read their logs, plot and compare them, and archive and delete them. The
package also reads the layout of each suite from its configuration. This means that the component scaling plot
works, and that tables and plots can use a quantity of the layout as a coordinate, as well as the labels that you
give the suites. The package can also search for the layouts of ACCESS-AM3 and ACCESS-rAM3.

One thing is not possible: the manager cannot create experiments, because nothing in the package writes changes
into a suite. `generate_experiments` therefore raises a `NotImplementedError` whenever it has an experiment to
create. `generate_scaling_experiments` raises it too, but only once it has searched for the layouts, which can take
a while for a large number of cores.

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
in `release-n96e-3.0`. ACCESS-rAM3 has two nests, and the process grid of each is set with one variable,
`rg01_rs01_m01_nproc` and `rg01_rs02_m01_nproc`, as two numbers separated by a comma. We make three ACCESS-AM3
experiments, each with a different process grid:

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

## Reading a configuration from its control

The presets describe the released suites. If your ACCESS-AM3 suite is on a branch or a fork that they do not
describe, you can read its configuration from the suite itself with
[`am3_from_control`][access.profiling.models.um_suites.am3_from_control]. It reads the grid of the UM from the
sizes that the UM reads itself, `global_row_length` and `global_rows` in the `[namelist:nlsizes]` section of
`app/um/rose-app.conf`. For example, we read the configuration of the release that we cloned above:

```python
from access.profiling.models.um_suites import am3_from_control

application = am3_from_control(release)
print(application.name, application.components[0].grid.shape)
```

This prints:

```
n96e (192, 144)
```

The configuration is named after its resolution, `n96e` for a grid of 192 longitudes, so the suite of a release
gives back its preset. To give it another name, pass `name`, as in `am3_from_control(release, name="n96e-amip")`.
Nothing is fetched: the suite has to be on disk already.

## Registering the suites

The manager is created like any other manager, with the configuration of the suite and, optionally, its control:

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
for nx, ny in [(12, 12), (16, 16), (24, 24)]:
    suite = f"am3-n96e-{nx}x{ny}"
    manager.add_rose_experiment(suite, run_path=Path.home() / "cylc-run" / suite, labels={"ranks": nx * ny})
```

The `labels` are optional. They are keys and values of your own, which say what each suite is. They must be JSON
values, such as strings, numbers and lists, since they are kept in the record of the suite. Here we record the
number of ranks of the atmosphere, which we plot against later.

The status of each suite is read from its run directory. A suite is DONE if all its tasks succeeded, FAILED if
one of them failed, RUNNING if some tasks are still going, and NEW if it has not run. Note that if you register a
suite without its run directory, it stays NEW, because the manager has nowhere to read its status from.

The experiment has the name of the suite, which is the name of its directory in the working directory. It can be
anything without a `/`, except a name of the form `exp-<digits>`. The manager writes a record into the directory of
the suite, with the run directory you gave. You therefore register each suite only once: a manager created in a
later session finds the suites again from their records.
[Managing experiments](../user-guide/managing-experiments.md#what-the-manager-keeps-on-disk) describes the record.

## Searching for layouts

We chose the process grids of our three suites by hand. For the next suites of a study, you can let the manager
search for them with [`select_layouts`][access.profiling.manager.ProfilingManager.select_layouts], as
[Finding the best layout](../user-guide/best-layout.md) describes. The UM of ACCESS-AM3 needs an even number of
processes along x, and subdomains no more than four times as long as they are wide. An allocation strategy that
gives the UM every core then leaves the process grid as the only choice. We search for the process grids of 576
cores:

```python
from access.config.parallel_allocation_strategies import FixedAllocation, RootAllocation

allocations = RootAllocation(subcomponents={"UM": FixedAllocation(core_fraction=1.0)})
layouts = manager.select_layouts(576, allocations=allocations)
for layout in layouts:
    print(manager.application.layout_key(layout))
```

This prints:

```
am3-layout_n96e_atm_16x36
am3-layout_n96e_atm_18x32
am3-layout_n96e_atm_24x24
am3-layout_n96e_atm_32x18
am3-layout_n96e_atm_36x16
am3-layout_n96e_atm_48x12
```

Each key names the process grid of the UM, and its threads where it runs more than one a rank. To run the UM with
two threads a rank, give the allocation `thread_range=(2, 2)`. The values to write into the copy of a suite are
given by [`config_changes`][access.profiling.rose_configuration.RoseSuiteConfiguration.config_changes]:

```python
print(manager.application.config_changes(layouts[0]))
```

This prints:

```
{'rose-suite.conf': {'jinja2:suite.rc': {'MAIN_ATM_PROCX': '16', 'MAIN_ATM_PROCY': '36', 'MAIN_OMPTHR_ATM': '1'}}}
```

Write them into the `[jinja2:suite.rc]` section of the copy, as in [Preparing the suites](#preparing-the-suites),
and register it. The ranks of the I/O server are never part of a layout, so the suite keeps those of its control.
For ACCESS-rAM3, each of the two nests is a component of its own, `UM rs01` and `UM rs02`. Each nest runs as a job
of its own, so each can take every core: give both a `WholeRangeAllocation()`. The layouts of ACCESS-CM3 cannot be
searched for, since its preset does not state the grid of its UM.

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
many CPUs the layout needs, from the `[jinja2:suite.rc]` section of the configuration that the run used. This is
the copy that `rose suite-run` keeps in the run directory, `log/rose-suite-run.conf`, or the `rose-suite.conf` of
the suite if there is no such copy. The layout of the suite is read from the same file. For
the UM, this is the product of the two extents of its process grid, plus the ranks of the I/O server, times the
OpenMP threads of each rank. The ranks of any other component that the suite runs are then added, such as those of
the ocean and the sea ice of ACCESS-CM3. The two nests of ACCESS-rAM3 run as jobs of their own, so that suite needs
the CPUs of the larger nest. Next, the manager looks at the job scripts that Cylc submitted,
`log/job/<cycle>/<task>/NN/job`, and takes the smallest `#PBS -l ncpus` request that is large enough for the
layout. If no job requested that many CPUs, the number of CPUs of the layout is used.

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
        component="UM",
        relabel={"atmos_main_cycle19820101T0000Z": "UM"},
    )
]
fig = manager.plot_scaling_data(groups, tmax, xlabel="Cores")
```

The group names the component of its regions, the UM. The logs of a suite do not say which component they belong
to, so only the group can say it. The scaling plot does not need it, but the component scaling plot does: it plots
each region against the cores of its component, which the manager reads from the layout of each suite. We plot the
same task against the cores of the UM:

```python
fig = manager.plot_component_scaling_data(groups, tmax, xlabel="Cores of the UM")
```

The UM is the only component of ACCESS-AM3, so the two plots differ only where a job asked PBS for more cores than
the UM puts to work. This is the case for a suite with an I/O server, and for a job that asked for whole nodes.
For example, the 256 cores of our 16 by 16 suite are not a whole number of Gadi's nodes, of 48 or 104 cores, so a
job of that suite that asked for whole nodes asked for more.

If several suites use the same number of CPUs, select the best one first with
[`select_best_experiments`](../user-guide/scaling-studies.md#selecting-the-best-layouts), as for any other manager.
You can also compare suites that differ in something other than their layout, such as their OpenMP threads or
their I/O server. Register them in the same way and compare them region by region, as in
[Comparing experiments](../user-guide/comparing-experiments.md).

The manager also knows the labels of each suite. To plot the time against the ranks of the atmosphere, we pass the
key of the label to [`plot_metric`][access.profiling.manager.ProfilingManager.plot_metric]:

```python
fig = manager.plot_metric(groups, tmax, "ranks", xlabel="Ranks of the atmosphere")
```

A quantity of the layout works in the same way, without a label. For example,
[`component_cores("UM")`][access.profiling.manager.component_cores] in place of `"ranks"` plots the time against
the cores of the UM.

[Configuration choices](../user-guide/configuration-choices.md#plotting-against-a-setting) describes this plot, and
the tables that [`gather`][access.profiling.manager.ProfilingManager.gather] makes from the same coordinates.

## Archiving

[`archive_experiments`][access.profiling.cylc_manager.CylcRoseManager.archive_experiments] puts each suite and its
run directory in the same archive, under `experiment/` and `runs/`. By default, `.svn` and `share` directories and
`*.nc` files are not archived. Symlinks are not followed either. This means that a symlink that points outside the
directory of the suite, or outside its run directory, is left out.

## Other suites

You can profile other suites without writing any code. All the package needs to know is which codes the suite
runs, which variables of its `rose-suite.conf` set the layout of each, and which parsers read the logs of its
tasks. This is given as a [`RoseSuiteConfiguration`][access.profiling.rose_configuration.RoseSuiteConfiguration],
put together from one component for each code. For example, for a suite that runs one UM over a grid of 192 by 144
points, and sets its process grid with `ATM_PROCX` and `ATM_PROCY` and its OpenMP threads with `ATM_OMPTHR`:

```python
from access.config.parallel_domain import Domain

from access.profiling import RoseProcessGrid, RoseSuiteConfiguration
from access.profiling.models.components import UM_CONSTRAINTS
from access.profiling.models.um_suites import UM_SUITE_PARSERS

MY_SUITE = RoseSuiteConfiguration(
    name="my-suite",
    model_name="My model",
    components=(
        RoseProcessGrid(
            name="UM",
            token="atm",
            variables=("ATM_PROCX", "ATM_PROCY"),
            grid=Domain((192, 144)),
            threads_variable="ATM_OMPTHR",
            local_constraints=UM_CONSTRAINTS,
        ),
    ),
    parsers=UM_SUITE_PARSERS,
)
```

A [`RoseProcessGrid`][access.profiling.rose_configuration.RoseProcessGrid] is a code that divides a grid among its
ranks along a process grid, as the UM does. Its fields are:

- `name`, the name of the component in allocation strategies and in a `RegionGroup`;
- `token`, its short name in the key of each layout, which identifies the experiments of a study, so do not change
  it once a study has experiments;
- `variables`, either the names of the two variables that set the process grid, or the name of the single variable
  that sets both numbers, separated by a comma;
- `grid`, the grid that the code divides. Without one, the layout of a suite is still read, but cannot be searched
  for;
- `threads_variable` and `io_server_variable`, the variables that set the OpenMP threads of each rank and the ranks
  of the I/O server, if the suite has them. Without a threads variable, every rank runs one thread;
- `local_constraints`, the rules that every layout of the code follows.
  [`UM_CONSTRAINTS`][access.profiling.models.components.UM_CONSTRAINTS] are the UM's: an even number of processes
  along x, and subdomains no more than four times as long as they are wide.

A code that is given a number of ranks and nothing else is a
[`RoseRanks`][access.profiling.rose_configuration.RoseRanks], with the name of its variable. This is how
[`CM3_O25KM`][access.profiling.models.um_suites.CM3_O25KM] gives their cores to the ocean and the sea ice of
ACCESS-CM3. The components of a suite run side by side, each on cores of its own, and their cores add up. If they
run instead as jobs of their own, one after the other, they share the cores: pass
`core_sharing=CoreSharing.SHARED`, with `CoreSharing` from `access.config.parallel_component`, as
[`RAM3`][access.profiling.models.um_suites.RAM3] does for its two nests.
