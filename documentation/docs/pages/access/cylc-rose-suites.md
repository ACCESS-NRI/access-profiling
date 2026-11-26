# Cylc Rose suites

ACCESS-AM3, ACCESS-rAM3 and ACCESS-CM3 are run as Cylc suites configured with Rose. The manager for such suites is
[`CylcRoseManager`][access.profiling.cylc_manager.CylcRoseManager]. As with Payu, the package creates the
experiments for you. Each experiment is a copy of a control suite, with its own parallelisation layout written into
its files. The package then runs the suites, reads their logs and plots the results. In this tutorial we go through
all the steps for ACCESS-AM3, and point out where ACCESS-rAM3 is different. If you want to profile another suite,
see [Other suites](#other-suites).

## What is supported

With [`CylcRoseManager`][access.profiling.cylc_manager.CylcRoseManager] you can generate experiments, run them, read
their logs, plot and compare them, and archive and delete them. Both ways of generating experiments work:
[`generate_scaling_experiments`][access.profiling.manager.ProfilingManager.generate_scaling_experiments] and
[`generate_experiments`][access.profiling.manager.ProfilingManager.generate_experiments]. A suite that failed can be
created again with corrected changes, and run again from the beginning. The package can search for the layouts of
ACCESS-AM3 and ACCESS-rAM3. It also reads the layout of each suite from its configuration. This means that the
component scaling plot works, and that tables and plots can use a quantity of the layout as a coordinate.

You can also register suites that you have prepared yourself, as [Suites prepared by hand](#suites-prepared-by-hand)
shows. Note that each suite runs once: there are no repeated runs of a suite, as there are of a Payu experiment.

The manager works with Cylc 7 suites that are run with `rose suite-run`. It expects to find a `cylc-suite.db`
database, the suite log in `log/suite/log`, and the job logs of every task in
`log/job/<cycle>/<task>/NN/job.out`.

## Getting the control suite

On Gadi, Cylc suites are run from a persistent session, and Cylc is available as a module of the `hr22` project.
Start a session and load Cylc:

```bash
persistent-sessions start -p $PROJECT am3-scaling
echo "am3-scaling.${USER}.${PROJECT}.ps.gadi.nci.org.au" > ~/.persistent-sessions/cylc-session
module use /g/data/hr22/modulefiles
module load cylc7
```

The control suite is the suite that every experiment is a copy of. The package never fetches it, so that it never
asks for your credentials. You check it out yourself, in a terminal, where you can answer such a prompt. Get the
suite as follows:

=== "ACCESS-AM3"

    ```bash
    mkdir -p /scratch/$PROJECT/model_scaling/ACCESS-AM3/release-n96e-3.0
    cd /scratch/$PROJECT/model_scaling/ACCESS-AM3/release-n96e-3.0
    git clone git@github.com:ACCESS-NRI/ACCESS-AM3-configs.git -b release-n96e-3.0 config
    ```

    This clones the release into the working directory of the study, under the name `config`, which is where the
    manager looks for it. The working directory used here is only an example, so adapt it to your own setup. Note
    that access to the ACCESS-AM3 configurations repository is restricted.

=== "ACCESS-rAM3"

    ```bash
    mosrs-auth
    rosie checkout u-by395/nci_access_ram3
    ```

    `rosie checkout` puts the suite in `~/roses/u-by395`. The manager copies it from there.

## Reading a configuration from its control

The presets describe the released suites. If your ACCESS-AM3 suite is on a branch or a fork that they do not
describe, you can read its configuration from the suite itself with
[`am3_from_control`][access.profiling.models.um_suites.am3_from_control]. It reads the grid of the UM from the
sizes that the UM reads itself, `global_row_length` and `global_rows` in the `[namelist:nlsizes]` section of
`app/um/rose-app.conf`. For example, we read the configuration of the release that we cloned above:

```python
from pathlib import Path

from access.profiling.models.um_suites import am3_from_control

control_dir = Path("/scratch/tm70/model_scaling/ACCESS-AM3/release-n96e-3.0/config")
application = am3_from_control(control_dir)
print(application.name, application.components[0].grid.shape)
```

Remember to adapt the path to where you cloned the suite. This prints:

```
n96e (192, 144)
```

The configuration is named after its resolution, `n96e` for a grid of 192 longitudes, so the suite of a release
gives back its preset. To give it another name, pass `name`, as in `am3_from_control(control_dir, name="n96e-amip")`.
Nothing is fetched: the suite has to be on disk already.

## Creating the manager

The manager is created like any other manager, with the configuration of the suite and its control:

```python
from access.profiling import CylcRoseManager
from access.profiling.models.um_suites import AM3_N96E, AM3_N96E_SOURCE

manager = CylcRoseManager(
    work_dir=Path("/scratch/tm70/model_scaling/ACCESS-AM3/release-n96e-3.0"),
    archive_dir=Path("/g/data/tm70/model_scaling/ACCESS-AM3/release-n96e-3.0"),
    application=AM3_N96E,
    control=AM3_N96E_SOURCE,
)
```

As before, adapt the working directory and the archive directory to your own setup. The control tells the manager
where the control suite is. For a [`GitControlSource`][access.profiling.control.GitControlSource], such as
`AM3_N96E_SOURCE`, this is its `directory` in the working directory, `config` by default. Its repository and its
release are only kept as a record of what the study was based on: nothing is cloned.

For ACCESS-rAM3, use [`RAM3`][access.profiling.models.um_suites.RAM3] as the application. It has no preset control,
so pass an [`ExistingDirectoryControlSource`][access.profiling.control.ExistingDirectoryControlSource] with the path
of the suite that you checked out, such as `ExistingDirectoryControlSource(Path.home() / "roses" / "u-by395")`.

## Searching for layouts

The manager searches for the layouts of a study with
[`select_layouts`][access.profiling.manager.ProfilingManager.select_layouts], as
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
two threads a rank, give the allocation `thread_range=(2, 2)`. The values that a layout writes into its copy of the
suite are given by [`config_changes`][access.profiling.rose_configuration.RoseSuiteConfiguration.config_changes]:

```python
print(manager.application.config_changes(layouts[0]))
```

This prints:

```
{'rose-suite.conf': {'jinja2:suite.rc': {'MAIN_ATM_PROCX': '16', 'MAIN_ATM_PROCY': '36', 'MAIN_OMPTHR_ATM': '1'}}}
```

The ranks of the I/O server are never part of a layout, so every copy keeps those of the control. For ACCESS-rAM3,
each of the two nests is a component of its own, `UM rs01` and `UM rs02`. Each nest runs as a job of its own, so
each can take every core: give both a `WholeRangeAllocation()`. The layouts of ACCESS-CM3 cannot be searched for,
since its preset does not state the grid of its UM.

## Generating the experiments

We now generate the experiments of a scaling study on 3, 6 and 12 nodes of 48 cores, with
[`generate_scaling_experiments`][access.profiling.manager.ProfilingManager.generate_scaling_experiments]. A study
usually changes a few other things in every suite too, such as the project that the jobs are charged to and the
length of the run. These changes go in `control_options`, and are made to every copy before its own changes:

```python
control_options = {
    "rose-suite.conf": {
        "jinja2:suite.rc": {
            "STORAGE_PROJECT": "'tm70'",
            "COMPUTE_PROJECT": "'tm70'",
            "EXPT_RUNLEN": "'P1M'",
        },
    },
    "site/nci_gadi.rc": {"per_cpu=2": "per_cpu=4"},
}
manager.generate_scaling_experiments(
    [3, 6, 12], cores_per_node=48, walltime=2.0, allocations=allocations, control_options=control_options
)
print(len(manager.experiments))
print(manager.experiments["exp-0001"].path)
```

Replace `tm70` with your own project. This prints the number of experiments, and where the first of them is:

```
17
/scratch/tm70/model_scaling/ACCESS-AM3/release-n96e-3.0/exp-0001/n96e-541dd259-exp-0001
```

There is one experiment for each layout: five of 144 cores, and six each of 288 and 576 cores. Each copy of the
suite states its process grid in `MAIN_ATM_PROCX` and `MAIN_ATM_PROCY`, and the walltime of two hours in
`MAIN_CLOCK`, as `'PT2H00M'`. The change to `site/nci_gadi.rc` raises the memory that each job asks for per CPU.
As for any manager, you can call `generate_scaling_experiments` again safely: the layouts that already have an
experiment are skipped.

### The changes of an experiment

The control options, like the changes of a
[configuration choice](../user-guide/configuration-choices.md), are given for each file of the suite. The file is
named by its path relative to the directory of the suite. What you give for it depends on the kind of file:

| File | What you give | Example |
|---|---|---|
| A Rose configuration, whose name ends in `.conf`, such as `rose-suite.conf` or `app/um/rose-app.conf` | The settings of each section: `{section: {setting: value}}`. The settings before the first section go under `""`. | `{"jinja2:suite.rc": {"EXPT_RUNLEN": "'P1M'"}}` |
| Any other file, such as `suite.rc` or a file of `site/` | The texts to replace: `{old text: new text}` | `{"per_cpu=2": "per_cpu=4"}` |

A section is named as in its header, without the brackets. A value is always a string, written as the file is to
state it. The variables of `[jinja2:suite.rc]` are Jinja2 values, which the `suite.rc` of the suite is written
with, so a string carries its own quotes, as in `"'tm70'"`. These are the rules of a Rose configuration:

- A setting that the section does not have is added at the end of the section.
- A setting that the suite ignores, written `!KEY=` or `!!KEY=`, is enabled. This is what `rose suite-run -D` does
  with such a setting.
- A section that the file does not have is refused, and so is one that the file ignores.
- The rest of the file is kept as it is, comments included.

For example, the setting `root-dir` of `rose-suite.conf` says where Rose puts the run directory of a suite. It
comes before the first section. To put the runs of a study in a directory of your project on `/scratch`, the
control options for `rose-suite.conf` include this setting:

```python
{"rose-suite.conf": {"": {"root-dir": "*=/scratch/tm70/model_scaling"}}}
```

Adapt the directory to your own project. In any other file, a text is replaced wherever it occurs. The changes are
made in this order: the control options, then the changes of the experiment, then its walltime. Each change is made
in the file as the changes before it left it, so where two of them set the same setting, the later one wins.

A file that you change can be a symbolic link, as `rose-suite.conf` is in the suites of ACCESS-AM3. The file is read
through the link, and the copy has the changed file in place of the link, with the permissions of the file that the
link leads to. That file is left as it was. This means that another name for it does not see the change.

A change that cannot be made is refused before anything is written, and the study takes no number for it. This is
the case for a section that the file does not have, for a value that is not one line of text, and for a text that
does not occur. It is also the case for a file that is not in the suite, or that is reached through a symbolic
link to a directory, since writing it would change a file in the directory that the link leads to.
[Troubleshooting](#troubleshooting) lists the messages.

ACCESS-rAM3 states the walltime of its jobs in `site/nci-gadi/suite-adds.rc`, and not in a variable of
`rose-suite.conf`. `generate_scaling_experiments` therefore refuses to write a walltime for it. Instead, you
generate its experiments with [`generate_experiments`][access.profiling.manager.ProfilingManager.generate_experiments],
with one [`ConfigurationChoice`][access.profiling.experiment.ConfigurationChoice] for each layout that
`select_layouts` returns. The walltime is then a text to replace, among the control options. For example, with a
`manager` created with `RAM3`:

```python
from access.config.parallel_allocation_strategies import WholeRangeAllocation

from access.profiling import ConfigurationChoice

allocations = RootAllocation(subcomponents={"UM rs01": WholeRangeAllocation(), "UM rs02": WholeRangeAllocation()})
layouts = manager.select_layouts(576, allocations=allocations)
walltime = {"site/nci-gadi/suite-adds.rc": {"-l walltime = 3600": "-l walltime = 6:0:0"}}
manager.generate_experiments([ConfigurationChoice(layout=layout) for layout in layouts], control_options=walltime)
```

The walltime is in the control options, and not in the choices, so these experiments are those of their layouts,
as the experiments of a scaling study are. Calling `generate_scaling_experiments` or `generate_experiments` again
for the same layouts creates nothing new.

### Where the experiments are

Each experiment is a directory of the working directory, named after the experiment. It holds the record of the
experiment and its copy of the suite. This is how the working directory of our study looks:

```
/scratch/tm70/model_scaling/ACCESS-AM3/release-n96e-3.0/
├── access-profiling-study.json      the study file
├── config/                          the control suite
├── exp-0001/
│   ├── access-profiling.json        the record of the experiment
│   └── n96e-541dd259-exp-0001/      its copy of the suite
└── exp-0002/
    └── ...
```

The suite of each experiment is named after the application, `n96e`, 8 hexadecimal digits that identify the working
directory, and the experiment. Why such a long name? `rose suite-run` names a suite after the directory it is run
from, and Cylc runs all your suites in `~/cylc-run/<suite>`. Every study numbers its experiments from `exp-0001`, so
the digits keep two studies from running in the same directory. They are the same in every session of a study.
This is also why the name of an application has to be made of letters, digits, `_`, `.` and `-`.

The run directory of each experiment is `~/cylc-run/<suite>`, as for any suite. Where the suite sets a `root-dir`,
Rose puts the run there, and makes `~/cylc-run/<suite>` a link to it. The copy leaves out the directories of the
version control system, `.git` and `.svn`. It keeps the symbolic links of the suite as links, other than those of
the files that the changes name, and the files keep their permissions.

## Running the suites

[`run_experiments`][access.profiling.cylc_manager.CylcRoseManager.run_experiments] runs every NEW suite with
`rose suite-run`, from the directory of its copy. We run the suites of our study:

```python
manager.run_experiments()
```

The manager then checks how each suite is going by reading the database in its run directory, as described in
[Experiments and their statuses](../user-guide/index.md#experiments-and-their-statuses). A suite fails as soon as
one of its tasks fails. You can also run a suite by hand, with `rose suite-run` in its directory, and the manager
follows it in the same way.

## Running failed suites again

To run the failed suites again, use `run_experiments(retry_failed=True)`. A failed suite is run with
`rose suite-run --new`, which first deletes its run directory, as `rose suite-clean` does. This means that the
suite starts again from the beginning, and that the logs and the database of the failed run are lost. Look at them
before you run the suite again.

If a run failed because of one of its changes, such as a walltime that was too short, correct it and create the
failed experiments again, as
[Managing experiments](../user-guide/managing-experiments.md#running-failed-experiments-again) describes. For
example, we give every suite twice as long:

```python
manager.generate_scaling_experiments(
    [3, 6, 12],
    cores_per_node=48,
    walltime=4.0,
    allocations=allocations,
    control_options=control_options,
    regenerate_failed=True,
)
manager.run_experiments(retry_failed=True)
```

The changes are written into the copy that each failed experiment already has. Its Rose configurations are edited
where they are, so a correction that you made there by hand is kept. Its other files are copied from the control
again before their texts are replaced. This is because replacing a text a second time can change it twice: for
example, replacing `3600` by `36000` a second time gives `360000`.

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
[Scaling studies](../user-guide/scaling-studies.md). Our study has several suites for each number of CPUs, so we
first keep the best of them with
[`select_best_experiments`](../user-guide/scaling-studies.md#selecting-the-best-layouts). For example, we plot the
time that the `atmos_main` task of ACCESS-AM3 took for the first cycle of the run:

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
best = manager.select_best_experiments("cylc_tasks", "atmos_main_cycle19820101T0000Z", tmax)
fig = manager.plot_scaling_data(groups, tmax, experiments=best, xlabel="Cores")
```

The group names the component of its regions, the UM. The logs of a suite do not say which component they belong
to, so only the group can say it. The scaling plot does not need it, but the component scaling plot does: it plots
each region against the cores of its component, which the manager reads from the layout of each suite. Where
several suites give the UM the same number of cores, it keeps the fastest of them. We plot the same task against
the cores of the UM:

```python
fig = manager.plot_component_scaling_data(groups, tmax, xlabel="Cores of the UM")
```

The UM is the only component of ACCESS-AM3, so the two plots differ only where a job asked PBS for more cores than
the UM puts to work. This is the case for a suite with an I/O server, and for a job that asked for whole nodes.
For example, a process grid of 16 by 16 puts 256 cores to work, which are not a whole number of Gadi's nodes, of
48 or 104 cores. A job of that suite that asked for whole nodes therefore asked for more.

You can also compare suites that differ in something other than their layout, such as their OpenMP threads or
their I/O server. Generate them with `generate_experiments`, with one configuration choice for each, and compare
them region by region, as in [Comparing experiments](../user-guide/comparing-experiments.md).

Tables and plots can also use a quantity of the layout of each suite as a coordinate. To plot the time against the
cores of the UM, we pass [`component_cores("UM")`][access.profiling.manager.component_cores] to
[`plot_metric`][access.profiling.manager.ProfilingManager.plot_metric]:

```python
from access.profiling.manager import component_cores

fig = manager.plot_metric(groups, tmax, component_cores("UM"), xlabel="Cores of the UM")
```

The labels of the suites work in the same way: pass the key of a label in place of `component_cores("UM")`.
[Configuration choices](../user-guide/configuration-choices.md#plotting-against-a-setting) describes this plot, and
the tables that [`gather`][access.profiling.manager.ProfilingManager.gather] makes from the same coordinates.

## Archiving

[`archive_experiments`][access.profiling.cylc_manager.CylcRoseManager.archive_experiments] puts each suite and its
run directory in the same archive, under `experiment/` and `runs/`. By default, `.svn` and `share` directories and
`*.nc` files are not archived. Symlinks are not followed either. This means that a symlink that points outside the
directory of the suite, or outside its run directory, is left out.

## Deleting experiments

[`delete_experiments`][access.profiling.manager.ProfilingManager.delete_experiments] deletes the copy of each suite
and its run directory, together with its record. Where the run directory is a link, as Rose makes it for a
`root-dir`, the directory that it leads to is deleted, and then the link. The control suite is never deleted.
[Managing experiments](../user-guide/managing-experiments.md#deleting-experiments) describes the rest.

## Suites prepared by hand

You can also register a suite that you have prepared yourself, such as a copy of the control that you changed by
hand. Put it in the working directory, and register it with
[`add_rose_experiment`][access.profiling.cylc_manager.CylcRoseManager.add_rose_experiment]. It needs the name of
the directory of the suite in the working directory, and the directory where Cylc runs the suite. This is usually
`~/cylc-run/<suite>`, as in the example, so adapt it if Cylc runs your suites somewhere else. The directory does
not have to exist yet:

```python
suite = "am3-n96e-24x24-io0"
manager.add_rose_experiment(suite, run_path=Path.home() / "cylc-run" / suite, labels={"io_ranks": 0})
```

The `labels` are optional. They are keys and values of your own, which say what each suite is. They must be JSON
values, such as strings, numbers and lists, since they are kept in the record of the suite. Here we record the
number of ranks of the I/O server, which a table or a plot can use as a coordinate.

The status of each suite is read from its run directory. A suite is DONE if all its tasks succeeded, FAILED if
one of them failed, RUNNING if some tasks are still going, and NEW if it has not run. Note that if you register a
suite without its run directory, it stays NEW, because the manager has nowhere to read its status from.

The experiment has the name of the suite, which is the name of its directory in the working directory. It can be
anything without a `/`, except a name of the form `exp-<digits>`, which is how generated experiments are named. The
manager writes a record into the directory of the suite, with the run directory you gave. You therefore register
each suite only once: a manager created in a later session finds the suites again from their records.
[Managing experiments](../user-guide/managing-experiments.md#what-the-manager-keeps-on-disk) describes the record.
Note that a suite prepared by hand is never created again by `regenerate_failed`, since the manager does not know
what changes it has.

## Troubleshooting

These are the messages you may see when you generate the experiments of a suite, and what they mean:

| Message | What it means |
|---|---|
| `The control suite is not in ...: there is no rose-suite.conf there. ...` | The manager has no suite to copy. Check the suite out into the directory that the message names, and try again. |
| `... states its walltime in no variable of rose-suite.conf, ...` | The suite states its walltime in another file, as ACCESS-rAM3 does. Generate its experiments with `generate_experiments`, as in [The changes of an experiment](#the-changes-of-an-experiment). |
| `... has no section [...] in use, ...` | The file has no such section, or ignores it. Check the name of the section in the file. |
| `The changes to ... replace '...', which does not occur in it. ...` | The control writes the text in another way, or an earlier change has replaced it already. Copy the text from the file. |
| `The changes set ... to ..., which is not a string. ...` | A value of a Rose configuration is given as a number. Give it as a string, as the file is to state it. |
| `The changes name the file ..., which is reached through the symbolic link ... to a directory. ...` | Writing the file would change a file in the directory that the link leads to. Name the file through that directory instead, if it is in the suite. |
| `The name of the application, '...', cannot begin the name of a suite, ...` | The name has a character that the name of a directory of a suite cannot have. Give the configuration another name. |

## Other suites

You can profile other suites without writing any code. All the package needs to know is which codes the suite
runs, which variables of its `rose-suite.conf` set the layout of each, and which parsers read the logs of its
tasks. This is given as a [`RoseSuiteConfiguration`][access.profiling.rose_configuration.RoseSuiteConfiguration],
put together from one component for each code. For example, for a suite that runs one UM over a grid of 192 by 144
points, and sets its process grid with `ATM_PROCX` and `ATM_PROCY`, its OpenMP threads with `ATM_OMPTHR` and its
walltime with `ATM_CLOCK`:

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
    walltime_variable="ATM_CLOCK",
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

The `walltime_variable` of the configuration is the variable of `[jinja2:suite.rc]` that sets the walltime of the
job that runs the layout. The manager writes the walltime there as an ISO 8601 duration, such as `'PT2H00M'`.
Without one, the manager refuses to write a walltime for the suite, as it does for ACCESS-rAM3. A variable is used
for one setting only, so a configuration that names the same variable twice is refused.

A code that is given a number of ranks and nothing else is a
[`RoseRanks`][access.profiling.rose_configuration.RoseRanks], with the name of its variable. This is how
[`CM3_O25KM`][access.profiling.models.um_suites.CM3_O25KM] gives their cores to the ocean and the sea ice of
ACCESS-CM3. The components of a suite run side by side, each on cores of its own, and their cores add up. If they
run instead as jobs of their own, one after the other, they share the cores: pass
`core_sharing=CoreSharing.SHARED`, with `CoreSharing` from `access.config.parallel_component`, as
[`RAM3`][access.profiling.models.um_suites.RAM3] does for its two nests.
