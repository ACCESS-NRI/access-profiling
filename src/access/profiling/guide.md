# Writing a study notebook

This guide is for anyone who writes a Jupyter notebook that runs a profiling study with access-profiling, coding
agents included. It says how to lay the notebook out, so that a person can read it from the top and change the study
by changing a line. You can print it in a terminal with `python -m access.profiling guide`. The user guide, at
<https://access-profiling.readthedocs.io>, explains each step at length.

## One decision per line

A notebook states the decisions of a study. These are the application and its control, the directories, the control
options, the allocation strategy, the sizes, the walltime, and the regions, tables and plots to look at. Everything
else is the package's to work out. In particular:

- Write no experiment name, such as `exp-0003`. The experiments are numbered, so their names say nothing about them.
  Pick experiments out by a coordinate instead, with `select_experiments` and `select_best_experiments`. A
  coordinate is a label, `"ncpus"`, or a quantity of the parallelisation layout, such as `component_cores("ocn")`.
  The names of the experiments to delete, such as those that failed, are taken from the table of `status`.
- Write no path to an experiment that the manager generated, since it finds them again from their records. The one
  path under the working directory that a notebook names is that of an experiment run some other way, which it adds
  with `add_experiment_from_directory`.
- Set no attribute of the manager in a cell of its own. What a call needs is an argument of that call, such as the
  `nruns` of `run_experiments`, or of the manager, such as its `control_options`.
- Configure no logging, and use no `%autoreload` and no loop that waits for jobs. Generating, running and archiving
  return a table of the experiments they acted on, warnings are shown without any configuration, and
  `manager.status()` says how the runs are going whenever you ask.
- Do not repeat a change that the application already makes to the control. Printing the manager shows these
  changes, and the control options win where both change the same setting.

## The kinds of study

Each kind of study starts with one call:

| Study | The call that plans it |
|---|---|
| A scaling study, of the run time over several numbers of cores | `plan_scaling_experiments`, with several sizes in `num_nodes_list` |
| The best layout for a budget | `plan_scaling_experiments`, with one size |
| A setting varied across the sizes, such as a time step | One `plan_scaling_experiments` for each value, with its `changes` and `labels` |
| A comparison of configurations, on given layouts | `plan_experiments`, with one `ConfigurationChoice` for each configuration |
| A comparison of experiments that were run some other way | `add_experiment_from_directory`, or `add_rose_experiment` for a Rose suite |
| The variability from one run to the next | Any of the above, run with `run_experiments(nruns=3)` with Payu |

For ACCESS-OM3, `block_size_choices` makes the choices of a study of CICE6's block size.

## The cells, in order

Each code cell does one thing, and has a short Markdown cell before it that says what the cell decides and why. A
code cell ends with the value to show, such as the plan or a table, rather than with a `print`. The code cells of a
scaling study of ACCESS-OM3 at 100 km show the order. The directories are only examples, so adapt them to your own
project.

The first cell imports what the notebook uses, and records the versions it runs with:

```python
from access.config import FreeAllocation, MaxWastedCoreFractionConstraint, RankRatioGroupConstraint, RootAllocation

from access.profiling import PayuManager, Region, show_versions
from access.profiling.models.om3 import OM3_MC_100KM, OM3_MC_100KM_JRA_RYF_SOURCE

show_versions()
```

The manager states the application, its control, the directories and the control options. Ending the cell with
`manager` shows the application, the control, the working and archive directories and the control options, with the
changes that the application makes to the control itself:

```python
manager = PayuManager(
    work_dir="/scratch/tm70/model_scaling/ACCESS-OM3/MC_100km_jra_ryf/work",
    archive_dir="/g/data/tm70/model_scaling/ACCESS-OM3/MC_100km_jra_ryf/archive",
    application=OM3_MC_100KM,
    control=OM3_MC_100KM_JRA_RYF_SOURCE,
    lab_dir="/scratch/tm70/model_scaling/ACCESS-OM3/MC_100km_jra_ryf/lab",
    control_options={"config.yaml": {"postscript": "REMOVE", "mem": "REMOVE"}},
)
manager
```

The control sources of the package name their repositories by SSH, which needs an SSH key registered with GitHub.
Without one, give a `GitControlSource` with the repository's HTTPS URL instead.

The allocation strategy says how many cores each component may get, and which layouts to leave out. The mediator, the
data atmosphere, the sea ice and the data runoff take turns on the cores of `shared`. A component under `shared` that
the strategy does not name takes all of those cores, so these four need no line of their own. The strategy is:

```python
allocations = RootAllocation(
    subcomponents={
        "ocn": FreeAllocation(min_core_fraction=0.85, core_step=8),
        "shared": FreeAllocation(min_core_fraction=0.05, core_step=8),
    },
    local_constraints=(MaxWastedCoreFractionConstraint(max_fraction=0.01),),
    group_constraints=(RankRatioGroupConstraint(name_a="ocn", name_b="shared", min_ratio=9.0),),
)
```

The plan says what generating it creates, and creates nothing. Ending the cell with `plan` shows the number of
experiments, and their sizes, layouts and walltimes. Every experiment is a job, so read the number before generating
the plan, and make the strategy stricter if it is more than the study needs:

```python
plan = manager.plan_scaling_experiments(
    num_nodes_list=[1, 2, 3, 4],
    cores_per_node=104,
    walltime=lambda num_nodes: 4.0 / num_nodes,
    allocations=allocations,
)
plan
```

Generating the plan creates its experiments, in a cell of its own:

```python
manager.generate(plan)
```

Running them submits their jobs, in a cell of its own too:

```python
manager.run_experiments()
```

The status says how the runs are going. It asks the workflow engine each time, so this cell can be run as often as
you like:

```python
manager.status()
```

The regions are what the tables and the plots show. Each is one line, with the label that the tables and the plots
give it, and the component whose cores it is plotted against:

```python
whole = Region("ESMF", "[ensemble] RunPhase1", label="Whole model", component="ACCESS-OM3 MC-100km")
ocean = Region("ESMF", "[OCN] RunPhase1", label="MOM6", component="ocn")
sea_ice = Region("ESMF", "[ICE] RunPhase1", label="CICE6", component="ice")
```

Parsing returns the results, which never change: a selection returns new results. Here the cell keeps the fastest
experiment of each size, and shows the times of the regions:

```python
results = manager.parse_profiling_data()
best = results.select_best_experiments(by=whole)
best.table([whole, ocean, sea_ice], "ncpus")
```

Each plot is then a cell of its own. The scaling plot draws the speedup and the efficiency of the whole model:

```python
best.plot_scaling_data(whole, xlabel="Cores occupied by the job");
```

The component plot draws each region against the cores of its own component:

```python
ax = best.plot_component_scaling_data([whole, ocean, sea_ice])
ax.set(xlabel="Cores of the component", ylabel="Maximum time (s)");
```

A plot returns the axes it drew on, and shows nothing itself. The notebook shows the figure at the end of the cell,
and the semicolon keeps it from also printing the axes. The finished experiments are archived last, in a cell of its
own:

```python
manager.archive_experiments()
```

## In a later session

A new session runs the cells of the imports, the manager, the strategy and the plan again. These create nothing,
and the plan then counts the experiments that the study already has. Generating the plan again creates nothing twice,
and running the experiments again submits only those that have not run. Parsing, and the tables and plots after it,
can be run again whenever more runs have finished.

## Cells that act on the study

Generating, running, archiving, adding, forgetting and deleting act on the study: they submit jobs that are charged to
a project, or write and delete files. An agent writes these cells, and leaves running them to a person, unless it is
asked to run them. Planning, the status, parsing, the tables and the plots change nothing, so they can be run at any
time.
