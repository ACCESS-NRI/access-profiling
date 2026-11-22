# Managing experiments

Once the experiments of a study have been submitted, there are still a few things to do. In this tutorial we check how
the experiments are going, see which experiment is which, come back to a study in a later session, rerun the
experiments that failed, and archive and delete experiments. All of this works in the same way for every workflow
engine. We use the `manager`, `allocations` and `control_options` of [Generating and running
experiments](experiments.md). The details that are specific to one workflow engine are on its own page: [Experiments
with Payu](../access/payu.md) and [Cylc Rose suites](../access/cylc-rose-suites.md).

The manager tells you what it is doing through Python's `logging` module. By default you only see the warnings.
To see more, turn on the INFO messages:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

## Following the experiments

To see how the experiments are going, call
[`update_statuses`][access.profiling.manager.ProfilingManager.update_statuses], which checks how each run went.
The experiments are in `manager.experiments`, by name, so you can then print their statuses:

```python
manager.update_statuses()
for name, experiment in manager.experiments.items():
    print(name, experiment.status.name)
```

This prints the name and the status of every experiment. The status always refers to the latest run:

| Status | Meaning |
|---|---|
| NEW | The manager has not submitted the experiment, and the workflow engine has no record of a run. |
| RUNNING | The experiment was submitted, and its latest run has not recorded an outcome yet. |
| DONE | The latest run succeeded. Only DONE and ARCHIVED experiments are parsed. |
| FAILED | The latest run failed, or its job ended before Payu could record an outcome. |
| ARCHIVED | The experiment has been archived, and is read from its archive from then on. |

The manager gets this information from the workflow engine. Payu keeps it in
[a job file for every run](../access/payu.md#following-the-experiments), and Cylc in the database of each suite.

## Telling the experiments apart

The experiments that the manager creates are numbered, `exp-0001` onwards, in the order they were created. The
name of an experiment therefore says nothing about it. To see which parallelisation layout each experiment runs,
ask the manager for the key of its layout:

```python
for name in manager.experiments:
    print(name, manager.layout_key(name))
```

For the ACCESS-ESM1.6 study of [Generating and running experiments](experiments.md), the first lines are:

```text
exp-0001 esm1p6-layout_piControl_atm_10x5_mom_7x7_ice_4
exp-0002 esm1p6-layout_piControl_atm_10x5_mom_8x6_ice_4
exp-0003 esm1p6-layout_piControl_atm_10x5_mom_7x7_ice_3
```

The key says how each component divides its cores. The layout itself is also there, as
`manager.experiments[name].layout`. Printing the manager shows both the experiments and the keys.

## What the manager keeps on disk

The manager writes two kinds of files, and it is from them that it knows what each experiment is.

The first is the record of an experiment, `<work_dir>/<experiment>/access-profiling.json`. It is written when the
experiment is created or added. It holds the version of its format, and what differs from one experiment to the
next:

| Entry | What it holds |
|---|---|
| `version` | The version of the format of the record. |
| `path` | Where the experiment is, relative to the working directory. |
| `run_path` | Where its runs are, when they are kept somewhere else, as the runs of a Cylc suite are. |
| `complete` | Whether the experiment was added as one that had already finished. |
| `layout` | The layout the experiment was created with, process grids included. |
| `changes` | What the study changed in the control beyond the layout: the changes of a [configuration choice](configuration-choices.md). A scaling study changes nothing more. |
| `labels` | Keys and values of your own, which you can give in a configuration choice or when you add an experiment. |
| `ncpus` | The number of CPUs the job was charged for. It stays `null` in the working directory, and is filled in only in the copy of the record that goes into the archive. |

An entry that is not known is `null`. This is the case for the layout and the changes of an experiment that you
added yourself.

The second is the study file, `<work_dir>/access-profiling-study.json`. It holds the version of its format, the
highest number given to an experiment so far, and the history of the study. The history has one entry for each call
that created, added or deleted experiments:

| Entry | What it holds |
|---|---|
| `when` | When the call was made, in UTC. |
| `call` | The method that was called. |
| `access_profiling_version` | The version of the package that made the call. |
| `manager`, `application` | The class of the manager, and the class and the name of the application. |
| `control` | The control that the experiments were created from. |
| `engine` | What the workflow engine adds. For Payu, this is the laboratory and the control options. |
| `experiments` | The names of the experiments that the call created, added or deleted. |

Note that neither file holds a status. The status of an experiment changes, so the manager always reads it from the
workflow engine.

Each file states the version of its own format, because a record can be read from an archive long after it was
written. The manager checks the version when it reads a file. If the file is in another format, is not a JSON
object or lacks one of its entries, the manager raises a `ValueError` that names the file.

The manager writes both files whole. It writes the new content to a temporary file beside the old one, and then puts
it in its place. This means that a session stopped part way through a write, for example because the disk is full,
leaves the file as it was.

## Picking a study up in a later session

A study often takes longer than one Python session. When you create the manager again in a new session, it finds
your experiments by itself. It registers every experiment of the working directory that has a record, and every
archive of the archive directory. The statuses are only kept in memory, so the experiments of the working directory
come back with the status NEW, and the manager then checks how their runs went. This is done by `update_statuses`,
which `run_experiments` and `parse_profiling_data` also call first.

You can also call `generate_scaling_experiments` or `generate_experiments` again with the same arguments. Do not
worry, nothing is created or submitted twice. The manager recognises the layouts and the configuration choices that
already have an experiment from the records, not from the names of the experiments.

Note that a study has one application. The study file records the application of every call, so the manager
refuses, with a `ValueError`, a study made with another one. Two configurations of a model can run the same
components on the same grids, as the pre-industrial and historical configurations of ACCESS-ESM1.5 do. Without this
check, a manager of one would take the experiments of the other for its own.

It is also possible to add an existing experiment that was created in some other way, with
[`add_experiment_from_directory`][access.profiling.manager.ProfilingManager.add_experiment_from_directory]. The
directory of the experiment must be inside the working directory, and the manager assumes that the experiment has
finished. For example, to add an experiment that was set up by hand:

```python
from pathlib import Path

manager.add_experiment_from_directory(
    "piControl-by-hand",
    Path("/scratch/tm70/model_scaling/ACCESS-ESM1.6/piControl/piControl-by-hand"),
    labels={"set up": "by hand"},
)
```

Adapt the name and the path to your own experiment. The experiment keeps the name you give it. The name is also the
name of a directory, so it cannot contain a `/`. It can be anything else except a name of the form `exp-<digits>`,
which is how the manager names the experiments it creates. The
`labels` are optional. They must be JSON values, such as strings, numbers and lists, since they are kept in the
record. The manager writes a record for the experiment, in `<work_dir>/<name>`, so you only add it once. You can
then compare these experiments with each other, and with the ones the manager created, as shown in
[Comparing experiments](comparing-experiments.md).

!!! note
    Experiments created with an earlier version of the package have no record. Their archives are still found,
    parsed and plotted. Those that are still in the working directory have to be added once, with
    `add_experiment_from_directory`. The manager does not recognise either as the experiment of a layout, so
    `generate_scaling_experiments` creates their layouts again, under new names.

## Running failed experiments again

Sometimes experiments fail. Once you know why, fix the problem and create the failed experiments again:

```python
manager.generate_scaling_experiments(
    num_nodes_list=[1, 2, 3, 4, 5, 6, 8],
    cores_per_node=104,
    walltime=lambda num_nodes: 4 * 4 / num_nodes,  # a longer walltime than the first time
    allocations=allocations,
    control_options=control_options,
    regenerate_failed=True,
)
manager.run_experiments(retry_failed=True)
```

With `regenerate_failed=True`, the failed experiments are updated using the new arguments. The manager first asks the
workflow engine how each run went, so this also finds the experiments that failed in an earlier session. The experiments
that are running or that have finished are not touched. Note that an updated experiment keeps the status FAILED until it
runs again. `run_experiments(retry_failed=True)` then submits the failed experiments, together with any new ones. See
[Experiments with Payu](../access/payu.md#running-failed-experiments-again) to find out where Payu leaves the outputs of
a failed run, and how it cleans up before running again.

If the failure had nothing to do with the experiment itself, such as a node failure, there is nothing to fix. In
that case you only need `run_experiments(retry_failed=True)`.

Note that `regenerate_failed` is only for the experiments of a scaling study. An experiment created from a
[configuration choice](configuration-choices.md) is what its changes say, so corrected changes are another
experiment. Delete the failed experiment and create the corrected one with `generate_experiments`.

## Archiving

When an experiment has finished, you can archive it. This keeps its results in a safe place, and lets you find
them again in a later session.
[`archive_experiments`][access.profiling.manager.ProfilingManager.archive_experiments] writes every DONE
experiment to a `<experiment>.tar.gz` file in the archive directory and marks it ARCHIVED. From then on, the
manager reads the experiment from its archive. Note that the directory of the experiment is not deleted.

The archive starts with the record of the experiment, now with its number of CPUs, so the manager reads both
without extracting the archive. If the number of CPUs cannot be read, the record leaves it `null`, with a warning,
and the experiment is archived all the same. The study file is copied into the archive directory as well. This
means that the history of a study is still there once its working directory is gone.

An archive is written whole, to a temporary file that takes the name of the archive only once it is complete. If
archiving stops part way, no archive is left, and you can simply archive the experiment again. An archive that
cannot be read at all, such as an empty file, is skipped with a warning when the manager is created.

You can choose what goes into the archive. `exclude_dirs` and `exclude_files` leave out the directories and files
inside the experiment that match the given patterns. `follow_symlinks` decides what happens with symlinks. A
symlink that points to something inside the experiment is always kept as a symlink, so its target is only archived
once. A symlink that points to something outside the experiment is replaced by its target when symlinks are
followed. Otherwise it is left out, with an INFO message, because the archive could not be extracted with it. Each
manager has its own defaults, which you can find on its page: [Payu](../access/payu.md#archiving) and
[Cylc Rose suites](../access/cylc-rose-suites.md#archiving).

## Deleting experiments

There are two ways of removing experiments.
[`delete_experiment`][access.profiling.manager.ProfilingManager.delete_experiment] only makes the manager forget
an experiment. It removes the record of the experiment and adds the call to the study file, but deletes none of
the files of the experiment. Note that the archive of an archived experiment therefore stays, and a manager that you
create later finds the experiment again from it. On a study that you cannot write to, such as a colleague's, the
manager forgets the experiment in this session only, with a warning.
[`delete_experiments`][access.profiling.manager.ProfilingManager.delete_experiments] really deletes the
experiments, including what their workflow engine keeps for them. Since this cannot be undone, we recommend trying
it first with `dry_run=True`, which only shows what would be deleted:

```python
manager.delete_experiments(experiments=["exp-0001"], dry_run=True)
manager.delete_experiments(all_experiments=True)
```

The number of a deleted experiment is not given to another one. If you create its layout again, the new experiment
gets the next number.

See [Experiments with Payu](../access/payu.md#deleting-experiments) for what exactly is deleted with Payu.

## Troubleshooting

If something does not work as expected, the manager usually tells you why. These are the messages you may see,
and what they mean:

| Message | What it means |
|---|---|
| `No layouts found for N nodes (C cores). Check the bounds and the constraints of the allocation strategy.` | The strategy admits no parallelisation layout at that size. Loosen its bounds or constraints, which [Counting the layouts](experiments.md#counting-the-layouts) shows how to check. |
| `More than M layouts found for C cores. Only the first M enumerated will be used, ...` | `max_layouts` truncated the search, keeping the first layouts found rather than the best ones. Tighten the strategy instead. |
| `Experiment <name> already exists for layout <key>. Skipping addition.` | Generation found a layout the manager already has an experiment for. Expected when generating a study again. |
| `Experiment <name> already exists for this configuration choice. Skipping addition.` | `generate_experiments` found a choice the manager already has an experiment for. Expected when generating a study again. |
| `Experiment <name> keeps the labels it has, ...` | A choice the manager already has an experiment for was given with other labels. The experiment keeps its own. |
| `Two of the configuration choices are the same configuration: ...` | Two choices of one call have the same layout and the same changes. Give each configuration once. |
| `Both the layout and the configuration choice give a list for <file> > <key>, ...` | A choice with a layout gives a list for a key the application also gives a list for. Leave the list out of the choice. |
| `The changes of a configuration choice must be JSON values...` | The changes or the labels of a choice hold something a record cannot, such as a tuple, a path or NaN. Use lists, strings and finite numbers. |
| `The labels of an experiment must be JSON values...` | The labels of an experiment that you are adding hold something a record cannot, such as a tuple, a path or NaN. Use lists, strings and finite numbers. The experiment is not added. |
| `The study of the working directory '<work_dir>' and the archive directory '<archive_dir>' was made with <class> '<name>', ...` | The manager was created with another application than the one the study was made with. Create the manager with that application, or give it directories of its own. |
| `Experiment '<name>' was given a layout that is not a layout of '<application>', ...` | The manager was created with another application than the one its experiments were generated with, and the study file does not say so, for example because it is missing. Printing the manager shows `Layout: not a layout of <application>` for them. Create the manager with their application, or give it directories of its own. |
| `The record '<file>' states the format version <N>, and this version of access-profiling reads version <M>. ...` | A record or the study file was written in a format that the installed version of the package does not read. Update the package if a later version of it wrote the file. |
| `The record '<file>' cannot be read, since ...` | A record or the study file is damaged, or was not written by the package. Restore the file. Without its record, an experiment is no longer known to the manager, so it has to be added or generated again. Do not remove the study file: the highest number it records is what keeps the numbers of deleted experiments from being given again. |
| `Skipping the archive '<file>', which cannot be read: ...` | An archive is damaged, for example because an archiving with an earlier version of the package stopped part way. Archive the experiment again with `overwrite=True`, or remove the file. |
| `The number of CPUs of experiment '<name>' could not be read (...), ...` | The experiment was archived without its number of CPUs. The manager reads it from the archive when a selection or a plot needs it, and raises an error if it cannot. |
| `Could not update the working directory '<work_dir>' (...), so experiment '<name>' is forgotten in this session only, ...` | `delete_experiment` could not remove the record, for example because you cannot write to the study. A manager created later finds the experiment again. |
| `Experiment name '<name>' is not the name of a single directory, ...` | An experiment was added under a name with a `/`, or under `.` or `..`. Its record is kept in a directory of that name in the working directory, so add it under a name without a `/`. |
| `Experiment name '<name>' is of the form this manager gives the experiments it generates. ...` | An experiment was added under a name such as `exp-0003`. Add it under another name. |
| `No new experiments to run. Will skip execution.` | Every experiment has been submitted or has finished. Pass `retry_failed=True` to run the failed ones. |
| `Experiment '<name>' is not completed (status: RUNNING). Skipping parsing profiling data.` | Only finished experiments are parsed. Wait for the run to finish. |
| `Profiling data still has a 'run' dimension. ...` | The experiments ran several times. See [Reducing the runs](run-to-run-variability.md#reducing-the-runs). |
| `Several selected experiments use the same number of CPUs ...` | See [Selecting the best layouts](scaling-studies.md#selecting-the-best-layouts). |
| `Experiment '<name>' has no label '<key>'. ...` | A table or a plot was asked for a label that one of its experiments does not have. Restrict the selection with `experiments=[...]`. |
| `Experiment '<name>' has the value <value> for '<coordinate>', which is not a number, ...` | `plot_metric` places the experiments along the x-axis by a number. Use the coordinate as the `series`, or draw a bar chart. |

Some messages are specific to one workflow engine. You can find them on its page, such as
[Experiments with Payu](../access/payu.md#troubleshooting).
