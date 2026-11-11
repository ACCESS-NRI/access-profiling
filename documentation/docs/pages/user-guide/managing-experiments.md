# Managing experiments

Once the experiments of a study have been submitted, there are still a few things to do. In this tutorial we
check how the experiments are going, come back to a study in a later session, rerun the experiments that failed,
and archive and delete experiments. All of this works in the same way for every workflow engine. We use the
`manager`, `allocations` and `control_options` of [Generating and running experiments](experiments.md). The
details that are specific to one workflow engine are on its own page:
[Experiments with Payu](../access/payu.md) and [Cylc Rose suites](../access/cylc-rose-suites.md).

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

## Picking a study up in a later session

A study often takes longer than one Python session. The manager only keeps the statuses in memory, so when you
create it again in a new session, it does not know about your experiments any more. It finds the archived ones by
itself, in its archive directory. To get the other experiments back, create the manager as before and call
`generate_scaling_experiments` again with the same arguments. Do not worry, nothing is created or submitted twice.
The experiments come back with the status NEW, and the manager then checks how their runs went. This is done by
`update_statuses`, which `run_experiments` and `parse_profiling_data` also call first.

It is also possible to add an existing experiment that was created in some other way, with
[`add_experiment_from_directory`][access.profiling.manager.ProfilingManager.add_experiment_from_directory]. The
directory of the experiment must be inside the working directory, and the manager assumes that the experiment has
finished. For example, to add an experiment that was set up by hand:

```python
from pathlib import Path

manager.add_experiment_from_directory(
    "piControl-by-hand",
    Path("/scratch/tm70/model_scaling/ACCESS-ESM1.6/piControl/piControl-by-hand"),
)
```

Adapt the name and the path to your own experiment. You can then compare these experiments with each other, and
with the ones the manager created, as shown in [Comparing experiments](comparing-experiments.md).

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

With `regenerate_failed=True`, the failed experiments are updated using the new arguments. The experiments that
are running or that have finished are not touched. Note that an updated experiment keeps the status FAILED until
it runs again. `run_experiments(retry_failed=True)` then submits the failed experiments, together with any new
ones. See [Experiments with Payu](../access/payu.md#running-failed-experiments-again) to find out where Payu
leaves the outputs of a failed run, and how it cleans up before running again.

If the failure had nothing to do with the experiment itself, such as a node failure, there is nothing to fix. In
that case you only need `run_experiments(retry_failed=True)`.

## Archiving

When an experiment has finished, you can archive it. This keeps its results in a safe place, and lets you find
them again in a later session.
[`archive_experiments`][access.profiling.manager.ProfilingManager.archive_experiments] writes every DONE
experiment to a `<experiment>.tar.gz` file in the archive directory and marks it ARCHIVED. From then on, the
manager reads the experiment from its archive. Note that the directory of the experiment is not deleted.

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
an experiment, without deleting anything on disk.
[`delete_experiments`][access.profiling.manager.ProfilingManager.delete_experiments] really deletes the
experiments, including what their workflow engine keeps for them. Since this cannot be undone, we recommend trying
it first with `dry_run=True`, which only shows what would be deleted:

```python
manager.delete_experiments(experiments=["esm1p6-layout_piControl_atm_18x14_mom_16x16_ice_12"], dry_run=True)
manager.delete_experiments(all_experiments=True)
```

See [Experiments with Payu](../access/payu.md#deleting-experiments) for what exactly is deleted with Payu.

## Troubleshooting

If something does not work as expected, the manager usually tells you why. These are the messages you may see,
and what they mean:

| Message | What it means |
|---|---|
| `No layouts found for N nodes (C cores). Check the bounds and the constraints of the allocation strategy.` | The strategy admits no parallelisation layout at that size. Loosen its bounds or constraints, which [Counting the layouts](experiments.md#counting-the-layouts) shows how to check. |
| `More than M layouts found for C cores. Only the first M enumerated will be used, ...` | `max_layouts` truncated the search, keeping the first layouts found rather than the best ones. Tighten the strategy instead. |
| `Experiment <name> already exists. Skipping addition.` | Generation found an experiment the manager already holds. Expected when generating a study again. |
| `No new experiments to run. Will skip execution.` | Every experiment has been submitted or has finished. Pass `retry_failed=True` to run the failed ones. |
| `Experiment '<name>' is not completed (status: RUNNING). Skipping parsing profiling data.` | Only finished experiments are parsed. Wait for the run to finish. |
| `Profiling data still has a 'run' dimension. ...` | The experiments ran several times. See [Reducing the runs](run-to-run-variability.md#reducing-the-runs). |
| `Several selected experiments use the same number of CPUs ...` | See [Selecting the best layouts](scaling-studies.md#selecting-the-best-layouts). |

Some messages are specific to one workflow engine. You can find them on its page, such as
[Experiments with Payu](../access/payu.md#troubleshooting).
