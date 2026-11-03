# Managing experiments

Once the experiments of a study have been submitted, the manager follows them through to the end. In this tutorial
we check on them, pick a study up again in a later session, run failed experiments again, and archive and delete
experiments, with any workflow engine. We use the `manager`, `allocations` and `control_options` of
[Generating and running experiments](experiments.md). What is particular to an engine is described on its page:
[Experiments with Payu](../access/payu.md) and [Cylc Rose suites](../access/cylc-rose-suites.md).

The manager reports what it does through Python's `logging` module, and only its warnings are shown by default.
To follow its progress as well, turn on its INFO messages:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

## Following the experiments

[`update_statuses`][access.profiling.manager.ProfilingManager.update_statuses] reads what became of every run from
the records the workflow engine leaves behind, and `manager.experiments` holds the experiments by name. Bring the
statuses up to date and print them:

```python
manager.update_statuses()
for name, experiment in manager.experiments.items():
    print(name, experiment.status.name)
```

This prints the name and the status of every experiment. The status is what the records say of the latest run:

| Status | Meaning |
|---|---|
| NEW | The manager has not submitted the experiment, and the workflow engine has no record of a run. |
| RUNNING | The experiment was submitted, and its latest run has not recorded an outcome yet. |
| DONE | The latest run succeeded. Only DONE and ARCHIVED experiments are parsed. |
| FAILED | The latest run failed, or its job ended before Payu could record an outcome. |
| ARCHIVED | The experiment has been archived, and is read from its archive from then on. |

Payu keeps those records in [a job file for every run](../access/payu.md#following-the-experiments), and Cylc in
the database of each suite.

## Picking a study up in a later session

The statuses are held in memory, so a manager created in a new session only knows about the experiments it can
find. It finds the archived ones in its archive directory by itself. To get the others back, create the manager as
before and call `generate_scaling_experiments` again with the same arguments. The experiments come back with the
status NEW. `update_statuses`, which `run_experiments` and `parse_profiling_data` also call first, then reads what
became of their runs, so an experiment that has already run is not submitted a second time.

An experiment created some other way can be added with
[`add_experiment_from_directory`][access.profiling.manager.ProfilingManager.add_experiment_from_directory]. Its
directory must be inside the working directory, and the experiment is taken to have finished. For example, to add
an experiment set up by hand:

```python
from pathlib import Path

manager.add_experiment_from_directory(
    "piControl-by-hand",
    Path("/scratch/tm70/model_scaling/ACCESS-ESM1.6/piControl/piControl-by-hand"),
)
```

[Comparing experiments](comparing-experiments.md) compares experiments added this way with each other, and with the
ones the manager generated.

## Running failed experiments again

Once you know the cause of a failure, correct it and generate the failed experiments again:

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

With `regenerate_failed=True`, the changes of every failed experiment are worked out afresh from the arguments of
the call and applied to it, while the experiments that are running or have finished are left alone. A regenerated
experiment still reads as FAILED until it runs again. `run_experiments(retry_failed=True)` then submits the failed
experiments as well as the new ones. [Experiments with Payu](../access/payu.md#running-failed-experiments-again)
says where Payu leaves the outputs of a failed run, and how it clears a run that had archived output.

A failure that needs no change to the experiment, such as a node failure, only needs
`run_experiments(retry_failed=True)`.

## Archiving

[`archive_experiments`][access.profiling.manager.ProfilingManager.archive_experiments] writes every DONE
experiment to a `<experiment>.tar.gz` file in the archive directory and marks it ARCHIVED. From then on the
experiment is parsed from its archive, and a manager created later finds it there. The experiment's directory is
left as it is.

`exclude_dirs` and `exclude_files` leave out the directories and files inside the experiment that match their
patterns, and `follow_symlinks` decides whether the targets of symlinks are archived in their place. A symlink to
something inside the experiment is kept as a symlink, so that what it points to is archived once. A symlink to
something outside the experiment is archived as what it points to when symlinks are followed. Otherwise it is left
out, with an INFO message, since the archive could not be extracted with it. Every manager has defaults of its own,
given on its page: [Payu](../access/payu.md#archiving) and
[Cylc Rose suites](../access/cylc-rose-suites.md#archiving).

## Deleting experiments

[`delete_experiment`][access.profiling.manager.ProfilingManager.delete_experiment] only makes the manager forget
an experiment, and deletes nothing on disk.
[`delete_experiments`][access.profiling.manager.ProfilingManager.delete_experiments] deletes experiments for good,
with whatever their workflow engine needs to remove them. We recommend trying it first with `dry_run=True`, which
only reports what would be deleted:

```python
manager.delete_experiments(experiments=["esm1p6-layout_piControl_atm_18x14_mom_16x16_ice_12"], dry_run=True)
manager.delete_experiments(all_experiments=True)
```

[Experiments with Payu](../access/payu.md#deleting-experiments) describes what the Payu manager removes.

## Troubleshooting

These are the messages you may see, and what they mean:

| Message | What it means |
|---|---|
| `No layouts found for N nodes (C cores). Check the bounds and the constraints of the allocation strategy.` | The strategy admits no layout at that size. Loosen its bounds or constraints, which [Counting the layouts](experiments.md#counting-the-layouts) shows how to check. |
| `More than M layouts found for C cores. Only the first M enumerated will be used, ...` | `max_layouts` truncated the search, keeping the first layouts found rather than the best ones. Tighten the strategy instead. |
| `Experiment <name> already exists. Skipping addition.` | Generation found an experiment the manager already holds. Expected when generating a study again. |
| `No new experiments to run. Will skip execution.` | Every experiment has been submitted or has finished. Pass `retry_failed=True` to run the failed ones. |
| `Experiment '<name>' is not completed (status: RUNNING). Skipping parsing profiling data.` | Only finished experiments are parsed. Wait for the run to finish. |
| `Profiling data still has a 'run' dimension. ...` | The experiments ran several times. See [Reducing the runs](run-to-run-variability.md#reducing-the-runs). |
| `Several selected experiments use the same number of CPUs ...` | See [Selecting the best layouts](scaling-studies.md#selecting-the-best-layouts). |

The messages of a particular engine are listed on its page, such as
[Experiments with Payu](../access/payu.md#troubleshooting).
