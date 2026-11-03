# Experiments with Payu

[`PayuManager`][access.profiling.payu_manager.PayuManager] is the manager for applications run with
[Payu](https://payu.readthedocs.io/en/stable/), as the ACCESS-ESM1.6 and ACCESS-OM3 presets are. It generates
their experiments with [experiment-generator](https://access-experiment-generator.access-hive.org.au/), submits
them on NCI's Gadi with [experiment-runner](https://access-experiment-runner.access-hive.org.au/), and reads what
Payu records about every run. [Generating and running experiments](../user-guide/experiments.md) goes through a
study with it, and [Managing experiments](../user-guide/managing-experiments.md) follows the study to its end. What
is particular to Payu is described here, with the `manager` and the `control_options` of the first of those pages.

## Before you start

To run a study, you need:

- An NCI project with enough compute allocation for every experiment of the study. Each experiment is a PBS job,
  and a study easily has hundreds of them.
- A working directory on `/scratch`, where the experiments are generated and run, and an archive directory on
  `/g/data`, where the finished experiments are kept.
- Access to the control's repository. The presets name their releases by `git@github.com:` URLs, which need an
  SSH key registered with GitHub on Gadi. If you have none, pass a control with the repository's HTTPS URL instead:
  `GitControlSource("https://github.com/ACCESS-NRI/access-esm1.6-configs.git", "release-piControl-2.1")`.
- An environment with access-profiling installed, as described in [Getting started](../getting-started.md#on-gadi),
  on a login node or in an ARE session.

## Generating the experiments

The Payu manager takes one option of its own in
[`generate_scaling_experiments`][access.profiling.manager.ProfilingManager.generate_scaling_experiments]:
`control_options`, the changes made to the control before any experiment is created from it, keyed by file. They
follow the format of [experiment-generator](https://access-experiment-generator.access-hive.org.au/), in which
`"REMOVE"` deletes a key. [Generating the experiments](../user-guide/experiments.md#generating-the-experiments) shows
the options the releases of ACCESS-ESM1.6 and ACCESS-OM3 need.

The control is cloned into the working directory, under `config`, and the control options are committed there on
a branch named `ctrl`. Every experiment is then a branch of `ctrl`, named after its layout, carrying the changes the
configuration writes for that layout, its walltime and its name.

## Running the experiments

[`run_experiments`][access.profiling.payu_manager.PayuManager.run_experiments] submits every NEW experiment through
experiment-runner. Each experiment's branch is cloned into a directory of its own, `<work_dir>/<experiment>/config`,
from which Payu runs it, and `payu run` submits the PBS job. Payu keeps the outputs of each run in the experiment's
archive, under `archive/outputNNN`, and its job records under `archive/payu_jobs`.

The experiments start from the initial conditions of the control. To start every experiment from a restart
directory of another experiment's archive instead, set `startfrom_restart` to `"<experiment>/restartNNN"` before
running them. It is `"cold"` by default.

### Running an experiment several times

[`nruns`][access.profiling.payu_manager.PayuManager.nruns] sets how many times each experiment runs, 1 by default,
and the runs an experiment has not archived yet are submitted together, as one `payu run -n <runs>`. Payu makes
those runs successive: each continues the simulation from the restart files of the one before. The runs therefore
time consecutive periods of the same length, which differ in what is simulated as well as in where they run.

Payu's `repeat` option starts every run from the initial conditions of the first instead, so that all the runs time
the same simulation. It is a setting of `config.yaml`, and so goes in the control options. To turn it on:

```python
control_options["config.yaml"]["repeat"] = True
```

A repeat run keeps no restart files, and its outputs are still numbered `output000`, `output001` and so on.

!!! warning "Set `repeat` when the study is first generated"
    The control options are applied to the control every time the study is generated, but an experiment that
    already exists keeps the control it was made from. Set `repeat` the first time the study is generated. Payu
    also refuses to start a repeat run when restart files are archived, so keep `startfrom_restart` at its default,
    `"cold"`.

[Run-to-run variability](../user-guide/run-to-run-variability.md) measures how much the timings of the runs vary,
and reduces them to one value.

## Following the experiments

Payu writes a job file for every run of an experiment, under `archive/payu_jobs`, and records whether the run
succeeded in it as it finishes. The status of an experiment is what the job file of its latest run says: NEW when
there is none, RUNNING until the run records an outcome while PBS still holds its job, and then DONE or FAILED.

A run that PBS kills, because it ran out of walltime or lost a node, never gets to record its outcome. For a run
without one, the manager therefore asks PBS about its job, with `qstat -x`. A job that PBS reports as finished, or
no longer knows, was killed, and the experiment reads as FAILED. Note that where `qstat` cannot be run or does not
answer, such an experiment goes on reading as RUNNING.

In a later session, generating the study again with the same arguments brings its experiments back, as
[Managing experiments](../user-guide/managing-experiments.md#picking-a-study-up-in-a-later-session) describes. The
experiment generator finds their branches already there and applies the same changes to them again, which changes
nothing.

## Running failed experiments again

The outputs of a failed run are in the experiment's directory, `<work_dir>/<experiment>/config`, and in its PBS
output files. Regenerating a failed experiment applies its changes to its branch afresh. A failed experiment that
had already archived some output is swept with `payu sweep --hard` before it is submitted again. That removes
everything its earlier runs archived, so that it runs again from the start.

## Archiving

The Payu manager leaves out `.git` and `restart*` directories and `*.nc` files by default, and follows symlinks, so
that the outputs Payu keeps in its laboratory are archived with the experiment. Note that passing `exclude_files`
or `exclude_dirs` replaces the defaults, so repeat them when adding to them. The ACCESS-ESM1.6 study of
[access-model-scaling](https://github.com/ACCESS-NRI/access-model-scaling), for example, also leaves out the
UM's output files:

```python
manager.archive_experiments(exclude_files=["*.nc", "aiihca*"])
```

## Deleting experiments

[`delete_experiments`][access.profiling.payu_manager.PayuManager.delete_experiments] runs `payu sweep --hard` in
each experiment it deletes, which removes what Payu keeps for it in its laboratory, and then deletes its directory.

With `remove_repo_dir=True`, the clone of the control under the working directory is deleted as well, once
nothing needs it. That clone holds the branch of every experiment of the study, and the experiments' own
directories are cloned from it. It is therefore kept while an experiment left is NEW, RUNNING or FAILED, and so
may still be run, and the manager logs which experiments kept it at INFO.

## Troubleshooting

These are the messages of Payu and of experiment-runner you may see, and what they mean:

| Message | What it means |
|---|---|
| `-- You have duplicated runs for ... hence not submitting this job!` | A job of that experiment is still queued or running, so experiment-runner does not submit another. |
| `The control '<label>' states no start point. ...` | The Payu manager branches every experiment from a particular revision of the control. Give an `ExistingDirectoryControlSource` its `revision`. |
