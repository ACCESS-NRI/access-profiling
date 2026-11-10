# Experiments with Payu

[`PayuManager`][access.profiling.payu_manager.PayuManager] is the manager for applications that are run with
[Payu](https://payu.readthedocs.io/en/stable/), such as ACCESS-ESM1.6 and ACCESS-OM3. It creates the experiments with
[experiment-generator](https://access-experiment-generator.access-hive.org.au/), and submits them on NCI's Gadi with
[experiment-runner](https://access-experiment-runner.access-hive.org.au/). It then uses the records that Payu keeps of
every run. If you are new to the package, start with [Generating and running experiments](../user-guide/experiments.md)
and [Managing experiments](../user-guide/managing-experiments.md), which go through a whole study with Payu. Here you
will find the details that are specific to Payu. The examples below are based on the `manager` and the `control_options`
of the first of those pages.

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

With Payu, [`generate_scaling_experiments`][access.profiling.manager.ProfilingManager.generate_scaling_experiments]
takes one extra option, `control_options`. These are changes that are applied to the control before the experiments are
created, given for each file. They use the format of
[experiment-generator](https://access-experiment-generator.access-hive.org.au/), where `"REMOVE"` deletes a key.
[Generating the experiments](../user-guide/experiments.md#generating-the-experiments) shows the options the releases of
ACCESS-ESM1.6 and ACCESS-OM3 need.

So what happens when the experiments are created? First, the control is cloned into the working directory, under
`config`, and the control options are committed there on a branch named `ctrl`. Then one branch is created from `ctrl`
for each experiment. The branch is named after the parallelisation layout, and contains the changes for that layout, its
walltime and its name.

## Running the experiments

[`run_experiments`][access.profiling.payu_manager.PayuManager.run_experiments] submits every NEW experiment with
experiment-runner. The branch of each experiment is cloned into its own directory,
`<work_dir>/<experiment>/config`. Payu runs the experiment from there, and `payu run` submits the PBS job. Payu
keeps the outputs of each run in the archive of the experiment, under `archive/outputNNN`, and the records of its
jobs under `archive/payu_jobs`.

By default, the experiments start from the initial conditions of the control. It is also possible to start them
from the restart files of another experiment. To do this, set `startfrom_restart` to
`"<experiment>/restartNNN"` before running them. Its default value is `"cold"`.

### Running an experiment several times

You can run each experiment more than once by setting
[`nruns`][access.profiling.payu_manager.PayuManager.nruns], which is 1 by default. The runs that are still missing
are submitted together, as one `payu run -n <runs>`. By default, Payu runs them one after the other, and each run
continues the simulation from the restart files of the previous one. This means that the runs simulate consecutive
periods of the same length. They are not exactly the same simulation.

If you want all the runs to time exactly the same simulation, use the `repeat` option of Payu. Every run then
starts from the same initial conditions as the first one. This is a setting of `config.yaml`, so it goes in the
control options. To turn it on:

```python
control_options["config.yaml"]["repeat"] = True
```

Note that a repeat run does not keep restart files. Its outputs are still numbered `output000`, `output001` and so
on.

!!! warning "Set `repeat` when the study is first generated"
    Changing the control options later has no effect on the experiments that already exist. You therefore have
    to set `repeat` the first time you create the study. Payu also refuses to start a repeat run if there are
    archived restart files, so keep `startfrom_restart` at its default value, `"cold"`.

See [Run-to-run variability](../user-guide/run-to-run-variability.md) to find out how much the timings of the
runs vary, and how to get one value per experiment.

## Following the experiments

How does the manager know the status of an experiment? Payu writes a job file for every run, under
`archive/payu_jobs`, and when the run finishes it records there whether the run succeeded. The manager looks at
the job file of the latest run. If there is none, the experiment is NEW. If the run has not finished and PBS still
has its job, the experiment is RUNNING. Otherwise it is DONE or FAILED.

There is one special case. If PBS kills a run, for example because it ran out of walltime or lost a node, the run
never gets the chance to record that it failed. When a job file has no result, the manager therefore asks PBS
about the job, with `qstat -x`. If PBS says that the job has finished, or does not know the job any more, the job
was killed, and the experiment is FAILED. Note that if `qstat` is not available or does not answer, the experiment
stays RUNNING.

In a later session, you get your experiments back by creating the study again with the same arguments, as
described in [Managing experiments](../user-guide/managing-experiments.md#picking-a-study-up-in-a-later-session).
This is safe: the branches of the experiments already exist, and applying the same changes to them again does
nothing.

## Running failed experiments again

To find out why a run failed, look at its outputs. They are in the directory of the experiment,
`<work_dir>/<experiment>/config`, and in its PBS output files. When you create a failed experiment again, its
changes are applied to its branch once more. If the failed experiment had already archived some output, it is
cleaned with `payu sweep --hard` before it is submitted again. This removes everything that its earlier runs
archived, so the experiment starts again from the beginning.

## Archiving

By default, the Payu manager does not archive `.git` and `restart*` directories and `*.nc` files. It follows symlinks,
so that the outputs that Payu keeps in its laboratory are archived together with the experiment. Note that if you pass
`exclude_files` or `exclude_dirs`, they replace the defaults. If you only want to add to the defaults, you have to
repeat them. The ACCESS-ESM1.6 study of [access-model-scaling](https://github.com/ACCESS-NRI/access-model-scaling), for
example, also leaves out the UM's output files:

```python
manager.archive_experiments(exclude_files=["*.nc", "aiihca*"])
```

## Deleting experiments

For each experiment it deletes,
[`delete_experiments`][access.profiling.payu_manager.PayuManager.delete_experiments] first runs
`payu sweep --hard`, which removes what Payu keeps for the experiment in its laboratory. It then deletes the
directory of the experiment.

With `remove_repo_dir=True`, the clone of the control in the working directory is also deleted, but only when it
is safe to do so. This clone has the branches of all the experiments of the study, and the experiments are cloned
from it. For this reason it is kept as long as there are experiments that may still be run, which are the ones
that are NEW, RUNNING or FAILED. When this happens, the manager tells you which experiments they are, in an INFO
message.

## Troubleshooting

These are the messages of Payu and of experiment-runner you may see, and what they mean:

| Message | What it means |
|---|---|
| `-- You have duplicated runs for ... hence not submitting this job!` | A job of that experiment is still queued or running, so experiment-runner does not submit another. |
| `The control '<label>' states no start point. ...` | The Payu manager branches every experiment from a particular revision of the control. Give an `ExistingDirectoryControlSource` its `revision`. |
