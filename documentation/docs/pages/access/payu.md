# Experiments with Payu

[`PayuManager`][access.profiling.payu_manager.PayuManager] is the manager for applications that are run with
[Payu](https://payu.readthedocs.io/en/stable/), such as ACCESS-ESM1.5, ACCESS-ESM1.6, ACCESS-OM2 and ACCESS-OM3. It
creates the experiments with [experiment-generator](https://access-experiment-generator.access-hive.org.au/), and
submits them on NCI's Gadi with [experiment-runner](https://access-experiment-runner.access-hive.org.au/). It then
uses the records that Payu keeps of every run. If you are new to the package, start with
[Generating and running experiments](../user-guide/experiments.md) and
[Managing experiments](../user-guide/managing-experiments.md), which go through a whole study with Payu. Here you
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

The releases of ACCESS-OM2 need two more options. Their ocean runs on a masked layout. Among the inputs of the ocean
submodel there is a mask table of the ranks that only hold land, and it is named after the process grid it was made
for. MOM5 refuses to run another process grid with it, so the table has to go, as the generated layouts run without a
mask. The `input` list of a submodel is merged item by item, so the option restates the inputs of the ocean without
the table, in the slot of the ocean:

```python
control_options = {"config.yaml": {"submodels": ["PRESERVE", {"input": [...]}, "PRESERVE"]}}
```

The alternative is the `mask: true` option of Payu on the ocean submodel. It needs a reference table,
`mask_table.<masked>.<nx>x<ny>`, among the inputs for every layout of the study. The second option concerns the sea
ice. The presets write the grid and the blocks of CICE5 for `cice_auscom.exe`, the executable of
`access-om2/2026.08.002` and later, which reads its grid when it runs. The `+wombatlite` releases already run it,
but the `-4.1` and `_bgc` releases run a CICE5 compiled for their layout. For these, the control options also load
that module:

```python
control_options["config.yaml"]["modules"] = {"use": ["/g/data/vk83/modules"], "load": ["access-om2/2026.08.002"]}
```

So what happens when the experiments are created? First, the control is cloned into the working directory, under
`config`, and the control options are committed there on a branch named `ctrl`. Then one branch is created from `ctrl`
for each experiment. The branch is named after the parallelisation layout, and contains the changes for that layout, its
walltime and its name.

## Reading a configuration from its control

The presets describe the configurations as their controls were when the package was released. If your configuration
is on another branch, a fork or a commit, you can read it from the files of the control instead, with the
`from_control` of its model:
[`ESM16Configuration.from_control`][access.profiling.models.esm16.ESM16Configuration.from_control],
[`ESM15Configuration.from_control`][access.profiling.models.esm15.ESM15Configuration.from_control],
[`OM2Configuration.from_control`][access.profiling.models.om2.OM2Configuration.from_control] and
[`OM3Configuration.from_control`][access.profiling.models.om3.OM3Configuration.from_control]. Each one reads the
files in which its model states the layout. For the models with submodels, these are `config.yaml` and the CICE
namelist. For ACCESS-OM3, they are `nuopc.runconfig`, `ice_in` and `MOM_input`. The configuration is named after
the `jobname` of `config.yaml`, unless you give it a name. A control of another model is refused.

The files have to be on disk, but the manager that would clone them cannot be created before the configuration.
This is what [`clone_control`][access.profiling.payu_manager.clone_control] is for. It clones the control first, in
the same way as generating the study does: into the working directory, under the directory of the control, at its
start point, with experiment-generator. When the study is generated later, the clone is found there and reused, so
nothing is fetched twice, and a control that is already cloned is left alone. This is the only place where the
package fetches anything, and it is an explicit call. If the repository of the control asks for credentials, it
does so here, from a terminal, and never while a configuration is being created.

```python
from access.profiling import GitControlSource, OM3Configuration, PayuManager
from access.profiling.models.om3 import OM3_CONFIGS_REPOSITORY
from access.profiling.payu_manager import clone_control

control = GitControlSource(OM3_CONFIGS_REPOSITORY, "dev-MC_100km_jra_ryf")
om3 = OM3Configuration.from_control(clone_control(work_dir, control, OM3Configuration.model_type), name="MC-100km")
manager = PayuManager(work_dir, archive_dir, om3, control)
```

An [`ExistingDirectoryControlSource`][access.profiling.control.ExistingDirectoryControlSource] needs no clone:
read the configuration from its `path`. What you get is what the control states. For ACCESS-OM3, this means the
block size that the control fixes for CICE6. The presets choose it from the number of ranks instead, so that every
rank holds [`OM3_CICE6_BLOCKS_PER_RANK_RANGE`][access.profiling.models.om3.OM3_CICE6_BLOCKS_PER_RANK_RANGE] blocks
at any number of cores. If you want the same for a configuration read from a control, replace the partitioning:

```python
from dataclasses import replace

from access.profiling.models.om3 import OM3_CICE6_BLOCKS_PER_RANK_RANGE

om3 = replace(om3, sea_ice=replace(om3.sea_ice, block_shape=None, blocks_per_rank_range=OM3_CICE6_BLOCKS_PER_RANK_RANGE))
```

The reader of ACCESS-OM3 warns you when the control fixes a `LAYOUT` or a `MASKTABLE` in `MOM_input` without
`AUTO_MASKTABLE`, as the control options have to remove them. The reader of ACCESS-OM2 refuses a control whose
CICE5 is compiled for its grid, as in the `-4.1` and `_bgc` releases. These are profiled with the preset of their
resolution instead.

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
