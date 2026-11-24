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
- Three directories. The working directory is where the experiments are generated. The directory for the
  laboratories, on `/scratch`, is where Payu runs them (see [The laboratory](#the-laboratory)). The archive
  directory, on `/g/data`, is where the finished experiments are kept.
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

The releases of ACCESS-OM2 need two more options. The first one concerns the ocean. In the releases, MOM5 runs a
parallelisation layout with a mask. A mask table, which is one of the inputs of the ocean submodel, lists the ranks
that only hold land, so that they are not run. The table is named after the process grid it was made for, and MOM5
refuses to run any other process grid with it. The generated layouts run without a mask, so the table has to be
removed from the inputs. The `input` list of a submodel is merged item by item. The option therefore gives the inputs
of the ocean again, without the table, in the position of the ocean among the submodels. In this example, `...`
stands for those inputs:

```python
control_options = {"config.yaml": {"submodels": ["PRESERVE", {"input": [...]}, "PRESERVE"]}}
```

The alternative is the `mask: true` option of Payu on the ocean submodel. It needs a reference table,
`mask_table.<masked>.<nx>x<ny>`, among the inputs for every layout of the study.

The second option concerns the sea ice. The presets write the grid and the blocks of CICE5 for `cice_auscom.exe`.
This is the executable of `access-om2/2026.08.002` and later, and it reads its grid when it runs. The `+wombatlite`
releases already run it. The `-4.1` and `_bgc` releases run a CICE5 that is compiled for their layout instead. For
these, the control options also load that module:

```python
control_options["config.yaml"]["modules"] = {"use": ["/g/data/vk83/modules"], "load": ["access-om2/2026.08.002"]}
```

So what happens when the experiments are created? First, the control is cloned into the working directory, under
`config`, and the control options are committed there on a branch named `ctrl`, together with the laboratory of the
study. Then one branch is created from `ctrl` for each experiment. The branch has the name of the experiment, such
as `exp-0001`, and contains the changes for its parallelisation layout, its walltime and its name.

### The laboratory

Payu runs every experiment in a laboratory. During a run, the files of the experiment are in
`<laboratory>/work/<experiment>`. When the run is over, Payu moves its outputs and the records of its jobs to
`<laboratory>/archive/<experiment>`. The directory of the experiment links to both, as `work` and `archive`, and the
manager reads everything through these links.

Each study has a laboratory of its own, in the directory that you give the manager as `lab_dir`. The laboratory is
named after the model and 8 hexadecimal digits that identify the working directory of the study, for example
`/scratch/tm70/model_scaling/access-esm1.6-763e5952`. This means that one `lab_dir` can hold the laboratories of all
your studies. The manager writes the laboratory into the `config.yaml` of the control, so that every experiment and
every Payu command uses it. It also logs it when it creates the experiments, and you can find it in its
[`laboratory`][access.profiling.payu_manager.PayuManager.laboratory] property. Put `lab_dir` on `/scratch`, which is
meant for the large files that the runs write.

Why does each study need its own laboratory? Without one, Payu uses its default,
`/scratch/<project>/<user>/<model>`, which every study of the model and all your other Payu experiments share. Every
study numbers its experiments in the same way, from `exp-0001`, so two studies would share directories. A study
would then skip the experiments that another study has already run, and read their results instead. Deleting an
experiment would also delete the runs of the other study. For this reason, a manager without a `lab_dir` refuses to
create experiments, and the control options cannot name a laboratory. A manager that only reads, archives or deletes
experiments needs no `lab_dir`. The experiments created before the study had a laboratory keep the one that their
branch states.

The `PAYU_LAB_PATH` environment variable would get in the way. When it is set, Payu uses it instead of the laboratory
in `config.yaml`, but only on the login node: experiment-runner submits `payu run` without a laboratory, so the PBS
job never receives it. The experiments would then run in one laboratory while the manager reads another. For this
reason, the manager refuses to create, run or delete experiments while `PAYU_LAB_PATH` is set.

Note that the first clone of the control still uses the control's own laboratory, as it is made before the laboratory
of the study is written into the control. Payu leaves a few empty directories there, and a `metadata.yaml` under
`archive/config-ctrl-<id>`.

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
nothing is fetched twice. A control that is already cloned is left alone.

This is the only place where the package fetches anything, and it is an explicit call. If the repository of the
control asks for credentials, it does so here, from a terminal, and never while a configuration is being created.

In this example we read the 100 km configuration of ACCESS-OM3 from its development branch, and then create the
manager with it. `work_dir`, `archive_dir` and `lab_dir` are the three directories of your study:

```python
from access.profiling import GitControlSource, OM3Configuration, PayuManager
from access.profiling.models.om3 import OM3_CONFIGS_REPOSITORY
from access.profiling.payu_manager import clone_control

control = GitControlSource(OM3_CONFIGS_REPOSITORY, "dev-MC_100km_jra_ryf")
om3 = OM3Configuration.from_control(clone_control(work_dir, control, OM3Configuration.model_type), name="MC-100km")
manager = PayuManager(work_dir, archive_dir, om3, control, lab_dir=lab_dir)
```

An [`ExistingDirectoryControlSource`][access.profiling.control.ExistingDirectoryControlSource] needs no clone:
read the configuration from its `path`. A configuration that is read in this way is what its control states. For
ACCESS-OM3, this means the block size that the control fixes for CICE6. The presets choose it from the number of
ranks instead, so that every rank holds 5 to 8 blocks at any number of cores
([`OM3_CICE6_BLOCKS_PER_RANK_RANGE`][access.profiling.models.om3.OM3_CICE6_BLOCKS_PER_RANK_RANGE]). If you want the
same for a configuration read from a control, replace the partitioning:

```python
from dataclasses import replace

from access.profiling.models.om3 import OM3_CICE6_BLOCKS_PER_RANK_RANGE

om3 = replace(
    om3,
    sea_ice=replace(om3.sea_ice, block_shape=None, blocks_per_rank_range=OM3_CICE6_BLOCKS_PER_RANK_RANGE),
)
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

In a later session, the manager finds your experiments again from their records, as described in
[Managing experiments](../user-guide/managing-experiments.md#picking-a-study-up-in-a-later-session). You can also
create the study again with the same arguments. This is safe: the layouts that already have an experiment are
skipped, so the generator is not called for them.

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
directory of the experiment. The branch of the experiment stays in the clone of the control. This is why the
number of a deleted experiment is not given to another one: a new experiment would be created on top of that
branch.

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
| `Cannot create experiments without a lab_dir: ...` | The manager has nowhere to put the laboratory of the study. Create it again with a `lab_dir` on `/scratch`. |
| `PAYU_LAB_PATH is set, to '...'. ...` | Payu would run the experiments in another laboratory than the one the manager reads. Unset the variable, for example with `del os.environ["PAYU_LAB_PATH"]`, and try again. |
| `The control options name a Payu laboratory, ...` | The laboratory of a study is the manager's to choose. Remove `laboratory` from the control options, and give the manager the directory that is to hold it as `lab_dir`. |
