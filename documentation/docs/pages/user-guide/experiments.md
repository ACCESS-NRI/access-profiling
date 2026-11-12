# Generating and running experiments

In this tutorial we will create the experiments of a profiling study and run them. We will go through all the
steps, from creating the manager to submitting the jobs. Our example is a scaling study, which runs the application
on several numbers of cores. If you only want to run it on one number of cores, the steps are the same, as shown in
[Best layout for a budget](best-layout.md).

We will do this for two of the ACCESS models, ACCESS-ESM1.6 and ACCESS-OM3, which are both run with Payu. Where the
two models need different code, we show both side by side in tabs. [Experiments with Payu](../access/payu.md) has
more details about Payu itself. Once the experiments have been submitted, see
[Managing experiments](managing-experiments.md) to follow them, and [Analysing profiling data](analysing-data.md)
to look at their results.

## Creating the manager

To create a manager, we need four things: a working directory, where the experiments are created and run, an
archive directory, where the finished experiments are kept, the application, and its control. Here we create a
[`PayuManager`][access.profiling.payu_manager.PayuManager]:

=== "ACCESS-ESM1.6"

    ```python
    from pathlib import Path

    from access.profiling import PayuManager
    from access.profiling.models.esm16 import ESM16_PI_CONTROL, ESM16_PI_CONTROL_SOURCE

    manager = PayuManager(
        work_dir=Path("/scratch/tm70/model_scaling/ACCESS-ESM1.6/piControl"),
        archive_dir=Path("/g/data/tm70/model_scaling/ACCESS-ESM1.6/piControl/archive"),
        application=ESM16_PI_CONTROL,
        control=ESM16_PI_CONTROL_SOURCE,
    )
    ```

=== "ACCESS-OM3"

    ```python
    from pathlib import Path

    from access.profiling import PayuManager
    from access.profiling.models.om3 import OM3_MC_25KM, OM3_MC_25KM_JRA_RYF_SOURCE

    manager = PayuManager(
        work_dir=Path("/scratch/tm70/model_scaling/ACCESS-OM3/MC_25km_jra_ryf"),
        archive_dir=Path("/g/data/tm70/model_scaling/ACCESS-OM3/MC_25km_jra_ryf/archive"),
        application=OM3_MC_25KM,
        control=OM3_MC_25KM_JRA_RYF_SOURCE,
    )
    ```

The two directories are only examples, so remember to adapt them to your own setup, replacing `tm70` with your own
project. The controls used here are the official releases of the two models, `release-piControl-2.1` and
`release-MC_25km_jra_ryf-2.0-beta`. If you want to profile another release, or a fork, keep the same application
and pass a different [`GitControlSource`][access.profiling.control.GitControlSource] as the control.

## Choosing an allocation strategy

Next we have to decide which parallelisation layouts we want to run. This is done with an allocation strategy, which
says how many cores each component of the application may get. It can also add other constraints. Without an allocation
strategy, the manager would find every layout the application can run, and there are a lot of them: 555,868 for
ACCESS-ESM1.6 on 520 cores! The limits of a strategy can be given as fractions of the total number of cores, so the same
strategy usually works for all the sizes of a study. These are the strategies we use:

=== "ACCESS-ESM1.6"

    ```python
    from access.config.parallel_allocation_strategies import FreeAllocation, RootAllocation
    from access.config.parallel_constraints import SubdomainAspectRatioConstraint

    from access.profiling.models.esm16 import ESM16_PI_CONTROL_CORES

    released_total = sum(ESM16_PI_CONTROL_CORES.values())


    def band(component, tolerance, **kwargs):
        """Cores within a tolerance of the share the component has in the release, as fractions of the total."""
        share = ESM16_PI_CONTROL_CORES[component] / released_total
        return FreeAllocation(
            min_core_fraction=share * (1 - tolerance),
            max_core_fraction=min(1.0, share * (1 + tolerance)),
            **kwargs,
        )


    near_square = (SubdomainAspectRatioConstraint(max_ratio=1.5),)
    allocations = RootAllocation(
        subcomponents={
            "UM7": band("UM7", 0.05, local_constraints=near_square),
            "MOM5": band("MOM5", 0.05, local_constraints=near_square),
            "CICE5": band("CICE5", 0.25),
        }
    )
    ```

    Here the atmosphere and the ocean get about the same share of the cores as in the released configuration, to
    within 5%, and their subdomains have to be close to square. The sea ice is given more freedom. The reason is
    that each CICE5 rank holds one block spanning the whole y extent of the grid. CICE5 can therefore only run on
    numbers of cores that divide the 360 points of the grid along x, and a narrow range often contains none of
    them.

=== "ACCESS-OM3"

    ```python
    from access.config.parallel_allocation_strategies import FreeAllocation, RootAllocation, WholeRangeAllocation
    from access.config.parallel_constraints import MaxWastedCoreFractionConstraint, RankRatioGroupConstraint


    def allocations(num_nodes):
        """MOM6 takes at least 85% of the cores, and the components sharing the rest take turns on all of it."""
        step = 15 if num_nodes >= 12 else 5 if num_nodes >= 4 else 2
        return RootAllocation(
            subcomponents={
                "ocn": FreeAllocation(min_core_fraction=0.85, core_step=step),
                "shared": FreeAllocation(
                    min_core_fraction=0.05,
                    core_step=step,
                    subcomponents={
                        "cpl": WholeRangeAllocation(),
                        "atm": WholeRangeAllocation(),
                        "ice": WholeRangeAllocation(),
                        "rof": WholeRangeAllocation(),
                    },
                ),
            },
            local_constraints=(MaxWastedCoreFractionConstraint(max_fraction=0.01),),
            group_constraints=(
                RankRatioGroupConstraint(name_a="ocn", name_b="shared", min_ratio=9.0),
                RankRatioGroupConstraint(name_a="shared", name_b="ocn", min_ratio=0.1),
            ),
        )
    ```

    In ACCESS-OM3, the mediator, the data atmosphere, the sea ice and the data runoff take turns on the same
    cores, which are called `shared` here. The ocean runs next to them on cores of its own. With this strategy,
    the ocean gets at least 85% of the cores, and nine to ten times as many as the shared components. Each shared
    component uses all the shared cores, and at most 1% of the cores are left idle. `core_step` only allows
    numbers of cores that are multiples of 2, 5 or 15, which reduces the number of layouts for the larger sizes.
    Note that the step depends on the number of nodes, which is why the strategy is a function.

You can find the names of the components of each model in [ACCESS models](../access/index.md).

## Counting the layouts

Before creating any experiment, it is a good idea to check how many layouts the strategy finds for each size of
the study. Remember that every layout becomes an experiment:

=== "ACCESS-ESM1.6"

    ```python
    for num_nodes in [1, 2, 3, 4, 5, 6, 8]:
        layouts = manager.select_layouts(num_nodes * 104, allocations=allocations)
        print(num_nodes, len(layouts))
    ```

    This prints:

    ```text
    1 8
    2 20
    3 87
    4 76
    5 156
    6 146
    8 317
    ```

=== "ACCESS-OM3"

    ```python
    for num_nodes in [1, 2, 4, 8, 16, 26, 32]:
        layouts = manager.select_layouts(num_nodes * 104, allocations=allocations(num_nodes))
        print(num_nodes, len(layouts))
    ```

    This prints:

    ```text
    1 1
    2 2
    4 1
    8 3
    16 2
    26 3
    32 5
    ```

!!! warning "Every layout is a job"
    The ACCESS-ESM1.6 strategy above finds 810 layouts over the seven sizes. This means that the study would have
    810 experiments, and running it would submit 810 jobs! If that is more than you want to pay for, make the
    strategy stricter. You can also use `max_layouts` to limit the number of layouts for each size, but be aware
    that it keeps the first layouts found, which are not necessarily the best ones.

## Generating the experiments

We are now ready to create the experiments. This is done with
[`generate_scaling_experiments`][access.profiling.manager.ProfilingManager.generate_scaling_experiments], which
finds the layouts for every size of the study and creates one experiment for each of them:

=== "ACCESS-ESM1.6"

    ```python
    def walltime(num_nodes):
        """2.5 hours on 4 nodes, scaled with the inverse of the number of nodes."""
        return 2.5 * 4 / num_nodes


    control_options = {
        "config.yaml": {
            "manifest": {"reproduce": {"exe": False}},
            "postscript": "REMOVE",
        }
    }

    manager.generate_scaling_experiments(
        num_nodes_list=[1, 2, 3, 4, 5, 6, 8],
        cores_per_node=104,
        walltime=walltime,
        allocations=allocations,
        control_options=control_options,
    )
    ```

    The control options are changes that are applied to the control before the experiments are created. Here
    they tell Payu not to insist on the exact executables of the release, and they remove the post-processing
    script of the release, which is not needed for profiling.

=== "ACCESS-OM3"

    ```python
    from ruamel.yaml.scalarstring import DoubleQuotedScalarString


    def walltime(num_nodes):
        """2 hours on 27 nodes, scaled with the inverse of the number of nodes, and at most 24 hours."""
        return min(2.0 * 27 / num_nodes, 24)


    control_options = {
        "config.yaml": {
            "modules": {"use": ["/g/data/vk83/modules"], "load": ["access-om3/2026.05.004"]},
            "mem": "REMOVE",
            "manifest": {"reproduce": {"exe": False}},
            "postscript": "REMOVE",
            "collate": "REMOVE",
            "archive": "REMOVE",
            "env": {
                "ESMF_RUNTIME_PROFILE": DoubleQuotedScalarString("on"),
                "ESMF_RUNTIME_PROFILE_OUTPUT": DoubleQuotedScalarString("SUMMARY"),
            },
        },
        "MOM_input": {
            "AUTO_MASKTABLE": "True",
            "MASKTABLE": "REMOVE",
            "LAYOUT": "REMOVE",
            "IO_LAYOUT": "REMOVE",
        },
        "nuopc.runconfig": {
            "CLOCK_attributes": {"stop_n": 2, "stop_option": "nmonths", "restart_n": 2, "restart_option": "nmonths"},
        },
    }

    manager.generate_scaling_experiments(
        num_nodes_list=[1, 2, 4, 8, 16, 26, 32],
        cores_per_node=104,
        walltime=walltime,
        allocations=allocations,
        control_options=control_options,
    )
    ```

    The control options are changes that are applied to the control before the experiments are created. The
    release fixes the decomposition of MOM6 with a `LAYOUT` of 72 × 48 and a `MASKTABLE` that goes with it, and
    these only work for one number of ocean cores. In our study MOM6 chooses its own decomposition, so we remove
    both, together with the `IO_LAYOUT` that goes with that decomposition, and set `AUTO_MASKTABLE`. We also remove the
    memory request, which was chosen for the number of cores of the release, and shorten the run to two months.
    The `modules` entry tells Payu where to find the modules and which build of ACCESS-OM3 to load. Adapt its
    path and its version to the build you want to profile. Finally, the environment variables turn on the ESMF
    profiling summary. Note that they are written with quotes, because otherwise Payu would read `on` as a
    boolean.

The arguments are:

- `num_nodes_list`: the sizes of the study, in nodes: several for a [scaling study](scaling-studies.md), or one
  for the [best layout of a budget](best-layout.md). Fractions of a node are allowed.
- `cores_per_node`: the size of a node, 104 cores in the examples, which run on the Sapphire Rapids nodes of NCI's
  Gadi.
- `walltime`: the walltime of each experiment in hours, as a number or as a function of the number of nodes.
- `allocations`: the allocation strategy, or a function of the number of nodes returning one.

Any other argument is passed on to the workflow engine of the manager. For the Payu manager, this is
`control_options`, which is described in more detail in
[Experiments with Payu](../access/payu.md#generating-the-experiments).

The experiments now exist, with the status NEW, but nothing has been submitted yet. Note that you can call
`generate_scaling_experiments` again without any risk: the experiments that already exist are skipped. This is
useful to add more sizes to a study later, as only the new experiments are created.

## Running the experiments

Finally, we submit them:

```python
manager.run_experiments()
```

[`run_experiments`][access.profiling.payu_manager.PayuManager.run_experiments] submits all the NEW experiments and
marks them as RUNNING. It does not wait for them to finish, so you can close the Python session while they run and
come back later.

Some options depend on the workflow engine. With Payu, for example, it is possible to run every experiment several
times, or to start it from the restart files of another experiment, as
[Experiments with Payu](../access/payu.md#running-the-experiments) describes.

## Registering existing experiments

If you already have experiments that were run in some other way, you do not need to create them again. You can
register them with the manager, and then parse, compare and archive them like any other experiment.
[Comparing experiments](comparing-experiments.md#registering-experiments) shows how.
