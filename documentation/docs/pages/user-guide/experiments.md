# Generating and running experiments

A manager generates the experiments of a study from the application and its control, one for each layout the
study asks for, and runs them with its workflow engine. In this tutorial we will go through those steps, from
creating the manager to submitting the experiments. Our example is a scaling study, made of the layouts of several
sizes; [Best layout for a budget](best-layout.md) generates those of a single size in the same way.

We will generate studies of two applications the package ships presets for, ACCESS-ESM1.6 and ACCESS-OM3, with the
manager for Payu, the workflow engine that runs them. The two differ in their allocation strategies and in the
changes their controls need, so we show them side by side in tabs. [Experiments with Payu](../access/payu.md)
covers what is particular to Payu. Once the experiments have been submitted,
[Managing experiments](managing-experiments.md) follows them, and [Analysing profiling data](analysing-data.md)
reads their results.

## Creating the manager

A manager takes a working directory, where the experiments are generated and run, an archive directory, where the
finished experiments are kept, the application, and its control. Here we create a
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

Remember to replace `tm70` with your own project. The controls are the releases the presets describe,
`release-piControl-2.1` and `release-MC_25km_jra_ryf-2.0-beta`. To profile another release, or a fork, pass a
different [`GitControlSource`][access.profiling.control.GitControlSource] and keep the preset.

## Choosing an allocation strategy

An allocation strategy says how many cores each component may receive, keyed by the component names of the
application, and adds the constraints the study wants on top of the application's own. Without one, the search
finds every layout the application can run, which is 555,868 of them for ACCESS-ESM1.6 on 520 cores. Bounds given
as fractions of the total resolve to a different number of cores at every size of the study, so one strategy
usually serves the whole study. These are the strategies we use:

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

    The atmosphere and the ocean keep their shares of the released configuration to within 5%, and their
    subdomains close to square. The sea ice gets a wider band. Each CICE5 rank holds one block spanning the whole
    y extent of the grid, so the only core counts it can run on are the divisors of the grid's 360 points along
    x. A narrow band around its share often contains none of them.

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

    The mediator, the data atmosphere, the sea ice and the data runoff take turns on one range of cores, the
    `shared` component of the tree, and the ocean runs beside them on a range of its own. The strategy gives the
    ocean at least 85% of the cores, and keeps it at nine to ten times the size of the shared range. Every
    component of the shared range gets the whole of it, and at most 1% of the cores are left idle. `core_step` only
    offers core counts that are multiples of 2, 5 or 15, which thins out the layouts of the larger sizes. Note
    that the step depends on the number of nodes, so the strategy is a function of it.

The component names of the presets are listed in [ACCESS models](../access/index.md).

## Counting the layouts

Before generating anything, we count the layouts the strategy admits at every size of the study, since every layout
becomes an experiment:

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
    The ACCESS-ESM1.6 strategy above admits 810 layouts over seven sizes. Generating the study would create 810
    experiments, and running it would submit 810 jobs. Tighten the strategy until the count is one you are
    prepared to pay for. `max_layouts` caps the number of layouts enumerated at each size, but it keeps the first
    ones found rather than the best ones.

## Generating the experiments

Now we generate the experiments.
[`generate_scaling_experiments`][access.profiling.manager.ProfilingManager.generate_scaling_experiments] searches
for the layouts of every size of the study and creates one experiment for each of them:

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

    The control options stop Payu from insisting on the executables recorded in the release's manifest, and
    remove the release's post-processing script, which a profiling run has no use for.

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

    The release pins MOM6's decomposition with a `LAYOUT` of 72 × 48 and a `MASKTABLE` written for it, which no
    other number of ocean cores can run. `OM3_MC_25KM` leaves the decomposition to MOM6, so the control options
    remove both, along with the `IO_LAYOUT` that divides that layout, and set `AUTO_MASKTABLE`. They also remove
    the memory request, which is sized for the release's own core count, and shorten the run to two months. The
    environment variables turn on the ESMF profiling summary. Note that they are written quoted, since Payu would
    otherwise read `on` back as a boolean.

The arguments are:

- `num_nodes_list`: the sizes of the study, in nodes: several for a [scaling study](scaling-studies.md), or one
  for the [best layout of a budget](best-layout.md). Fractions of a node are allowed.
- `cores_per_node`: the size of a node, 104 cores in the examples, which run on the Sapphire Rapids nodes of NCI's
  Gadi.
- `walltime`: the walltime of each experiment in hours, as a number or as a function of the number of nodes.
- `allocations`: the allocation strategy, or a function of the number of nodes returning one.

Any other argument is an option of the manager's workflow engine. The Payu manager takes `control_options`, the
changes made to the control before any experiment is created from it;
[Experiments with Payu](../access/payu.md#generating-the-experiments) describes them.

The manager now holds the experiments with the status NEW; nothing has been submitted yet. Note that calling
`generate_scaling_experiments` again skips every experiment the manager already holds, so extending a study to more
sizes only creates the new experiments.

## Running the experiments

Finally, we submit them:

```python
manager.run_experiments()
```

[`run_experiments`][access.profiling.payu_manager.PayuManager.run_experiments] submits every NEW experiment through
the manager's workflow engine. It returns as soon as they have been submitted, and marks them RUNNING. It does not
wait for them to finish, so you can close the Python session while they run.

How each experiment runs is up to the workflow engine. The Payu manager, for example, can run every experiment
several times, and start it from the restart files of another, as
[Experiments with Payu](../access/payu.md#running-the-experiments) describes.

## Registering existing experiments

If you have experiments that were run some other way, the manager can register them instead of generating them,
and then parses, compares and archives them like the others.
[Comparing experiments](comparing-experiments.md#registering-experiments) shows how.
