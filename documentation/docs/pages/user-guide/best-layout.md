# Best layout for a budget

A budget of cores can be shared among the components of an application, and decomposed over each of them, in many
ways, and the layouts do not all run equally fast. In this tutorial we run every layout of one budget that an
allocation strategy admits, and pick the fastest. The experiments are generated as in
[Generating and running experiments](experiments.md), with a single size. We use the `manager`, `walltime`,
`allocations` and `control_options` of that page, and the `groups` of
[Choosing the regions](analysing-data.md#choosing-the-regions). The figure is drawn from synthetic data.

## Choosing the layouts

=== "ACCESS-ESM1.6"

    The released configuration gives the UM, MOM5 and CICE5 256, 240 and 12 cores. We fix those numbers, which
    leaves the search to choose their process grids alone:

    ```python
    from access.config.parallel_allocation_strategies import FixedAllocation, RootAllocation

    from access.profiling.models.esm16 import ESM16_PI_CONTROL_CORES

    released = RootAllocation(
        subcomponents={component: FixedAllocation(cores) for component, cores in ESM16_PI_CONTROL_CORES.items()}
    )
    layouts = manager.select_layouts(5 * 104, allocations=released)
    for layout in layouts:
        print(manager.application.experiment_name(layout))
    ```

    This prints the names of the layouts:

    ```text
    esm1p6-layout_piControl_atm_16x16_mom_10x24_ice_12
    esm1p6-layout_piControl_atm_16x16_mom_12x20_ice_12
    esm1p6-layout_piControl_atm_16x16_mom_15x16_ice_12
    esm1p6-layout_piControl_atm_16x16_mom_16x15_ice_12
    esm1p6-layout_piControl_atm_16x16_mom_20x12_ice_12
    esm1p6-layout_piControl_atm_16x16_mom_24x10_ice_12
    esm1p6-layout_piControl_atm_16x16_mom_30x8_ice_12
    esm1p6-layout_piControl_atm_32x8_mom_10x24_ice_12
    esm1p6-layout_piControl_atm_32x8_mom_12x20_ice_12
    esm1p6-layout_piControl_atm_32x8_mom_15x16_ice_12
    esm1p6-layout_piControl_atm_32x8_mom_16x15_ice_12
    esm1p6-layout_piControl_atm_32x8_mom_20x12_ice_12
    esm1p6-layout_piControl_atm_32x8_mom_24x10_ice_12
    esm1p6-layout_piControl_atm_32x8_mom_30x8_ice_12
    ```

    All 14 leave the same 12 of the 520 cores idle, and differ only in the process grids of the UM, 16 × 16 or
    32 × 8, and of MOM5. The release's own layout is the fourth. If you let the shares of the components vary as
    well, with the bands of [Getting started](../getting-started.md#previewing-a-study), you get 156 layouts on
    the same five nodes.

=== "ACCESS-OM3"

    With the allocation strategy of
    [Generating and running experiments](experiments.md#choosing-an-allocation-strategy), 26 nodes, the size of
    the release, admit three layouts:

    ```python
    layouts = manager.select_layouts(26 * 104, allocations=allocations(26))
    for layout in layouts:
        print(manager.application.experiment_name(layout))
    ```

    This prints:

    ```text
    om3-layout_MC-25km_atm_255_cpl_255_ice_255_ocn_2445_rof_255
    om3-layout_MC-25km_atm_270_cpl_270_ice_270_ocn_2430_rof_270
    om3-layout_MC-25km_atm_255_cpl_255_ice_255_ocn_2430_rof_255
    ```

    They leave 4, 4 and 19 of the 2704 cores idle.

## Generating and running them

[`generate_scaling_experiments`][access.profiling.manager.ProfilingManager.generate_scaling_experiments] generates
the layouts of every size in its `num_nodes_list`, so a list of one generates those of a single budget:

=== "ACCESS-ESM1.6"

    ```python
    manager.generate_scaling_experiments(
        num_nodes_list=[5],
        cores_per_node=104,
        walltime=walltime,
        allocations=released,
        control_options=control_options,
    )
    ```

=== "ACCESS-OM3"

    ```python
    manager.generate_scaling_experiments(
        num_nodes_list=[26],
        cores_per_node=104,
        walltime=walltime,
        allocations=allocations,
        control_options=control_options,
    )
    ```

Then run them:

```python
manager.run_experiments()
```

ACCESS-ESM1.6 requests the whole budget from the scheduler as `ncpureq`, and ACCESS-OM3 as `ncpus`, so every
layout runs as a job of the same size, whichever cores it leaves idle: here, 14 jobs of five nodes, or 3 of 26.

## Picking the fastest

We rank the layouts by the time the application as a whole took, which is a region of one log for each model:

=== "ACCESS-ESM1.6"

    ```python
    log, region = "payu", "payu_model_run_duration_seconds"
    ```

=== "ACCESS-OM3"

    ```python
    log, region = "ESMF", "[ensemble] RunPhase1"
    ```

Then parse the logs and select the fastest experiment:

```python
from access.profiling.metrics import tmax

manager.parse_profiling_data()
names = [manager.application.experiment_name(layout) for layout in layouts]
parsed = [name for name in names if name in manager.data]
best = manager.select_best_experiments(log, region, tmax, experiments=parsed)
```

[`select_best_experiments`][access.profiling.manager.ProfilingManager.select_best_experiments] keeps the fastest
experiment of every number of CPUs. All these layouts occupy the same number, so `best` holds a single name.
Passing the experiments restricts the choice to the layouts of this budget, which matters when the manager holds
other experiments as well, and leaves out those that have not finished. Note that an experiment without parsed
data would make the method raise a `KeyError`. Rank the layouts by a region covering the whole run, as these do;
the region of a single component would pick the layout that is best for that component alone.

## Ranking every layout

`select_best_experiments` returns one experiment for each number of CPUs, so to rank all the layouts of a budget we
use the data itself:

```python
times = {name: float(manager.data[name][log][tmax].sel(region=region).pint.dequantify()) for name in parsed}
ranking = sorted(times, key=times.get)
for name in ranking:
    print(f"{times[name]:8.1f} s  {name}")
```

This prints the layouts from the fastest to the slowest, with the time each took.

## Comparing the fastest layouts

[`plot_bar_chart`][access.profiling.manager.ProfilingManager.plot_bar_chart] sets the fastest layouts side by side,
region by region. We take the first three of the ranking:

```python
fig = manager.plot_bar_chart(groups, tmax, experiments=ranking[:3])
```

<figure markdown="span">
  ![Grouped bar chart of time per region for three experiments](../../assets/figures/bar-chart.png)
  <figcaption>Three layouts of the same 832 cores, sharing them out differently between the ocean and the sea
  ice. Synthetic data.</figcaption>
</figure>

In the example, giving the sea ice more cores makes it faster, and yet the whole model gets slower. Why? The cores
come from the ocean, which the whole model waits for. The chart widens with every bar, so compare a few layouts at
a time. [Comparing experiments](comparing-experiments.md) explains the chart further.

## Using the fastest layout

The changes that run a layout are those the application writes for it. The Payu manager also commits them on the
layout's branch of the clone of the control, `<work_dir>/config`, together with its walltime and its name. Ask the
application for the changes of the fastest layout:

```python
from pprint import pprint

pprint(manager.application.config_changes(layouts[names.index(best[0])]))
```

For the release's own layout of ACCESS-ESM1.6, for example, they are:

```text
{'atmosphere/um_env.yaml': {'UM_ATM_NPROCX': '16',
                            'UM_ATM_NPROCY': '16',
                            'UM_NPES': '256'},
 'config.yaml': {'ncpureq': 520,
                 'submodels': [[{'ncpus': 256},
                                {'ncpus': 240},
                                {'exe': ['cice_access.exe'], 'ncpus': 12},
                                'PRESERVE']]},
 'ice/cice_in.nml': {'domain_nml': {'block_size_x': '30',
                                    'block_size_y': '300',
                                    'max_blocks': '1',
                                    'nprocs': '12',
                                    'nx_global': '360',
                                    'ny_global': '300'}},
 'ocean/input.nml': {'ocean_model_nml': {'layout': '16, 15'}}}
```

The layouts of one budget often differ by little, so a ranking is only as reliable as the timings are repeatable.
[Run-to-run variability](run-to-run-variability.md) shows how to measure that, and how to rank the layouts by their
fastest or their typical run.
