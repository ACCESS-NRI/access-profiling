# ACCESS models

access-profiling implements its general parts for the ACCESS climate models developed at
[ACCESS-NRI](https://github.com/ACCESS-NRI), which run on NCI's Gadi supercomputer:

- a manager for each of the workflow engines they run with: [Payu](payu.md), for ACCESS-ESM1.5, ACCESS-ESM1.6,
  ACCESS-OM2 and ACCESS-OM3, and [Cylc and Rose](cylc-rose-suites.md), for ACCESS-AM3 and ACCESS-rAM3;
- parsers for the [log formats](log-formats.md) of their components and of those engines;
- presets for each model, with the release each describes as a control.

Each preset is described below: the components a layout divides cores among, the requirements every layout meets,
the logs a run writes, and the cores the release itself runs on. A configuration on a branch, a fork or a commit the
presets do not describe is read from the control's own files with its model's `from_control`, as
[Experiments with Payu](payu.md#reading-a-configuration-from-its-control) shows. The [Models API](../api/models.md)
has the full details.

## ACCESS-ESM1.6

| | |
|---|---|
| Preset | [`ESM16_PI_CONTROL`][access.profiling.models.esm16.ESM16_PI_CONTROL], the pre-industrial control |
| Control | [`ESM16_PI_CONTROL_SOURCE`][access.profiling.models.esm16.ESM16_PI_CONTROL_SOURCE], `release-piControl-2.1` of `access-esm1.6-configs` |
| Manager | `PayuManager` |
| Components | `UM7` on the N96 grid, 192 × 144; `MOM5` on the one-degree grid, 360 × 300; `CICE5` on the ocean's grid |
| Whole model | `ACCESS-ESM1.6 piControl` |
| Release cores | `UM7` 256, `MOM5` 240 and `CICE5` 12, on 5 nodes of 104 cores ([`ESM16_PI_CONTROL_CORES`][access.profiling.models.esm16.ESM16_PI_CONTROL_CORES]) |

The three components run side by side, each with one thread per rank. Every layout gives the UM an even number of
processes along x, keeps the subdomains of the UM and MOM5 no more than four times longer than they are wide, and
leaves at most 10% of its cores idle. Each CICE5 rank holds one block spanning the whole y extent of its grid, so
CICE5 can only run on a number of cores that divides 360.

A layout is written into `config.yaml`, which requests the whole budget of the layout as `ncpureq` and gives every
submodel its cores, and into the files stating each component's decomposition: `atmosphere/um_env.yaml`,
`ocean/input.nml` and `ice/cice_in.nml`. The sea ice runs `cice_access.exe`, the configuration's
`sea_ice_executable`, and is told its grid, its blocks and its number of processes in `ice/cice_in.nml`.

| Log | Parser | Component | Read from |
|---|---|---|---|
| `UM` | `UMProfilingParser` | `UM7` | The UM's standard output, `atmosphere/<UM_STDOUT_FILE>0` |
| `UM_Total_Walltime` | `UMTotalRuntimeParser` | `UM7` | The same file |
| `MOM5` | `FMSProfilingParser(has_hits=False)` | `MOM5` | The model's standard output, `<model>.out` |
| `CICE5` | `CICE5ProfilingParser` | `CICE5` | `ice/ice_diag.d` |
| `payu` | `PayuJSONProfilingParser` | | Payu's job file of each run |

The other released configurations, historical, esm-piControl, the scenario runs and so on, share the grids,
the components and the core counts of the pre-industrial control. They differ in name, so that their experiments
do not share names, and in their control. To profile the historical configuration, for example, make a copy of the
preset under its name, and point a control source at its release:

```python
from dataclasses import replace

from access.profiling import ESM16_PI_CONTROL, GitControlSource
from access.profiling.models.esm16 import ESM16_CONFIGS_REPOSITORY

historical = replace(ESM16_PI_CONTROL, name="historical")
historical_source = GitControlSource(ESM16_CONFIGS_REPOSITORY, "release-historical-1.1")
```

The AMIP configuration runs the atmosphere alone, with no ocean, no sea ice and nothing to couple, and is the
preset [`ESM16_AMIP`][access.profiling.models.esm16.ESM16_AMIP]: `release-amip-1.0` runs it on 208 cores.

## ACCESS-ESM1.5

| | |
|---|---|
| Presets | [`ESM15_PREINDUSTRIAL`][access.profiling.models.esm15.ESM15_PREINDUSTRIAL], the pre-industrial configuration, and [`ESM15_HISTORICAL`][access.profiling.models.esm15.ESM15_HISTORICAL], the historical one, both with concentration-driven greenhouse gases |
| Controls | [`ESM15_PREINDUSTRIAL_SOURCE`][access.profiling.models.esm15.ESM15_PREINDUSTRIAL_SOURCE] and [`ESM15_HISTORICAL_SOURCE`][access.profiling.models.esm15.ESM15_HISTORICAL_SOURCE], `release-preindustrial+concentrations-2.0` and `release-historical+concentrations-2.0` of `access-esm1.5-configs` |
| Manager | `PayuManager` |
| Components | `UM7` on the N96 grid, 192 × 144; `MOM5` on the one-degree grid, 360 × 300; `CICE4` on 12 ranks |
| Whole model | `ACCESS-ESM1.5 preindustrial+concentrations` |
| Release cores | `UM7` 240, `MOM5` 180 and `CICE4` 12, on 9 nodes of 48 cores ([`ESM15_PREINDUSTRIAL_CORES`][access.profiling.models.esm15.ESM15_PREINDUSTRIAL_CORES]) |

The UM and MOM5 run as in ACCESS-ESM1.6, with the same requirements on their layouts. CICE4 is the difference: its
grid, its blocks and its rank count are compiled into its executable, `cice_access_360x300_12x1_12p.exe`, so every
layout gives it 12 ranks ([`ESM15_CICE4_RANKS`][access.profiling.models.esm15.ESM15_CICE4_RANKS]) and nothing is
written for it. An allocation strategy that keeps the sea ice off 12 cores finds no layout. Payu names the model
`access`, which is what the configuration's `model_type` says.

A layout is written into `config.yaml`, which requests the whole budget of the layout as `ncpureq` and gives every
submodel its cores, and into `atmosphere/um_env.yaml` and `ocean/input.nml`, which state the UM's and MOM5's
decompositions.

| Log | Parser | Component | Read from |
|---|---|---|---|
| `UM` | `UMProfilingParser` | `UM7` | The UM's standard output, `atmosphere/<UM_STDOUT_FILE>0` |
| `UM_Total_Walltime` | `UMTotalRuntimeParser` | `UM7` | The same file |
| `MOM5` | `FMSProfilingParser(has_hits=False)` | `MOM5` | The model's standard output, `access.out` |
| `CICE4` | `CICE5ProfilingParser` | `CICE4` | `ice/ice_diag.d`, which CICE4 writes in CICE5's format |
| `payu` | `PayuJSONProfilingParser` | | Payu's job file of each run |

## ACCESS-OM2

| | |
|---|---|
| Presets | [`OM2_1DEG`][access.profiling.models.om2.OM2_1DEG], [`OM2_025DEG`][access.profiling.models.om2.OM2_025DEG] and [`OM2_01DEG`][access.profiling.models.om2.OM2_01DEG], one per resolution: the forcing and the biogeochemistry of a release change its core counts, not how it is parallelised |
| Control | A `GitControlSource` of [`OM2_CONFIGS_REPOSITORY`][access.profiling.models.om2.OM2_CONFIGS_REPOSITORY] at the release, such as `release-1deg_jra55_ryf-4.1` |
| Manager | `PayuManager` |
| Components | `YATM`, the data atmosphere, on one core; `MOM5` on the grid of the resolution, 360 × 300, 1440 × 1080 or 3600 × 2700; `CICE5` on the same grid |
| Whole model | `ACCESS-OM2 1deg`, `ACCESS-OM2 025deg` and `ACCESS-OM2 01deg` |
| Release cores | `MOM5` 216 and `CICE5` 24 at one degree ([`OM2_1DEG_CORES`][access.profiling.models.om2.OM2_1DEG_CORES]), 1456 and 361 at a quarter ([`OM2_025DEG_CORES`][access.profiling.models.om2.OM2_025DEG_CORES]), 4358 and 722 at a tenth ([`OM2_01DEG_CORES`][access.profiling.models.om2.OM2_01DEG_CORES]), and `YATM` 1 in each |

YATM is serial, so every layout gives it one core. MOM5's subdomains are at most four times longer than they are
wide, as in ACCESS-ESM1.6, and every layout leaves at most 10% of its cores idle. CICE5 keeps the block size its
control pins. At one degree the blocks are distributed cartesian in one row, so the search chooses how the 24 block
columns are shared out and CICE5's ranks divide 24; at a quarter and a tenth of a degree they are distributed
roundrobin and sectrobin, which form no process grid, so the search chooses CICE5's rank count and nothing else.

A layout is written into `config.yaml`, which requests the whole budget as `ncpureq` and gives every submodel its
cores, into `ocean/input.nml`, which states MOM5's process grid, and into `ice/cice_in.nml`, which states CICE5's
rank count, its grid and its blocks per rank. Two things in the releases need control options, described in
[Experiments with Payu](payu.md#generating-the-experiments). MOM5 runs on a masked layout, so its release core
counts are the process grid's product less the ranks a mask table among the ocean's inputs removes, and a generated
layout runs unmasked once the table is taken out. And the sea ice runs `cice_auscom.exe`, the runtime-grid CICE5 of
`access-om2/2026.08.002` and later, which the `+wombatlite` releases run already; the `-4.1` and `_bgc` releases run
executables compiled for their layout, so a study of one loads that module.

| Log | Parser | Component | Read from |
|---|---|---|---|
| `MOM5` | `FMSProfilingParser(has_hits=False)` | `MOM5` | The model's standard output, `access-om2.out` |
| `CICE5` | `CICE5ProfilingParser` | `CICE5` | `ice/ice_diag.d` |
| `payu` | `PayuJSONProfilingParser` | | Payu's job file of each run |

YATM writes no timers this package reads.

## ACCESS-OM3

| | |
|---|---|
| Presets | [`OM3_MC_25KM`][access.profiling.models.om3.OM3_MC_25KM], MOM6 and CICE6 at 25 km, which every released configuration is; and, under development, [`OM3_MC_100KM`][access.profiling.models.om3.OM3_MC_100KM] and [`OM3_MCW_100KM`][access.profiling.models.om3.OM3_MCW_100KM] at 100 km, without and with WW3, [`OM3_MC_8KM`][access.profiling.models.om3.OM3_MC_8KM] at 8 km, and the pan-Antarctic regional configurations at 4 km, [`OM3_MC_PANAN4KM`][access.profiling.models.om3.OM3_MC_PANAN4KM] and, with ice shelves, [`OM3_MC_PANAN4KM_ISF`][access.profiling.models.om3.OM3_MC_PANAN4KM_ISF] |
| Control | [`OM3_MC_25KM_SOURCE`][access.profiling.models.om3.OM3_MC_25KM_SOURCE], `release-MC_25km_jra_ryf-2.0-beta` of `access-om3-configs` |
| Manager | `PayuManager` |
| Components | `shared`, the range of cores `cpl`, `atm`, `ice` and `rof` take turns on, `ocn` beside it, and `wav` beside that where there are waves; all on the configuration's grid, 1440 × 1152 at 25 km |
| Whole model | `ACCESS-OM3 MC-25km`, `ACCESS-OM3 MC-100km`, and so on |
| Release cores | 275 for each of `cpl`, `atm`, `ice` and `rof`, and 2429 for `ocn`, on 26 nodes of 104 cores ([`OM3_MC_25KM_CORES`][access.profiling.models.om3.OM3_MC_25KM_CORES]); each development configuration's cores are beside its preset, as [`OM3_MC_100KM_CORES`][access.profiling.models.om3.OM3_MC_100KM_CORES] |

The mediator (`cpl`), the data atmosphere (`atm`), the sea ice (`ice`) and the data runoff (`rof`) run in turn on the
`shared` range, and the ocean (`ocn`) runs beside them on a range of its own, as do the waves (`wav`) of a
configuration that has them. Every component has one thread per rank. Every layout leaves at most 10% of its cores
idle, and gives each CICE6 rank 5 to 8 blocks: the blocks are distributed as the configuration's control distributes
them, and their size is chosen from the number of ranks.

A layout is written into `config.yaml`, which requests the whole budget as `ncpus`, into the `PELAYOUT_attributes`
of `nuopc.runconfig`, and into the grid and the block size of `ice_in`. The ocean's decomposition is left to MOM6, so the layout
search chooses no process grid for it and `MOM_input` is not changed. Note that the 25 km release, the 8 km
configuration and the pan-Antarctic ones pin a `LAYOUT` and a `MASKTABLE` in `MOM_input`, which a study has to
remove through its control options, as shown in
[Generating the experiments](../user-guide/experiments.md#generating-the-experiments).

| Log | Parser | Component | Read from |
|---|---|---|---|
| `MOM6` | `FMSProfilingParser(has_hits=True)` | `ocn` | The model's standard output, `<model>.out` |
| `CICE6` | `CICE5ProfilingParser` | `ice` | `log/ice.log` |
| `ESMF` | `ESMFSummaryProfilingParser` | | `ESMF_Profile.summary`, written only when `ESMF_RUNTIME_PROFILE` is `on` and `ESMF_RUNTIME_PROFILE_OUTPUT` is `SUMMARY` |
| `payu` | `PayuJSONProfilingParser` | | Payu's job file of each run |

All three model logs are optional, so a run that does not write one of them is still parsed. The ESMF summary
times the whole coupled model, so its regions need a `component` in the groups that plot them against
component cores.

A configuration on another grid, or whose sea ice distributes its blocks differently, needs no code: it is an
[`OM3Configuration`][access.profiling.models.om3.OM3Configuration] with a `CICEPartitioning` of its own, as the
presets are. Other arrangements, such as a MOM6 layout the search chooses and writes into `MOM_input`, a data sea
ice, or a shared realm starting at an offset within its range, are a subclass of `OM3Configuration` returning the
components it wants from `shared` and `concurrent`, built from the package's own
[`CICEComponent`][access.profiling.models.components.CICEComponent] and
[`PlainComponent`][access.profiling.payu_configuration.PlainComponent]; writing the PE layout out and reading it
back stay the base class's.

## ACCESS-AM3 and ACCESS-rAM3

| | ACCESS-AM3 | ACCESS-rAM3 |
|---|---|---|
| Preset | [`AM3_N96E`][access.profiling.models.um_suites.AM3_N96E] | [`RAM3`][access.profiling.models.um_suites.RAM3] |
| Control | [`AM3_N96E_SOURCE`][access.profiling.models.um_suites.AM3_N96E_SOURCE], `release-n96e-3.0` of the ACCESS-AM3 configurations | None: the suite is checked out by hand |
| Manager | `CylcRoseManager` | `CylcRoseManager` |
| Layout variables | `MAIN_ATM_PROCX` and `MAIN_ATM_PROCY` | `rg01_rs01_m01_nproc`, as `x,y` |
| Threads and I/O server | `MAIN_OMPTHR_ATM` and `MAIN_IOS_NPROC` | None |

Both read the UM timers of their tasks with the same two parsers, as the `<task>_cycle<cycle>_UM_regions` and
`<task>_cycle<cycle>_UM_total` logs. [Cylc Rose suites](cylc-rose-suites.md) explains how their experiments are
prepared and analysed.
