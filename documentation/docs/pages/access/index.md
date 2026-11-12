# ACCESS models

access-profiling comes with everything needed to profile the ACCESS climate models developed at
[ACCESS-NRI](https://github.com/ACCESS-NRI), which run on NCI's Gadi supercomputer:

- a manager for each of their workflow engines: [Payu](payu.md), for ACCESS-ESM1.5, ACCESS-ESM1.6, ACCESS-OM2 and
  ACCESS-OM3, and [Cylc and Rose](cylc-rose-suites.md), for ACCESS-AM3 and ACCESS-rAM3;
- parsers for the [log formats](log-formats.md) of their components and of these workflow engines;
- presets for each model, together with the official release that each describes, to use as the control.

Each preset is described below. You will find the components of the model, the rules that every parallelisation layout
follows, the logs that a run writes, and the number of cores used by the release. If your configuration is on a
branch, a fork or a commit that the presets do not describe, you can read it from the files of the control with the
`from_control` of its model, as [Experiments with Payu](payu.md#reading-a-configuration-from-its-control) shows. For
the full details, see the [Models API](../api/models.md).

## ACCESS-ESM1.6

| | |
|---|---|
| Preset | [`ESM16_PI_CONTROL`][access.profiling.models.esm16.ESM16_PI_CONTROL], the pre-industrial control |
| Control | [`ESM16_PI_CONTROL_SOURCE`][access.profiling.models.esm16.ESM16_PI_CONTROL_SOURCE], `release-piControl-2.1` of `access-esm1.6-configs` |
| Manager | `PayuManager` |
| Components | `UM7` on the N96 grid, 192 × 144; `MOM5` on the one-degree grid, 360 × 300; `CICE5` on the ocean's grid |
| Whole model | `ACCESS-ESM1.6 piControl` |
| Release cores | `UM7` 256, `MOM5` 240 and `CICE5` 12, on 5 nodes of 104 cores ([`ESM16_PI_CONTROL_CORES`][access.profiling.models.esm16.ESM16_PI_CONTROL_CORES]) |

The three components run at the same time, each on its own cores, with one thread per rank. Every layout follows
a few rules. The UM gets an even number of processes along x. The subdomains of the UM and of MOM5 are at most
four times longer than they are wide. And at most 10% of the cores are left idle. Each CICE5 rank holds one block
spanning the whole y extent of its grid, so CICE5 can only run on a number of cores that divides 360.

To run a layout, a few files are changed. `config.yaml` gets the number of cores of each submodel, and requests
the total number of cores of the layout as `ncpureq`. The decomposition of each component goes into its own file:
`atmosphere/um_env.yaml`, `ocean/input.nml` and `ice/cice_in.nml`. The sea ice runs `cice_access.exe`, which is
the `sea_ice_executable` of the configuration, and gets its grid, its blocks and its number of processes from
`ice/cice_in.nml`.

| Log | Parser | Component | Read from |
|---|---|---|---|
| `UM` | `UMProfilingParser` | `UM7` | The UM's standard output, `atmosphere/<UM_STDOUT_FILE>0` |
| `UM_Total_Walltime` | `UMTotalRuntimeParser` | `UM7` | The same file |
| `MOM5` | `FMSProfilingParser(has_hits=False)` | `MOM5` | The model's standard output, `<model>.out` |
| `CICE5` | `CICE5ProfilingParser` | `CICE5` | `ice/ice_diag.d` |
| `payu` | `PayuJSONProfilingParser` | | Payu's job file of each run |

There are other released configurations of ACCESS-ESM1.6, such as historical, esm-piControl and the scenario
runs. They have the same grids, the same components and the same numbers of cores as the pre-industrial control,
so you can profile them without writing any code. Only two things change: the name, so that the experiments of
different configurations have different names, and the control. To profile the historical configuration, for
example, make a copy of the preset with a new name, and create a control for its release:

```python
from dataclasses import replace

from access.profiling import ESM16_PI_CONTROL, GitControlSource
from access.profiling.models.esm16 import ESM16_CONFIGS_REPOSITORY

historical = replace(ESM16_PI_CONTROL, name="historical")
historical_source = GitControlSource(ESM16_CONFIGS_REPOSITORY, "release-historical-1.1")
```

The AMIP configuration runs the atmosphere on its own, without an ocean, a sea ice or a coupler. It is the preset
[`ESM16_AMIP`][access.profiling.models.esm16.ESM16_AMIP], and `release-amip-1.0` runs it on 208 cores.

## ACCESS-ESM1.5

| | |
|---|---|
| Presets | [`ESM15_PREINDUSTRIAL`][access.profiling.models.esm15.ESM15_PREINDUSTRIAL], the pre-industrial configuration, and [`ESM15_HISTORICAL`][access.profiling.models.esm15.ESM15_HISTORICAL], the historical one, both with concentration-driven greenhouse gases |
| Controls | [`ESM15_PREINDUSTRIAL_SOURCE`][access.profiling.models.esm15.ESM15_PREINDUSTRIAL_SOURCE] and [`ESM15_HISTORICAL_SOURCE`][access.profiling.models.esm15.ESM15_HISTORICAL_SOURCE], `release-preindustrial+concentrations-2.0` and `release-historical+concentrations-2.0` of `access-esm1.5-configs` |
| Manager | `PayuManager` |
| Components | `UM7` on the N96 grid, 192 × 144; `MOM5` on the one-degree grid, 360 × 300; `CICE4` on 12 ranks |
| Whole model | `ACCESS-ESM1.5 preindustrial+concentrations` |
| Release cores | `UM7` 240, `MOM5` 180 and `CICE4` 12, on 9 nodes of 48 cores ([`ESM15_PREINDUSTRIAL_CORES`][access.profiling.models.esm15.ESM15_PREINDUSTRIAL_CORES]) |

The UM and MOM5 run as in ACCESS-ESM1.6, and their layouts follow the same rules. The difference is CICE4. Its grid,
its blocks and its number of ranks are compiled into its executable, `cice_access_360x300_12x1_12p.exe`. Every
layout therefore gives it 12 ranks ([`ESM15_CICE4_RANKS`][access.profiling.models.esm15.ESM15_CICE4_RANKS]), and
nothing is written for it. Note that an allocation strategy that does not allow the sea ice 12 cores finds no
layout. Payu calls the model `access`, and this is the `model_type` of the configuration.

To run a layout, a few files are changed. `config.yaml` gets the number of cores of each submodel, and requests
the total number of cores of the layout as `ncpureq`. The decompositions of the UM and of MOM5 go into
`atmosphere/um_env.yaml` and `ocean/input.nml`.

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

YATM is serial, so every layout gives it one core. The subdomains of MOM5 are at most four times longer than they
are wide, as in ACCESS-ESM1.6, and at most 10% of the cores are left idle. CICE5 keeps the block size that its
control fixes. At one degree, the blocks are distributed cartesian in one row. The search then chooses how the 24
columns of blocks are shared out, so the number of CICE5 ranks divides 24. At a quarter and at a tenth of a degree,
the blocks are distributed roundrobin and sectrobin. These distributions form no process grid, so the search only
chooses the number of CICE5 ranks.

To run a layout, a few files are changed. `config.yaml` gets the number of cores of each submodel, and requests
the total number of cores of the layout as `ncpureq`. The process grid of MOM5 goes into `ocean/input.nml`, and the
number of ranks of CICE5, its grid and its blocks per rank into `ice/cice_in.nml`. Two things in the releases need
control options, which are described in [Experiments with Payu](payu.md#generating-the-experiments). First, MOM5
runs on a masked layout. The number of cores of the release is the product of the process grid, less the ranks that
a mask table among the inputs of the ocean removes. A generated layout runs without a mask, once the table is taken
out. Second, the sea ice runs `cice_auscom.exe`, the CICE5 of `access-om2/2026.08.002` and later, which reads its
grid when it runs. The `+wombatlite` releases already run it, but the `-4.1` and `_bgc` releases run executables
compiled for their layout, so a study of one of them loads that module.

| Log | Parser | Component | Read from |
|---|---|---|---|
| `MOM5` | `FMSProfilingParser(has_hits=False)` | `MOM5` | The model's standard output, `access-om2.out` |
| `CICE5` | `CICE5ProfilingParser` | `CICE5` | `ice/ice_diag.d` |
| `payu` | `PayuJSONProfilingParser` | | Payu's job file of each run |

YATM writes no timers that this package reads.

## ACCESS-OM3

| | |
|---|---|
| Presets | [`OM3_MC_25KM`][access.profiling.models.om3.OM3_MC_25KM], MOM6 and CICE6 at 25 km, which is the configuration of every release so far; and, under development, [`OM3_MC_100KM`][access.profiling.models.om3.OM3_MC_100KM] and [`OM3_MCW_100KM`][access.profiling.models.om3.OM3_MCW_100KM] at 100 km, without and with WW3, [`OM3_MC_8KM`][access.profiling.models.om3.OM3_MC_8KM] at 8 km, and the pan-Antarctic regional configurations at 4 km, [`OM3_MC_PANAN4KM`][access.profiling.models.om3.OM3_MC_PANAN4KM] and, with ice shelves, [`OM3_MC_PANAN4KM_ISF`][access.profiling.models.om3.OM3_MC_PANAN4KM_ISF] |
| Control | [`OM3_MC_25KM_SOURCE`][access.profiling.models.om3.OM3_MC_25KM_SOURCE], `release-MC_25km_jra_ryf-2.0-beta` of `access-om3-configs` |
| Manager | `PayuManager` |
| Components | `shared`, the range of cores `cpl`, `atm`, `ice` and `rof` take turns on, `ocn` beside it, and `wav` beside that where there are waves; all on the configuration's grid, 1440 × 1152 at 25 km |
| Whole model | `ACCESS-OM3 MC-25km`, `ACCESS-OM3 MC-100km`, and so on |
| Release cores | 275 for each of `cpl`, `atm`, `ice` and `rof`, and 2429 for `ocn`, on 26 nodes of 104 cores ([`OM3_MC_25KM_CORES`][access.profiling.models.om3.OM3_MC_25KM_CORES]); each development configuration's cores are beside its preset, as [`OM3_MC_100KM_CORES`][access.profiling.models.om3.OM3_MC_100KM_CORES] |

In ACCESS-OM3, the mediator (`cpl`), the data atmosphere (`atm`), the sea ice (`ice`) and the data runoff (`rof`)
take turns on the same cores, which are called `shared`. The ocean (`ocn`) runs next to them on cores of its own,
and so do the waves (`wav`) of a configuration that has them. Every component has one thread per rank. Every layout
leaves at most 10% of its cores idle, and gives each CICE6 rank 5 to 8 blocks. The blocks are distributed as in the
control of the configuration, and their size is chosen from the number of ranks.

To run a layout, a few files are changed. `config.yaml` requests the total number of cores as `ncpus`, the
`PELAYOUT_attributes` of `nuopc.runconfig` get the cores of each component, and `ice_in` gets the grid and the
block size. MOM6 chooses its own decomposition, so there is no process grid for the ocean and `MOM_input` is not
changed. Note, however, that the 25 km release, the 8 km configuration and the pan-Antarctic ones fix a `LAYOUT`
and a `MASKTABLE` in `MOM_input`. A study has to remove them with its control options, as shown in [Generating the
experiments](../user-guide/experiments.md#generating-the-experiments).

| Log | Parser | Component | Read from |
|---|---|---|---|
| `MOM6` | `FMSProfilingParser(has_hits=True)` | `ocn` | The model's standard output, `<model>.out` |
| `CICE6` | `CICE5ProfilingParser` | `ice` | `log/ice.log` |
| `ESMF` | `ESMFSummaryProfilingParser` | | `ESMF_Profile.summary`, written only when `ESMF_RUNTIME_PROFILE` is `on` and `ESMF_RUNTIME_PROFILE_OUTPUT` is `SUMMARY` |
| `payu` | `PayuJSONProfilingParser` | | Payu's job file of each run |

The three model logs are optional, which means that a run is still parsed if one of them is missing. The ESMF
summary covers the whole coupled model. When you plot its regions against the cores of a component, you therefore
have to say which `component` in the groups.

You do not need to write any code for a configuration on another grid, or whose sea ice distributes its blocks
differently. It is an [`OM3Configuration`][access.profiling.models.om3.OM3Configuration] with a `CICEPartitioning`
of its own, like the presets. Other arrangements need a subclass of `OM3Configuration`. This is the case for a MOM6
layout that the search chooses and writes into `MOM_input`, for a data sea ice, or for a shared realm that starts
at an offset within its range. The subclass returns the components it wants from `shared` and `concurrent`, built
from the package's own [`CICEComponent`][access.profiling.models.components.CICEComponent] and
[`PlainComponent`][access.profiling.payu_configuration.PlainComponent]. Writing the PE layout and reading it back
are still done by the base class.

## ACCESS-AM3 and ACCESS-rAM3

| | ACCESS-AM3 | ACCESS-rAM3 |
|---|---|---|
| Preset | [`AM3_N96E`][access.profiling.models.um_suites.AM3_N96E] | [`RAM3`][access.profiling.models.um_suites.RAM3] |
| Control | [`AM3_N96E_SOURCE`][access.profiling.models.um_suites.AM3_N96E_SOURCE], `release-n96e-3.0` of the ACCESS-AM3 configurations | None: the suite is checked out by hand |
| Manager | `CylcRoseManager` | `CylcRoseManager` |
| Layout variables | `MAIN_ATM_PROCX` and `MAIN_ATM_PROCY` | `rg01_rs01_m01_nproc`, as `x,y` |
| Threads and I/O server | `MAIN_OMPTHR_ATM` and `MAIN_IOS_NPROC` | None |

For both models, the UM timers of each task are read with the same two parsers, and give the logs
`<task>_cycle<cycle>_UM_regions` and `<task>_cycle<cycle>_UM_total`. See [Cylc Rose suites](cylc-rose-suites.md)
to learn how to prepare and analyse their experiments.
