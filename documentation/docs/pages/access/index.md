# ACCESS models

access-profiling comes with everything needed to profile the ACCESS climate models developed at
[ACCESS-NRI](https://github.com/ACCESS-NRI), which run on NCI's Gadi supercomputer:

- a manager for each of their workflow engines: [Payu](payu.md), for ACCESS-ESM1.5, ACCESS-ESM1.6, ACCESS-OM2 and
  ACCESS-OM3, and [Cylc and Rose](cylc-rose-suites.md), for ACCESS-AM3, ACCESS-rAM3 and ACCESS-CM3;
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
different configurations have different names, and the control. The package has a control source for each of
them, at its latest release:

| Configuration | Control | Release |
|---|---|---|
| `piControl` | `ESM16_PI_CONTROL_SOURCE` | `release-piControl-2.1` |
| `historical` | `ESM16_HISTORICAL_SOURCE` | `release-historical-1.3` |
| `amip` | `ESM16_AMIP_SOURCE` | `release-amip-1.0` |
| `esm-piControl` | `ESM16_ESM_PICONTROL_SOURCE` | `release-esm-piControl-2.1` |
| `esm-historical` | `ESM16_ESM_HISTORICAL_SOURCE` | `release-esm-historical-1.3` |
| `1pctCO2` | `ESM16_1PCTCO2_SOURCE` | `release-1pctCO2-1.0` |
| `1pctCO2-bgc` | `ESM16_1PCTCO2_BGC_SOURCE` | `release-1pctCO2-bgc-1.0` |
| `1pctCO2-rad` | `ESM16_1PCTCO2_RAD_SOURCE` | `release-1pctCO2-rad-1.0` |
| `abrupt-0p5xCO2` | `ESM16_ABRUPT_0P5XCO2_SOURCE` | `release-abrupt-0p5xCO2-2.0` |
| `abrupt-2xCO2` | `ESM16_ABRUPT_2XCO2_SOURCE` | `release-abrupt-2xCO2-2.0` |
| `abrupt-4xCO2` | `ESM16_ABRUPT_4XCO2_SOURCE` | `release-abrupt-4xCO2-1.0` |
| `esm-flat10` | `ESM16_ESM_FLAT10_SOURCE` | `release-esm-flat10-1.0` |
| `esm-flat10-cdr` | `ESM16_ESM_FLAT10_CDR_SOURCE` | `release-esm-flat10-cdr-1.0` |
| `esm-flat10-zec` | `ESM16_ESM_FLAT10_ZEC_SOURCE` | `release-esm-flat10-zec-1.0` |
| `scen7-h` | `ESM16_SCEN7_H_SOURCE` | `release-scen7-h-1.0` |
| `scen7-vl` | `ESM16_SCEN7_VL_SOURCE` | `release-scen7-vl-1.0` |
| `esm-scen7-h` | `ESM16_ESM_SCEN7_H_SOURCE` | `release-esm-scen7-h-1.0` |
| `esm-scen7-vl` | `ESM16_ESM_SCEN7_VL_SOURCE` | `release-esm-scen7-vl-1.0` |

To profile one of them, use the preset under its name, together with its control:

```python
from dataclasses import replace

from access.profiling.models.esm16 import ESM16_HISTORICAL_SOURCE, ESM16_PI_CONTROL

historical = replace(ESM16_PI_CONTROL, name="historical")
```

The manager is then created with `historical` as its application and `ESM16_HISTORICAL_SOURCE` as its control.

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
| Presets | [`OM2_1DEG`][access.profiling.models.om2.OM2_1DEG], [`OM2_025DEG`][access.profiling.models.om2.OM2_025DEG] and [`OM2_01DEG`][access.profiling.models.om2.OM2_01DEG], one per resolution |
| Controls | One per released configuration, at its latest release: [`OM2_1DEG_JRA55_RYF_SOURCE`][access.profiling.models.om2.OM2_1DEG_JRA55_RYF_SOURCE] and the others in the table below |
| Manager | `PayuManager` |
| Components | `YATM`, the data atmosphere, on one core; `MOM5` on the grid of the resolution, 360 × 300, 1440 × 1080 or 3600 × 2700; `CICE5` on the same grid |
| Whole model | `ACCESS-OM2 1deg`, `ACCESS-OM2 025deg` and `ACCESS-OM2 01deg` |
| Release cores | `MOM5` 216 and `CICE5` 24 at one degree ([`OM2_1DEG_CORES`][access.profiling.models.om2.OM2_1DEG_CORES]), 1456 and 361 at a quarter ([`OM2_025DEG_CORES`][access.profiling.models.om2.OM2_025DEG_CORES]), 4358 and 722 at a tenth ([`OM2_01DEG_CORES`][access.profiling.models.om2.OM2_01DEG_CORES]), and `YATM` 1 in each |

YATM is serial, so every layout gives it one core. The subdomains of MOM5 are at most four times longer than they
are wide, as in ACCESS-ESM1.6, and at most 10% of the cores are left idle. CICE5 keeps the block size that its
control fixes. At one degree, the blocks are distributed cartesian in one row. The search then chooses how the 24
columns of blocks are shared out, so the number of CICE5 ranks divides 24. At a quarter of a degree the blocks are
distributed roundrobin, and at a tenth of a degree sectrobin. These two distributions form no process grid, so the
search only chooses the number of CICE5 ranks.

To run a layout, a few files are changed. `config.yaml` gets the number of cores of each submodel, and requests
the total number of cores of the layout as `ncpureq`. The process grid of MOM5 goes into `ocean/input.nml`, and the
number of ranks of CICE5, its grid and its blocks per rank into `ice/cice_in.nml`.

Two things in the releases need control options, which are described in
[Experiments with Payu](payu.md#generating-the-experiments). First, MOM5 runs a layout with a mask in the releases.
A mask table, which is one of the inputs of the ocean, removes the ranks that only hold land. The number of cores of
the release is therefore the product of the process grid, less the ranks that the table removes. A generated layout
runs without a mask, so the table has to be removed. Second, the sea ice has to run `cice_auscom.exe`. This is the
CICE5 of `access-om2/2026.08.002` and later, which reads its grid when it runs. The `+wombatlite` releases already
run it. The `-4.1` and `_bgc` releases run executables that are compiled for their layout instead, so a study of
one of them has to load that module.

| Log | Parser | Component | Read from |
|---|---|---|---|
| `MOM5` | `FMSProfilingParser(has_hits=False)` | `MOM5` | The model's standard output, `access-om2.out` |
| `CICE5` | `CICE5ProfilingParser` | `CICE5` | `ice/ice_diag.d` |
| `payu` | `PayuJSONProfilingParser` | | Payu's job file of each run |

YATM writes no timers that this package reads.

The forcing and the biogeochemistry of a release change its number of cores, but not how it is parallelised. Each
release is therefore profiled with the preset of its resolution and with its own control. If several releases share
a working directory, give the preset the name of the release. The package has a control source for each released
configuration, at its latest release:

| Configuration | Control | Release |
|---|---|---|
| `1deg_jra55_ryf` | `OM2_1DEG_JRA55_RYF_SOURCE` | `release-1deg_jra55_ryf-4.1` |
| `1deg_jra55_ryf_bgc` | `OM2_1DEG_JRA55_RYF_BGC_SOURCE` | `release-1deg_jra55_ryf_bgc-3.0` |
| `1deg_jra55_ryf+wombatlite` | `OM2_1DEG_JRA55_RYF_WOMBATLITE_SOURCE` | `release-1deg_jra55_ryf+wombatlite-3.0` |
| `1deg_jra55_iaf` | `OM2_1DEG_JRA55_IAF_SOURCE` | `release-1deg_jra55_iaf-4.1` |
| `1deg_jra55_iaf_bgc` | `OM2_1DEG_JRA55_IAF_BGC_SOURCE` | `release-1deg_jra55_iaf_bgc-3.0` |
| `1deg_jra55_iaf+wombatlite` | `OM2_1DEG_JRA55_IAF_WOMBATLITE_SOURCE` | `release-1deg_jra55_iaf+wombatlite-3.0` |
| `025deg_jra55_ryf` | `OM2_025DEG_JRA55_RYF_SOURCE` | `release-025deg_jra55_ryf-4.1` |
| `025deg_jra55_ryf_bgc` | `OM2_025DEG_JRA55_RYF_BGC_SOURCE` | `release-025deg_jra55_ryf_bgc-3.0` |
| `025deg_jra55_ryf+wombatlite` | `OM2_025DEG_JRA55_RYF_WOMBATLITE_SOURCE` | `release-025deg_jra55_ryf+wombatlite-3.0` |
| `025deg_jra55_iaf` | `OM2_025DEG_JRA55_IAF_SOURCE` | `release-025deg_jra55_iaf-4.1` |
| `025deg_jra55_iaf_bgc` | `OM2_025DEG_JRA55_IAF_BGC_SOURCE` | `release-025deg_jra55_iaf_bgc-3.0` |
| `025deg_jra55_iaf+wombatlite` | `OM2_025DEG_JRA55_IAF_WOMBATLITE_SOURCE` | `release-025deg_jra55_iaf+wombatlite-3.0` |
| `01deg_jra55_ryf` | `OM2_01DEG_JRA55_RYF_SOURCE` | `release-01deg_jra55_ryf-4.1` |
| `01deg_jra55_ryf_bgc` | `OM2_01DEG_JRA55_RYF_BGC_SOURCE` | `release-01deg_jra55_ryf_bgc-3.0` |
| `01deg_jra55_ryf+wombatlite` | `OM2_01DEG_JRA55_RYF_WOMBATLITE_SOURCE` | `release-01deg_jra55_ryf+wombatlite-3.0` |
| `01deg_jra55_iaf` | `OM2_01DEG_JRA55_IAF_SOURCE` | `release-01deg_jra55_iaf-4.1` |
| `01deg_jra55_iaf_bgc` | `OM2_01DEG_JRA55_IAF_BGC_SOURCE` | `release-01deg_jra55_iaf_bgc-3.0` |
| `01deg_jra55_iaf+wombatlite` | `OM2_01DEG_JRA55_IAF_WOMBATLITE_SOURCE` | `release-01deg_jra55_iaf+wombatlite-3.0` |

## ACCESS-OM3

| | |
|---|---|
| Presets | [`OM3_MC_25KM`][access.profiling.models.om3.OM3_MC_25KM], MOM6 and CICE6 at 25 km. Every release so far runs this configuration. The others are under development: [`OM3_MC_100KM`][access.profiling.models.om3.OM3_MC_100KM] and [`OM3_MCW_100KM`][access.profiling.models.om3.OM3_MCW_100KM] at 100 km, without and with WW3; [`OM3_MC_8KM`][access.profiling.models.om3.OM3_MC_8KM] at 8 km; and the pan-Antarctic regional configurations at 4 km, [`OM3_MC_PANAN4KM`][access.profiling.models.om3.OM3_MC_PANAN4KM] and, with ice shelves, [`OM3_MC_PANAN4KM_ISF`][access.profiling.models.om3.OM3_MC_PANAN4KM_ISF] |
| Controls | One per released configuration, at its latest release, and one per configuration under development, at its `dev-*` branch: [`OM3_MC_25KM_JRA_RYF_SOURCE`][access.profiling.models.om3.OM3_MC_25KM_JRA_RYF_SOURCE] and the others in the table below |
| Manager | `PayuManager` |
| Components | `shared`, the range of cores `cpl`, `atm`, `ice` and `rof` take turns on, `ocn` beside it, and `wav` beside that where there are waves; all on the configuration's grid, 1440 × 1152 at 25 km |
| Whole model | `ACCESS-OM3 MC-25km`, `ACCESS-OM3 MC-100km`, and so on |
| Release cores | 275 for each of `cpl`, `atm`, `ice` and `rof`, and 2429 for `ocn`, on 26 nodes of 104 cores ([`OM3_MC_25KM_CORES`][access.profiling.models.om3.OM3_MC_25KM_CORES]). The cores of each configuration under development are in a constant named after its preset, such as [`OM3_MC_100KM_CORES`][access.profiling.models.om3.OM3_MC_100KM_CORES] |

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

Every release so far is the 25 km configuration. The other configurations are under development on `dev-*`
branches, and the package has a control source for each of them as well:

| Configuration | Control | Release |
|---|---|---|
| `MC_25km_jra_ryf` | `OM3_MC_25KM_JRA_RYF_SOURCE` | `release-MC_25km_jra_ryf-2.0-beta` |
| `MC_25km_jra_iaf` | `OM3_MC_25KM_JRA_IAF_SOURCE` | `release-MC_25km_jra_iaf-2.0-beta` |
| `MC_25km_jra_ryf+wombatlite` | `OM3_MC_25KM_JRA_RYF_WOMBATLITE_SOURCE` | `release-MC_25km_jra_ryf+wombatlite-1.0-beta` |
| `MC_25km_jra_iaf+wombatlite` | `OM3_MC_25KM_JRA_IAF_WOMBATLITE_SOURCE` | `release-MC_25km_jra_iaf+wombatlite-1.0-beta` |
| `MC_100km_jra_ryf` | `OM3_MC_100KM_JRA_RYF_SOURCE` | `dev-MC_100km_jra_ryf` |
| `MC_100km_jra_iaf` | `OM3_MC_100KM_JRA_IAF_SOURCE` | `dev-MC_100km_jra_iaf` |
| `MC_100km_jra_ryf+wombatlite` | `OM3_MC_100KM_JRA_RYF_WOMBATLITE_SOURCE` | `dev-MC_100km_jra_ryf+wombatlite` |
| `MC_100km_jra_iaf+wombatlite` | `OM3_MC_100KM_JRA_IAF_WOMBATLITE_SOURCE` | `dev-MC_100km_jra_iaf+wombatlite` |
| `MCW_100km_era_iaf` | `OM3_MCW_100KM_ERA_IAF_SOURCE` | `dev-MCW_100km_era_iaf` |
| `MC_8km_jra_ryf` | `OM3_MC_8KM_JRA_RYF_SOURCE` | `dev-MC_8km_jra_ryf` |
| `MC_panan4km_jra_ryf` | `OM3_MC_PANAN4KM_JRA_RYF_SOURCE` | `dev-MC_panan4km_jra_ryf` |
| `MC_panan4km_jra_ryf+isf` | `OM3_MC_PANAN4KM_JRA_RYF_ISF_SOURCE` | `dev-MC_panan4km_jra_ryf+isf` |

You do not need to write any code for a configuration on another grid, or for one whose sea ice distributes its
blocks differently. Such a configuration is an [`OM3Configuration`][access.profiling.models.om3.OM3Configuration]
with a `CICEPartitioning` of its own, like the presets. Other arrangements need a subclass of `OM3Configuration`.
This is the case for a process grid of MOM6 that the search chooses and writes into `MOM_input`, for a data sea
ice, or for a component that starts at an offset within the shared range of cores. The subclass returns the
components that it wants from `shared` and `concurrent`. It builds them from the
[`CICEComponent`][access.profiling.models.components.CICEComponent] and the
[`PlainComponent`][access.profiling.payu_configuration.PlainComponent] of the package. The base class still writes
the `PELAYOUT_attributes` and reads them back.

## ACCESS-AM3, ACCESS-rAM3 and ACCESS-CM3

| | ACCESS-AM3 | ACCESS-rAM3 | ACCESS-CM3 |
|---|---|---|---|
| Presets | [`AM3_N96E`][access.profiling.models.um_suites.AM3_N96E] and [`AM3_N512E`][access.profiling.models.um_suites.AM3_N512E] | [`RAM3`][access.profiling.models.um_suites.RAM3] | [`CM3_O25KM`][access.profiling.models.um_suites.CM3_O25KM] |
| Controls | [`AM3_N96E_SOURCE`][access.profiling.models.um_suites.AM3_N96E_SOURCE] and [`AM3_N512E_SOURCE`][access.profiling.models.um_suites.AM3_N512E_SOURCE], `release-n96e-3.0` and `release-n512e-1.0` of the ACCESS-AM3 configurations | None: the suite is checked out by hand | [`CM3_O25KM_SOURCE`][access.profiling.models.um_suites.CM3_O25KM_SOURCE], the `cm3_O25km` branch of the ACCESS-CM3 configurations, as there is no release yet |
| Manager | `CylcRoseManager` | `CylcRoseManager` | `CylcRoseManager` |
| Layout variables | `MAIN_ATM_PROCX` and `MAIN_ATM_PROCY` | `rg01_rs01_m01_nproc`, as `x,y` | `ATM_NPROCX` and `ATM_NPROCY` |
| Threads and I/O server | `MAIN_OMPTHR_ATM` and `MAIN_IOS_NPROC` | None | `FLUME_IOS_NPROC`; one thread per rank |
| Other components | None | None | `OCN_NPROC` and `ICE_NPROC`, the cores of MOM6 and CICE6, added to the UM's |

For the three models, the UM timers of each task are read with the same two parsers, and give the logs
`<task>_cycle<cycle>_UM_regions` and `<task>_cycle<cycle>_UM_total`. The logs that the ocean and the sea ice of
ACCESS-CM3 write are not read. See [Cylc Rose suites](cylc-rose-suites.md) to learn how to prepare and analyse
their experiments.
