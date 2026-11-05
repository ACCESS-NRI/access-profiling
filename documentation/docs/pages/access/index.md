# ACCESS models

access-profiling implements its general parts for the ACCESS climate models developed at
[ACCESS-NRI](https://github.com/ACCESS-NRI), which run on NCI's Gadi supercomputer:

- a manager for each of the workflow engines they run with: [Payu](payu.md), for ACCESS-ESM1.6 and ACCESS-OM3, and
  [Cylc and Rose](cylc-rose-suites.md), for ACCESS-AM3 and ACCESS-rAM3;
- parsers for the [log formats](log-formats.md) of their components and of those engines;
- a preset for each model, with the release it describes as a control.

Each preset is described below: the components a layout divides cores among, the requirements every layout meets,
the logs a run writes, and the cores the release itself runs on. The [Models API](../api/models.md) has the full
details.

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

## ACCESS-OM3

| | |
|---|---|
| Preset | [`OM3_MC_25KM`][access.profiling.models.om3.OM3_MC_25KM], MOM6 and CICE6 at 25 km |
| Control | [`OM3_MC_25KM_SOURCE`][access.profiling.models.om3.OM3_MC_25KM_SOURCE], `release-MC_25km_jra_ryf-2.0-beta` of `access-om3-configs` |
| Manager | `PayuManager` |
| Components | `shared`, the range of cores `cpl`, `atm`, `ice` and `rof` take turns on, and `ocn` beside it; all on the 1440 × 1152 tripolar grid |
| Whole model | `ACCESS-OM3 MC-25km` |
| Release cores | 275 for each of `cpl`, `atm`, `ice` and `rof`, and 2429 for `ocn`, on 26 nodes of 104 cores ([`OM3_MC_25KM_RELEASE_CORES`][access.profiling.models.om3.OM3_MC_25KM_RELEASE_CORES]) |

The mediator (`cpl`), the data atmosphere (`atm`), the sea ice (`ice`) and the data runoff (`rof`) run in turn on the
`shared` range, and the ocean (`ocn`) runs beside them on a range of its own. Every component has one thread per
rank. A configuration with waves adds a `wav` range of their own. Every layout leaves at most 10% of its cores
idle, and gives each CICE6 rank 5 to 8 blocks: the blocks are distributed roundrobin, and their size is chosen from
the number of ranks.

A layout is written into `config.yaml`, which requests the whole budget as `ncpus`, into the `PELAYOUT_attributes`
of `nuopc.runconfig`, and into the block size of `ice_in`. The preset leaves the ocean's decomposition to MOM6
(`auto_ocean_layout`), so the layout search chooses no process grid for it and `MOM_input` is not changed. Note
that the release does pin a `LAYOUT` and a `MASKTABLE` in `MOM_input`, which a study has to remove through its
control options, as shown in [Generating the experiments](../user-guide/experiments.md#generating-the-experiments).

| Log | Parser | Component | Read from |
|---|---|---|---|
| `MOM6` | `FMSProfilingParser(has_hits=True)` | `ocn` | The model's standard output, `<model>.out` |
| `CICE6` | `CICE5ProfilingParser` | `ice` | `log/ice.log` |
| `ESMF` | `ESMFSummaryProfilingParser` | | `ESMF_Profile.summary`, written only when `ESMF_RUNTIME_PROFILE` is `on` and `ESMF_RUNTIME_PROFILE_OUTPUT` is `SUMMARY` |
| `payu` | `PayuJSONProfilingParser` | | Payu's job file of each run |

All three model logs are optional, so a run that does not write one of them is still parsed. The ESMF summary
times the whole coupled model, so its regions need a `component` in the groups that plot them against
component cores.

Other configurations of ACCESS-OM3 are built from [`OM3Configuration`][access.profiling.models.om3.OM3Configuration]
directly. For example, this is a 100 km configuration whose sea ice is decomposed over a process grid, and which
has waves:

```python
from access.config.parallel_domain import Domain

from access.profiling import CICEPartitioning, OM3Configuration

grid = Domain(shape=(360, 324))
om3_mcw_100km = OM3Configuration(
    name="MCW-100km",
    ocean=grid,
    sea_ice=CICEPartitioning(grid=(360, 324)),
    waves=grid,
    atmosphere=grid,
    runoff=grid,
)
```

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
