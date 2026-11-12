# ACCESS models

access-profiling comes with everything needed to profile the ACCESS climate models developed at
[ACCESS-NRI](https://github.com/ACCESS-NRI), which run on NCI's Gadi supercomputer:

- a manager for each of their workflow engines: [Payu](payu.md), for ACCESS-ESM1.6 and ACCESS-OM3, and
  [Cylc and Rose](cylc-rose-suites.md), for ACCESS-AM3 and ACCESS-rAM3;
- parsers for the [log formats](log-formats.md) of their components and of these workflow engines;
- a preset for each model, together with its official release to use as the control.

Each preset is described below. You will find the components of the model, the rules that every parallelisation layout
follows, the logs that a run writes, and the number of cores used by the release. For the full details, see the [Models
API](../api/models.md).

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

## ACCESS-OM3

| | |
|---|---|
| Preset | [`OM3_MC_25KM`][access.profiling.models.om3.OM3_MC_25KM], MOM6 and CICE6 at 25 km |
| Control | [`OM3_MC_25KM_SOURCE`][access.profiling.models.om3.OM3_MC_25KM_SOURCE], `release-MC_25km_jra_ryf-2.0-beta` of `access-om3-configs` |
| Manager | `PayuManager` |
| Components | `shared`, the range of cores `cpl`, `atm`, `ice` and `rof` take turns on, and `ocn` beside it; all on the 1440 × 1152 tripolar grid |
| Whole model | `ACCESS-OM3 MC-25km` |
| Release cores | 275 for each of `cpl`, `atm`, `ice` and `rof`, and 2429 for `ocn`, on 26 nodes of 104 cores ([`OM3_MC_25KM_RELEASE_CORES`][access.profiling.models.om3.OM3_MC_25KM_RELEASE_CORES]) |

In ACCESS-OM3, the mediator (`cpl`), the data atmosphere (`atm`), the sea ice (`ice`) and the data runoff (`rof`)
take turns on the same cores, which are called `shared`. The ocean (`ocn`) runs next to them on cores of its own.
Every component has one thread per rank. A configuration with waves also has a `wav` component, on cores of its
own. Every layout leaves at most 10% of its cores idle, and gives each CICE6 rank 5 to 8 blocks. The blocks are
distributed roundrobin, and their size is chosen from the number of ranks.

To run a layout, a few files are changed. `config.yaml` requests the total number of cores as `ncpus`, the
`PELAYOUT_attributes` of `nuopc.runconfig` get the cores of each component, and `ice_in` gets the block size. With this
preset, MOM6 chooses its own decomposition (`auto_ocean_layout`), so there is no process grid for the ocean and
`MOM_input` is not changed. Note, however, that the release fixes a `LAYOUT` and a `MASKTABLE` in `MOM_input`. A study
has to remove them with its control options, as shown in [Generating the
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

For other configurations of ACCESS-OM3, create an
[`OM3Configuration`][access.profiling.models.om3.OM3Configuration] yourself. For example, this is a 100 km
configuration with waves, whose sea ice is decomposed over a process grid:

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

For both models, the UM timers of each task are read with the same two parsers, and give the logs
`<task>_cycle<cycle>_UM_regions` and `<task>_cycle<cycle>_UM_total`. See [Cylc Rose suites](cylc-rose-suites.md)
to learn how to prepare and analyse their experiments.
