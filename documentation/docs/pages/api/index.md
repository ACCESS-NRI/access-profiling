# API reference

This reference is generated from the docstrings in the source code. Its pages follow the way the package is
organised: the general modules first, and then those implementing them for the ACCESS models.

| Page | Modules | What they cover |
|---|---|---|
| [Managers](managers.md) | `manager` | Generating, running, archiving and analysing experiments, whatever the workflow engine |
| [Applications](applications.md) | `application` | What a study profiles, and the logs it writes |
| [Control sources](control.md) | `control` | Where the control configuration comes from |
| [Experiments](experiments.md) | `experiment` | Experiments, their statuses and their logs |
| [Parsers](parsers.md) | `parser` | Reading profiling logs |
| [Metrics](metrics.md) | `metrics` | The quantities parsed for each region |
| [Scaling and plotting](plotting.md) | `scaling`, `plotting_utils` | Speedup, efficiency and the plots |
| [Payu](payu.md) | `payu_manager`, `payu_configuration`, `payujson_parser` | Applications run with Payu |
| [Cylc and Rose](cylc-rose.md) | `cylc_manager`, `rose_configuration`, `cylc_parser` | Cylc suites configured with Rose |
| [Models](models.md) | `models.components`, `models.esm16`, `models.om3`, `models.cice`, `models.um_suites` | The ACCESS models, the components of their codes and their presets |
| [Log formats](log-formats.md) | `fms_parser`, `esmf_parser`, `cice5_parser`, `um_parser` | The timers of the components of the ACCESS models |

All module names are relative to `access.profiling`.

## What to import from where

The names a study uses most are available from the package itself:

```python
from access.profiling import (
    RegionGroup,            # choosing what to plot
    PayuManager,            # the managers for the workflow engines of the ACCESS models
    CylcRoseManager,
    ESM16_PI_CONTROL,       # the presets of the ACCESS models, and their controls
    ESM16_PI_CONTROL_SOURCE,
    OM3_MC_25KM,
    OM3_MC_25KM_SOURCE,
    AM3_N96E,
    AM3_N96E_SOURCE,
    RAM3,
)
```

The package also exports the other types a study or an application is written with. From the general layer,
these are the base class of the managers, `ProfilingManager`, the experiments and their statuses,
`ProfilingExperiment` and `ProfilingExperimentStatus`, and the logs and how an application declares them,
`ProfilingLog`, `LogSpec` and `log_at`. It also exports the application types, the control sources,
`ProfilingMetric` and the parsers, and, for the ACCESS models, the configuration types, the component types and the
CICE partitioning types.

Everything else is imported from the module that defines it. This includes the metrics themselves and the
functions, such as the scaling functions:

```python
from access.profiling.metrics import tavg, tmax
from access.profiling.scaling import parallel_efficiency
```
