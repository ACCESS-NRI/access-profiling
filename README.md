# access-profiling

![CI](https://github.com/ACCESS-NRI/access-profiling/actions/workflows/ci.yml/badge.svg) [![codecov](https://codecov.io/github/ACCESS-NRI/access-profiling/graph/badge.svg?token=KtmrCtSyMv)](https://codecov.io/github/ACCESS-NRI/access-profiling) [![Documentation](https://readthedocs.org/projects/access-profiling/badge/?version=latest)](https://access-profiling.access-hive.org.au/) [![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](https://opensource.org/license/apache-2-0) [![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

## About

access-profiling is a package aimed at simplifying and automating the process of profiling and optimising parallel
applications. It has been developed with the ACCESS climate models in mind, but it is general enough to be useful
for any type of parallel application. It has been designed to be light-weight and easily extensible. It is not
meant to replace other specialised profiling and optimising tools, but rather to complement them.

A study is defined by an application, the workflow engine that runs it, and the configuration every experiment of
the study changes. access-profiling generates the experiments of a study from a search over the ways of dividing
cores among the application's components, or registers experiments run some other way. It runs and tracks them,
reads the profiling logs they write into one form, and selects, plots and compares what they measured.

It implements this for the climate models developed at [ACCESS-NRI](https://github.com/ACCESS-NRI), which run on
NCI's Gadi. For them it has the Payu and Cylc Rose workflow engines, parsers for the timers of FMS, ESMF, CICE and
the UM, and presets for ACCESS-ESM1.5, ACCESS-ESM1.6, ACCESS-OM2, ACCESS-OM3, ACCESS-AM3 and ACCESS-rAM3.

## Key features

### General

- A layout search over every way of dividing a number of cores among an application's components that the
  application can run and the study asks for.
- Generation, submission, tracking, archiving and clean-up of experiments, and registration of experiments run in
  other ways.
- Parsers that turn every profiling log into an xarray dataset of metrics, with their units, for each region it
  times.
- Repeated runs of every experiment, kept apart in the parsed data and reduced to the fastest run, or to the
  minimum, mean or median over the runs.
- Bar charts comparing experiments region by region, plots of parallel speedup and efficiency and of each component
  against its own cores, and selection of the fastest experiment of each number of cores.
- New applications, log formats and workflow engines, added as configuration classes, parsers and managers.

### For the ACCESS models

- Managers for Payu, through experiment-generator and experiment-runner, and for Cylc Rose suites.
- Parsers for the timers of FMS (MOM5 and MOM6), ESMF, CICE and the UM, and for Payu's and Cylc's own records.
- Presets for ACCESS-ESM1.5, ACCESS-ESM1.6, ACCESS-OM2 and ACCESS-OM3, driven by Payu, and for ACCESS-AM3 and
  ACCESS-rAM3, driven by Cylc and
  Rose.

## Documentation

The documentation is at [access-profiling.access-hive.org.au](https://access-profiling.access-hive.org.au/). Its
user guide explains how experiments are run and their data analysed, and goes through comparisons of experiments,
run-to-run variability, the search for the best layout of a budget and scaling studies. It also describes the
implementations for the ACCESS models, and has a developer guide and an API reference.

## Installation

### Using pip

You can install the latest release directly from PyPI:
```shell
pip install access-profiling
```

### Using conda

The package is also available from the `accessnri` channel:
```shell
conda install -c accessnri -c conda-forge access-profiling
```

### From source

If you prefer to install from source:
```shell
git clone https://github.com/ACCESS-NRI/access-profiling.git
cd access-profiling
pip install .
```

## Usage

As an example, list the layouts of the ACCESS-ESM1.6 pre-industrial control that give each component the cores it
has in the release, on five nodes of 104 cores:

```python
from pathlib import Path

from access.config.parallel_allocation_strategies import FixedAllocation, RootAllocation

from access.profiling import PayuManager
from access.profiling.models.esm16 import ESM16_PI_CONTROL, ESM16_PI_CONTROL_SOURCE
from access.profiling.models.esm16 import ESM16_PI_CONTROL_CORES

manager = PayuManager(
    work_dir=Path("/scratch/tm70/model_scaling/ACCESS-ESM1.6/piControl"),
    archive_dir=Path("/g/data/tm70/model_scaling/ACCESS-ESM1.6/piControl/archive"),
    application=ESM16_PI_CONTROL,
    control=ESM16_PI_CONTROL_SOURCE,
)
released = RootAllocation(
    subcomponents={component: FixedAllocation(cores) for component, cores in ESM16_PI_CONTROL_CORES.items()}
)
for layout in manager.select_layouts(5 * 104, allocations=released):
    print(ESM16_PI_CONTROL.experiment_name(layout))
```

This prints the 14 ways of arranging those cores into process grids, the release's own
`esm1p6-layout_piControl_atm_16x16_mom_16x15_ice_12` among them.

The [best layout for a budget](https://access-profiling.access-hive.org.au/latest/user-guide/best-layout/) guide
runs these 14 layouts and picks the fastest, the
[getting started](https://access-profiling.access-hive.org.au/latest/getting-started/) page previews the layouts of a
study step by step, and the [user guide](https://access-profiling.access-hive.org.au/latest/user-guide/) goes through
every kind of study.

## Development installation

If you intend to contribute to the package or modify it, we recommend working inside a virtual environment.

1. Create and activate a virtual environment:
```shell
# Create a virtual environment
python3 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate
```

2. Install the package in editable mode, with its development and test dependencies:
```shell
pip install -e ".[devel,test]"
```
In editable mode, changes to the source code take effect without reinstalling the package. The development
dependencies, such as the testing tools, are installed as well.

3. Run the test suite:
```shell
pytest
```

The [contributing guide](https://access-profiling.access-hive.org.au/latest/developer-guide/contributing/) describes
the code style, the docstring conventions and how to build the documentation.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request if you would like to add features, fix
bugs, or improve the documentation.

For significant contributions, we recommend discussing proposed changes in an issue before opening a pull request.

## License

This project is licensed under the Apache 2.0 License.
