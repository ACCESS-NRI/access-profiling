# access-profiling

![CI](https://github.com/ACCESS-NRI/access-profiling/actions/workflows/ci.yml/badge.svg) [![codecov](https://codecov.io/github/ACCESS-NRI/access-profiling/graph/badge.svg?token=KtmrCtSyMv)](https://codecov.io/github/ACCESS-NRI/access-profiling) [![Documentation](https://readthedocs.org/projects/access-profiling/badge/?version=latest)](https://access-profiling.readthedocs.io/) [![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](https://opensource.org/license/apache-2-0) [![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

## About

access-profiling is a package aimed at simplifying and automating the process of profiling and optimising parallel
applications. It has been developed with the ACCESS climate models in mind, but it is general enough to be useful
for any type of parallel application. It has been designed to be light-weight and easily extensible. It is not
meant to replace other specialised profiling and optimising tools, but rather to complement them.

A profiling study is defined by three parts. These are the application that is profiled, the workflow engine that runs
it, and the experiment that is modified to create the other experiments. access-profiling creates the experiments of a
study, runs them and keeps track of them. It then reads the profiling logs they write, and lets you select, plot and
compare the timings. It is also possible to use experiments that were run in some other way.

The package comes with everything needed to profile the climate models developed at
[ACCESS-NRI](https://github.com/ACCESS-NRI), which run on NCI's Gadi: ACCESS-ESM1.5, ACCESS-ESM1.6, ACCESS-OM2,
ACCESS-OM3, ACCESS-AM3, ACCESS-rAM3 and ACCESS-CM3.

## Key features

### General

- A search for the ways of dividing a number of cores among the components of an application.
- Creating, running, tracking, archiving and deleting experiments.
- Registering existing experiments that were run in some other way.
- Parsers that read every profiling log into an xarray dataset, with the timings of each region and their units.
- Running every experiment several times, to see how much the timings vary.
- Bar charts that compare experiments region by region, and plots of parallel speedup and efficiency.
- Selecting the fastest experiment for each number of cores.
- Support for new applications, log formats and workflow engines.

### For the ACCESS models

- Managers for Payu and for Cylc Rose suites.
- Parsers for the timers of FMS (MOM5 and MOM6), ESMF, CICE and the UM, and for the records of Payu and Cylc.
- Presets for ACCESS-ESM1.5, ACCESS-ESM1.6, ACCESS-OM2 and ACCESS-OM3, run with Payu, and for ACCESS-AM3,
  ACCESS-rAM3 and ACCESS-CM3, run with Cylc and
  Rose.

## Documentation

The documentation is at [access-profiling.readthedocs.io](https://access-profiling.readthedocs.io/). It
has a user guide, which shows how to run experiments and analyse their data, with a guide for each kind of
profiling study. It also describes what the package provides for the ACCESS models, and has a developer guide and
an API reference.

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

As an example, list the parallelisation layouts of the ACCESS-ESM1.6 pre-industrial control that give each component the
cores it has in the release, on five nodes of 104 cores:

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

The two directories are only examples, so adapt them to your own setup, replacing `tm70` with your own project. The
code prints the 14 ways of arranging those cores into process grids, the release's own
`esm1p6-layout_piControl_atm_16x16_mom_16x15_ice_12` among them.

To learn more, start with the
[getting started](https://access-profiling.readthedocs.io/latest/getting-started/) page, which goes through
this example step by step. The
[best layout for a budget](https://access-profiling.readthedocs.io/latest/user-guide/best-layout/) guide then
runs these 14 layouts and picks the fastest, and the
[user guide](https://access-profiling.readthedocs.io/latest/user-guide/) covers every kind of study.

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

The [contributing guide](https://access-profiling.readthedocs.io/latest/developer-guide/contributing/) describes
the code style, the docstring conventions and how to build the documentation.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request if you would like to add features, fix
bugs, or improve the documentation.

For significant contributions, we recommend discussing proposed changes in an issue before opening a pull request.

## License

This project is licensed under the Apache 2.0 License.
