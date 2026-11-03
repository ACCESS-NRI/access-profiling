# Getting started

In this tutorial we will install access-profiling and try it on one of the applications it ships presets for, the
pre-industrial control of the ACCESS-ESM1.6 climate model. We will not run anything: we will only look at the
layouts a study could run it in on five nodes. Nothing here submits a job or writes to disk, so you can follow it
on any machine.

## Installation

access-profiling needs Python 3.11 or later. Install it with pip, with conda, or from the sources:

=== "pip"

    ```bash
    pip install access-profiling
    ```

=== "conda"

    ```bash
    conda install -c accessnri -c conda-forge access-profiling
    ```

=== "From source"

    ```bash
    git clone https://github.com/ACCESS-NRI/access-profiling.git
    cd access-profiling
    pip install .
    ```

Then check that the package can be imported:

```bash
python -c "import access.profiling; print(access.profiling.__version__)"
```

This prints the version you have just installed.

### On Gadi

The experiments of the ACCESS models run on NCI's Gadi. There, install the package in a virtual environment of your
own, for example under `/scratch`:

```bash
module load python3/3.12.1
python3 -m venv /scratch/$PROJECT/$USER/profiling-venv
source /scratch/$PROJECT/$USER/profiling-venv/bin/activate
pip install access-profiling
```

This environment also provides the `payu` command, which the experiments of the Payu presets run with. If you prefer
to work from a JupyterLab session on [ARE](https://opus.nci.org.au/spaces/Help/pages/162431120/ARE+User+Guide), give
the path of the environment in the advanced settings, under *Python or Conda virtual environment base*. A small
session is enough, since the experiments run in jobs of their own.

## Previewing a study

A study profiles an application, which here is the pre-industrial control configuration of ACCESS-ESM1.6. Every
experiment of the study is a change to a control configuration, here the release that configuration comes from. A
manager brings the two together, and here it is the one for Payu, the workflow engine ACCESS-ESM1.6 runs with. The
manager is also given a working directory, where the experiments are generated and run, and an archive directory,
where they are kept once they have finished. We start by creating it:

```python
from pathlib import Path

from access.profiling import ESM16_PI_CONTROL, ESM16_PI_CONTROL_SOURCE, PayuManager

manager = PayuManager(
    work_dir=Path("/scratch/tm70/model_scaling/ACCESS-ESM1.6/piControl"),
    archive_dir=Path("/g/data/tm70/model_scaling/ACCESS-ESM1.6/piControl/archive"),
    application=ESM16_PI_CONTROL,
    control=ESM16_PI_CONTROL_SOURCE,
)
print(manager)
```

Creating a manager writes nothing to disk. It only looks in the archive directory for experiments archived by an
earlier study, and finds none. The output should be:

```text
<PayuManager>
    Application: piControl
    Control: release-piControl-2.1
    Working directory: PosixPath('/scratch/tm70/model_scaling/ACCESS-ESM1.6/piControl')
    Archive directory: PosixPath('/g/data/tm70/model_scaling/ACCESS-ESM1.6/piControl/archive')
    Experiments:
    Data:
        No parsed data.
```

The layouts a study runs are chosen by an allocation strategy, which says how many cores each component may receive.
Here we allow each component a share of the cores within 5% of its share in the released configuration, or within
25% for the sea ice, and we require the atmosphere's and the ocean's subdomains to be close to square:

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

Note that the components are named as the application names them: `UM7`, `MOM5` and `CICE5`. Now ask for the
layouts of five nodes of 104 cores, the size of the released configuration:

```python
layouts = manager.select_layouts(5 * 104, allocations=allocations)
print(len(layouts))
```

The output should be:

```text
156
```

There are 156 of them, sorted by the number of cores they leave idle, so the first one uses all 520 cores. A layout
gives each component a number of cores and, since all three components decompose a grid themselves, a process grid.
Take a look at the first one:

```python
layout = layouts[0]
for component in layout.sub_layouts:
    print(component.name, component.n_cores, component.decomposition.grid.shape)
```

This prints:

```text
UM7 252 (18, 14)
MOM5 256 (16, 16)
CICE5 12 (12,)
```

The experiment that would run this layout takes its name from it, so the same layout always gives the same
experiment. Ask the application for the name:

```python
print(ESM16_PI_CONTROL.experiment_name(layout))
```

The output should be:

```text
esm1p6-layout_piControl_atm_18x14_mom_16x16_ice_12
```

Finally, ask the application for the changes that running this layout makes to the control's files:

```python
from pprint import pprint

pprint(ESM16_PI_CONTROL.config_changes(layout))
```

This prints the changes keyed by file:

```text
{'atmosphere/um_env.yaml': {'UM_ATM_NPROCX': '18',
                            'UM_ATM_NPROCY': '14',
                            'UM_NPES': '252'},
 'config.yaml': {'ncpureq': 520,
                 'submodels': [[{'ncpus': 252},
                                {'ncpus': 256},
                                {'exe': ['cice_access.exe'], 'ncpus': 12},
                                'PRESERVE']]},
 'ice/cice_in.nml': {'domain_nml': {'block_size_x': '30',
                                    'block_size_y': '300',
                                    'max_blocks': '1',
                                    'nprocs': '12',
                                    'nx_global': '360',
                                    'ny_global': '300'}},
 'ocean/input.nml': {'ocean_model_nml': {'layout': '16, 16'}}}
```

Each component's file receives its decomposition, and `config.yaml` the cores of every submodel and the 520 cores the
job asks for. Generating the study would turn each of the 156 layouts into an experiment like this one, and running
it would submit one job for each of them. [Concepts](user-guide/index.md) explains the pieces we have used here, and
[Generating and running experiments](user-guide/experiments.md) shows how a study is generated and run. If you want
to run these 156 layouts and keep the fastest, go on to [Best layout for a budget](user-guide/best-layout.md); to run
the layouts of several sizes, to [Scaling studies](user-guide/scaling-studies.md). Experiments run some other way can
be read and compared without generating anything, as [Comparing experiments](user-guide/comparing-experiments.md)
shows. [ACCESS models](access/index.md) describes the other presets, and the workflow engines and log formats of the
ACCESS models.
