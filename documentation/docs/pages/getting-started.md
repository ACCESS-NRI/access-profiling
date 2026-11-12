# Getting started

In this tutorial we will install access-profiling and try it on one of the ACCESS climate models, the
pre-industrial control configuration of ACCESS-ESM1.6. We will not run the model. We will only look at the
different ways a profiling study could run it on five nodes. Nothing here submits a job or writes to disk, so you
can follow it on any machine.

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
own. Here we create it under `/scratch`, but the path is only an example, so adapt it to your own setup:

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

A profiling study needs three things. The first is the application to profile, which here is the pre-industrial
control configuration of ACCESS-ESM1.6. The second is the control, the experiment that is modified to create the
other experiments, which here is the official release of that configuration. The third is a manager, which creates
and runs the experiments. ACCESS-ESM1.6 is run with Payu, so we use the manager for Payu. The manager also needs
a working directory, where the experiments are created and run, and an archive directory, where they are kept once
they have finished. We start by creating it:

```python
from pathlib import Path

from access.profiling import PayuManager
from access.profiling.models.esm16 import ESM16_PI_CONTROL, ESM16_PI_CONTROL_SOURCE

manager = PayuManager(
    work_dir=Path("/scratch/tm70/model_scaling/ACCESS-ESM1.6/piControl"),
    archive_dir=Path("/g/data/tm70/model_scaling/ACCESS-ESM1.6/piControl/archive"),
    application=ESM16_PI_CONTROL,
    control=ESM16_PI_CONTROL_SOURCE,
)
print(manager)
```

The two directories are only examples, so adapt them to your own setup, replacing `tm70` with your own project.
Creating a manager does not write anything to disk. It only looks in the archive directory, to see if there are
experiments from an earlier study, and here there are none. With the directories above, the output should be:

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

ACCESS-ESM1.6 has three components: the atmosphere, the ocean and the sea ice. A parallelisation layout, or layout for
short, says how many cores each of them gets. To choose which layouts to look at, we use an allocation strategy. Here we
ask for layouts that are close to the released configuration: each component gets about the same share of the cores as
in the release, to within 5%, or 25% for the sea ice. We also ask for the subdomains of the atmosphere and of the ocean
to be close to square:

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

There are 156 of them! They are sorted by the number of cores they leave idle, so the first one uses all 520
cores. For each component, a layout gives the number of cores and the process grid, which is how the component
splits its domain among its processes. Take a look at the first one:

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

Each layout becomes one experiment, and the name of the experiment is built from the layout. This means that the
same layout always gives the same experiment. Ask the application for the name:

```python
print(ESM16_PI_CONTROL.experiment_name(layout))
```

The output should be:

```text
esm1p6-layout_piControl_atm_18x14_mom_16x16_ice_12
```

Finally, to run this layout, some files of the control have to be changed. Ask the application which ones:

```python
from pprint import pprint

pprint(ESM16_PI_CONTROL.config_changes(layout))
```

This prints the changes to make in each file:

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

The file of each component gets its process grid, and `config.yaml` gets the number of cores of each component and the
520 cores of the whole job. In a real study, each of the 156 layouts would become an experiment like this one, and each
experiment would be submitted as a job. That is it for a first look! From here, [Concepts](user-guide/index.md) explains
the pieces we have used, and [Generating and running experiments](user-guide/experiments.md) shows how to create and run
a study. If you want to run these 156 layouts and keep the fastest, go on to [Best layout for a
budget](user-guide/best-layout.md). To run the model on several numbers of cores, see [Scaling
studies](user-guide/scaling-studies.md). If you already have experiments that were run in some other way, [Comparing
experiments](user-guide/comparing-experiments.md) shows how to read and compare them. Finally, [ACCESS
models](access/index.md) describes the other ACCESS models that the package supports, with their workflow engines and
log formats.
