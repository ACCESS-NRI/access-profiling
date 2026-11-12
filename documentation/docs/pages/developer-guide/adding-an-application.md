# Adding an application

Do you want to profile an application that the package does not support? You can add it by writing a
configuration class, which is a subclass of [`Application`][access.profiling.application.Application]. The class
has to answer a few questions. What are the components of the application? Which logs does a run write? What is
the name of the experiment that runs a given parallelisation layout? And how is a layout written into the files of the
application, and read back from them? Some workflow engines need a little more. An application that is run with
Payu, for example, is a subclass of
[`PayuConfiguration`][access.profiling.payu_configuration.PayuConfiguration]. And if Payu couples its components
as submodels, each with its own cores in `config.yaml`, the class is a subclass of
[`SubmodelsConfiguration`][access.profiling.payu_configuration.SubmodelsConfiguration], which writes and reads the
layout for you. What is left to write is a component for each code that is new to the package: what the code
decomposes, what it writes, and which logs it produces.

In this tutorial we write a component and a configuration class for a small coupled application that is run with
Payu. Its atmosphere is a toy code, and its ocean is MOM5. The two run at the same time, each on its own cores. The
atmosphere is a new component, and for the ocean we reuse the component that comes with the package.
[`ESM16Configuration`][access.profiling.models.esm16.ESM16Configuration] is a complete example of the same thing,
and the components of the ACCESS codes are in
[`access.profiling.models.components`][access.profiling.models.components].

Note that you do not need to write any code for a new configuration of an application that the package already
supports: see [ACCESS models](../access/index.md) for the ACCESS models. The same is true for a new Rose suite,
which is a
[`RoseSuiteConfiguration`][access.profiling.rose_configuration.RoseSuiteConfiguration] value, as shown in
[Cylc Rose suites](../access/cylc-rose-suites.md#other-suites).

## The component

A component is a frozen dataclass, and a subclass of
[`PayuComponent`][access.profiling.payu_configuration.PayuComponent]. It has two names. The `name` is the name of
the component in the component tree, and so the name you use for it in an allocation strategy or in a
`RegionGroup`. The `token` is the short form that appears in the names of the experiments. The component then says
which grid the search decomposes for it, which rules every layout of it has to follow, which logs it writes, and
what its own files have to say for a given layout. Our atmosphere decomposes its grid over a process grid, which it
reads from its namelist, and writes FMS timers into a log of its own:

```python
from dataclasses import dataclass

from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import LocalConstraint
from access.config.parallel_domain import Domain

from access.profiling import FMSProfilingParser, LogSpec, PayuComponent, log_at
from access.profiling.payu_configuration import require_process_grid


@dataclass(frozen=True)
class ToyAtmosphere(PayuComponent):
    """A toy atmosphere, decomposing its grid over the process grid its namelist states."""

    grid: Domain
    name = "atm"
    token = "atm"

    @property
    def domain(self) -> Domain:
        return self.grid

    @property
    def local_constraints(self) -> tuple[LocalConstraint, ...]:
        return ()

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        return (
            LogSpec(
                "atmosphere",
                log_at("atmosphere/timers.log"),
                FMSProfilingParser(has_hits=True),
                component=self.name,
            ),
        )

    def config_changes(self, layout: ComponentLayout) -> dict:
        nx, ny = require_process_grid(self.name, layout)
        return {"atmosphere/input.nml": {"decomposition_nml": {"nprocx": nx, "nprocy": ny}}}
```

A log spec has four parts. The first is the name of the log, which is the name you use in `manager.data` and in a
`RegionGroup`. The second says where to find the file in an output directory. The third is the parser that reads the
log, and the fourth is the component that the log belongs to. [`log_at`][access.profiling.application.log_at] locates a
file at a fixed path in the output directory, and
[`payu_model_stdout`][access.profiling.payu_configuration.payu_model_stdout], particular to Payu, locates the
standard output Payu captured from the run. Mark a log `optional=True` if a run may legitimately not write it.

`config_changes` returns the changes to make to the files of the component in order to run a layout. The changes
are given for each file, with its path relative to the control directory, in the format of
[experiment-generator](https://access-experiment-generator.access-hive.org.au/).
[`require_process_grid`][access.profiling.payu_configuration.require_process_grid] reads the process grid that the
search chose. A layout that is read back from an experiment has no process grid. For such a layout,
`require_process_grid` raises a `ValueError`, which is what `config_changes` has to raise for a layout that it
cannot write.

Some components have nothing that the search can decompose, such as a data component or a serial one. These are
[`PlainComponent`][access.profiling.payu_configuration.PlainComponent]s, with `n_ranks` set when the model fixes
their number of ranks.

Only put in the constraints of a component the rules that are always needed, as nobody can relax them later.
Preferences, such as subdomains that are close to square, belong in the allocation strategy of a study.

## The configuration

Our application is coupled by Payu as submodels, so its configuration class is a subclass of
`SubmodelsConfiguration`. In the package, these classes are frozen, keyword-only dataclasses. Whatever changes from
one configuration of the application to another is a field of the class, and each configuration is an instance.
The class has one thing to implement, `submodels`. It pairs each submodel that the control declares, in the same
order as the control, with the component that runs in it:

```python
from access.profiling import MOM5Component, PayuSubmodel, SubmodelsConfiguration


@dataclass(frozen=True, kw_only=True)
class ToyConfiguration(SubmodelsConfiguration):
    """A coupled application whose toy atmosphere and MOM5 ocean run side by side."""

    name: str
    atmosphere: Domain
    ocean: Domain
    model_type: str = "access"
    model_name: str = "Toy"
    experiment_prefix: str = "toy-layout"

    @property
    def submodels(self) -> tuple[PayuSubmodel, ...]:
        return (
            PayuSubmodel("atmosphere", "model", ToyAtmosphere(self.atmosphere)),
            PayuSubmodel("ocean", "mom", MOM5Component(self.ocean)),
        )
```

Every application has a `name` and an `experiment_prefix`, and both are used in the names of the experiments. The
`name` is different for each configuration of the application. The `experiment_prefix` is the same for the whole
application, so that you can tell its experiments from those of other applications in the same working directory.
`model_name` and `name` together give the name of the root of the component tree, here `Toy demo`. This is the
component you name in a `RegionGroup` for the regions that are timed over the whole job. `model_type` is specific
to Payu. It is the name that Payu gives to the model, and the experiment generator checks that Payu knows it.

A [`PayuSubmodel`][access.profiling.payu_configuration.PayuSubmodel] has the name and the Payu model type of the
submodel, as they are in `config.yaml`, and the component. It can also name the executable to run the submodel
with, if the configuration uses one other than the control's. A submodel in which the package changes nothing,
such as a coupler, has no component. Its slot is then written as `"PRESERVE"`. This is needed because the
experiment generator merges the list of submodels item by item, and cuts it to the length of the new list, so
every slot has to be there. [`ESM16Configuration`][access.profiling.models.esm16.ESM16Configuration] does this
for OASIS.

Everything else follows from the submodels. The component tree puts the components side by side. The logs are
those of the components. The name of an experiment is made of the tokens of the components and of what each one
received. The changes request the whole budget of the layout as `ncpureq`, give every submodel its cores, and add
the changes to the files of each component. And the layout is read back from `config.yaml`.

## Trying it out

The class is now complete, so let us try it. We create an instance for one configuration of the application, and
use it with a manager like any other application. We then ask for the layouts of 96 cores that give each
component about half of them. The two directories of the manager are only examples, so adapt them to your own
setup:

```python
from pathlib import Path

from access.config.parallel_allocation_strategies import FreeAllocation, RootAllocation
from access.config.parallel_constraints import SubdomainAspectRatioConstraint

from access.profiling import GitControlSource, PayuManager

TOY_DEMO = ToyConfiguration(name="demo", atmosphere=Domain(shape=(96, 72)), ocean=Domain(shape=(120, 100)))

manager = PayuManager(
    work_dir=Path("/scratch/tm70/toy"),
    archive_dir=Path("/g/data/tm70/toy/archive"),
    application=TOY_DEMO,
    control=GitControlSource("https://github.com/<organisation>/<repository>.git", "<tag>"),
)
half = FreeAllocation(
    min_core_fraction=0.45,
    max_core_fraction=0.55,
    local_constraints=(SubdomainAspectRatioConstraint(max_ratio=1.5),),
)
layouts = manager.select_layouts(96, allocations=RootAllocation(subcomponents={"atm": half, "MOM5": half}))
print(len(layouts))
print(TOY_DEMO.experiment_name(layouts[0]))
print(TOY_DEMO.config_changes(layouts[0]))
```

Note that the strategy uses the names that the components give themselves, `atm` and `MOM5`. This prints the
number of layouts found, the name of the first one, and the changes that run it:

```text
7
toy-layout_demo_atm_8x6_mom_8x6
{'config.yaml': {'ncpureq': 96, 'submodels': [[{'ncpus': 48}, {'ncpus': 48}]]}, 'atmosphere/input.nml': {'decomposition_nml': {'nprocx': 8, 'nprocy': 6}}, 'ocean/input.nml': {'ocean_model_nml': {'layout': '8, 6'}}}
```

Note the list inside a list under `submodels`. The experiment generator reads a simple list as one value for each
experiment, so a list has to be wrapped in another list.

## Other arrangements

Not every model has components that are submodels side by side. A model that is arranged differently is a direct
subclass of `PayuConfiguration`, and implements the five members of `Application` itself, from whatever components
it has. ACCESS-OM3 is such a model. Its mediator, its data components and its sea ice take turns on one range of
cores, and NUOPC lays them out. [`OM3Configuration`][access.profiling.models.om3.OM3Configuration] shows how this
is described.

## Adding it to the package

The package ships the configuration classes of the ACCESS models. Each model has its own module in
`src/access/profiling/models/`, with its class and its presets, and the components of their codes are in
`src/access/profiling/models/components.py`. If you want to add yours to the package:

1. Write the components for the codes that are new to the package, the configuration class, and a preset for each
   released configuration worth shipping. Write a `GitControlSource` for the release that each preset describes.
   Give the class a `from_control`, as the ACCESS models have. It reads a configuration from the files of a
   control, and finds the submodels by their Payu model types. Give every public constant a docstring, so that it
   appears in the API reference.
2. Export the class and the new components from `src/access/profiling/__init__.py`, and add them to `__all__`. The
   presets and their control sources are not exported: a study imports them from the module of the model.
3. Write the tests, in `tests/test_<model>.py`, and those of a new component in `tests/test_components.py`. The
   package's tests cover every line. The tests of ACCESS-ESM1.6 and ACCESS-OM3 show what to check: that every
   layout of the release can be found, that the names of the layouts are distinct, that `config_changes` writes
   what the model reads, and that `parse_layout` reads it back.
4. Add the module to the [Models](../api/models.md) page of the API reference, in
   `documentation/docs/pages/api/models.md`, and describe the model in
   `documentation/docs/pages/access/index.md`.
