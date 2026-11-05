# Adding an application

An application the package does not know is added as a new configuration class, a subclass of
[`Application`][access.profiling.application.Application]. Every application states the components it divides
cores among, the logs its runs write, the name of the experiment that runs a layout, and how a layout is written
into its files and read back out of them. Where its workflow engine needs more, the class subclasses that engine's
base instead: an application run with Payu subclasses
[`PayuConfiguration`][access.profiling.payu_configuration.PayuConfiguration], and one Payu couples as submodels,
each given its cores in `config.yaml`, subclasses
[`SubmodelsConfiguration`][access.profiling.payu_configuration.SubmodelsConfiguration], which then writes and reads
the layout itself. What is left to write is a component for each code the package does not know: what that code
decomposes, writes and logs.

In this tutorial we write one for a small coupled application run with Payu, whose atmosphere, a toy code, and
ocean, MOM5, run side by side. The atmosphere is a new component, and the ocean reuses the package's own.
[`ESM16Configuration`][access.profiling.models.esm16.ESM16Configuration] is a complete example of the same thing,
and the components of the ACCESS codes are in
[`access.profiling.models.components`][access.profiling.models.components].

Note that a new setup of an application the package already knows needs no code: see
[ACCESS models](../access/index.md) for the presets of the ACCESS models. Neither does a new Rose suite, which is a
[`RoseSuiteConfiguration`][access.profiling.rose_configuration.RoseSuiteConfiguration] value, as shown in
[Cylc Rose suites](../access/cylc-rose-suites.md#other-suites).

## The component

A component is a frozen dataclass subclassing [`PayuComponent`][access.profiling.payu_configuration.PayuComponent].
It names itself twice: `name` is how the component tree names it, and so the key a caller uses for it in an
allocation strategy or a `RegionGroup`, and `token` is how the names of the experiments abbreviate it. It then
states the grid the search decomposes for it, the requirements every layout of it must meet, the logs it writes,
and what its own files have to say for a given layout. Our atmosphere decomposes its grid over a process grid its
namelist states, and writes FMS timers into a log of its own:

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
            LogSpec("atmosphere", log_at("atmosphere/timers.log"), FMSProfilingParser(has_hits=True), component=self.name),
        )

    def config_changes(self, layout: ComponentLayout) -> dict:
        nx, ny = require_process_grid(self.name, layout)
        return {"atmosphere/input.nml": {"decomposition_nml": {"nprocx": nx, "nprocy": ny}}}
```

A log spec has a name, under which the log is found in `manager.data` and named in a `RegionGroup`, and a locator
saying where the file is in an output directory. It also names the parser that reads it, and the component whose
cores its regions ran on. [`log_at`][access.profiling.application.log_at] locates a file at a fixed path in the
output directory, and [`payu_model_stdout`][access.profiling.payu_configuration.payu_model_stdout], particular to
Payu, locates the standard output Payu captured from the run. Mark a log `optional=True` if a run may legitimately
not write it.

`config_changes` returns the changes to the component's own files that run a layout of it, keyed by the path of each
file relative to the control directory, in the format of
[experiment-generator](https://access-experiment-generator.access-hive.org.au/).
[`require_process_grid`][access.profiling.payu_configuration.require_process_grid] reads the process grid the search
chose, and raises the `ValueError` the contract promises when the layout states none, as one read back from a
configuration does. A component that decomposes nothing the search can choose, such as a data component or a
serial one, is a [`PlainComponent`][access.profiling.payu_configuration.PlainComponent], with `n_ranks` where the
model fixes its rank count. Leave the preferences of a study, such as near-square subdomains, to its allocation
strategy: a component's constraints are the requirements every layout of it must meet, which a caller cannot relax.

## The configuration

The configuration of an application Payu couples as submodels subclasses `SubmodelsConfiguration`. It is a frozen,
keyword-only dataclass, so that a configuration is a value: what varies between setups of the application is a
field, and a setup is an instance. Its one member, `submodels`, pairs each submodel the control declares, in the
control's order, with the component that runs in it:

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

`name` tells the setups of the application apart in the names of their experiments, and `experiment_prefix` tells
the application's experiments from other applications' in a shared working directory. Every application has both.
`model_name` names the root of the component tree together with `name`, here `Toy demo`, which is the component a
`RegionGroup` names for the regions timed over the whole job. `model_type`, particular to Payu, is the model as
Payu names it, which the experiment generator checks against Payu's list of models.

A [`PayuSubmodel`][access.profiling.payu_configuration.PayuSubmodel] carries the submodel's name and Payu model
type, as `config.yaml` states them, the component, and the executable to run it with where the configuration names
one other than the control's. A submodel the package sets nothing in, such as a coupler, has no component: its slot
is written as `"PRESERVE"`, because the generator merges the submodels list positionally and truncates it to the
length of the edit, so every slot has to be accounted for.
[`ESM16Configuration`][access.profiling.models.esm16.ESM16Configuration] does this for OASIS.

Everything else follows from the submodels: the component tree, which puts the components side by side, the logs,
the name of an experiment, made of the components' tokens and what each received, the changes, which ask for the
whole budget of the layout as `ncpureq` and give every submodel its cores before adding each component's own files,
and reading a layout back out of `config.yaml`.

## Trying it out

Once you have the class, a setup of the application is an instance of it, and a manager drives it like any other.
We create a setup, and ask for the layouts of 96 cores that give each component about half of them:

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

Note that the strategy names the components as they name themselves, `atm` and `MOM5`. This prints the number of
layouts found, the name of the first one, and the changes that run it:

```text
7
toy-layout_demo_atm_8x6_mom_8x6
{'config.yaml': {'ncpureq': 96, 'submodels': [[{'ncpus': 48}, {'ncpus': 48}]]}, 'atmosphere/input.nml': {'decomposition_nml': {'nprocx': 8, 'nprocy': 6}}, 'ocean/input.nml': {'ocean_model_nml': {'layout': '8, 6'}}}
```

A list value is wrapped in another list, because the generator reads a bare list as one value per experiment.

## Other arrangements

A model whose components are not submodels side by side subclasses `PayuConfiguration` directly, and implements the
five members of `Application` itself from whatever components it has. ACCESS-OM3 is one: its mediator, data
components and sea ice take turns on one range of cores, and NUOPC lays them out, which
[`OM3Configuration`][access.profiling.models.om3.OM3Configuration] describes.

## Adding it to the package

The configurations shipped with the package are those of the ACCESS models. Each lives in a module of its own under
`src/access/profiling/models/`, together with its presets, and the components of their codes in
`src/access/profiling/models/components.py`. To add yours:

1. Write the components of the codes the package does not know, the configuration class, a preset for each released
   configuration worth shipping, and a `GitControlSource` for the release each preset describes. Give every public
   constant a docstring, so that it appears in the API reference.
2. Export the class and the presets from `src/access/profiling/__init__.py`, and add them to `__all__`.
3. Write the tests, in `tests/test_<model>.py`, and those of a new component in `tests/test_components.py`. The
   package's tests cover every line. The tests of ACCESS-ESM1.6 and ACCESS-OM3 show what to check: that every layout
   of the release can be found, that the names of the layouts are distinct, that `config_changes` writes what the
   model reads, and that `parse_layout` reads it back.
4. Add the module to the [Models](../api/models.md) page of the API reference, in
   `documentation/docs/pages/api/models.md`, and describe the model in
   `documentation/docs/pages/access/index.md`.
