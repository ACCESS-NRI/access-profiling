# Adding an application

An application the package does not know is added as a new configuration class, a subclass of
[`Application`][access.profiling.application.Application]. Every application states the components it divides
cores among, the logs its runs write, the name of the experiment that runs a layout, and how a layout is written
into its files and read back out of them. Where its workflow engine needs more, the class subclasses that engine's
base instead: an application run with Payu subclasses
[`PayuConfiguration`][access.profiling.payu_configuration.PayuConfiguration].

In this tutorial we write one for a small coupled application run with Payu, whose atmosphere and ocean run side
by side, each decomposing a domain of its own. The members we implement are those of every application, and we
mark what is particular to Payu as such.
[`ESM16Configuration`][access.profiling.models.esm16.ESM16Configuration] and
[`OM3Configuration`][access.profiling.models.om3.OM3Configuration], the configurations of two of the ACCESS models,
are complete examples of the same thing.

Note that a new setup of an application the package already knows needs no code: see
[ACCESS models](../access/index.md) for the presets of the ACCESS models. Neither does a new Rose suite, which is a
[`RoseSuiteConfiguration`][access.profiling.rose_configuration.RoseSuiteConfiguration] value, as shown in
[Cylc Rose suites](../access/cylc-rose-suites.md#other-suites).

## The configuration class

The configuration of an application run with Payu subclasses `PayuConfiguration`. The ones in the package are frozen
dataclasses, so that a configuration is a value: what varies between setups of the application is a field, and a
setup is an instance. We start with the fields of ours:

```python
from dataclasses import dataclass
from pathlib import Path

from access.config import YAMLParser
from access.config.parallel_component import ComponentLayout, ParallelComponent
from access.config.parallel_constraints import FixedThreadsPerRankConstraint, MaxWastedCoreFractionConstraint
from access.config.parallel_domain import Domain

from access.profiling import FMSProfilingParser, PayuConfiguration
from access.profiling.application import LogSpec
from access.profiling.payu_configuration import payu_model_stdout


@dataclass(frozen=True)
class ToyConfiguration(PayuConfiguration):
    """A coupled application whose atmosphere and ocean run side by side, each decomposing a domain of its own."""

    name: str
    atmosphere: Domain
    ocean: Domain
    model_type: str = "access"
    experiment_prefix: str = "toy-layout"
```

`name` tells the setups of the application apart in the names of their experiments, and `experiment_prefix` tells
the application's experiments from other applications' in a shared working directory. Every application has both.
`model_type`, particular to Payu, is the model as Payu names it, which the experiment generator checks against
Payu's list of models.

The rest of the class, which we write in the following sections, implements the five members every application
has.

### The component tree

[`parallel_component`][access.profiling.application.Application.parallel_component] returns the component tree:
the components the cores are divided among, the domains they decompose, and the requirements every layout of the
application must meet, whatever the study. Ours has two components side by side, each with one thread per rank:

```python
    @property
    def parallel_component(self) -> ParallelComponent:
        one_thread = (FixedThreadsPerRankConstraint(n_threads=1),)
        return ParallelComponent(
            name=f"Toy {self.name}",
            subcomponents=(
                ParallelComponent(name="atm", domain=self.atmosphere, local_constraints=one_thread),
                ParallelComponent(name="ocn", domain=self.ocean, local_constraints=one_thread),
            ),
            local_constraints=(MaxWastedCoreFractionConstraint(max_fraction=0.1),),
        )
```

The names of the components are the keys of an allocation strategy, and the name of the root is the whole
application, for the regions plotted against the cores of the whole job. A component that runs on cores shared with
others in turn, as the ACCESS-OM3 mediator and sea ice do, goes under a parent with `CoreSharing.SHARED`. A
component that decomposes its domain itself when it runs, as MOM6 does under `AUTO_MASKTABLE`, carries no domain.
Leave the preferences of a study, such as near-square subdomains, to its allocation strategy.

### The logs

[`logs`][access.profiling.application.Application.logs] declares the profiling logs the components write, each as
a [`LogSpec`][access.profiling.application.LogSpec]. Our ocean writes FMS timers into its standard output:

```python
    @property
    def logs(self) -> tuple[LogSpec, ...]:
        return (LogSpec("ocean", payu_model_stdout(), FMSProfilingParser(has_hits=True), component="ocn"),)
```

A log spec has a name, under which the log is found in `manager.data` and named in a `RegionGroup`, and a locator
saying where the file is in an output directory. It also names the parser that reads it, and the component whose
cores its regions ran on. [`log_at`][access.profiling.application.log_at] locates a file at a fixed path in the
output directory, and `payu_model_stdout`, particular to Payu, locates the standard output Payu captured from the
run. Mark a log `optional=True` if a run may legitimately not write it. Note that the Payu manager adds Payu's own
job timings, as the `payu` log, to every Payu configuration.

### The experiment name

[`experiment_name`][access.profiling.application.Application.experiment_name] names the experiment that runs a
layout. It has to be different for every layout and the same every time for the same one, since the Payu manager,
for example, names the experiment's git branch, its directory and its archive after it. We name ours after the two
process grids:

```python
    def experiment_name(self, layout: ComponentLayout) -> str:
        atm, ocn = layout.sub_layouts
        atm_grid, ocn_grid = ("x".join(map(str, c.decomposition.grid.shape)) for c in (atm, ocn))
        return f"{self.experiment_prefix}_{self.name}_atm_{atm_grid}_ocn_{ocn_grid}"
```

Note that the layout's `sub_layouts` come in the order of the tree's `subcomponents`.

### Writing a layout and reading it back

[`config_changes`][access.profiling.application.Application.config_changes] returns the changes to the control's
files that run a layout, keyed by the path of each file relative to the control directory. The Payu manager
applies them with the experiment generator, so for Payu they follow its format:

```python
    def config_changes(self, layout: ComponentLayout) -> dict:
        atm, ocn = layout.sub_layouts
        atm_x, atm_y = atm.decomposition.grid.shape
        ocn_x, ocn_y = ocn.decomposition.grid.shape
        return {
            "config.yaml": {"submodels": [[{"ncpus": atm.n_cores}, {"ncpus": ocn.n_cores}]]},
            "atmosphere/input.nml": {"decomposition_nml": {"nprocx": atm_x, "nprocy": atm_y}},
            "ocean/input.nml": {"ocean_model_nml": {"layout": f"{ocn_x}, {ocn_y}"}},
        }
```

A list value is wrapped in another list, because the generator reads a bare list as one value per experiment. It
then merges the inner list with the control's item by item, and truncates it to the length of the new one, so every
item has to be accounted for: [`ESM16Configuration`][access.profiling.models.esm16.ESM16Configuration] marks the
coupler it changes nothing in with `"PRESERVE"`.

[`parse_layout`][access.profiling.application.Application.parse_layout] is the inverse: it reads the layout an
experiment ran back out of its files. The manager needs it for the experiments it did not generate itself in the
current session, to plot them against their components' cores. What the files do not say, such as the process
grids here, is left out:

```python
    def parse_layout(self, output_dir: Path) -> ComponentLayout | None:
        config_path = output_dir / "config.yaml"
        if not config_path.is_file():
            return None
        submodels = YAMLParser().parse(config_path.read_text()).get("submodels", [])
        components = tuple(
            ComponentLayout(name=name, n_cores=n, n_ranks=n, threads_per_rank=1, decomposition=None)
            for name, n in zip(("atm", "ocn"), (submodel["ncpus"] for submodel in submodels), strict=True)
        )
        cores = sum(component.n_cores for component in components)
        return ComponentLayout(
            name=f"Toy {self.name}",
            n_cores=cores,
            n_ranks=cores,
            threads_per_rank=None,
            decomposition=None,
            sub_layouts=components,
        )
```

Keep the two methods next to each other: a change to what one writes is a change to what the other reads.

## Trying it out

Once you have the class, a setup of the application is an instance of it, and a manager drives it like any other.
We create a setup, and ask for the layouts of 96 cores that give each component about half of them:

```python
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
layouts = manager.select_layouts(96, allocations=RootAllocation(subcomponents={"atm": half, "ocn": half}))
print(len(layouts))
print(TOY_DEMO.experiment_name(layouts[0]))
print(TOY_DEMO.config_changes(layouts[0]))
```

This prints the number of layouts found, the name of the first one, and the changes that run it:

```text
7
toy-layout_demo_atm_8x6_ocn_8x6
{'config.yaml': {'submodels': [[{'ncpus': 48}, {'ncpus': 48}]]}, 'atmosphere/input.nml': {'decomposition_nml': {'nprocx': 8, 'nprocy': 6}}, 'ocean/input.nml': {'ocean_model_nml': {'layout': '8, 6'}}}
```

## Adding it to the package

The configurations shipped with the package are those of the ACCESS models. Each lives in a module of its own under
`src/access/profiling/models/`, together with its presets. To add yours:

1. Write the configuration class, a preset for each released configuration worth shipping, and a
   `GitControlSource` for the release each preset describes. Give every public constant a docstring, so that it
   appears in the API reference.
2. Export the class and the presets from `src/access/profiling/__init__.py`, and add them to `__all__`.
3. Write the tests, in `tests/test_<model>.py`. The package's tests cover every line. The tests of ACCESS-ESM1.6
   and ACCESS-OM3 show what to check: that every layout of the release can be found, that the names of the
   layouts are distinct, that `config_changes` writes what the model reads, and that `parse_layout` reads it
   back.
4. Add the module to the [Models](../api/models.md) page of the API reference, in
   `documentation/docs/pages/api/models.md`, and describe the model in
   `documentation/docs/pages/access/index.md`.
