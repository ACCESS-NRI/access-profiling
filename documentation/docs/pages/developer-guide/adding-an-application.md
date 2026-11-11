# Adding an application

Do you want to profile an application that the package does not support? You can add it by writing a
configuration class, which is a subclass of [`Application`][access.profiling.application.Application]. The class
has to answer a few questions. What are the components of the application? Which logs does a run write? What is
the name of the experiment that runs a given parallelisation layout? And how is a layout written into the files of the
application, and read back from them? Some workflow engines need a little more. An application that is run with
Payu, for example, is a subclass of
[`PayuConfiguration`][access.profiling.payu_configuration.PayuConfiguration].

In this tutorial we write a configuration class for a small coupled application that is run with Payu. It has an
atmosphere and an ocean, which run at the same time, each with its own domain. Most of what we write is needed for
every application, and we point out what is specific to Payu.
[`ESM16Configuration`][access.profiling.models.esm16.ESM16Configuration] and
[`OM3Configuration`][access.profiling.models.om3.OM3Configuration], the configurations of two of the ACCESS models,
are complete examples of the same thing.

Note that you do not need to write any code for a new configuration of an application that the package already
supports: see [ACCESS models](../access/index.md) for the ACCESS models. The same is true for a new Rose suite,
which is a
[`RoseSuiteConfiguration`][access.profiling.rose_configuration.RoseSuiteConfiguration] value, as shown in
[Cylc Rose suites](../access/cylc-rose-suites.md#other-suites).

## The configuration class

Our application is run with Payu, so its class is a subclass of `PayuConfiguration`. In the package, these
classes are frozen dataclasses. Whatever changes from one configuration of the application to another is a field
of the class, and each configuration is an instance. We start with the fields of ours:

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

Every application has a `name` and an `experiment_prefix`, and both are used in the names of the experiments. The
`name` is different for each configuration of the application. The `experiment_prefix` is the same for the whole
application, so that you can tell its experiments from those of other applications in the same working directory.
`model_type` is specific to Payu. It is the name that Payu gives to the model, and the experiment generator checks
that Payu knows it.

We write the rest of the class in the following sections. There are five things to implement, and they are the
same for every application.

### The component tree

[`parallel_component`][access.profiling.application.Application.parallel_component] describes how the
application is parallelised. It returns a tree with the components of the application, their domains, and the
rules that every layout has to follow for the application to run. Ours has two components that run at the same
time, each with one thread per rank:

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

The names of the components are the ones you use in an allocation strategy. The name of the root stands for the
whole application, and is used for the regions that are plotted against the cores of the whole job. Two cases
deserve a mention. Components that take turns on the same cores, as the mediator and the sea ice of ACCESS-OM3 do,
go under a parent with `CoreSharing.SHARED`. A component that chooses its own decomposition when it runs, as MOM6
does with `AUTO_MASKTABLE`, has no domain. Only put here the rules that are always needed. Preferences, such as
subdomains that are close to square, belong in the allocation strategy of a study.

### The logs

[`logs`][access.profiling.application.Application.logs] lists the profiling logs that the components write. Each
log is a [`LogSpec`][access.profiling.application.LogSpec]. Our ocean writes FMS timers into its standard output:

```python
    @property
    def logs(self) -> tuple[LogSpec, ...]:
        return (LogSpec("ocean", payu_model_stdout(), FMSProfilingParser(has_hits=True), component="ocn"),)
```

A log spec has four parts. The first is the name of the log, which is the name you use in `manager.data` and in a
`RegionGroup`. The second says where to find the file in an output directory. The third is the parser that reads the
log, and the fourth is the component that the log belongs to. [`log_at`][access.profiling.application.log_at] locates a
file at a fixed path in the output directory, and `payu_model_stdout`, particular to Payu, locates the standard output
Payu captured from the run. Mark a log `optional=True` if a run may legitimately not write it. Note that the Payu
manager adds Payu's own job timings, as the `payu` log, to every Payu configuration.

### The experiment name

[`experiment_name`][access.profiling.application.Application.experiment_name] gives the name of the experiment
that runs a layout. There are two rules: different layouts must have different names, and the same layout must
always have the same name. The reason is that the name is used in many places. The Payu manager, for example, uses
it for the git branch, the directory and the archive of the experiment. We build ours from the two process grids:

```python
    def experiment_name(self, layout: ComponentLayout) -> str:
        atm, ocn = layout.sub_layouts
        atm_grid, ocn_grid = ("x".join(map(str, c.decomposition.grid.shape)) for c in (atm, ocn))
        return f"{self.experiment_prefix}_{self.name}_atm_{atm_grid}_ocn_{ocn_grid}"
```

Note that the layout's `sub_layouts` come in the order of the tree's `subcomponents`.

### Writing a layout and reading it back

[`config_changes`][access.profiling.application.Application.config_changes] returns the changes to make to the
files of the control in order to run a layout. The changes are given for each file, with its path relative to the
control directory. The Payu manager applies them with the experiment generator, so for Payu they have to be in its
format:

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

Be aware of how the experiment generator deals with lists. A list has to be wrapped in another list, because the
generator reads a simple list as one value for each experiment. The inner list is then merged with the list of the
control item by item, and cut to the length of the new one. This means that you have to give every item of the
list: [`ESM16Configuration`][access.profiling.models.esm16.ESM16Configuration] marks the
coupler it changes nothing in with `"PRESERVE"`.

[`parse_layout`][access.profiling.application.Application.parse_layout] does the opposite: it reads the layout of
an experiment from its files. The manager needs this for the experiments that it did not create in the current
session, in order to plot them against the cores of their components. If some information is not in the files,
such as the process grids here, it is left out:

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

We recommend keeping the two methods next to each other. If you change what one of them writes, you also have to
change what the other one reads.

## Trying it out

The class is now complete, so let us try it. We create an instance for one configuration of the application, and
use it with a manager like any other application. We then ask for the layouts of 96 cores that give each
component about half of them. The two directories of the manager are only examples, so adapt them to your own
setup:

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

The package ships the configuration classes of the ACCESS models. Each model has its own module in
`src/access/profiling/models/`, with its class and its presets. If you want to add yours to the package:

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
