# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""What a Payu-driven application states about itself.

Everything here knows that Payu is the runner: that it archives a run under `config.yaml`, that it names the
model's standard output after the `model` key that file states, that the components of a coupled model are the
`submodels` that file declares, each given its cores outright, and that `ncpureq` is what the job asks for. None of
it belongs in the generic contract, which must hold for a runner that does none of those things.

It is kept apart from `payu_manager` deliberately. That module reaches for the experiment generator and the
experiment runner, and so for Payu itself; an application only describes what is to be profiled and should
not have to import a job-submission stack to do it.

A Payu application is put together from components, one per code the model runs, so that a configuration
arranges components rather than restating what each one decomposes, writes and logs. The arrangement is the
configuration's: `SubmodelsConfiguration` runs its components side by side, one per submodel, which is how Payu
couples ACCESS-ESM1.5, ACCESS-ESM1.6 and ACCESS-OM2; ACCESS-OM3's realms share a range of cores and are laid out by
NUOPC, which its own configuration class describes. The components of the ACCESS codes are in
`models.components`; the one here, `PlainComponent`, names no code.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from access.config import YAMLParser
from access.config.parallel_component import ComponentLayout, ParallelComponent
from access.config.parallel_constraints import (
    FixedThreadsPerRankConstraint,
    LocalConstraint,
    MaxWastedCoreFractionConstraint,
)
from access.config.parallel_domain import Domain

from access.profiling.application import Application, LogLocator, LogSpec

logger = logging.getLogger(__name__)

MAX_WASTED_CORE_FRACTION: float = 0.1
"""Ceiling on the fraction of its cores a layout of a Payu model may leave idle.

This bounds what counts as a reasonable layout at all, rather than the budget of any particular study. Constraints
are cumulative, so a caller can only tighten it: a study that wants no waste at all says so in its own allocation
strategy.
"""

# The marker the experiment generator reads as "leave this slot as the control has it".
_PRESERVE: str = "PRESERVE"


def payu_model_stdout() -> LogLocator:
    """Returns a locator for the standard output Payu captured from the model.

    Payu names it after the model it ran, which the archived `config.yaml` records, so the name is read from
    there rather than assumed.

    Returns:
        LogLocator: The locator.
    """

    def locate(output_dir: Path) -> Path | None:
        config_path = output_dir / "config.yaml"
        if not config_path.is_file():
            return None
        payu_config = YAMLParser().parse(config_path.read_text())
        model = payu_config.get("model")
        return None if model is None else output_dir / f"{model}.out"

    return locate


@dataclass(frozen=True)
class FixedRanksConstraint(LocalConstraint):
    """A component's rank count must be the one the model fixes.

    For a component that can run on one rank count alone: a serial one, such as ACCESS-OM2's data atmosphere, or
    one whose decomposition is compiled into its executable, such as ACCESS-ESM1.5's CICE4. Reads the rank count
    alone, so it applies to a component that carries no domain.

    Args:
        n_ranks (int): The rank count. Must be positive.

    Raises:
        ValueError: If the rank count is not positive.
    """

    n_ranks: int

    def __post_init__(self) -> None:
        if self.n_ranks < 1:
            raise ValueError(f"FixedRanksConstraint.n_ranks must be positive. Got {self.n_ranks}.")

    def is_satisfied(self, layout: ComponentLayout) -> bool:
        return layout.n_ranks == self.n_ranks


class PayuComponent(ABC):
    """Abstract base class for one component of a Payu model: one of the codes it runs.

    A component states what the generic contract asks for per component, so that a configuration is put together
    from components rather than restating them: the grid the search decomposes for it, if any, the requirements
    every layout of it must meet, the profiling logs it writes, and what its own files have to say for a given
    layout. The components of the ACCESS codes are in `models.components`.

    Attributes:
        name (str): How the component tree names it, and so the key a caller uses for it in an allocation strategy
            or a RegionGroup.
        token (str): How `experiment_name` abbreviates it, where a configuration names its experiments after what
            each component received. User-facing: changing it changes the names of the experiments on disk.
        logs (tuple[LogSpec, ...]): The profiling logs it writes, each naming it as the component its regions ran
            on.

    These three are declared as attributes rather than abstract properties, for the reason `Application` gives:
    a subclass satisfies them with a dataclass field, a class attribute or a property, whichever it has.
    """

    name: str
    token: str
    logs: tuple[LogSpec, ...]

    @property
    @abstractmethod
    def domain(self) -> Domain | None:
        """Returns the grid the search decomposes for this component.

        Returns:
            Domain | None: The grid, or None where the component decomposes nothing the search can choose.
        """

    @property
    @abstractmethod
    def local_constraints(self) -> tuple[LocalConstraint, ...]:
        """Returns the requirements every layout of this component must meet, whatever the study.

        One thread per rank is not among them: the configuration adds that to every component.

        Returns:
            tuple[LocalConstraint, ...]: The constraints.
        """

    @abstractmethod
    def config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the changes to this component's own files that run a given layout of it.

        Args:
            layout (ComponentLayout): What the component was given, as the layout search returned it.

        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control
                directory, in the format of experiment-generator. Empty where the files state nothing the layout
                decides.

        Raises:
            ValueError: If the layout cannot be realised by this component.
        """


@dataclass(frozen=True, kw_only=True)
class PlainComponent(PayuComponent):
    """A component the search chooses a rank count for and nothing else, and whose files state nothing of it.

    What such a component receives is cores. A data component's mesh is decomposed by ESMF over whatever ranks it
    is given, and MOM6 works its own decomposition out under AUTO_MASKTABLE, so there is no process grid to choose
    or to write. A serial component, or one whose decomposition is compiled into its executable, takes the one rank
    count it can, which `n_ranks` pins.

    Args:
        name (str): How the component tree names it.
        token (str): How `experiment_name` abbreviates it.
        n_ranks (int | None): The rank count the model fixes, or None where the search chooses it.
        logs (tuple[LogSpec, ...]): The profiling logs it writes, if any.

    Raises:
        ValueError: If the rank count is fixed to something nothing could run on.
    """

    name: str
    token: str
    n_ranks: int | None = None
    logs: tuple[LogSpec, ...] = ()

    def __post_init__(self) -> None:
        # Checked here rather than when the constraint is built, so that a configuration pinning a component
        # to no ranks is refused when it is built instead of once the search reads its tree.
        if self.n_ranks is not None and self.n_ranks < 1:
            raise ValueError(
                f"PlainComponent {self.name!r} is pinned to {self.n_ranks} rank(s), which nothing could run "
                "on: n_ranks must be positive."
            )

    @property
    def domain(self) -> None:
        return None

    @property
    def local_constraints(self) -> tuple[LocalConstraint, ...]:
        return () if self.n_ranks is None else (FixedRanksConstraint(n_ranks=self.n_ranks),)

    def config_changes(self, layout: ComponentLayout) -> dict:
        return {}


def require_process_grid(name: str, layout: ComponentLayout) -> tuple[int, int]:
    """Returns the process grid a component was given, refusing a layout that states none.

    Only a layout the search produced carries a decomposition. One read back from a configuration states what
    each component was given and not how it divided its domain, so it cannot be written out again - see
    `Application.parse_layout`. Saying so here keeps `config_changes` raising the ValueError its docstring
    promises rather than an AttributeError from deep inside it.

    Args:
        name (str): The component, for the error message.
        layout (ComponentLayout): What that component was given.

    Returns:
        tuple[int, int]: The two extents of its process grid.

    Raises:
        ValueError: If the layout states no decomposition for this component.
    """
    if layout.decomposition is None:
        raise ValueError(
            f"The layout of {name!r} states no decomposition, so there is no process grid to write into its "
            "configuration files. Only a layout the search produced carries one; a layout read back from a "
            "configuration does not."
        )
    nx, ny = layout.decomposition.grid
    return nx, ny


@dataclass(frozen=True)
class ControlSubmodel:
    """One submodel as a control's `config.yaml` declares it: what a reader of the control needs of it.

    Args:
        name (str): The submodel's name.
        model (str): Its `model`, the Payu model type that runs it.
        ncpus (int | None): The cores the control gives it, where it states them.
        exe (str | None): The executable the control names for it, where it names one.
    """

    name: str
    model: str
    ncpus: int | None = None
    exe: str | None = None


def _payu_config(directory: Path) -> dict:
    """Returns the `config.yaml` of a control directory, parsed.

    Args:
        directory (Path): The control directory.

    Returns:
        dict: The configuration.

    Raises:
        ValueError: If the directory holds no `config.yaml`.
    """
    config_path = directory / "config.yaml"
    if not config_path.is_file():
        raise ValueError(f"The control at {directory} has no config.yaml, so it is not a Payu control.")
    return YAMLParser().parse(config_path.read_text())


def read_submodels(directory: Path) -> tuple[ControlSubmodel, ...]:
    """Returns the submodels a control's `config.yaml` declares, in its order.

    The readers of the models build a configuration from these, by the Payu model type each submodel names,
    which is the vocabulary `config.yaml` and Payu share.

    Args:
        directory (Path): The control directory.

    Returns:
        tuple[ControlSubmodel, ...]: The submodels.

    Raises:
        ValueError: If the directory holds no `config.yaml`, if it declares no submodels, or if a submodel
            states no name or no model.
    """
    submodels = _payu_config(directory).get("submodels")
    if not submodels:
        raise ValueError(f"The config.yaml of the control at {directory} declares no submodels.")
    declared = []
    for submodel in submodels:
        name, model = submodel.get("name"), submodel.get("model")
        if name is None or model is None:
            raise ValueError(f"A submodel of the control at {directory} states no name or no model: {submodel}.")
        declared.append(ControlSubmodel(name, model, submodel.get("ncpus"), submodel.get("exe")))
    return tuple(declared)


def control_name(directory: Path, name: str | None = None) -> str:
    """Returns the name a configuration read from a control takes: the one given, else `config.yaml`'s jobname.

    Args:
        directory (Path): The control directory.
        name (str | None): The name the caller chose, if any.

    Returns:
        str: The name.

    Raises:
        ValueError: If no name is given and the control states no jobname either.
    """
    if name is not None:
        return name
    jobname = _payu_config(directory).get("jobname")
    if not jobname:
        raise ValueError(
            f"The config.yaml of the control at {directory} states no jobname, so the configuration has to be "
            "given a name."
        )
    return str(jobname)


@dataclass(frozen=True)
class PayuSubmodel:
    """One of the submodels a Payu control declares, paired with the component that runs in it.

    Args:
        name (str): The submodel's name in `config.yaml`, which the control chose: `atmosphere`, `ocean`, `ice`.
        model (str): The submodel's `model`, the Payu model type that runs it: `um`, `mom`, `cice5`, `oasis`.
        component (PayuComponent | None): The component it runs, or None for a submodel this package sets nothing
            in, such as the OASIS coupler, whose slot is marked PRESERVE.
        exe (str | None): The executable to run it with, where the configuration names one other than the
            control's.
    """

    name: str
    model: str
    component: PayuComponent | None = None
    exe: str | None = None


class PayuConfiguration(Application):
    """Abstract base class for a configuration of a model driven by Payu.

    Attributes:
        model_type (str): The model type identifier, as Payu defines it, e.g. "access-esm1.6". Declared as an
            attribute for the same reason as Application's own two.
        model_name (str): The model's name, which names the root of the component tree together with `name`:
            `ACCESS-ESM1.6 piControl` is the root of the pre-industrial control's tree, and so the component a
            RegionGroup names for the regions plotted against the cores of the whole job.
    """

    model_type: str
    model_name: str

    @staticmethod
    def _leaf(component: PayuComponent) -> ParallelComponent:
        """Returns the leaf of the component tree a component becomes.

        Every component of the models Payu runs takes one thread per rank, which is what lets `parse_layout`
        read cores back as ranks.

        Args:
            component (PayuComponent): The component.

        Returns:
            ParallelComponent: The leaf, carrying the component's grid and its requirements.
        """
        return ParallelComponent(
            name=component.name,
            domain=component.domain,
            local_constraints=(FixedThreadsPerRankConstraint(n_threads=1), *component.local_constraints),
        )

    def _root(self, subcomponents: Iterable[ParallelComponent]) -> ParallelComponent:
        """Returns the root of the component tree, named after the model and the configuration.

        Args:
            subcomponents (Iterable[ParallelComponent]): The components, in the order the tree declares them.

        Returns:
            ParallelComponent: The root, holding the one requirement every layout of a Payu model meets.
        """
        return ParallelComponent(
            name=f"{self.model_name} {self.name}",
            subcomponents=tuple(subcomponents),
            local_constraints=(MaxWastedCoreFractionConstraint(max_fraction=MAX_WASTED_CORE_FRACTION),),
        )

    @staticmethod
    def _component_logs(components: Iterable[PayuComponent]) -> tuple[LogSpec, ...]:
        """Returns the profiling logs the given components write, in their order.

        Args:
            components (Iterable[PayuComponent]): The components.

        Returns:
            tuple[LogSpec, ...]: Their logs.
        """
        return tuple(spec for component in components for spec in component.logs)


class SubmodelsConfiguration(PayuConfiguration):
    """Abstract base class for a Payu model whose components run side by side, each a submodel of `config.yaml`.

    This is how Payu couples ACCESS-ESM1.5, ACCESS-ESM1.6 and ACCESS-OM2: the control declares its submodels in
    order, each with its cores, and the job asks for their sum. A subclass says which submodels a configuration
    declares and which component runs in each, as a frozen dataclass whose fields are what varies between the
    configurations of the model; everything else follows from that.

    The experiment generator merges a list positionally and then truncates it to the length of the edit, so the
    submodels are written with a slot for every one the control declares: a shorter list would silently delete
    whatever the control declared after it, the OASIS coupler for one. A submodel this package sets nothing in
    is marked PRESERVE.
    """

    @property
    @abstractmethod
    def submodels(self) -> tuple[PayuSubmodel, ...]:
        """Returns the submodels the control declares, in its order, each with the component that runs in it.

        Both the order and the length matter: they are what a layout's core counts are merged against.

        Returns:
            tuple[PayuSubmodel, ...]: The submodels.
        """

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError(
                f"{type(self).__name__}.name must be non-empty: it is what tells one configuration's experiments "
                "from another's, and two configurations of a model can have identical layouts."
            )
        # Also what makes a component that cannot be built, over a grid it cannot write out, fail here rather
        # than once a layout has been found for it.
        if not self.components:
            raise ValueError(
                f"{self.model_name} configuration {self.name!r} runs no component. There is nothing to profile."
            )

    @property
    def components(self) -> tuple[PayuComponent, ...]:
        """Returns the components this configuration runs, in the order the submodels declare them."""
        return tuple(submodel.component for submodel in self.submodels if submodel.component is not None)

    @property
    def parallel_component(self) -> ParallelComponent:
        """Returns the component tree describing how this configuration is parallelised.

        The components run side by side, each on a range of cores of its own, so they are siblings and their
        cores add up. Only the ones the configuration runs are there.

        The tree carries only the requirements that hold for every layout of this configuration, whatever is
        being studied. Constraints are cumulative and cannot be relaxed by a caller, so anything that is a
        choice rather than a requirement belongs in the allocation strategy instead.

        Returns:
            ParallelComponent: Root of the component tree.
        """
        return self._root(self._leaf(component) for component in self.components)

    @property
    def logs(self) -> tuple[LogSpec, ...]:
        """Returns the profiling logs this configuration writes, those of each component that writes one."""
        return self._component_logs(self.components)

    def _component_layouts(self, layout: ComponentLayout) -> tuple[tuple[PayuComponent, ComponentLayout], ...]:
        """Returns what each component was given, paired with the component.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            tuple[tuple[PayuComponent, ComponentLayout], ...]: Each component and its layout, in tree order.

        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        components = self.components
        if len(layout.sub_layouts) != len(components):
            raise ValueError(
                f"The layout {layout.name!r} has {len(layout.sub_layouts)} component(s), but {self.model_name} "
                f"configuration {self.name!r} runs {len(components)}: {[component.name for component in components]}."
            )
        # ComponentLayout.sub_layouts is documented to come in the order of ParallelComponent.subcomponents,
        # which is the order of the components, so zipping is the inverse of how the tree was built.
        return tuple(zip(components, layout.sub_layouts, strict=True))

    def experiment_name(self, layout: ComponentLayout) -> str:
        """Returns the name of the experiment holding a given layout.

        The name records the configuration and then what each component received: its process grid where it
        decomposes one, and its ranks otherwise. It is therefore distinct for every distinct layout, and the
        same layout always produces the same name, which is what lets a manager tell whether it already has
        an experiment for a layout before building one. The configuration is part of it because the layout is
        not enough: two configurations of a model can run the same components, on the same grids, on the same
        number of cores.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            str: The experiment name.

        Raises:
            ValueError: If the layout is not a layout of this configuration.
        """
        parts = []
        for component, sub_layout in self._component_layouts(layout):
            if sub_layout.decomposition is None:
                parts.append(f"{component.token}_{sub_layout.n_ranks}")
            else:
                parts.append(f"{component.token}_" + "x".join(str(ranks) for ranks in sub_layout.decomposition.grid))
        return f"{self.experiment_prefix}_{self.name}_{'_'.join(parts)}"

    def config_changes(self, layout: ComponentLayout) -> dict:
        """Returns the configuration file changes needed to run this configuration with a given layout.

        Every component's cores go into `config.yaml`'s submodels, in the order the control declares them and
        with a slot for every one of them, and each component additionally has its own files changed as it says.

        The job is asked for the whole budget of the layout, idle cores included, through `ncpureq`. Payu would
        otherwise request what the submodels sum to, and only rounds that up to whole nodes past the first one:
        two layouts of the same single node budget would then run as jobs of different sizes, and `parse_ncpus`,
        which reads back what the job was given, would set them apart. Asking for the budget is what makes the
        job occupy what the study meant it to.

        Args:
            layout (ComponentLayout): Layout of the components, as returned by the layout search.

        Returns:
            dict: Changes to apply, keyed by the path of each configuration file relative to the control
                directory.

        Raises:
            ValueError: If the layout is not a layout of this configuration, or if a component that writes
                a process grid carries no decomposition - which is what a layout read back from a
                configuration looks like, since those state cores and not grids.
        """
        given = {component.name: sub_layout for component, sub_layout in self._component_layouts(layout)}
        slots: list = []
        changes: dict = {}
        for submodel in self.submodels:
            if submodel.component is None:
                slots.append(_PRESERVE)
                continue
            sub_layout = given[submodel.component.name]
            slot: dict = {"ncpus": sub_layout.n_cores}
            if submodel.exe is not None:
                slot["exe"] = [submodel.exe]
            slots.append(slot)
            changes.update(submodel.component.config_changes(sub_layout))
        return {"config.yaml": {"ncpureq": layout.n_cores, "submodels": [slots]}, **changes}

    def parse_layout(self, output_dir: Path) -> ComponentLayout | None:
        """Returns the layout an experiment ran, from the Payu submodels its `config.yaml` declares.

        Payu gives each submodel its cores outright, so the configuration states this directly. What it does
        not state is how a component divides its domain, so the layout that comes back carries no
        decompositions.

        A submodel this configuration does not declare is left out rather than guessed at, and a file naming
        none of them produces nothing at all.

        Args:
            output_dir (Path): Directory holding the configuration files. Must contain a `config.yaml`.

        Returns:
            ComponentLayout | None: The layout, or None if `config.yaml` names no component of this
                configuration.
        """
        config_path = output_dir / "config.yaml"
        payu_config = YAMLParser().parse(config_path.read_text())
        components = {
            submodel.name: submodel.component for submodel in self.submodels if submodel.component is not None
        }

        sub_layouts = []
        for submodel in payu_config.get("submodels", []):
            component = components.get(submodel.get("name"))
            if component is None:
                logger.debug(f"Submodel {submodel.get('name')!r} of {config_path} is not a component of this model.")
                continue
            n_cores = submodel.get("ncpus")
            if not n_cores:
                logger.debug(f"Submodel {submodel.get('name')!r} of {config_path} states no core count.")
                continue
            # One thread per rank throughout, as the tree requires, so ranks are cores.
            sub_layouts.append(
                ComponentLayout(
                    name=component.name, n_cores=n_cores, n_ranks=n_cores, threads_per_rank=1, decomposition=None
                )
            )

        if not sub_layouts:
            logger.debug(f"No component of this model is named among the submodels of {config_path}.")
            return None

        # The components run side by side, so the root spends what they spend between them. Any cores left
        # idle to fill a node are not the layout's; the manager's parse_ncpus is what reports those.
        used = sum(sub_layout.n_cores for sub_layout in sub_layouts)
        return ComponentLayout(
            name=f"{self.model_name} {self.name}",
            n_cores=used,
            n_ranks=used,
            threads_per_rank=None,
            decomposition=None,
            sub_layouts=tuple(sub_layouts),
        )
