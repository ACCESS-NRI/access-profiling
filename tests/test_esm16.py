# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import dataclasses
from pathlib import Path
from unittest import mock

import pytest
from access.config.parallel_allocation_strategies import FixedAllocation, FreeAllocation, RootAllocation
from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import (
    SubdomainAspectRatioConstraint,
)
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid
from conftest import decomposition_of, grid_of

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.control import GitControlSource
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.models.esm16 import (
    ESM16_1DEG_OCEAN,
    ESM16_AMIP_SUBMODELS,
    ESM16_CICE5_NAME,
    ESM16_CICE5_NX_GLOBAL,
    ESM16_MAX_SUBDOMAIN_ASPECT_RATIO,
    ESM16_MAX_WASTED_CORE_FRACTION,
    ESM16_MOM5_NAME,
    ESM16_N96_ATMOSPHERE,
    ESM16_PI_CONTROL,
    ESM16_PI_CONTROL_CORES,
    ESM16_UM7_NAME,
    ESM16Configuration,
)
from access.profiling.payu_manager import PayuManager
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

PI_CONTROL_NODES = 5.0
PI_CONTROL_CORES_PER_NODE = 104
PI_CONTROL_TOTAL_CORES = int(PI_CONTROL_NODES * PI_CONTROL_CORES_PER_NODE)
PI_CONTROL_UM7_GRID = (16, 16)
PI_CONTROL_MOM5_GRID = (16, 15)
PI_CONTROL_CICE5_RANKS = 12
PI_CONTROL_IDLE_CORES = 12
PI_CONTROL_BRANCH = "esm1p6-layout_piControl_atm_16x16_mom_16x15_ice_12"
PI_CONTROL_ALLOCATIONS = RootAllocation(
    subcomponents={
        ESM16_UM7_NAME: FixedAllocation(256, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
        ESM16_MOM5_NAME: FixedAllocation(240, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
        ESM16_CICE5_NAME: FixedAllocation(12),
    },
)


@pytest.fixture(scope="function")
def esm16():
    """The released pre-industrial control configuration, which is the one this package ships."""
    return ESM16_PI_CONTROL


@pytest.fixture(scope="function")
def manager(esm16):
    return PayuManager(
        Path("/fake/test_path"),
        Path("/fake/archive_path"),
        esm16,
        GitControlSource("https://example.com/repo", "commit"),
    )


def _select(configuration, total_cores, **kwargs):
    """The layouts of a configuration at a size, through the manager that searches for them.

    Args:
        configuration (ESM16Configuration): The configuration to search over.
        total_cores (int): Cores the layouts must distribute among the components.
        **kwargs: As for ProfilingManager.select_layouts.

    Returns:
        list[ComponentLayout]: The layouts found, fewest idle cores first.
    """
    manager = PayuManager(Path("/fake/test_path"), Path("/fake/archive_path"), configuration)
    return manager.select_layouts(total_cores, **kwargs)


@pytest.fixture(scope="function")
def pi_control_layout(esm16):
    """The released ACCESS-ESM1.6 PI control layout, picked out of the ones the layout search returns.

    Its core split does not determine the layout on its own: three layouts share it at this size, all leaving
    the same 12 cores idle. What matters here is that the released one is among them.
    """

    layouts = _select(esm16, PI_CONTROL_TOTAL_CORES, allocations=PI_CONTROL_ALLOCATIONS)
    released = [
        layout
        for layout in layouts
        if (grid_of(layout.sub_layouts[0]), grid_of(layout.sub_layouts[1]))
        == (PI_CONTROL_UM7_GRID, PI_CONTROL_MOM5_GRID)
    ]
    assert len(released) == 1, "The layout search should find the released PI control layout exactly once."
    return released[0]


def test_esm16_pi_control_layout(pi_control_layout):
    """Test that the layout search reproduces the released ACCESS-ESM1.6 PI control configuration."""

    um7, mom5, cice5 = pi_control_layout.sub_layouts
    assert um7.decomposition.grid.shape == PI_CONTROL_UM7_GRID
    assert mom5.decomposition.grid.shape == PI_CONTROL_MOM5_GRID
    assert cice5.n_ranks == PI_CONTROL_CICE5_RANKS
    assert pi_control_layout.idle_cores == PI_CONTROL_IDLE_CORES


def test_esm16_experiment_name(esm16, pi_control_layout):
    """The name an experiment holding the released layout is given."""

    assert esm16.experiment_name(pi_control_layout) == PI_CONTROL_BRANCH


def test_esm16_config_changes(esm16, pi_control_layout):
    """What the released layout comes to in the configuration files."""

    changes = esm16.config_changes(pi_control_layout)
    assert changes["config.yaml"]["submodels"] == [
        [
            {"ncpus": 256},
            {"ncpus": 240},
            {"ncpus": 12, "exe": ["cice_access.exe"]},
            "PRESERVE",
        ]
    ]
    assert changes["atmosphere/um_env.yaml"] == {
        "UM_ATM_NPROCX": "16",
        "UM_ATM_NPROCY": "16",
        "UM_NPES": "256",
    }
    assert changes["ocean/input.nml"] == {"ocean_model_nml": {"layout": ["16,15"]}}
    assert changes["ice/cice_in.nml"] == {
        "domain_nml": {
            "nprocs": "12",
            "nx_global": "360",
            "ny_global": "300",
            "block_size_x": "30",
            "block_size_y": "300",
            "max_blocks": "1",
        }
    }


def test_esm16_layout_requires_esm16_layout(esm16):
    """Both methods read the components in the order the tree declares them, so a layout of another model is
    refused rather than read as if it were one of these."""

    other_model = ComponentLayout(
        name="other-model",
        n_cores=4,
        n_ranks=4,
        threads_per_rank=None,
        decomposition=None,
        sub_layouts=(
            ComponentLayout(
                name="other-component",
                n_cores=4,
                n_ranks=4,
                threads_per_rank=1,
                decomposition=DomainDecompositionSpec(Domain((8, 8)), MPICartesianGrid((2, 2))),
            ),
        ),
    )
    with pytest.raises(ValueError, match="runs 3"):
        esm16.experiment_name(other_model)
    with pytest.raises(ValueError, match="runs 3"):
        esm16.config_changes(other_model)


def _esm16_scaling_allocations(atm_ocn_tolerance: float = 0.05, ice_tolerance: float = 0.25) -> RootAllocation:
    """An allocation strategy following the proportions of the ACCESS-ESM1.6 PI control configuration.

    This is the kind of strategy a caller supplies to the layout search: it is the study's own choice, not a
    requirement of ACCESS-ESM1.6, which is why it lives here rather than in access.profiling.models.esm16.

    Every bound is a fraction of the total, so the strategy this returns is a single object usable at every core
    count of a scaling study - which is the whole reason the layout search understands fractions.

    CICE5 gets a wider band than the other two. Each of its ranks takes one block spanning the full y extent, so
    the core counts it admits are the divisors of ESM16_CICE5_NX_GLOBAL, and they thin out as the count grows: a
    +/-5% band around its proportional share is [142, 158] at 5200 cores, which contains no divisor of 360 at all,
    and no layout would be found.
    """
    pi_control_cores = sum(ESM16_PI_CONTROL_CORES.values())

    def band(name: str, tolerance: float, **kwargs) -> FreeAllocation:
        share = ESM16_PI_CONTROL_CORES[name] / pi_control_cores
        return FreeAllocation(
            min_core_fraction=share * (1.0 - tolerance),
            max_core_fraction=min(1.0, share * (1.0 + tolerance)),
            **kwargs,
        )

    aspect_ratio = (SubdomainAspectRatioConstraint(max_ratio=1.5),)
    return RootAllocation(
        subcomponents={
            ESM16_UM7_NAME: band(ESM16_UM7_NAME, atm_ocn_tolerance, local_constraints=aspect_ratio),
            ESM16_MOM5_NAME: band(ESM16_MOM5_NAME, atm_ocn_tolerance, local_constraints=aspect_ratio),
            ESM16_CICE5_NAME: band(ESM16_CICE5_NAME, ice_tolerance),
        },
    )


# One strategy for the whole scaling study, built once and reused at every core count below.
ESM16_SCALING_ALLOCATIONS = _esm16_scaling_allocations()


@pytest.mark.parametrize("total_cores", [PI_CONTROL_TOTAL_CORES, 1040, 5200])
def test_esm16_caller_supplied_allocations(esm16, total_cores):
    """Test that one fractional allocation strategy generates usable ACCESS-ESM1.6 layouts at every size."""

    layouts = _select(esm16, total_cores, allocations=ESM16_SCALING_ALLOCATIONS)
    assert layouts, f"No layout found for {total_cores} cores."

    for layout in layouts:
        um7, _, cice5 = layout.sub_layouts
        # Executables are only available for an exact number of CICE5 blocks per rank
        assert ESM16_CICE5_NX_GLOBAL % cice5.n_ranks == 0
        # The UM requires an even number of processes along x
        assert grid_of(um7)[0] % 2 == 0
        # UM_NPES is written from n_ranks and must match the process grid, or the UM hangs at startup
        atm_nx, atm_ny = grid_of(um7)
        assert um7.n_ranks == atm_nx * atm_ny


def test_esm16_component_tree_bounds_layouts_on_its_own(esm16):
    """Test that the component tree rules out unreasonable layouts without any strategy constraints.

    The bounds in the tree are the ones a caller cannot relax, so they must hold for every layout the search
    returns when the caller states no preferences at all.
    """

    layouts = _select(esm16, PI_CONTROL_TOTAL_CORES, max_layouts=200)
    assert layouts, "The component tree alone should still admit layouts."

    for layout in layouts:
        assert layout.idle_cores / layout.n_cores <= ESM16_MAX_WASTED_CORE_FRACTION
        for component in (layout.sub_layouts[0], layout.sub_layouts[1]):
            local_shape = decomposition_of(component).mean_local_shape
            assert max(local_shape) / min(local_shape) <= ESM16_MAX_SUBDOMAIN_ASPECT_RATIO
        assert ESM16_CICE5_NX_GLOBAL % layout.sub_layouts[2].n_ranks == 0


@mock.patch("access.profiling.payu_manager.ExperimentGenerator")
def test_esm16_generate_scaling_experiments(mock_experiment_generator, manager):
    """Test that ACCESS-ESM1.6 scaling experiments can be generated end to end."""

    manager.generate_scaling_experiments(
        num_nodes_list=[PI_CONTROL_NODES],
        control_options={},
        cores_per_node=PI_CONTROL_CORES_PER_NODE,
        walltime=2.0,  # hrs
        allocations=PI_CONTROL_ALLOCATIONS,
    )

    config = mock_experiment_generator.call_args[0][0]
    assert config["model_type"] == "access-esm1.6"

    # One perturbation experiment per layout, numbered sequentially. The released core split leaves the process
    # grids open, so this is every layout that fits it, not the released one alone.
    perturbations = config["Perturbation_Experiment"]
    assert list(perturbations) == [f"Experiment_{n}" for n in range(1, len(perturbations) + 1)]

    released = [block for block in perturbations.values() if block["branches"] == [PI_CONTROL_BRANCH]]
    assert len(released) == 1, "The released PI control layout should generate exactly one experiment."
    assert released[0]["config.yaml"]["walltime"] == "2:00:00"
    assert PI_CONTROL_BRANCH in manager.experiments


def _config_text(submodels: list[dict]) -> str:
    """A Payu configuration declaring the given submodels."""

    lines = ["model: access", "submodels:"]
    for submodel in submodels:
        lines.append(f"  - name: {submodel['name']}")
        if "ncpus" in submodel:
            lines.append(f"    ncpus: {submodel['ncpus']}")
    return "\n".join(lines) + "\n"


class TestESM16ParseLayout:
    """Reading back what an ACCESS-ESM1.6 experiment gave each of its components."""

    @staticmethod
    def _config(tmp_path: Path, submodels: list[dict]) -> Path:
        """Writes a Payu configuration declaring the given submodels, and returns its directory."""

        lines = ["model: access", "submodels:"]
        for submodel in submodels:
            lines.append(f"  - name: {submodel['name']}")
            if "ncpus" in submodel:
                lines.append(f"    ncpus: {submodel['ncpus']}")
        (tmp_path / "config.yaml").write_text("\n".join(lines) + "\n")
        return tmp_path

    def test_it_reads_every_component(self, esm16, tmp_path):
        path = self._config(
            tmp_path,
            [
                {"name": "atmosphere", "ncpus": 256},
                {"name": "ocean", "ncpus": 240},
                {"name": "ice", "ncpus": 12},
            ],
        )

        layout = esm16.parse_layout(path)

        cores = {sub.name: sub.n_cores for sub in layout.sub_layouts}
        assert cores == {ESM16_UM7_NAME: 256, ESM16_MOM5_NAME: 240, ESM16_CICE5_NAME: 12}
        # One thread per rank throughout this model, and the root spends what its components spend.
        assert all(sub.threads_per_rank == 1 and sub.n_ranks == sub.n_cores for sub in layout.sub_layouts)
        assert layout.n_cores == 508
        assert layout.idle_cores == 0

    def test_the_cores_agree_with_what_generation_wrote(self, esm16, pi_control_layout):
        """The round trip that matters: what config_changes writes is what this reads back.

        Over the configuration's own declaration of the control's submodels, which is what both sides use:
        the slots it sets nothing in are marked PRESERVE and are not written at all.
        """

        changes = esm16.config_changes(pi_control_layout)
        (submodels,) = changes["config.yaml"]["submodels"]
        written = [
            {"name": name, **submodel}
            for (name, _), submodel in zip(esm16.submodels, submodels, strict=True)
            if submodel != "PRESERVE"
        ]
        assert len(written) == 3, "the three components, with OASIS's slot preserved rather than written"

        with mock.patch.object(Path, "read_text", return_value=_config_text(written)):
            layout = esm16.parse_layout(Path("/fake/expt"))

        assert {sub.name: sub.n_cores for sub in layout.sub_layouts} == {
            sub.name: sub.n_cores for sub in pi_control_layout.sub_layouts
        }

    def test_a_submodel_it_does_not_know_is_left_out(self, esm16, tmp_path):
        path = self._config(tmp_path, [{"name": "atmosphere", "ncpus": 256}, {"name": "something-else", "ncpus": 8}])

        layout = esm16.parse_layout(path)

        assert [sub.name for sub in layout.sub_layouts] == [ESM16_UM7_NAME]

    def test_a_submodel_stating_no_cores_is_left_out(self, esm16, tmp_path):
        path = self._config(tmp_path, [{"name": "atmosphere", "ncpus": 256}, {"name": "ice"}])

        layout = esm16.parse_layout(path)

        assert [sub.name for sub in layout.sub_layouts] == [ESM16_UM7_NAME]

    def test_a_configuration_naming_no_component_at_all(self, esm16, tmp_path):
        """Nothing to say rather than a layout with nothing in it, which would not be constructible."""

        path = self._config(tmp_path, [{"name": "something-else", "ncpus": 8}])
        assert esm16.parse_layout(path) is None

    def test_a_configuration_with_no_submodels(self, esm16, tmp_path):
        (tmp_path / "config.yaml").write_text("model: access\n")
        assert esm16.parse_layout(tmp_path) is None


class TestESM16Logs:
    """Which profiling logs a configuration says it writes, and where they are."""

    @staticmethod
    def _output(tmp_path: Path, *, um: bool = True, mom5: bool = True, cice5: bool = True) -> Path:
        """Writes an output directory holding whichever of the three logs is asked for."""

        (tmp_path / "atmosphere").mkdir()
        (tmp_path / "atmosphere" / "um_env.yaml").write_text("UM_STDOUT_FILE: pe_output/atm.fort6.pe\n")
        (tmp_path / "config.yaml").write_text("model: access\n")
        if um:
            (tmp_path / "atmosphere" / "pe_output").mkdir()
            (tmp_path / "atmosphere" / "pe_output" / "atm.fort6.pe0").write_text("")
        if mom5:
            (tmp_path / "access.out").write_text("")
        if cice5:
            (tmp_path / "ice").mkdir()
            (tmp_path / "ice" / "ice_diag.d").write_text("")
        return tmp_path

    def test_every_log_is_found_and_read_with_its_own_parser(self, esm16, tmp_path):
        logs = esm16.component_logs(self._output(tmp_path))

        assert set(logs) == {"UM", "UM_Total_Walltime", "MOM5", "CICE5"}
        assert isinstance(logs["UM"].parser, UMProfilingParser)
        assert isinstance(logs["UM_Total_Walltime"].parser, UMTotalRuntimeParser)
        assert isinstance(logs["MOM5"].parser, FMSProfilingParser)
        assert isinstance(logs["CICE5"].parser, CICE5ProfilingParser)

    @pytest.mark.parametrize(
        ("absent", "missing"),
        [
            ({"um": False}, {"UM", "UM_Total_Walltime"}),
            ({"mom5": False}, {"MOM5"}),
            ({"cice5": False}, {"CICE5"}),
        ],
    )
    def test_a_log_a_run_did_not_write_is_absent(self, esm16, tmp_path, absent, missing):
        logs = esm16.component_logs(self._output(tmp_path, **absent))

        assert set(logs) == {"UM", "UM_Total_Walltime", "MOM5", "CICE5"} - missing

    def test_each_log_names_the_component_it_was_run_on(self, esm16):
        """The UM's is read twice, once for its regions and once for its total, so both name the atmosphere."""

        assert esm16.component_for_log("UM") == ESM16_UM7_NAME
        assert esm16.component_for_log("UM_Total_Walltime") == ESM16_UM7_NAME
        assert esm16.component_for_log("MOM5") == ESM16_MOM5_NAME
        assert esm16.component_for_log("CICE5") == ESM16_CICE5_NAME
        assert esm16.component_for_log("payu") is None, "not one of this configuration's"

    def test_a_configuration_declares_only_the_logs_its_components_write(self):
        amip = ESM16Configuration(name="amip", atmosphere=ESM16_N96_ATMOSPHERE, submodels=ESM16_AMIP_SUBMODELS)

        assert {spec.name for spec in amip.logs} == {"UM", "UM_Total_Walltime"}


class TestESM16AMIP:
    """release-amip-1.0 is the atmosphere alone: no ocean, no sea ice, nothing to couple."""

    @pytest.fixture()
    def amip(self):
        return ESM16Configuration(name="amip", atmosphere=ESM16_N96_ATMOSPHERE, submodels=ESM16_AMIP_SUBMODELS)

    def test_its_tree_is_the_atmosphere_alone(self, amip):
        tree = amip.parallel_component

        assert [component.name for component in tree.subcomponents] == [ESM16_UM7_NAME]

    def test_it_generates_layouts_of_the_atmosphere_alone(self, amip):
        layouts = _select(amip, 208, max_layouts=50)

        assert layouts
        for layout in layouts:
            (um7,) = layout.sub_layouts
            assert grid_of(um7)[0] % 2 == 0, "the UM still needs an even x"

    def test_it_changes_the_one_submodel_it_has(self, amip):
        layout = _select(amip, 208, max_layouts=50)[0]
        changes = amip.config_changes(layout)

        (submodels,) = changes["config.yaml"]["submodels"]
        assert len(submodels) == 1, "a one-submodel control is merged against a one-element list"
        assert "ocean/input.nml" not in changes
        assert "ice/cice_in.nml" not in changes

    def test_its_experiments_are_not_the_coupled_ones(self, amip, pi_control_layout, esm16):
        """Which is the whole point of the configuration being in the name."""

        assert amip.experiment_name(_select(amip, 208, max_layouts=50)[0]).startswith("esm1p6-layout_amip_")
        assert esm16.experiment_name(pi_control_layout).startswith("esm1p6-layout_piControl_")


def test_two_configurations_with_the_same_layout_name_different_experiments(esm16, pi_control_layout):
    """piControl-2.1 and esm-historical-1.3 have identical grids, components and core counts.

    A name derived from the layout alone is therefore one string for two experiments, and that string is a
    dict key, a git branch, a directory, an archive stem and config.yaml's own experiment key.
    """

    esm_historical = dataclasses.replace(esm16, name="esm-historical")

    assert esm_historical.parallel_component.subcomponents == esm16.parallel_component.subcomponents
    assert esm_historical.experiment_name(pi_control_layout) != esm16.experiment_name(pi_control_layout)
    assert esm_historical.experiment_name(pi_control_layout) == PI_CONTROL_BRANCH.replace("piControl", "esm-historical")


def test_a_configuration_needs_a_name_and_a_component():
    with pytest.raises(ValueError, match="name must be non-empty"):
        ESM16Configuration(atmosphere=ESM16_N96_ATMOSPHERE)
    with pytest.raises(ValueError, match="runs no component"):
        ESM16Configuration(name="empty")


def test_a_configuration_refuses_a_grid_it_could_not_write_out():
    with pytest.raises(ValueError, match="1-dimensional grid"):
        ESM16Configuration(name="odd", atmosphere=Domain((192,)))


def test_a_configuration_refuses_submodels_for_components_it_does_not_run():
    with pytest.raises(ValueError, match=f"\\['{ESM16_MOM5_NAME}'"):
        ESM16Configuration(name="amip", atmosphere=ESM16_N96_ATMOSPHERE, submodels=(("ocean", ESM16_MOM5_NAME),))


def test_the_released_ocean_grid_is_the_sea_ice_grid(esm16):
    """CICE5 runs on the ocean's grid, which is what lets one number describe both."""

    assert esm16.sea_ice.grid == ESM16_1DEG_OCEAN.shape
    assert esm16.sea_ice.grid[0] == ESM16_CICE5_NX_GLOBAL


class TestESM16WithoutAnAtmosphere:
    """An ACCESS-ESM1.6 configuration that runs the ocean and the sea ice but no UM.

    No released configuration drops the atmosphere - AMIP is the other way round, the atmosphere alone - but
    the class admits one, since it asks only for a component rather than for that component. The tree, the
    logs and the configuration changes each ask whether there is an atmosphere instead of assuming it, and
    these are the other side of those three questions: an implementation that assumed one would reach into
    `self.atmosphere.shape` and raise, or write an `atmosphere/um_env.yaml` into a control that has no
    atmosphere to read it.
    """

    OCEAN_ICE_SUBMODELS = (("ocean", ESM16_MOM5_NAME), ("ice", ESM16_CICE5_NAME), ("coupler", None))
    OCEAN_ICE_TOTAL_CORES = 260
    OCEAN_ICE_ALLOCATIONS = RootAllocation(
        subcomponents={
            ESM16_MOM5_NAME: FixedAllocation(240, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
            ESM16_CICE5_NAME: FixedAllocation(12),
        },
    )

    @pytest.fixture()
    def ocean_ice(self, esm16):
        """The released coupled configuration with its atmosphere taken out, submodels and all."""

        return dataclasses.replace(esm16, name="ocean-ice", atmosphere=None, submodels=self.OCEAN_ICE_SUBMODELS)

    @pytest.fixture()
    def ocean_ice_layout(self, ocean_ice):
        """A layout of it, picked by the ocean grid so that what it comes to can be written out in full."""

        layouts = _select(ocean_ice, self.OCEAN_ICE_TOTAL_CORES, allocations=self.OCEAN_ICE_ALLOCATIONS)
        picked = [layout for layout in layouts if grid_of(layout.sub_layouts[0]) == PI_CONTROL_MOM5_GRID]
        assert len(picked) == 1, "The layout search should find the released ocean grid exactly once."
        return picked[0]

    def test_its_tree_starts_with_the_ocean(self, ocean_ice):
        """The components are read back out of a layout by position, so a tree that kept an empty slot for the
        absent atmosphere would hand the ocean's cores to the UM."""

        tree = ocean_ice.parallel_component

        assert [component.name for component in tree.subcomponents] == [ESM16_MOM5_NAME, ESM16_CICE5_NAME]

    def test_it_declares_neither_um_log(self, ocean_ice):
        """A declared log that no run can write is looked for in every output directory and never found."""

        assert {spec.name for spec in ocean_ice.logs} == {"MOM5", "CICE5"}
        assert ocean_ice.component_for_log("UM") is None
        assert ocean_ice.component_for_log("UM_Total_Walltime") is None

    def test_it_writes_no_um_env(self, ocean_ice, ocean_ice_layout):
        """um_env.yaml is the UM's own file, and this control has no atmosphere directory for it to go in."""

        changes = ocean_ice.config_changes(ocean_ice_layout)

        assert "atmosphere/um_env.yaml" not in changes
        assert changes["config.yaml"]["submodels"] == [
            [{"ncpus": 240}, {"ncpus": 12, "exe": ["cice_access.exe"]}, "PRESERVE"]
        ]
        assert changes["ocean/input.nml"] == {"ocean_model_nml": {"layout": ["16,15"]}}

    def test_its_experiments_name_the_two_components_it_runs(self, ocean_ice, ocean_ice_layout):
        assert ocean_ice.experiment_name(ocean_ice_layout) == "esm1p6-layout_ocean-ice_mom_16x15_ice_12"


class TestESM16SeaIceThatDecomposesNothing:
    """A CICE5 whose blocks are distributed by a scheme that forms no process grid.

    ACCESS-ESM1.6 releases distribute `cartesian`, so their sea ice splits the x extent and the search picks
    the split. A release that distributed `roundrobin` instead would leave the search nothing to choose but a
    rank count, and the name of an experiment has to record what that component actually received. It cannot
    record a process grid, because there is none to record: a name built from one would raise on the absent
    decomposition, and a rank count is what distinguishes the layouts in the first place.
    """

    assert ESM16_PI_CONTROL.sea_ice is not None, "the released configuration has a sea ice"
    OPAQUE_CICE5 = dataclasses.replace(ESM16_PI_CONTROL.sea_ice, distribution_type="roundrobin")
    # Not a divisor of 360, so no sea ice that tiles the x extent could ever be given it. It is reachable only
    # because this one tiles nothing.
    OPAQUE_ICE_RANKS = 7
    OPAQUE_TOTAL_CORES = 520
    OPAQUE_ALLOCATIONS = RootAllocation(
        subcomponents={
            ESM16_UM7_NAME: FixedAllocation(256, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
            ESM16_MOM5_NAME: FixedAllocation(240, local_constraints=(SubdomainAspectRatioConstraint(1.5),)),
            ESM16_CICE5_NAME: FixedAllocation(OPAQUE_ICE_RANKS),
        },
    )

    @pytest.fixture()
    def roundrobin(self, esm16):
        """The released coupled configuration with a sea ice that distributes roundrobin."""

        return dataclasses.replace(esm16, sea_ice=self.OPAQUE_CICE5)

    @pytest.fixture()
    def roundrobin_layout(self, roundrobin):
        """A layout of it at the released core counts, bar the sea ice's, picked by the two process grids."""

        layouts = _select(roundrobin, self.OPAQUE_TOTAL_CORES, allocations=self.OPAQUE_ALLOCATIONS)
        picked = [
            layout
            for layout in layouts
            if (grid_of(layout.sub_layouts[0]), grid_of(layout.sub_layouts[1]))
            == (PI_CONTROL_UM7_GRID, PI_CONTROL_MOM5_GRID)
        ]
        assert len(picked) == 1, "The layout search should find the released process grids exactly once."
        return picked[0]

    def test_the_search_is_given_nothing_to_decompose(self, roundrobin, roundrobin_layout):
        """Which is the premise of the naming below, and the reason the sea ice can take 7 ranks at all."""

        assert roundrobin.parallel_component.subcomponents[2].domain is None
        assert roundrobin_layout.sub_layouts[2].decomposition is None
        assert roundrobin_layout.sub_layouts[2].n_ranks == self.OPAQUE_ICE_RANKS

    def test_it_is_named_by_its_rank_count_while_the_others_keep_their_grids(self, roundrobin, roundrobin_layout):
        """One name per distinct layout is what lets a manager tell whether it already has an experiment, so a
        component with no process grid still has to put what it received into the name."""

        assert roundrobin.experiment_name(roundrobin_layout) == "esm1p6-layout_piControl_atm_16x16_mom_16x15_ice_7"

    def test_it_writes_its_ranks_and_its_grid_but_no_block_size(self, roundrobin, roundrobin_layout):
        """A block size left over from a decomposition this scheme never made is how a namelist and the layout
        it is supposed to realise come apart."""

        changes = roundrobin.config_changes(roundrobin_layout)

        assert changes["ice/cice_in.nml"] == {"domain_nml": {"nprocs": "7", "nx_global": "360", "ny_global": "300"}}


def test_esm16_config_changes_refuses_a_layout_that_states_no_decomposition(esm16, tmp_path):
    """parse_layout states cores and not grids, so what it returns cannot be written straight back out.

    config_changes used to reach through the missing decomposition and die on an AttributeError, although
    its docstring promises a ValueError. The two are documented as inverses over the same files, so the
    round trip is the obvious thing for a caller to try.
    """
    (tmp_path / "config.yaml").write_text(
        "model: access\n"
        "submodels:\n"
        "  - name: atmosphere\n    ncpus: 256\n"
        "  - name: ocean\n    ncpus: 240\n"
        "  - name: ice\n    ncpus: 12\n"
    )
    parsed = esm16.parse_layout(tmp_path)
    assert parsed is not None

    with pytest.raises(ValueError, match="states no decomposition"):
        esm16.config_changes(parsed)
