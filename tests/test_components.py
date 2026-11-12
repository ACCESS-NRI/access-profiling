# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest
from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import ProcessGridDimEvenConstraint, SubdomainAspectRatioConstraint
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.models.cice import CICEPartitioning
from access.profiling.models.components import (
    MAX_SUBDOMAIN_ASPECT_RATIO,
    CICEComponent,
    MOM5Component,
    UMComponent,
)
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

N96 = Domain((192, 144))
ONE_DEGREE = Domain((360, 300))


def _decomposed(name: str, domain: Domain, grid: tuple[int, ...]) -> ComponentLayout:
    """A component's layout, decomposing the domain over the given process grid."""

    n_ranks = 1
    for ranks in grid:
        n_ranks *= ranks
    return ComponentLayout(
        name=name,
        n_cores=n_ranks,
        n_ranks=n_ranks,
        threads_per_rank=1,
        decomposition=DomainDecompositionSpec(domain, MPICartesianGrid(grid)),
    )


def _undecomposed(name: str, n_ranks: int) -> ComponentLayout:
    """A component's layout as parse_layout reads one back: cores, and no decomposition."""

    return ComponentLayout(name=name, n_cores=n_ranks, n_ranks=n_ranks, threads_per_rank=1, decomposition=None)


class TestUMComponent:
    """The UM, decomposing its grid over the process grid um_env.yaml states."""

    def test_it_is_named_and_abbreviated_the_same_in_every_model(self):
        assert UMComponent(N96).name == "UM7"
        assert UMComponent(N96).token == "atm"

    def test_it_decomposes_its_grid_with_an_even_x_and_bounded_subdomains(self):
        um = UMComponent(N96)

        assert um.domain == N96
        assert um.local_constraints == (
            ProcessGridDimEvenConstraint(dim=0),
            SubdomainAspectRatioConstraint(max_ratio=MAX_SUBDOMAIN_ASPECT_RATIO),
        )

    def test_it_refuses_a_grid_it_could_not_write_out(self):
        with pytest.raises(ValueError, match="1-dimensional grid"):
            UMComponent(Domain((192,)))

    def test_its_timers_are_read_twice_from_its_standard_output(self, tmp_path):
        (tmp_path / "atmosphere" / "pe_output").mkdir(parents=True)
        (tmp_path / "atmosphere" / "um_env.yaml").write_text("UM_STDOUT_FILE: pe_output/atm.fort6.pe\n")
        (tmp_path / "atmosphere" / "pe_output" / "atm.fort6.pe0").write_text("")

        logs = UMComponent(N96).logs

        assert [(spec.name, spec.component, spec.optional) for spec in logs] == [
            ("UM", "UM7", False),
            ("UM_Total_Walltime", "UM7", False),
        ]
        assert isinstance(logs[0].parser, UMProfilingParser)
        assert isinstance(logs[1].parser, UMTotalRuntimeParser)
        assert all(spec.locator(tmp_path) == tmp_path / "atmosphere" / "pe_output" / "atm.fort6.pe0" for spec in logs)

    def test_it_writes_its_process_grid_and_its_rank_count_as_strings(self):
        changes = UMComponent(N96).config_changes(_decomposed("UM7", N96, (16, 16)))

        assert changes == {"atmosphere/um_env.yaml": {"UM_ATM_NPROCX": "16", "UM_ATM_NPROCY": "16", "UM_NPES": "256"}}

    def test_it_refuses_a_layout_that_states_no_decomposition(self):
        with pytest.raises(ValueError, match="'UM7' states no decomposition"):
            UMComponent(N96).config_changes(_undecomposed("UM7", 256))


class TestMOM5Component:
    """MOM5, decomposing its grid over the layout input.nml states."""

    def test_it_is_named_and_abbreviated_the_same_in_every_model(self):
        assert MOM5Component(ONE_DEGREE).name == "MOM5"
        assert MOM5Component(ONE_DEGREE).token == "mom"

    def test_it_decomposes_its_grid_with_bounded_subdomains(self):
        mom5 = MOM5Component(ONE_DEGREE)

        assert mom5.domain == ONE_DEGREE
        assert mom5.local_constraints == (SubdomainAspectRatioConstraint(max_ratio=MAX_SUBDOMAIN_ASPECT_RATIO),)

    def test_it_refuses_a_grid_it_could_not_write_out(self):
        with pytest.raises(ValueError, match="3-dimensional grid"):
            MOM5Component(Domain((360, 300, 50)))

    def test_its_clocks_are_read_from_the_standard_output_payu_captured(self, tmp_path):
        (tmp_path / "config.yaml").write_text("model: access\n")

        (spec,) = MOM5Component(ONE_DEGREE).logs

        assert (spec.name, spec.component, spec.optional) == ("MOM5", "MOM5", False)
        assert isinstance(spec.parser, FMSProfilingParser)
        assert spec.locator(tmp_path) == tmp_path / "access.out"

    def test_it_writes_its_layout_as_one_namelist_string(self):
        """Not a list of the two extents: the generator renders a list with its own brackets, which is not Fortran."""

        changes = MOM5Component(ONE_DEGREE).config_changes(_decomposed("MOM5", ONE_DEGREE, (16, 15)))

        assert changes == {"ocean/input.nml": {"ocean_model_nml": {"layout": "16, 15"}}}

    def test_it_refuses_a_layout_that_states_no_decomposition(self):
        with pytest.raises(ValueError, match="'MOM5' states no decomposition"):
            MOM5Component(ONE_DEGREE).config_changes(_undecomposed("MOM5", 240))


class TestCICEComponent:
    """CICE, dividing its grid as its partitioning says, in whichever model couples it."""

    RUNTIME_CICE5 = CICEPartitioning(
        grid=(360, 300), writes_nprocs=True, grid_is_runtime=True, namelist_path="ice/cice_in.nml"
    )

    def test_it_is_cice5_as_a_payu_submodel_by_default(self, tmp_path):
        cice = CICEComponent(self.RUNTIME_CICE5)

        assert (cice.name, cice.token) == ("CICE5", "ice")
        (spec,) = cice.logs
        assert (spec.name, spec.component, spec.optional) == ("CICE5", "CICE5", False)
        assert isinstance(spec.parser, CICE5ProfilingParser)
        assert spec.locator(tmp_path) == tmp_path / "ice" / "ice_diag.d"

    def test_access_om3_names_its_realm_and_its_log_differently(self, tmp_path):
        cice6 = CICEComponent(
            CICEPartitioning(grid=(1440, 1152)), name="ice", log_name="CICE6", log_path="log/ice.log", optional=True
        )

        (spec,) = cice6.logs
        assert (spec.name, spec.component, spec.optional) == ("CICE6", "ice", True)
        assert spec.locator(tmp_path) == tmp_path / "log" / "ice.log"

    def test_what_the_search_chooses_follows_from_the_partitioning(self):
        cice = CICEComponent(self.RUNTIME_CICE5)

        assert cice.domain == self.RUNTIME_CICE5.domain == Domain((360,))
        assert cice.local_constraints == self.RUNTIME_CICE5.local_constraints

    def test_it_writes_what_the_partitioning_says_into_the_partitioning_s_namelist(self):
        layout = _decomposed("CICE5", Domain((360,)), (12,))

        changes = CICEComponent(self.RUNTIME_CICE5).config_changes(layout)

        assert changes == {
            "ice/cice_in.nml": {
                "domain_nml": {
                    "nprocs": "12",
                    "nx_global": "360",
                    "ny_global": "300",
                    "block_size_x": "30",
                    "block_size_y": "300",
                    "max_blocks": "1",
                }
            }
        }

    def test_it_writes_no_file_where_the_partitioning_writes_nothing(self):
        """CICE6 taking its rank count from its communicator, with a scheme forming no process grid."""

        opaque = CICEComponent(CICEPartitioning(grid=(360, 324), distribution_type="roundrobin"), name="ice")

        assert opaque.config_changes(_undecomposed("ice", 24)) == {}
