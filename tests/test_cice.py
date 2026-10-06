# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import dataclasses

import pytest
from access.config.parallel_constraints import DomainDivisibleByRanksConstraint, MinSubdomainSizeConstraint
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid

from access.profiling.models.cice import CICEPartitioning, CICEPartitioningMode

GRID = (360, 300)


def _decomposition(n_ranks: int, extent: int = GRID[0]) -> DomainDecompositionSpec:
    """A one-dimensional decomposition of an extent over the given ranks."""
    return DomainDecompositionSpec(Domain((extent,)), MPICartesianGrid((n_ranks,)))


class TestTheModeFollowsFromTheNamelist:
    """What a search may choose is decided by distribution_type and processor_shape, not by which CICE it is."""

    def test_cartesian_slenderx1_with_no_pinned_block_chooses_the_split(self):
        partitioning = CICEPartitioning(grid=GRID)

        assert partitioning.mode is CICEPartitioningMode.BLOCK_PER_RANK
        assert partitioning.domain == Domain((360,)), "the x extent in grid points"

    def test_rake_is_cartesian_for_this_purpose(self):
        """Both form a process grid by searching for a factorisation of the rank count."""

        assert CICEPartitioning(grid=GRID, distribution_type="rake").mode is CICEPartitioningMode.BLOCK_PER_RANK

    def test_a_pinned_block_size_leaves_only_the_block_columns(self):
        partitioning = CICEPartitioning(grid=GRID, block_shape=(30, 300))

        assert partitioning.mode is CICEPartitioningMode.BLOCK_COLUMNS
        assert partitioning.n_block_columns == 12
        assert partitioning.domain == Domain((12,)), "block columns, not grid points"

    def test_a_block_size_that_does_not_divide_the_extent_still_covers_it(self):
        """CICE rounds up: a last column narrower than the rest is padded rather than dropped."""

        assert CICEPartitioning(grid=GRID, block_shape=(50, 300)).n_block_columns == 8

    @pytest.mark.parametrize(
        ("distribution_type", "processor_shape"),
        [
            ("roundrobin", "square-ice"),
            ("sectrobin", "slenderX1"),
            ("sectcart", "slenderX1"),
            ("spacecurve", "slenderX1"),
            ("cartesian", "square-ice"),
            ("cartesian", "square-pop"),
            ("cartesian", "slenderX2"),
        ],
    )
    def test_everything_else_leaves_only_the_rank_count(self, distribution_type, processor_shape):
        """Either the distribution forms no process grid, or the shape is not one a 1-D domain can express."""

        partitioning = CICEPartitioning(grid=GRID, distribution_type=distribution_type, processor_shape=processor_shape)

        assert partitioning.mode is CICEPartitioningMode.OPAQUE
        assert partitioning.domain is None, "so the search chooses the rank count and nothing else"


class TestTheColumnsWhereNoBlockSizeIsPinned:
    """Where the control pins no block size there is nothing yet to divide the grid into, so a point is a column."""

    def test_a_search_that_chooses_the_split_counts_one_column_per_grid_point(self):
        """The domain it decomposes is the x extent itself, and the two have to be the same number."""

        partitioning = CICEPartitioning(grid=GRID)

        assert partitioning.mode is CICEPartitioningMode.BLOCK_PER_RANK
        assert partitioning.n_block_columns == 360, "the x extent, not the y one and not a single whole-grid block"
        assert partitioning.domain == Domain((partitioning.n_block_columns,))

    def test_an_opaque_sea_ice_counts_them_the_same_way(self):
        """Its scheme pins no block size either, so nothing has decided how wide a block is here either."""

        opaque = CICEPartitioning(grid=GRID, distribution_type="roundrobin", processor_shape="square-ice")

        assert opaque.mode is CICEPartitioningMode.OPAQUE
        assert opaque.n_block_columns == 360


class TestTheConstraintsFollowFromTheMode:
    """Derived rather than hand-written, because the wrong one raises mid-search or passes vacuously."""

    def test_a_chosen_split_must_tile_what_it_covers(self):
        """A rank left without a block aborts the run."""

        constraints = CICEPartitioning(grid=GRID).local_constraints

        assert any(isinstance(constraint, DomainDivisibleByRanksConstraint) for constraint in constraints)

    def test_a_block_may_not_be_thinner_than_its_ghost_cells(self):
        (minimum,) = [
            constraint
            for constraint in CICEPartitioning(grid=GRID, nghost=2).local_constraints
            if isinstance(constraint, MinSubdomainSizeConstraint)
        ]

        assert minimum.min_size == 2

    def test_block_columns_are_shared_out_whole_but_carry_no_size_floor(self):
        """The ghost width is a floor on a block, and the control already fixed how wide a block is."""

        constraints = CICEPartitioning(grid=GRID, block_shape=(30, 300)).local_constraints

        assert [type(constraint) for constraint in constraints] == [DomainDivisibleByRanksConstraint]

    def test_an_opaque_sea_ice_carries_none_at_all(self):
        """There is no decomposition for a constraint to read, so one that read it would raise mid-search."""

        assert CICEPartitioning(grid=GRID, distribution_type="roundrobin").local_constraints == ()


class TestWhatIsWritten:
    """Only what the layout decided, so that a stale entry never sits beside a fresh one."""

    def test_one_block_per_rank_states_its_width_and_that_there_is_one(self):
        partitioning = CICEPartitioning(grid=GRID, writes_nprocs=True, grid_is_runtime=True)

        assert partitioning.namelist_changes(12, _decomposition(12)) == {
            "nprocs": "12",
            "nx_global": "360",
            "ny_global": "300",
            "block_size_x": "30",
            "block_size_y": "300",
            "max_blocks": "1",
        }

    def test_a_compile_time_grid_is_not_written_back(self):
        """Nor is nprocs where CICE takes its rank count from the communicator it is given."""

        changes = CICEPartitioning(grid=GRID).namelist_changes(12, _decomposition(12))

        assert changes == {"block_size_x": "30", "block_size_y": "300", "max_blocks": "1"}

    def test_pinned_blocks_state_only_how_many_a_rank_holds(self):
        """The control fixed the block size, so writing one back would be writing over a tuned choice."""

        partitioning = CICEPartitioning(grid=GRID, block_shape=(30, 300))

        assert partitioning.namelist_changes(4, _decomposition(4, extent=12)) == {"max_blocks": "3"}

    def test_an_opaque_sea_ice_writes_nothing_but_its_rank_count(self):
        """And not even that, where the configuration does not state one."""

        opaque = CICEPartitioning(grid=GRID, distribution_type="roundrobin", processor_shape="square-ice")

        assert opaque.namelist_changes(275) == {}
        assert dataclasses.replace(opaque, writes_nprocs=True).namelist_changes(275) == {"nprocs": "275"}

    def test_the_distribution_and_the_shape_are_never_written(self):
        """A release's distribution is a tuned choice; overwriting it would change what is being measured."""

        changes = CICEPartitioning(grid=GRID, grid_is_runtime=True).namelist_changes(12, _decomposition(12))

        assert "distribution_type" not in changes
        assert "processor_shape" not in changes


class TestWhatIsRefused:
    """A layout the configured scheme cannot realise is refused rather than written out."""

    def test_more_ranks_than_there_are_blocks_to_hand_out(self):
        partitioning = CICEPartitioning(grid=GRID, block_shape=(90, 150))  # 4 x 2 blocks

        with pytest.raises(ValueError, match="fewer than the 9 ranks"):
            partitioning.check(9)
        partitioning.check(8)

    def test_a_mode_that_decomposes_a_domain_needs_the_decomposition(self):
        with pytest.raises(ValueError, match="no decomposition to write"):
            CICEPartitioning(grid=GRID).namelist_changes(12)

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"grid": (360,)}, "two positive extents"),
            ({"grid": (360, 0)}, "two positive extents"),
            ({"grid": GRID, "nghost": -1}, "must not be negative"),
            ({"grid": GRID, "block_shape": (30,)}, "two positive extents"),
            ({"grid": GRID, "block_shape": (400, 300)}, "does not fit inside the grid"),
        ],
    )
    def test_a_partitioning_that_describes_nothing(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            CICEPartitioning(**kwargs)

    @pytest.mark.parametrize("n_ranks", [0, -4])
    def test_a_rank_count_that_is_not_positive(self, n_ranks):
        """No scheme divides a grid among no ranks, and the grid has to be named or the report says nothing."""

        with pytest.raises(ValueError, match=rf"\(360, 300\) grid was given {n_ranks} ranks"):
            CICEPartitioning(grid=GRID).check(n_ranks)

    def test_a_rank_count_that_is_not_positive_never_reaches_the_namelist(self):
        """namelist_changes checks first - otherwise both of these would quietly write a layout nothing can run."""

        with pytest.raises(ValueError, match="was given 0 ranks"):
            CICEPartitioning(grid=GRID, writes_nprocs=True).namelist_changes(0, _decomposition(1))

        opaque = CICEPartitioning(grid=GRID, distribution_type="roundrobin", writes_nprocs=True)
        with pytest.raises(ValueError, match="was given -1 ranks"):
            opaque.namelist_changes(-1)
