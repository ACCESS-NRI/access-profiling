# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import dataclasses

import pytest
from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import DomainDivisibleByRanksConstraint, MinSubdomainSizeConstraint
from access.config.parallel_domain import Domain, DomainDecompositionSpec
from access.config.parallel_mpi_grid import MPICartesianGrid

from access.profiling.models.cice import (
    CICEBlocks,
    CICEBlocksPerRankConstraint,
    CICEPartitioning,
    CICEPartitioningMode,
    _cice_process_grid,
)

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


def _ice_layout(n_ranks: int) -> ComponentLayout:
    """A sea ice given the ranks, with no decomposition, as a search choosing only its rank count gives it."""
    return ComponentLayout(name="ice", n_cores=n_ranks, n_ranks=n_ranks, threads_per_rank=1, decomposition=None)


class TestCICEsOwnProcessGrid:
    """What proc_decomposition in CICE's ice_distribution.F90 forms, since cartesian blocks are split over it."""

    @pytest.mark.parametrize(
        ("n_ranks", "n_blocks", "processor_shape", "expected"),
        [
            (4, (8, 3), "slenderX1", (4, 1)),
            # The guess is 4 x 1, whose 4 ranks along x do not divide 3 block columns. Turned round, 1 x 4 does
            # divide 3 x 8 blocks, so that is the grid CICE takes.
            (4, (3, 8), "slenderX1", (1, 4)),
            # Neither 4 x 1 nor 1 x 4 divides 3 x 3 blocks, and there is no other guess to step down to, so CICE
            # settles for the first factorisation it met.
            (4, (3, 3), "slenderX1", (4, 1)),
            (6, (6, 4), "slenderX2", (3, 2)),
            (12, (12, 12), "square-ice", (4, 3)),  # the guess is along y for square-ice ...
            (12, (12, 12), "square-pop", (3, 4)),  # ... and along x for square-pop
            (12, (8, 9), "square-ice", (4, 3)),  # stepping y down from 3 to 2 finds nothing better
            (12, (6, 12), "square-ice", (3, 4)),  # 4 x 3 does not divide 6 x 12, but 3 x 4 does
            (12, (6, 10), "square-ice", (6, 2)),  # neither way round does, so y steps down to 2
            (1, (1, 1), "slenderX2", (1, 1)),
        ],
    )
    def test_the_process_grid_cice_forms(self, n_ranks, n_blocks, processor_shape, expected):
        n_blocks_x, n_blocks_y = n_blocks
        assert _cice_process_grid(n_ranks, n_blocks_x, n_blocks_y, processor_shape) == expected


class TestBlocksChosenForTheRankCount:
    """Several blocks per rank, whatever the rank count, by choosing the block size from it."""

    @pytest.mark.parametrize(
        ("distribution_type", "processor_shape"),
        [
            ("cartesian", "slenderX1"),
            ("rake", "slenderX2"),
            ("roundrobin", "square-ice"),
            ("sectrobin", "square-ice"),
            ("spacecurve", ""),
        ],
    )
    def test_asking_for_blocks_per_rank_leaves_only_the_rank_count(self, distribution_type, processor_shape):
        partitioning = CICEPartitioning(
            grid=GRID,
            distribution_type=distribution_type,
            processor_shape=processor_shape,
            blocks_per_rank_range=(5, 8),
        )

        assert partitioning.mode is CICEPartitioningMode.CHOSEN_BLOCKS
        assert partitioning.domain is None
        assert partitioning.local_constraints == (CICEBlocksPerRankConstraint(partitioning=partitioning),)

    @pytest.mark.parametrize("distribution_type", ["roundrobin", "sectrobin"])
    def test_a_distribution_forming_no_process_grid_goes_by_the_average(self, distribution_type):
        """1620 blocks over 275 ranks is 5.9 each on average, the ranks handed one more than the rest holding 6."""

        partitioning = CICEPartitioning(
            grid=(1440, 1152), distribution_type=distribution_type, blocks_per_rank_range=(5, 8)
        )

        assert partitioning.blocks(275) == CICEBlocks(block_shape=(32, 32), block_counts=(45, 36), blocks_per_rank=6)

    def test_a_grid_no_block_size_tiles_is_padded_as_little_as_can_be(self):
        """Two ranks holding 5 to 8 blocks each need 10 to 16 blocks between them.

        Both 101 and 7 are prime, so the block sizes that tile the 101 x 7 grid exactly give 1, 7, 101 or 707
        blocks and nothing in between. Some block column has to be padded, and the least padding is two columns
        51 points wide, the second of them one point short.
        """

        partitioning = CICEPartitioning(grid=(101, 7), distribution_type="roundrobin", blocks_per_rank_range=(5, 8))

        assert partitioning.blocks(2) == CICEBlocks(block_shape=(51, 1), block_counts=(2, 7), blocks_per_rank=7)

    @pytest.mark.parametrize("processor_shape", ["slenderX1", "slenderX2", "square-ice", "square-pop"])
    @pytest.mark.parametrize("n_ranks", [1, 6, 12, 24, 60])
    def test_cartesian_blocks_split_evenly_over_the_grid_cice_forms(self, processor_shape, n_ranks):
        partitioning = CICEPartitioning(grid=GRID, processor_shape=processor_shape, blocks_per_rank_range=(5, 8))

        blocks = partitioning.blocks(n_ranks)
        assert blocks is not None
        ranks_x, ranks_y = _cice_process_grid(n_ranks, blocks.block_counts[0], blocks.block_counts[1], processor_shape)
        assert ranks_x * ranks_y == n_ranks
        assert blocks.block_counts[0] % ranks_x == 0
        assert blocks.block_counts[1] % ranks_y == 0
        assert blocks.blocks_per_rank == (blocks.block_counts[0] // ranks_x) * (blocks.block_counts[1] // ranks_y)
        assert 5 <= blocks.blocks_per_rank <= 8
        for extent, size, count in zip(GRID, blocks.block_shape, blocks.block_counts, strict=True):
            assert -(-extent // size) == count, "the counts are what CICE makes of the size"

    def test_blocks_are_never_thinner_than_their_ghost_cells(self):
        partitioning = CICEPartitioning(
            grid=(12, 12), distribution_type="roundrobin", blocks_per_rank_range=(1, 144), nghost=4
        )

        blocks = partitioning.blocks(1)
        assert blocks is not None
        assert min(blocks.block_shape) >= 4

    def test_a_rank_count_no_block_size_suits(self):
        """With one processor row, 29 ranks need 29 block columns or 29 block rows, which this grid never has."""

        partitioning = CICEPartitioning(grid=GRID, blocks_per_rank_range=(5, 8))

        assert partitioning.blocks(29) is None
        assert not CICEBlocksPerRankConstraint(partitioning=partitioning).is_satisfied(_ice_layout(29))
        assert CICEBlocksPerRankConstraint(partitioning=partitioning).is_satisfied(_ice_layout(12))
        with pytest.raises(ValueError, match=r"No block size gives the 29 ranks .* \(5, 8\) blocks each"):
            partitioning.check(29)
        partitioning.check(12)

    def test_more_ranks_than_could_each_hold_enough_blocks(self):
        roundrobin = CICEPartitioning(grid=GRID, distribution_type="roundrobin", blocks_per_rank_range=(5, 8))

        assert roundrobin.blocks(360 * 300 // 5) is not None, "every block one point wide"
        assert roundrobin.blocks(360 * 300 // 5 + 1) is None

    def test_nothing_is_chosen_where_nothing_was_asked_for(self):
        assert CICEPartitioning(grid=GRID).blocks(12) is None
        assert CICEPartitioning(grid=GRID, blocks_per_rank_range=(5, 8)).blocks(0) is None

    def test_the_chosen_blocks_are_written_and_max_blocks_left_to_cice6(self):
        partitioning = CICEPartitioning(grid=GRID, blocks_per_rank_range=(5, 8))

        assert partitioning.namelist_changes(12) == {"block_size_x": "30", "block_size_y": "50", "max_blocks": "-1"}

    def test_the_rank_count_and_the_grid_are_written_with_them_where_they_are_stated(self):
        partitioning = CICEPartitioning(
            grid=GRID,
            distribution_type="roundrobin",
            blocks_per_rank_range=(5, 8),
            writes_nprocs=True,
            grid_is_runtime=True,
        )

        changes = partitioning.namelist_changes(12)
        assert {key: changes[key] for key in ("nprocs", "nx_global", "ny_global")} == {
            "nprocs": "12",
            "nx_global": "360",
            "ny_global": "300",
        }
        assert "distribution_type" not in changes
        assert "processor_shape" not in changes

    def test_a_rank_count_no_block_size_suits_is_never_written(self):
        with pytest.raises(ValueError, match="No block size gives the 29 ranks"):
            CICEPartitioning(grid=GRID, blocks_per_rank_range=(5, 8)).namelist_changes(29)

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"blocks_per_rank_range": (0, 8)}, "both positive and in that order"),
            ({"blocks_per_rank_range": (8, 5)}, "both positive and in that order"),
            ({"blocks_per_rank_range": (5,)}, "both positive and in that order"),
            ({"blocks_per_rank_range": (5, 8), "block_shape": (30, 300)}, "cannot be chosen as well"),
        ],
    )
    def test_blocks_per_rank_that_describe_nothing(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            CICEPartitioning(grid=GRID, **kwargs)
