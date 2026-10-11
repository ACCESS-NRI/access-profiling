# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""How a CICE sea ice divides its grid, and what a layout search may choose about it.

CICE is not handed a process grid. It is handed a *block size*, works out how many blocks cover the grid, and
then distributes those blocks over its ranks by whatever scheme `distribution_type` names. Only two of the
eight schemes - `cartesian` and `rake` - form a process grid at all, and they do it by searching for a
factorisation of the rank count that suits `processor_shape`; the other six never form a pair. So what a
layout search can choose about a CICE component depends on the two namelist strings the control configuration
sets, and the modes below are what those come to.

Except where the configuration asks for a number of blocks per rank instead. Several blocks per rank is how
CICE6 is meant to be run - small blocks balance the work and keep the cache warm - and the block size that
gives a rank count so many of them depends on that count. So a partitioning given `blocks_per_rank` chooses
the block size itself, from the rank count the search chose and whatever scheme the control configures, and
the search is left to choose that rank count alone.

This matters because the domain, the constraints and the namelist writer have to agree, and until now they
agreed only by hand: ACCESS-ESM1.6 hard-codes a one-dimensional domain on the grounds that the component is
CICE5, which is not the reason - it is one-dimensional because `slenderX1` with one block row is - and
ACCESS-OM3 turns a chosen two-dimensional process grid into block sizes for a release that distributes
`roundrobin`, where that grid means nothing. Deriving all three from the partitioning is what keeps them
together.

Both models' sea ice is described here because this is CICE's behaviour rather than either model's: ESM1.6
runs CICE5 and OM3 runs CICE6, and the question of what a search may choose is the same one.
"""

import math
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
from functools import cache

from access.config.parallel_component import ComponentLayout
from access.config.parallel_constraints import (
    DomainDivisibleByRanksConstraint,
    LocalConstraint,
    MinSubdomainSizeConstraint,
)
from access.config.parallel_domain import Domain, DomainDecompositionSpec

# The distribution types that form a process grid, by searching for a factorisation of the rank count that
# suits processor_shape. Every other scheme - roundrobin, sectrobin, sectcart, spacecurve, blkrobin, blkcart -
# maps blocks to ranks without forming one, so no choice of process grid is available to a search.
_CARTESIAN_DISTRIBUTIONS: frozenset[str] = frozenset({"cartesian", "rake"})

# The processor shape that puts every block in one row, so that the blocks tile the x extent alone and the
# decomposition is one-dimensional. The others - slenderX2, square-ice, square-pop - give two rows or an
# aspect-ratio search, neither of which a one-dimensional domain can express.
_SINGLE_ROW_SHAPE: str = "slenderX1"


class CICEPartitioningMode(Enum):
    """What a layout search may choose about a CICE component's decomposition.

    Attributes:
        BLOCK_PER_RANK: The search chooses how the x extent is split, each rank receiving one block spanning
            the full y extent. The block size follows from the rank count, so it is written out.
        BLOCK_COLUMNS: The control pins the block size, and the search chooses how the resulting block columns
            are shared out. The block size is not written; how many blocks a rank may hold is.
        OPAQUE: The search chooses the rank count and nothing else. Either the distribution forms no process
            grid, or the processor shape is not one a one-dimensional domain can express - and either way
            there is no decomposition here to choose or to write.
        CHOSEN_BLOCKS: The search chooses the rank count and nothing else, and the block size follows from it,
            so that each rank holds as many blocks as the partitioning asks for. Whatever the distribution:
            it is the block size that is chosen here, not the process grid, which CICE still forms itself.
    """

    BLOCK_PER_RANK = "block_per_rank"
    BLOCK_COLUMNS = "block_columns"
    OPAQUE = "opaque"
    CHOSEN_BLOCKS = "chosen_blocks"


@dataclass(frozen=True)
class CICEBlocks:
    """The blocks a CICE component is divided into for a given rank count.

    Args:
        block_shape (tuple[int, int]): The block size, as block_size_x and block_size_y.
        block_counts (tuple[int, int]): How many blocks cover the grid along x and along y, the last of them
            padded where the size does not divide the extent.
        blocks_per_rank (int): The most blocks any rank holds, before CICE drops the blocks that are all land.
    """

    block_shape: tuple[int, int]
    block_counts: tuple[int, int]
    blocks_per_rank: int


@dataclass(frozen=True)
class CICEPartitioning:
    """How one CICE component divides its grid among its ranks.

    Everything a search and a writer need follows from this: which mode applies, what domain the component
    carries into the tree, which constraints every layout of it must satisfy, and what has to be written into
    the namelist to realise a layout. None of it is stated twice.

    Args:
        grid (tuple[int, int]): The global grid, as nx_global and ny_global.
        distribution_type (str): The `distribution_type` of the control's domain_nml. Defaults to "cartesian".
        processor_shape (str): The `processor_shape` of the control's domain_nml. Defaults to "slenderX1".
            Independent of distribution_type, and read only by the schemes that form a process grid, which is
            why the two are kept apart rather than collapsed into one enum of scheme names: square-ice with
            roundrobin, which is what the ACCESS-OM3 25 km release runs, would otherwise be inexpressible.
        block_shape (tuple[int, int] | None): The block size the control pins, if it pins one. None (the
            default) leaves the block size to follow from the rank count.
        blocks_per_rank (tuple[int, int] | None): The fewest and the most blocks each rank should hold, both
            inclusive, counting land blocks too. Given, the block size is chosen from the rank count to fall
            in this range, whatever the distribution, and the rank counts no block size can give so many
            blocks are refused. None (the default) leaves the blocks per rank to the other modes. Meant for
            CICE6, which works out max_blocks itself: that is what lets it be written as -1.
        nghost (int): The ghost cells around each block, which is the floor on how thin a block may get.
            Defaults to 1, which is what both CICE5 and CICE6 use in ACCESS.
        writes_nprocs (bool): Whether `nprocs` is written into the namelist alongside the rest. Defaults to
            False, which leaves CICE to take its rank count from the communicator it is given.
        grid_is_runtime (bool): Whether the grid extents are namelist entries rather than compile-time
            parameters, and so have to be written out with the rest. Defaults to False.
        namelist_path (str): Path of the namelist file, relative to the control directory. Defaults to
            "ice_in", which is CICE6's name for it.
        namelist_group (str): Namelist group the entries belong to. Defaults to "domain_nml".

    Raises:
        ValueError: If the grid is not two-dimensional and positive, if a pinned block shape does not fit
            inside it, if the ghost width is negative, or if a range of blocks per rank is not one or comes with
            a pinned block size.
    """

    grid: tuple[int, int]
    distribution_type: str = "cartesian"
    processor_shape: str = "slenderX1"
    block_shape: tuple[int, int] | None = None
    blocks_per_rank: tuple[int, int] | None = None
    nghost: int = 1
    writes_nprocs: bool = False
    grid_is_runtime: bool = False
    namelist_path: str = "ice_in"
    namelist_group: str = "domain_nml"

    def __post_init__(self) -> None:
        if len(self.grid) != 2 or any(extent <= 0 for extent in self.grid):
            raise ValueError(
                f"CICEPartitioning.grid must be two positive extents, nx_global and ny_global. Got {self.grid}."
            )
        if self.nghost < 0:
            raise ValueError(f"CICEPartitioning.nghost must not be negative. Got {self.nghost}.")
        if self.block_shape is not None:
            if len(self.block_shape) != 2 or any(extent <= 0 for extent in self.block_shape):
                raise ValueError(f"CICEPartitioning.block_shape must be two positive extents. Got {self.block_shape}.")
            if any(block > extent for block, extent in zip(self.block_shape, self.grid, strict=True)):
                raise ValueError(
                    f"CICEPartitioning.block_shape {self.block_shape} does not fit inside the grid {self.grid}."
                )
        if self.blocks_per_rank is not None:
            if len(self.blocks_per_rank) != 2 or not 0 < self.blocks_per_rank[0] <= self.blocks_per_rank[1]:
                raise ValueError(
                    "CICEPartitioning.blocks_per_rank must be the fewest and the most blocks per rank, both "
                    f"positive and in that order. Got {self.blocks_per_rank}."
                )
            if self.block_shape is not None:
                raise ValueError(
                    f"CICEPartitioning pins the block size {self.block_shape} and asks for "
                    f"{self.blocks_per_rank} blocks per rank. A pinned size leaves how many blocks a rank holds "
                    "to the rank count, so the blocks cannot be chosen as well."
                )

    @property
    def mode(self) -> CICEPartitioningMode:
        """Returns what a layout search may choose about this component.

        Returns:
            CICEPartitioningMode: The mode, as the distribution type, the processor shape, whether the
                control pins a block size and whether blocks per rank are asked for decide it between them.
        """
        if self.blocks_per_rank is not None:
            return CICEPartitioningMode.CHOSEN_BLOCKS
        if self.distribution_type not in _CARTESIAN_DISTRIBUTIONS or self.processor_shape != _SINGLE_ROW_SHAPE:
            return CICEPartitioningMode.OPAQUE
        return CICEPartitioningMode.BLOCK_PER_RANK if self.block_shape is None else CICEPartitioningMode.BLOCK_COLUMNS

    @property
    def n_block_columns(self) -> int:
        """Returns how many block columns cover the grid, where the control pins a block size.

        CICE rounds up: a last column narrower than the rest is padded rather than dropped.

        Returns:
            int: The number of block columns, or the x extent itself where no block size is pinned, each
                point then being its own column until the search decides otherwise.
        """
        if self.block_shape is None:
            return self.grid[0]
        return -(-self.grid[0] // self.block_shape[0])

    @property
    def domain(self) -> Domain | None:
        """Returns the domain this component carries into the component tree.

        What the search decomposes is what it may actually choose, which is the x extent in grid points where
        each rank receives one block of it, the block columns where the control fixed their width, and
        nothing at all where the distribution forms no process grid.

        Returns:
            Domain | None: The domain, or None where the search chooses only a rank count.
        """
        match self.mode:
            case CICEPartitioningMode.BLOCK_PER_RANK:
                return Domain(shape=(self.grid[0],))
            case CICEPartitioningMode.BLOCK_COLUMNS:
                return Domain(shape=(self.n_block_columns,))
            case _:
                return None

    @property
    def local_constraints(self) -> tuple[LocalConstraint, ...]:
        """Returns the requirements every layout of this component must satisfy.

        Derived from the mode rather than written out per model, because a constraint that reads a dimension
        the domain does not have raises mid-search instead of rejecting a candidate, and one that reads a
        one-dimensional domain where it expected two is silently vacuous. Both are mistakes a hand-written
        list makes and this does not.

        Returns:
            tuple[LocalConstraint, ...]: The constraints, which is empty where the search chooses only a
                rank count and there is no decomposition for a constraint to read - unless the block size
                follows from that rank count, when the one requirement is that some block size does.
        """
        if self.mode is CICEPartitioningMode.CHOSEN_BLOCKS:
            return (CICEBlocksPerRankConstraint(partitioning=self),)
        if self.mode is CICEPartitioningMode.OPAQUE:
            return ()
        # The blocks have to tile what they cover exactly: a rank left without one aborts the run, and a
        # partial column under a cartesian distribution is not a column the scheme knows how to place. The
        # admissible rank counts therefore thin out as the total grows, so a strategy giving the sea ice a
        # narrow band of cores will often find no layout at all.
        constraints: tuple[LocalConstraint, ...] = (DomainDivisibleByRanksConstraint(),)
        if self.mode is CICEPartitioningMode.BLOCK_PER_RANK:
            # In grid points, so the floor on a block's width is the ghost width itself.
            constraints += (MinSubdomainSizeConstraint(min_size=self.nghost),)
        return constraints

    def blocks(self, n_ranks: int) -> CICEBlocks | None:
        """Returns the blocks this component is divided into for a given rank count, where it chooses them.

        Every block size covering the grid is a candidate, each count of blocks along an axis taken at the
        smallest size that gives it. The ones giving each rank as many blocks as blocks_per_rank asks for are
        kept: under cartesian and rake, on the process grid CICE itself would form for them, and only where
        their blocks split evenly over it, since otherwise some rank holds fewer than its share or none at all;
        under every other scheme, which hands blocks out one at a time, by the average. Of those, the one
        padding the grid least is chosen, then the squarest, then the one nearest the middle of the range.

        Args:
            n_ranks (int): The ranks the search gave this component.

        Returns:
            CICEBlocks | None: The blocks, or None where no block size gives that many ranks so many blocks
                each, or where this partitioning does not choose its blocks at all.
        """
        if self.blocks_per_rank is None or n_ranks <= 0:
            return None
        return _choose_blocks(self, n_ranks)

    def check(self, n_ranks: int) -> None:
        """Rejects a rank count this component could not actually run with.

        Asserting rather than writing: a release's distribution_type and processor_shape are a tuned choice,
        and overwriting them would change the thing being measured. A layout the configured scheme cannot
        realise is refused instead.

        Args:
            n_ranks (int): The ranks the search gave this component.

        Raises:
            ValueError: If the rank count is not positive, if it exceeds the blocks there are to hand out, or
                if no block size gives it the blocks per rank asked for.
        """
        if n_ranks <= 0:
            raise ValueError(f"A CICE component on the {self.grid} grid was given {n_ranks} ranks.")
        if self.mode is CICEPartitioningMode.CHOSEN_BLOCKS:
            if self.blocks(n_ranks) is None:
                raise ValueError(
                    f"No block size gives the {n_ranks} ranks of a CICE component on the {self.grid} grid "
                    f"{self.blocks_per_rank} blocks each under a {self.distribution_type} distribution with "
                    f"processor shape {self.processor_shape}."
                )
            return
        if self.block_shape is None:
            return
        n_blocks = self.n_block_columns * -(-self.grid[1] // self.block_shape[1])
        if n_ranks > n_blocks:
            raise ValueError(
                f"A CICE component on the {self.grid} grid with {self.block_shape} blocks has {n_blocks} of "
                f"them, which is fewer than the {n_ranks} ranks it was given. A rank with no block to work "
                "on aborts the run, so this layout cannot be realised with the block size the control pins."
            )

    def namelist_changes(self, n_ranks: int, decomposition: DomainDecompositionSpec | None = None) -> dict[str, str]:
        """Returns the namelist entries realising a given layout of this component.

        What is written is what the layout decided and nothing else. The grid extents go in only where they
        are namelist entries at all, the distribution and the processor shape never do, and a mode that
        decides no block size does not write one: a stale entry beside a fresh one is how a decomposition and
        the configuration realising it come apart.

        Args:
            n_ranks (int): The ranks the search gave this component.
            decomposition (DomainDecompositionSpec | None): The decomposition the search chose, where there
                was one to choose. None under OPAQUE, where there was not.

        Returns:
            dict[str, str]: The entries, as the strings a Fortran namelist is written from.

        Raises:
            ValueError: If the layout cannot be realised with the scheme the control configures, or if a mode
                that decomposes a domain was given no decomposition.
        """
        self.check(n_ranks)

        changes: dict[str, str] = {}
        if self.writes_nprocs:
            changes["nprocs"] = str(n_ranks)
        if self.grid_is_runtime:
            changes["nx_global"] = str(self.grid[0])
            changes["ny_global"] = str(self.grid[1])

        if self.mode is CICEPartitioningMode.CHOSEN_BLOCKS:
            # The block size follows from the rank count. max_blocks is left for CICE6 to work out from the
            # distribution it forms, which counts the land blocks it drops and whatever rake moves about.
            blocks = self.blocks(n_ranks)
            assert blocks is not None, "check() refuses a rank count no block size suits"
            changes["block_size_x"] = str(blocks.block_shape[0])
            changes["block_size_y"] = str(blocks.block_shape[1])
            changes["max_blocks"] = "-1"
            return changes

        if self.mode is CICEPartitioningMode.OPAQUE:
            return changes

        if decomposition is None:
            raise ValueError(
                f"A CICE component in {self.mode.value} mode was given no decomposition to write. Its domain "
                "reaches the component tree, so the search chooses one; this is a layout of something else."
            )

        if self.mode is CICEPartitioningMode.BLOCK_PER_RANK:
            # One block per rank, spanning the full y extent. max_blocks says as much, and the width follows
            # from how the search split the x extent.
            changes["block_size_x"] = str(decomposition.domain.shape[0] // decomposition.grid[0])
            changes["block_size_y"] = str(self.grid[1])
            changes["max_blocks"] = "1"
        else:
            # The control pinned the block size, so only how many of them a rank holds is left to say.
            changes["max_blocks"] = str(self.n_block_columns // decomposition.grid[0])
        return changes


@dataclass(frozen=True)
class CICEBlocksPerRankConstraint(LocalConstraint):
    """A CICE component's rank count must be one some block size gives the blocks per rank asked for.

    Reads the rank count alone, so it applies to a component that carries no domain - which is what one
    choosing its blocks is.

    Args:
        partitioning (CICEPartitioning): How the component divides its grid. Must ask for blocks per rank.
    """

    partitioning: CICEPartitioning

    def is_satisfied(self, layout: ComponentLayout) -> bool:
        return self.partitioning.blocks(layout.n_ranks) is not None


def _block_sizes(extent: int, min_size: int) -> dict[int, int]:
    """Returns every count of blocks that can cover an extent, with the smallest block size giving each.

    Args:
        extent (int): The extent to cover.
        min_size (int): The smallest a block may be.

    Returns:
        dict[int, int]: The block size, keyed by how many blocks of it cover the extent, CICE rounding up.
    """
    sizes = {}
    for count in range(1, extent + 1):
        size = -(-extent // count)
        if size >= min_size and -(-extent // size) == count:
            sizes[count] = size
    return sizes


def _cice_guesses(n_ranks: int, processor_shape: str) -> Iterator[tuple[int, int]]:
    """Yields the ranks along x and along y that proc_decomposition in ice_distribution.F90 tries, in order.

    It starts from a guess the processor shape names - along x for square-pop, along y for every other shape -
    and steps that dimension down to one, taking the other as the rank count divided by it. Not every guess
    factorises the rank count.

    Args:
        n_ranks (int): The ranks to arrange. Must be positive.
        processor_shape (str): The `processor_shape` of the control's domain_nml.

    Yields:
        tuple[int, int]: Each guess, as ranks along x and along y.
    """
    # CICE takes the square root in single precision and rounds half up, as Fortran's nint does. The precision
    # makes a difference only for rank counts in the millions.
    root = math.floor(math.sqrt(n_ranks) + 0.5)
    if processor_shape == "square-pop":
        for ranks_x in range(root, 0, -1):
            yield ranks_x, n_ranks // ranks_x
        return
    if processor_shape == "square-ice":
        start = root
    elif processor_shape == "slenderX1":
        start = 1
    else:
        start = min(2, n_ranks)
    for ranks_y in range(start, 0, -1):
        yield n_ranks // ranks_y, ranks_y


def _cice_process_grid(n_ranks: int, n_blocks_x: int, n_blocks_y: int, processor_shape: str) -> tuple[int, int]:
    """Returns the process grid CICE forms under cartesian and rake, as proc_decomposition does.

    Of the guesses that factorise the rank count, the first one the block counts divide evenly is taken, either
    way round. Failing that, CICE settles for the first factorisation it met, which leaves some ranks short.

    Args:
        n_ranks (int): The ranks to arrange. Must be positive.
        n_blocks_x (int): The blocks along x.
        n_blocks_y (int): The blocks along y.
        processor_shape (str): The `processor_shape` of the control's domain_nml.

    Returns:
        tuple[int, int]: The ranks along x and along y.
    """
    fallback: tuple[int, int] | None = None
    for ranks_x, ranks_y in _cice_guesses(n_ranks, processor_shape):
        if ranks_x * ranks_y != n_ranks:
            continue
        if n_blocks_x % ranks_x == 0 and n_blocks_y % ranks_y == 0:
            return ranks_x, ranks_y
        if n_blocks_x % ranks_y == 0 and n_blocks_y % ranks_x == 0:
            return ranks_y, ranks_x
        if fallback is None:
            fallback = (ranks_x, ranks_y)
    # The last guess is one along the dimension stepped down, which always factorises.
    assert fallback is not None
    return fallback


@cache
def _choose_blocks(partitioning: CICEPartitioning, n_ranks: int) -> CICEBlocks | None:
    """Returns the blocks CICEPartitioning.blocks describes, memoised: a search asks once per candidate."""

    assert partitioning.blocks_per_rank is not None
    fewest, most = partitioning.blocks_per_rank
    nx, ny = partitioning.grid
    min_size = max(1, partitioning.nghost)
    forms_grid = partitioning.distribution_type in _CARTESIAN_DISTRIBUTIONS

    best: tuple[tuple[int, float, float, int], CICEBlocks] | None = None
    x_sizes = _block_sizes(nx, min_size)
    y_sizes = _block_sizes(ny, min_size)
    for n_blocks_x, size_x in x_sizes.items():
        for n_blocks_y, size_y in y_sizes.items():
            n_blocks = n_blocks_x * n_blocks_y
            if forms_grid:
                ranks_x, ranks_y = _cice_process_grid(n_ranks, n_blocks_x, n_blocks_y, partitioning.processor_shape)
                if n_blocks_x % ranks_x or n_blocks_y % ranks_y:
                    continue
                per_rank = (n_blocks_x // ranks_x) * (n_blocks_y // ranks_y)
                if not fewest <= per_rank <= most:
                    continue
            else:
                if not fewest * n_ranks <= n_blocks <= most * n_ranks:
                    continue
                per_rank = -(-n_blocks // n_ranks)
            key = (
                n_blocks_x * size_x * n_blocks_y * size_y - nx * ny,
                abs(math.log(size_x / size_y)),
                abs(n_blocks / n_ranks - (fewest + most) / 2),
                -size_x,
            )
            if best is None or key < best[0]:
                best = (key, CICEBlocks((size_x, size_y), (n_blocks_x, n_blocks_y), per_rank))
    return None if best is None else best[1]
