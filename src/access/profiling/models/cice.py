# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""How a CICE sea ice divides its grid, and what a layout search may choose about it.

CICE is not handed a process grid. It is handed a *block size*, works out how many blocks cover the grid, and
then distributes those blocks over its ranks by whatever scheme `distribution_type` names. Only two of the
eight schemes - `cartesian` and `rake` - form a process grid at all, and they do it by searching for a
factorisation of the rank count that suits `processor_shape`; the other six never form a pair. So what a
layout search can choose about a CICE component depends on the two namelist strings the control configuration
sets, and the three modes below are what those come to.

This matters because the domain, the constraints and the namelist writer have to agree, and until now they
agreed only by hand: ACCESS-ESM1.6 hard-codes a one-dimensional domain on the grounds that the component is
CICE5, which is not the reason - it is one-dimensional because `slenderX1` with one block row is - and
ACCESS-OM3 turns a chosen two-dimensional process grid into block sizes for a release that distributes
`roundrobin`, where that grid means nothing. Deriving all three from the partitioning is what keeps them
together.

Both models' sea ice is described here because this is CICE's behaviour rather than either model's: ESM1.6
runs CICE5 and OM3 runs CICE6, and the question of what a search may choose is the same one.
"""

from dataclasses import dataclass
from enum import Enum

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
    """

    BLOCK_PER_RANK = "block_per_rank"
    BLOCK_COLUMNS = "block_columns"
    OPAQUE = "opaque"


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
            inside it, or if the ghost width is negative.
    """

    grid: tuple[int, int]
    distribution_type: str = "cartesian"
    processor_shape: str = "slenderX1"
    block_shape: tuple[int, int] | None = None
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

    @property
    def mode(self) -> CICEPartitioningMode:
        """Returns what a layout search may choose about this component.

        Returns:
            CICEPartitioningMode: The mode, as the distribution type, the processor shape and whether the
                control pins a block size decide it between them.
        """
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
                rank count and there is no decomposition for a constraint to read.
        """
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

    def check(self, n_ranks: int) -> None:
        """Rejects a rank count this component could not actually run with.

        Asserting rather than writing: a release's distribution_type and processor_shape are a tuned choice,
        and overwriting them would change the thing being measured. A layout the configured scheme cannot
        realise is refused instead.

        Args:
            n_ranks (int): The ranks the search gave this component.

        Raises:
            ValueError: If the rank count is not positive, or if it exceeds the blocks there are to hand out.
        """
        if n_ranks <= 0:
            raise ValueError(f"A CICE component on the {self.grid} grid was given {n_ranks} ranks.")
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
