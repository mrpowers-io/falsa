import random
from typing import Iterator

import pyarrow as pa

from falsa import H2ODatasetSizes

from .native import (
    generate_groupby,
    generate_join_dataset_big,
    generate_join_dataset_medium,
    generate_join_dataset_small,
)

NATIVE_I64_MAX_VALUE = 9_223_372_036_854_775_806


def _validate_int64(num: int, prefix: str) -> None:
    # We are passing values from Python as i64 and converted to u64/usize inside;
    # Better to catch it in Python instead of facing panic in native-part.
    if num < 0:
        raise ValueError(f"Negative values are not supported but got {prefix}={num}")
    if num > NATIVE_I64_MAX_VALUE:
        raise ValueError(f"Values are passed to native as int64; MAX={NATIVE_I64_MAX_VALUE} but got {prefix}={num}")


class GroupByGenerator:
    """A simple wrapper on top of native generator.

    The class takes care of random seeds generation, input validation
    and calculation of the size of all batches.
    """

    def __init__(
            self, size: H2ODatasetSizes | int, k: int, nas: int = 0, seed: int = 42, batch_size: int = 5_000_000
    ) -> None:
        _validate_int64(size, "size")
        if (nas < 0) or (nas > 100):
            raise ValueError(f"nas should be in [0, 100], but got {nas}")
        if (k < 0) or (k > size):
            raise ValueError(f"k should be positive and less than {size} but got {k}")
        if (batch_size <= 0) or (batch_size > size):
            raise ValueError(f"batch size should be positive and less than {size} but got {batch_size}")
        self.n: int = size
        self.k = k
        self.nas = nas

        num_batches = self.n // batch_size
        batches = [batch_size for _ in range(num_batches)]
        # A corner case when we need to add one more batch
        if self.n % batch_size != 0:
            batches.append(self.n % batch_size)

        # Generate a random seed per batch
        random.seed(seed)
        self.batches = [{"size": bs, "seed": random.randint(0, NATIVE_I64_MAX_VALUE)} for bs in batches]

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        for batch in self.batches:
            yield generate_groupby(self.n, self.k, self.nas, batch["seed"], batch["size"])


class JoinSmallGenerator:
    """A simple wrapper on top of native generator.

    The class takes care of random seeds generation, input validation
    and calculation of the size of all batches.
    """

    def __init__(
            self, size: H2ODatasetSizes | int, n_rows: int, k: int, seed: int = 42, keys_seed: int = 142, batch_size: int = 5_000_000
    ) -> None:
        _validate_int64(size, "size")
        if (k < 0) or (k > size):
            raise ValueError(f"k should be positive and less than {size} but got {k}")
        if (batch_size <= 0) or (batch_size > size):
            raise ValueError(f"batch size should be positive and less than {size} but got {batch_size}")
        self.n: int = size
        self.n_rows = n_rows
        self.k = k
        self.keys_seed = keys_seed

        num_batches = self.n_rows // batch_size
        batches = [batch_size for _ in range(num_batches)]
        # A corner case when we need to add one more batch
        if self.n_rows % batch_size != 0:
            batches.append(self.n_rows % batch_size)

        # Generate a random seed per batch
        random.seed(seed)
        self.batches = [{"size": bs, "seed": random.randint(0, NATIVE_I64_MAX_VALUE)} for bs in batches]

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        for batch in self.batches:
            yield generate_join_dataset_small(self.n, batch["seed"], self.keys_seed, batch["size"])


class JoinMediumGenerator:
    """A simple wrapper on top of native generator.

    The class takes care of random seeds generation, input validation
    and calculation of the size of all batches.
    """

    def __init__(
            self, size: H2ODatasetSizes | int, n_rows: int, k: int, seed: int = 42, keys_seed: int = 142, batch_size: int = 5_000_000
    ) -> None:
        _validate_int64(size, "size")
        if (k < 0) or (k > size):
            raise ValueError(f"k should be positive and less than {size} but got {k}")
        if (batch_size <= 0) or (batch_size > size):
            raise ValueError(f"batch size should be positive and less than {size} but got {batch_size}")
        self.n: int = size
        self.n_rows = n_rows
        self.k = k
        self.keys_seed = keys_seed

        num_batches = self.n_rows // batch_size
        batches = [batch_size for _ in range(num_batches)]
        # a corner case when we need to add one more batch
        if self.n_rows % batch_size != 0:
            batches.append(self.n_rows % batch_size)

        # Generate a random seed per batch
        random.seed(seed)
        self.batches = [{"size": bs, "seed": random.randint(0, NATIVE_I64_MAX_VALUE)} for bs in batches]

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        for batch in self.batches:
            yield generate_join_dataset_medium(self.n, batch["seed"], self.keys_seed, batch["size"])


class JoinBigGenerator:
    """A simple wrapper on top of native generator.

    The class takes care of random seeds generation, input validation
    and calculation of the size of all batches.
    """

    def __init__(
            self, size: H2ODatasetSizes | int, n_rows: int, k: int, nas: int, seed: int = 42, keys_seed: int = 142, batch_size: int = 5_000_000
    ) -> None:
        _validate_int64(size, "size")
        if (k < 0) or (k > size):
            raise ValueError(f"k should be positive and less than {size} but got {k}")
        if (batch_size <= 0) or (batch_size > size):
            raise ValueError(f"batch size should be positive and less than {size} but got {batch_size}")
        self.n: int = size
        self.n_rows = n_rows
        self.nas = nas
        self.k = k
        self.keys_seed = keys_seed

        num_batches = self.n // batch_size
        batches = [batch_size for _ in range(num_batches)]
        # A corner case when we need to add one more batch
        if self.n % batch_size != 0:
            batches.append(self.n % batch_size)

        # Generate a random seed per batch
        random.seed(seed)
        self.batches = [{"size": bs, "seed": random.randint(0, NATIVE_I64_MAX_VALUE)} for bs in batches]

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        for batch in self.batches:
            yield generate_join_dataset_big(self.n, self.nas, batch["seed"], self.keys_seed, batch["size"])
