from __future__ import annotations

from abc import ABC, abstractmethod
import random
from typing import Iterator

import numpy as np
import pyarrow as pa

from falsa import H2ODatasetSizes

from .native import (
    generate_groupby,
    generate_join_lhs,
    generate_join_rhs_big,
    generate_join_rhs_medium,
    generate_join_rhs_small,
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


class JoinGenerator(ABC):
    def __init__(
        self,
        size: H2ODatasetSizes | int,
        n_rows: int,
        k: int,
        nas: int,
        seed: int = 42,
        keys_seed: int = 142,
        batch_size: int = 5_000_000,
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
        self.nas = nas

        self.kk1 = self.generate_keys(int(self.n / 1e6))
        self.kk2 = self.generate_keys(int(self.n / 1e3))
        self.kk3 = self.generate_keys(int(self.n))

        num_batches = self.n_rows // batch_size
        batches = [batch_size for _ in range(num_batches)]
        # A corner case when we need to add one more batch
        if self.n_rows % batch_size != 0:
            batches.append(self.n_rows % batch_size)

        # Generate a random seed per batch
        random.seed(seed)
        self.batches = [{"size": bs, "seed": random.randint(0, NATIVE_I64_MAX_VALUE)} for bs in batches]
        self.get_keys()

    def generate_keys(self, nn: int) -> dict[str, np.ndarray]:
        total_size = int(nn * 1.1)
        npr = np.random.default_rng(seed=self.keys_seed)
        key = npr.permutation(total_size) + 1

        x_end = int(nn * 0.9)
        l_end = int(nn * 1.1)

        return {"x": key[:x_end], "l": key[x_end:nn], "r": key[nn:l_end]}

    def sample_all(self, size: int, arr: np.ndarray, seed: int) -> np.ndarray:
        assert len(arr) <= size, "Input length should be less than size!"

        npr = np.random.default_rng(seed=seed)
        if size > len(arr):
            extra = npr.choice(arr, size - len(arr), replace=True)
            result = np.concatenate([arr, extra])
        else:
            result = arr

        return result

    @abstractmethod
    def get_keys(self) -> None: ...


class JoinSmallGenerator(JoinGenerator):
    """A simple wrapper on top of native generator.

    The class takes care of random seeds generation, input validation
    and calculation of the size of all batches.
    """

    def get_keys(self) -> None:
        self.k1 = self.sample_all(int(self.n / 1e6), np.concatenate([self.kk1["x"], self.kk1["r"]]), self.keys_seed + 1)

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        prev_batch = 0
        for batch in self.batches:
            _t = prev_batch
            prev_batch += batch["size"]
            yield generate_join_rhs_small(
                self.n, batch["seed"], pa.array(self.k1[_t:prev_batch], type=pa.int64()), batch["size"]
            )


class JoinMediumGenerator(JoinGenerator):
    """A simple wrapper on top of native generator.

    The class takes care of random seeds generation, input validation
    and calculation of the size of all batches.
    """

    def get_keys(self) -> None:
        self.k1 = self.sample_all(int(self.n / 1e3), np.concatenate([self.kk1["x"], self.kk1["r"]]), self.keys_seed + 1)
        self.k2 = self.sample_all(int(self.n / 1e3), np.concatenate([self.kk2["x"], self.kk2["r"]]), self.keys_seed + 2)

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        prev_batch = 0
        for batch in self.batches:
            _t = prev_batch
            prev_batch += batch["size"]
            yield generate_join_rhs_medium(
                self.n,
                batch["seed"],
                pa.array(self.k1[_t:prev_batch], type=pa.int64()),
                pa.array(self.k2[_t:prev_batch], type=pa.int64()),
                batch["size"],
            )


class JoinBigGenerator(JoinGenerator):
    """A simple wrapper on top of native generator.

    The class takes care of random seeds generation, input validation
    and calculation of the size of all batches.
    """

    def get_keys(self) -> None:
        self.k1 = self.sample_all(self.n, np.concatenate([self.kk1["x"], self.kk1["r"]]), self.keys_seed + 1)
        self.k2 = self.sample_all(self.n, np.concatenate([self.kk2["x"], self.kk2["r"]]), self.keys_seed + 2)
        self.k3 = self.sample_all(self.n, np.concatenate([self.kk3["x"], self.kk3["r"]]), self.keys_seed + 3)

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        prev_batch = 0
        for batch in self.batches:
            _t = prev_batch
            prev_batch += batch["size"]
            yield generate_join_rhs_big(
                self.n,
                batch["seed"],
                pa.array(self.k1[_t:prev_batch], type=pa.int64()),
                pa.array(self.k2[_t:prev_batch], type=pa.int64()),
                pa.array(self.k3[_t:prev_batch], type=pa.int64()),
                batch["size"],
            )


class JoinLHSGenerator(JoinGenerator):
    """A simple wrapper on top of native generator.

    The class takes care of random seeds generation, input validation
    and calculation of the size of all batches.
    """

    def get_keys(self) -> None:
        self.k1 = self.sample_all(self.n, np.concatenate([self.kk1["x"], self.kk1["l"]]), self.keys_seed + 1)
        self.k2 = self.sample_all(self.n, np.concatenate([self.kk2["x"], self.kk2["l"]]), self.keys_seed + 2)
        self.k3 = self.sample_all(self.n, np.concatenate([self.kk3["x"], self.kk3["l"]]), self.keys_seed + 3)

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        prev_batch = 0
        for batch in self.batches:
            _t = prev_batch
            prev_batch += batch["size"]
            yield generate_join_lhs(
                self.n,
                batch["seed"],
                pa.array(self.k1[_t:prev_batch], type=pa.int64()),
                pa.array(self.k2[_t:prev_batch], type=pa.int64()),
                pa.array(self.k3[_t:prev_batch], type=pa.int64()),
                batch["size"],
            )
