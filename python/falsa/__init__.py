from enum import Enum


class H2ODatasetSizes(int, Enum):
    SMALL = 10_000_000 
    MEDIUM = 100_000_000
    BIG = 1_000_000_000

