# falsa

falsa makes it easy to generate sample datasets.

Here is how to generate a Parquet file with 100 million rows and 9 columns of data for example:

```
falsa groupby --path-prefix=~/data --size MEDIUM
```

![falsa example](https://github.com/mrpowers-io/falsa/blob/main/images/falsa_example.png)

Here are the first three rows of data in the file:

```
┌───────┬──────────┬──────────────┬─────┬─────┬────────┬─────┬─────┬───────────┐
│ id1   ┆ id2      ┆ id3          ┆ id4 ┆ id5 ┆ id6    ┆ v1  ┆ v2  ┆ v3        │
│ ---   ┆ ---      ┆ ---          ┆ --- ┆ --- ┆ ---    ┆ --- ┆ --- ┆ ---       │
│ str   ┆ str      ┆ str          ┆ i64 ┆ i64 ┆ i64    ┆ i64 ┆ i64 ┆ f64       │
╞═══════╪══════════╪══════════════╪═════╪═════╪════════╪═════╪═════╪═══════════╡
│ id038 ┆ id850817 ┆ id0000837021 ┆ 90  ┆ 8   ┆ 898164 ┆ 4   ┆ 15  ┆ 28.133477 │
│ id095 ┆ id73309  ┆ id0000312443 ┆ 3   ┆ 75  ┆ 177193 ┆ 1   ┆ 12  ┆ 91.555302 │
│ id055 ┆ id248099 ┆ id0000141631 ┆ 12  ┆ 94  ┆ 132406 ┆ 1   ┆ 3   ┆ 64.543029 │
└───────┴──────────┴──────────────┴─────┴─────┴────────┴─────┴─────┴───────────┘
```

With falsa, you can generate many sample datasets.

## Installation

### Pip install

In virtualenv with python 3.9+:

```sh
pip install git+https://github.com/mrpowers-io/falsa.git@main
falsa --help
```

### Maturin build

In virtualenv with python 3.9+:

```sh
maturin develop --release
falsa --help
```

## h2o datasets

The h2o datasets are used to benchmark query engines on a single machine, [see here](https://duckdblabs.github.io/db-benchmark/).

Here are [the original R Scripts](https://github.com/duckdblabs/db-benchmark/tree/main/_data) to generate the sample datasets.  These still work if you know how to run R (the large dataset generation can error out if you machine doesn't have sufficient memory).

falsa is good if you want to generate these datasets with a Python interface or if you are facing memory issues with the R scripts.

### h2o groupby dataset

The h2o groupby dataset has 9 columns and 10 million/100 million/1 billion rows of data.

Here are three representative rows of data:

```
┌───────┬──────────┬──────────────┬─────┬─────┬────────┬─────┬─────┬───────────┐
│ id1   ┆ id2      ┆ id3          ┆ id4 ┆ id5 ┆ id6    ┆ v1  ┆ v2  ┆ v3        │
│ ---   ┆ ---      ┆ ---          ┆ --- ┆ --- ┆ ---    ┆ --- ┆ --- ┆ ---       │
│ str   ┆ str      ┆ str          ┆ i64 ┆ i64 ┆ i64    ┆ i64 ┆ i64 ┆ f64       │
╞═══════╪══════════╪══════════════╪═════╪═════╪════════╪═════╪═════╪═══════════╡
│ id038 ┆ id850817 ┆ id0000837021 ┆ 90  ┆ 8   ┆ 898164 ┆ 4   ┆ 15  ┆ 28.133477 │
│ id095 ┆ id73309  ┆ id0000312443 ┆ 3   ┆ 75  ┆ 177193 ┆ 1   ┆ 12  ┆ 91.555302 │
│ id055 ┆ id248099 ┆ id0000141631 ┆ 12  ┆ 94  ┆ 132406 ┆ 1   ┆ 3   ┆ 64.543029 │
└───────┴──────────┴──────────────┴─────┴─────┴────────┴─────┴─────┴───────────┘
```

Here's a short description of the columns:

* id1: 100 distinct values between id001 and id100
* id2: 100 distinct values between id001 and id100
* id3: 1_000_000 distinct values
* id4: random float values between zero and 100
* id5: random integer values between zero and 100
* id6: random integer values between 1 and 1_000_000
* v1: integer values between 1 and 5
* v2: integer valuees between 1 and 15
* v3: floating values between zero and 100

Here's the detailed description of the table:

```
┌────────────┬───────────┬───────────┬──────────────┬───────────┬───┬───────────────┬──────────┬───────────┬───────────┐
│ statistic  ┆ id1       ┆ id2       ┆ id3          ┆ id4       ┆ … ┆ id6           ┆ v1       ┆ v2        ┆ v3        │
│ ---        ┆ ---       ┆ ---       ┆ ---          ┆ ---       ┆   ┆ ---           ┆ ---      ┆ ---       ┆ ---       │
│ str        ┆ str       ┆ str       ┆ str          ┆ f64       ┆   ┆ f64           ┆ f64      ┆ f64       ┆ f64       │
╞════════════╪═══════════╪═══════════╪══════════════╪═══════════╪═══╪═══════════════╪══════════╪═══════════╪═══════════╡
│ count      ┆ 100000000 ┆ 100000000 ┆ 100000000    ┆ 1e8       ┆ … ┆ 1e8           ┆ 1e8      ┆ 1e8       ┆ 1e8       │
│ null_count ┆ 0         ┆ 0         ┆ 0            ┆ 0.0       ┆ … ┆ 0.0           ┆ 0.0      ┆ 0.0       ┆ 0.0       │
│ mean       ┆ null      ┆ null      ┆ null         ┆ 50.500471 ┆ … ┆ 499977.133559 ┆ 3.000173 ┆ 8.0002679 ┆ 50.000731 │
│ std        ┆ null      ┆ null      ┆ null         ┆ 28.864911 ┆ … ┆ 288668.423121 ┆ 1.414225 ┆ 4.320694  ┆ 28.868118 │
│ min        ┆ id001     ┆ id001     ┆ id0000000001 ┆ 1.0       ┆ … ┆ 1.0           ┆ 1.0      ┆ 1.0       ┆ 0.000002  │
│ 25%        ┆ null      ┆ null      ┆ null         ┆ 26.0      ┆ … ┆ 249956.0      ┆ 2.0      ┆ 4.0       ┆ 24.999205 │
│ 50%        ┆ null      ┆ null      ┆ null         ┆ 51.0      ┆ … ┆ 499949.0      ┆ 3.0      ┆ 8.0       ┆ 50.002307 │
│ 75%        ┆ null      ┆ null      ┆ null         ┆ 75.0      ┆ … ┆ 749987.0      ┆ 4.0      ┆ 12.0      ┆ 75.002693 │
│ max        ┆ id100     ┆ id999999  ┆ id0001000000 ┆ 100.0     ┆ … ┆ 1e6           ┆ 5.0      ┆ 15.0      ┆ 100.0     │
└────────────┴───────────┴───────────┴──────────────┴───────────┴───┴───────────────┴──────────┴───────────┴───────────┘
```

The h2o dataset is useful for group by benchmarks.  For example, you can use id1 to do an aggregation on a low cardinality column and id3 to do an aggreation on a high cardinality column.
