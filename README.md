# Falsa

Falsa is a tool for generating H2O db-like-benchmark.
This implementation is unofficial! For the official implementation please check [DuckDB fork of orginial H2O project](https://github.com/duckdblabs/db-benchmark/tree/main/_data).

## Quick start

Falsa is built via maturin and pyo3. It works with python 3.9+. For maturin installation please follow [an official documentation](https://www.maturin.rs/installation).

### Maturin build

In virtualenv with python 3.9+:

```sh
maturin develop --release
falsa --help
```

### Pip install

In virtualenv with python 3.9+:
```sh
pip install git+https://github.com/mrpowers-io/falsa.git@main
falsa --help
```

## Supported output formats

At the moment the following output formats are supported:

- CSV
- Parquet
- Delta*

_*There is a problem with Delta at the moment: writing to Delta requires materialization of all `pyarrow` batches first and may be slow and tends to OOM-like errors. We are working on it now and will provide a patched version soon._
