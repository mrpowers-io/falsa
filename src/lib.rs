/*
Original inspirations of the generation logic:
- https://github.com/h2oai/db-benchmark/blob/master/_data/groupby-datagen.R
- https://github.com/h2oai/db-benchmark/blob/master/_data/join-datagen.R

A partial inspirations for the Rust code:
- https://github.com/MrPowers/farsante/blob/master/src/generators.rs

*/
use arrow::{
    array::{Float64Builder, Int64Builder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
    pyarrow::PyArrowType,
};
use pyo3::prelude::*;
use rand::distributions::Uniform;
use rand::seq::SliceRandom;
use rand::{distributions::Distribution, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::sync::Arc;

/**
Generate H2O group-by dataset.
Running this function multiple time with the same seed
will constantly return exactly the same batch!

:param n: int
    A total amount of rows in dataset. Should be positive.
    Passing a negative value or zero may tend to runtime errors / panic.
:param k: int
    An amount of grouping keys. Should be positive.
    Passing a negative value or zero may tend to runtime errors / panic.
:param nas: int
    A number from 1 to 100 that represent a percent of NULLs.
    Passing a value not from [0-100] may tend to unpredictable behavior.
:param seed: int
    A random seed value. Should be positive!
    Passing a negative value may tend to unpredictable behavior.
:param batch_size: int
    A size of the output batch.

:return: pyarrow.RecordBatch
*/
#[pyfunction]
fn generate_groupby(
    n: i64,
    k: i64,
    nas: i64,
    seed: i64,
    batch_size: i64,
) -> PyResult<PyArrowType<RecordBatch>> {
    let distr_k = Uniform::<i64>::try_from(1..=k)?;
    let distr_nk = Uniform::<i64>::try_from(1..=(n / k))?;
    let distr_5 = Uniform::<i64>::try_from(1..=5)?;
    let distr_15 = Uniform::<i64>::try_from(1..=15)?;
    let distr_float = Uniform::<f64>::try_from(0.0..=100.0)?;
    let distr_nas = Uniform::<i64>::try_from(0..=100)?;
    let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);

    let item_capacity = batch_size as usize; // validataion is on the python side

    let mut id1_builder = StringBuilder::with_capacity(item_capacity, item_capacity * 8 * 5); // id{:03}, utf8
    let mut id2_builder = StringBuilder::with_capacity(item_capacity, item_capacity * 8 * 5); // id{:03}, utf8
    let mut id3_builder = StringBuilder::with_capacity(item_capacity, item_capacity * 8 * 12); // id{:010}, utf8
    let mut id4_builder = Int64Builder::with_capacity(item_capacity);
    let mut id5_builder = Int64Builder::with_capacity(item_capacity);
    let mut id6_builder = Int64Builder::with_capacity(item_capacity);
    let mut v1_builder = Int64Builder::with_capacity(item_capacity);
    let mut v2_builder = Int64Builder::with_capacity(item_capacity);
    let mut v3_builder = Float64Builder::with_capacity(item_capacity);

    for _i in 0..batch_size {
        // id1, string in form id123, 123 from 1-K
        if distr_nas.sample(&mut rng) >= nas {
            id1_builder.append_value(format!("id{:03}", distr_k.sample(&mut rng)))
        } else {
            id1_builder.append_null()
        }
        // id2, string in form id123, 123 from 1-K
        if distr_nas.sample(&mut rng) >= nas {
            id2_builder.append_value(format!("id{:03}", distr_nk.sample(&mut rng)))
        } else {
            id2_builder.append_null()
        }
        // id3, string in form id1234567890, number from 1-N/K
        if distr_nas.sample(&mut rng) >= nas {
            id3_builder.append_value(format!("id{:010}", distr_nk.sample(&mut rng)))
        } else {
            id3_builder.append_null()
        }
        // id4, 1-K, int
        if distr_nas.sample(&mut rng) >= nas {
            id4_builder.append_value(distr_k.sample(&mut rng))
        } else {
            id4_builder.append_null()
        }
        // id5, 1-K, int
        if distr_nas.sample(&mut rng) >= nas {
            id5_builder.append_value(distr_k.sample(&mut rng))
        } else {
            id5_builder.append_null()
        }
        // id6, 1-N/K, int
        if distr_nas.sample(&mut rng) >= nas {
            id6_builder.append_value(distr_nk.sample(&mut rng))
        } else {
            id6_builder.append_null()
        }
        // v1, 1-5, int
        v1_builder.append_value(distr_5.sample(&mut rng));
        // v2, 1-15, int
        v2_builder.append_value(distr_15.sample(&mut rng));
        // v3, random float
        v3_builder.append_value(distr_float.sample(&mut rng));
    }

    let schema = Schema::new(vec![
        Field::new("id1", DataType::Utf8, true),
        Field::new("id2", DataType::Utf8, true),
        Field::new("id3", DataType::Utf8, true),
        Field::new("id4", DataType::Int64, true),
        Field::new("id5", DataType::Int64, true),
        Field::new("id6", DataType::Int64, true),
        Field::new("v1", DataType::Int64, false),
        Field::new("v2", DataType::Int64, false),
        Field::new("v3", DataType::Float64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id1_builder.finish()),
            Arc::new(id2_builder.finish()),
            Arc::new(id3_builder.finish()),
            Arc::new(id4_builder.finish()),
            Arc::new(id5_builder.finish()),
            Arc::new(id6_builder.finish()),
            Arc::new(v1_builder.finish()),
            Arc::new(v2_builder.finish()),
            Arc::new(v3_builder.finish()),
        ],
    ).unwrap();

    Ok(PyArrowType(batch))
}

/**
Generate H2O join small dataset.
Running this function multiple time with the same seed
will constantly return exactly the same batch!

:param n: int
    A total amount of rows in dataset. Should be positive, greater than 1e6.
    Passing a negative value or zero or value less than 1e6 may tend to runtime errors / panic.
:param seed: int
    A seed for the current batch. Should be positive.
    Passing a negative value may tend to unpredictable behavior.
:keys_seed: int
    A seed for generation of the join-keys. It should be the same for all the batches!
    Passing a negative value may tend to unpredictable behavior.
:param batch_size: int
    A size of the output batch.

:return: pyarrow.RecordBatch
*/
#[pyfunction]
fn generate_join_dataset_small(
    n: i64,
    seed: i64,
    keys_seed: i64,
    batch_size: i64,
) -> PyResult<PyArrowType<RecordBatch>> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);
    let mut keys_rng = ChaCha8Rng::seed_from_u64(keys_seed as u64);
    let mut k1: Vec<i64> = (1..=(n * 11 / 10 / 1_000_000)).collect(); // original R line: key1 = split_xlr(N/1e6)
    k1.shuffle(&mut keys_rng);

    let distr_float = Uniform::<f64>::try_from(1.0..=100.0)?;

    // original R line (43:44)
    // x = key[seq.int(1, n*0.9)],
    // l = key[seq.int(n*0.9+1, n)],
    // r = key[seq.int(n+1, n*1.1)]
    // we need (x, r) here
    let mut kx = k1
        .get(0..(n as usize * 9 / 10 / 1_000_000))
        .expect("internal indexing error with k1")
        .to_vec();
    let mut kr = k1
        .get((n as usize / 1_000_000)..(n as usize * 11 / 10 / 1_000_000))
        .expect("internal indexing error with k1")
        .to_vec();

    kx.append(&mut kr);

    let item_capacity = batch_size as usize; // validation is on the python side
    let len_of_max_key = (n * 11 / 10 / 1_000_000).to_string().len() + 2; // id{}, where {} is a number from a vec

    let mut id1_builder = Int64Builder::with_capacity(item_capacity);
    let mut id4_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_key); // utf8
    let mut v2_builder = Float64Builder::with_capacity(item_capacity);

    for _i in 0..batch_size {
        let k1 = kx.choose(&mut rng).unwrap(); // we know 100% that kx is non empty
        id1_builder.append_value(*k1);
        id4_builder.append_value(format!("id{}", k1));
        v2_builder.append_value(distr_float.sample(&mut rng));
    }

    let schema = Schema::new(vec![
        Field::new("id1", DataType::Int64, false),
        Field::new("id4", DataType::Utf8, false),
        Field::new("v2", DataType::Float64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id1_builder.finish()),
            Arc::new(id4_builder.finish()),
            Arc::new(v2_builder.finish()),
        ],
    )
    .unwrap();

    Ok(PyArrowType(batch))
}

/**
Generate H2O join medium dataset.
Running this function multiple time with the same seed
will constantly return exactly the same batch!

:param n: int
    A total amount of rows in dataset. Should be positive, greater than 1e6.
    Passing a negative value or zero or value less than 1e6 may tend to runtime errors / panic.
:param seed: int
    A seed for the current batch. Should be positive.
    Passing a negative value may tend to unpredictable behavior.
:keys_seed: int
    A seed for generation of the join-keys. It should be the same for all the batches!
    Passing a negative value may tend to unpredictable behavior.
:param batch_size: int
    A size of the output batch.

:return: pyarrow.RecordBatch
*/
#[pyfunction]
fn generate_join_dataset_medium(
    n: i64,
    seed: i64,
    keys_seed: i64,
    batch_size: i64,
) -> PyResult<PyArrowType<RecordBatch>> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);
    let mut keys_rng = ChaCha8Rng::seed_from_u64(keys_seed as u64);
    let mut k1: Vec<i64> = (1..=(n * 11 / 10 / 1_000_000)).collect(); // original R line: key1 = split_xlr(N/1e6)
    k1.shuffle(&mut keys_rng);

    let mut k2: Vec<i64> = (1..=(n * 11 / 10 / 1_000)).collect(); // original R line: key2 = split_xlr(N/1e3)
    k2.shuffle(&mut keys_rng);

    let distr_float = Uniform::<f64>::try_from(1.0..=100.0)?;

    // orginial line (43:44)
    // x = key[seq.int(1, n*0.9)],
    // l = key[seq.int(n*0.9+1, n)],
    // Sampling both (x, l) is equal to sampling from 1..n
    // where n = n / 1e6
    let mut k1x = k1
        .get(0..(n as usize * 9 / 10 / 1_000_000))
        .expect("internal indexing error with k1")
        .to_vec();
    let mut k1r = k1
        .get((n as usize / 1_000_000)..(n as usize * 11 / 10 / 1_000_000))
        .expect("internal indexing error with k1")
        .to_vec();

    k1x.append(&mut k1r);

    let mut k2x = k2
        .get(0..(n as usize * 9 / 10 / 1_000_000))
        .expect("internal indexing error with k2")
        .to_vec();
    let mut k2r = k2
        .get((n as usize / 1_000_000)..(n as usize * 11 / 10 / 1_000_000))
        .expect("internal indexing error with k2")
        .to_vec();

    k2x.append(&mut k2r);

    let item_capacity = batch_size as usize;
    let len_of_max_k1_key = (n * 11 / 10 / 1_000_000).to_string().len() + 2; // id{}, where {} is a number from a vec
    let len_of_max_k2_key = (n * 11 / 10 / 1_000).to_string().len() + 2; // the same

    let mut id1_builder = Int64Builder::with_capacity(item_capacity);
    let mut id2_builder = Int64Builder::with_capacity(item_capacity);
    let mut id4_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_k1_key); // utf8
    let mut id5_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_k2_key); // utf8
    let mut v2_builder = Float64Builder::with_capacity(item_capacity);

    for _i in 0..batch_size {
        let k1 = k1x.choose(&mut rng).unwrap(); // we know 100% that k1x is non empty
        let k2 = k2x.choose(&mut rng).unwrap(); // we know 100% that k2x is non empty

        id1_builder.append_value(*k1);
        id2_builder.append_value(*k2);
        id4_builder.append_value(format!("id{}", k1));
        id5_builder.append_value(format!("id{}", k2));
        v2_builder.append_value(distr_float.sample(&mut rng));
    }

    let schema = Schema::new(vec![
        Field::new("id1", DataType::Int64, false),
        Field::new("id2", DataType::Int64, false),
        Field::new("id4", DataType::Utf8, false),
        Field::new("id5", DataType::Utf8, false),
        Field::new("v2", DataType::Float64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id1_builder.finish()),
            Arc::new(id2_builder.finish()),
            Arc::new(id4_builder.finish()),
            Arc::new(id5_builder.finish()),
            Arc::new(v2_builder.finish()),
        ],
    )
    .unwrap();

    Ok(PyArrowType(batch))
}

/**
Generate H2O join big dataset.
Running this function multiple time with the same seed
will constantly return exactly the same batch!

:param n: int
    A total amount of rows in dataset. Should be positive, greater than 1e6.
    Passing a negative value or zero or a value less than 1e6 may tend to runtime errors / panic.
:param nas: int
    A number from 1 to 100 that represent a percent of NULLs.
    Passing a value not from [0-100] may tend to unpredictable behavior.
:param seed: int
    A seed for the current batch. Should be positive.
    Passing a negative value may tend to unpredictable behavior.
:keys_seed: int
    A seed for generation of the join-keys. It should be the same for all the batches!
    Passing a negative value may tend to unpredictable behavior.
:param batch_size: int
    A size of the output batch.

:return: pyarrow.RecordBatch
 */
#[pyfunction]
fn generate_join_dataset_big(
    n: i64,
    nas: i64,
    seed: i64,
    keys_seed: i64,
    batch_size: i64,
) -> PyResult<PyArrowType<RecordBatch>> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);
    let mut keys_rng = ChaCha8Rng::seed_from_u64(keys_seed as u64);
    let mut k1: Vec<i64> = (1..=(n * 11 / 10 / 1_000_000)).collect(); // original R line: key1 = split_xlr(N/1e6)
    k1.shuffle(&mut keys_rng);

    let mut k2: Vec<i64> = (1..=(n * 11 / 10 / 1_000)).collect(); // original R line: key2 = split_xlr(N/1e3)
    k2.shuffle(&mut keys_rng);

    let mut k3: Vec<i64> = (1..=(n * 11 / 10)).collect(); // original R line: key3 = split_xlr(N)
    k3.shuffle(&mut keys_rng);

    let distr_float = Uniform::<f64>::try_from(1.0..=100.0)?;
    let distr_nas = Uniform::<i64>::try_from(0..=100)?;

    // orginial line (43:44)
    // x = key[seq.int(1, n*0.9)],
    // l = key[seq.int(n*0.9+1, n)],
    // Sampling both (x, l) is equal to sampling from 1..n
    // where n = n / 1e6
    k1 = k1
        .get(0..(n as usize / 1_000_000))
        .expect("internal indexing error with k1")
        .to_vec();

    // same logic here
    // see https://github.com/h2oai/db-benchmark/blob/master/_data/join-datagen.R#L40
    // for details
    k2 = k2
        .get(0..(n as usize / 1_000))
        .expect("internal indexing error with k2")
        .to_vec();
    k3 = k3
        .get(0..(n as usize))
        .expect("internal indexing error with k3")
        .to_vec();

    let item_capacity = batch_size as usize;
    let len_of_max_k1_key = (n * 11 / 10 / 1_000_000).to_string().len() + 2; // id{}, where {} is a number from a vec
    let len_of_max_k2_key = (n * 11 / 10 / 1_000).to_string().len() + 2; // the same
    let len_of_max_k3_key = (n * 11 / 10).to_string().len() + 2; // the same

    let mut id1_builder = Int64Builder::with_capacity(item_capacity);
    let mut id2_builder = Int64Builder::with_capacity(item_capacity);
    let mut id3_builder = Int64Builder::with_capacity(item_capacity);
    let mut id4_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_k1_key); // utf8
    let mut id5_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_k2_key); // utf8
    let mut id6_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_k3_key); // utf8
    let mut v2_builder = Float64Builder::with_capacity(item_capacity);

    for _i in 0..batch_size {
        // We exactly know at the moment that k1, k2, k3 are non empty
        let k1 = k1.choose(&mut rng).unwrap();
        let k2 = k2.choose(&mut rng).unwrap();
        let k3 = k3.choose(&mut rng).unwrap();

        if distr_nas.sample(&mut rng) >= nas {
            id1_builder.append_value(*k1)
        } else {
            id1_builder.append_null()
        }
        if distr_nas.sample(&mut rng) >= nas {
            id2_builder.append_value(*k2)
        } else {
            id2_builder.append_null()
        }
        if distr_nas.sample(&mut rng) >= nas {
            id3_builder.append_value(*k3)
        } else {
            id3_builder.append_null()
        }
        id4_builder.append_value(format!("id{}", k1));
        id5_builder.append_value(format!("id{}", k2));
        id6_builder.append_value(format!("id{}", k2));
        if distr_nas.sample(&mut rng) >= nas {
            v2_builder.append_value(distr_float.sample(&mut rng));
        } else {
            v2_builder.append_null()
        }
    }

    let schema = Schema::new(vec![
        Field::new("id1", DataType::Int64, true),
        Field::new("id2", DataType::Int64, true),
        Field::new("id3", DataType::Int64, true),
        Field::new("id4", DataType::Utf8, false),
        Field::new("id5", DataType::Utf8, false),
        Field::new("id6", DataType::Utf8, false),
        Field::new("v2", DataType::Float64, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id1_builder.finish()),
            Arc::new(id2_builder.finish()),
            Arc::new(id3_builder.finish()),
            Arc::new(id4_builder.finish()),
            Arc::new(id5_builder.finish()),
            Arc::new(id6_builder.finish()),
            Arc::new(v2_builder.finish()),
        ],
    )
    .unwrap();

    Ok(PyArrowType(batch))
}

#[pymodule]
fn native(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate_groupby, m)?)?;
    m.add_function(wrap_pyfunction!(generate_join_dataset_small, m)?)?;
    m.add_function(wrap_pyfunction!(generate_join_dataset_medium, m)?)?;
    m.add_function(wrap_pyfunction!(generate_join_dataset_big, m)?)?;
    Ok(())
}
