/*
Original inspirations of the generation logic:
- https://github.com/h2oai/db-benchmark/blob/master/_data/groupby-datagen.R
- https://github.com/h2oai/db-benchmark/blob/master/_data/join-datagen.R

A partial inspirations for the Rust code:
- https://github.com/MrPowers/farsante/blob/master/src/generators.rs

*/
use arrow::{
    array::{ArrayData, Float64Builder, Int64Array, Int64Builder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
    pyarrow::PyArrowType,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rand::distr::Uniform;
use rand::{distr::Distribution, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::sync::Arc;

#[derive(Debug)]
struct UniformError(rand::distr::uniform::Error);
impl From<UniformError> for PyErr {
    fn from(error: UniformError) -> Self {
        PyErr::new::<PyValueError, _>(format!("{:?}", error))
    }
}

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
    let distr_k = Uniform::<i64>::try_from(1..=k).map_err(|e| UniformError(e))?;
    let distr_nk = Uniform::<i64>::try_from(1..=(n / k)).map_err(|e| UniformError(e))?;
    let distr_5 = Uniform::<i64>::try_from(1..=5).map_err(|e| UniformError(e))?;
    let distr_15 = Uniform::<i64>::try_from(1..=15).map_err(|e| UniformError(e))?;
    let distr_float = Uniform::<f64>::try_from(0.0..=100.0).map_err(|e| UniformError(e))?;
    let distr_nas = Uniform::<i64>::try_from(0..=100).map_err(|e| UniformError(e))?;
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
    )
    .unwrap();

    Ok(PyArrowType(batch))
}

#[pyfunction]
fn generate_join_lhs(
    n: i64,
    seed: i64,
    k1: PyArrowType<ArrayData>,
    k2: PyArrowType<ArrayData>,
    k3: PyArrowType<ArrayData>,
    batch_size: i64,
) -> PyResult<PyArrowType<RecordBatch>> {
    let k1_array = Int64Array::try_from(k1.0)?;
    let k2_array = Int64Array::try_from(k2.0)?;
    let k3_array = Int64Array::try_from(k3.0)?;
    let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);
    let distr_float = Uniform::<f64>::try_from(1.0..=100.0).map_err(|e| UniformError(e))?;
    let item_capacity = batch_size as usize; // validation is on the python side

    assert!(
        k1_array.len() == item_capacity,
        "Internal error: keys size mismatch: {} != {}",
        k1_array.len(),
        item_capacity,
    );
    assert!(
        k2_array.len() == item_capacity,
        "Internal error: keys size mismatch: {} != {}",
        k2_array.len(),
        item_capacity,
    );
    assert!(
        k2_array.len() == item_capacity,
        "Internal error: keys size mismatch: {} != {}",
        k2_array.len(),
        item_capacity,
    );

    let schema = Schema::new(vec![
        Field::new("id1", DataType::Int64, false),
        Field::new("id2", DataType::Int64, false),
        Field::new("id3", DataType::Int64, false),
        Field::new("id4", DataType::Utf8, false),
        Field::new("id5", DataType::Utf8, false),
        Field::new("id6", DataType::Utf8, false),
        Field::new("v1", DataType::Float64, false),
    ]);

    let len_of_max_key = (n * 11 / 10 / 1_000_000).to_string().len() + 2; // id{}, where {} is a number from a vec
    let mut id4_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_key); // utf8
    let mut id5_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_key); // utf8
    let mut id6_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_key); // utf8
    let mut v1_builder = Float64Builder::with_capacity(item_capacity);

    for _i in 0..item_capacity {
        id4_builder.append_value(format! {"id{}", k1_array.value(_i)});
        id5_builder.append_value(format! {"id{}", k2_array.value(_i)});
        id6_builder.append_value(format! {"id{}", k3_array.value(_i)});
        v1_builder.append_value(distr_float.sample(&mut rng));
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(k1_array),
            Arc::new(k2_array),
            Arc::new(k3_array),
            Arc::new(id4_builder.finish()),
            Arc::new(id5_builder.finish()),
            Arc::new(id6_builder.finish()),
            Arc::new(v1_builder.finish()),
        ],
    )
    .unwrap();

    Ok(PyArrowType(batch))
}

#[pyfunction]
fn generate_join_rhs_small(
    n: i64,
    seed: i64,
    k1: PyArrowType<ArrayData>,
    batch_size: i64,
) -> PyResult<PyArrowType<RecordBatch>> {
    let k1_array = Int64Array::try_from(k1.0)?;
    let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);
    let distr_float = Uniform::<f64>::try_from(1.0..=100.0).map_err(|e| UniformError(e))?;
    let item_capacity = batch_size as usize; // validation is on the python side

    assert!(
        k1_array.len() == item_capacity,
        "Internal error: keys size mismatch: {} != {}",
        k1_array.len(),
        item_capacity,
    );

    let schema = Schema::new(vec![
        Field::new("id1", DataType::Int64, false),
        Field::new("id4", DataType::Utf8, false),
        Field::new("v2", DataType::Float64, false),
    ]);

    let len_of_max_key = (n * 11 / 10 / 1_000_000).to_string().len() + 2; // id{}, where {} is a number from a vec
    let mut id4_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_key); // utf8
    let mut v2_builder = Float64Builder::with_capacity(item_capacity);

    for _i in 0..item_capacity {
        id4_builder.append_value(format! {"id{}", k1_array.value(_i)});
        v2_builder.append_value(distr_float.sample(&mut rng));
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(k1_array),
            Arc::new(id4_builder.finish()),
            Arc::new(v2_builder.finish()),
        ],
    )
    .unwrap();

    Ok(PyArrowType(batch))
}

#[pyfunction]
fn generate_join_rhs_medium(
    n: i64,
    seed: i64,
    k1: PyArrowType<ArrayData>,
    k2: PyArrowType<ArrayData>,
    batch_size: i64,
) -> PyResult<PyArrowType<RecordBatch>> {
    let k1_array: Int64Array = Int64Array::try_from(k1.0)?;
    let k2_array: Int64Array = Int64Array::try_from(k2.0)?;
    let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);
    let distr_float = Uniform::<f64>::try_from(1.0..=100.0).map_err(|e| UniformError(e))?;
    let item_capacity = batch_size as usize; // validation is on the python side

    assert!(
        k1_array.len() == item_capacity,
        "Internal error: keys size mismatch: {} != {}",
        k1_array.len(),
        item_capacity,
    );
    assert!(
        k2_array.len() == item_capacity,
        "Internal error: keys size mismatch: {} != {}",
        k2_array.len(),
        item_capacity,
    );
    assert!(
        k2_array.len() == item_capacity,
        "Internal error: keys size mismatch: {} != {}",
        k2_array.len(),
        item_capacity,
    );

    let schema = Schema::new(vec![
        Field::new("id1", DataType::Int64, false),
        Field::new("id2", DataType::Int64, false),
        Field::new("id4", DataType::Utf8, false),
        Field::new("id5", DataType::Utf8, false),
        Field::new("v2", DataType::Float64, false),
    ]);

    let len_of_max_key = (n * 11 / 10 / 1_000_000).to_string().len() + 2; // id{}, where {} is a number from a vec
    let mut id4_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_key); // utf8
    let mut id5_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_key); // utf8
    let mut v2_builder = Float64Builder::with_capacity(item_capacity);

    for _i in 0..item_capacity {
        id4_builder.append_value(format! {"id{}", k1_array.value(_i)});
        id5_builder.append_value(format! {"id{}", k2_array.value(_i)});
        v2_builder.append_value(distr_float.sample(&mut rng));
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(k1_array),
            Arc::new(k2_array),
            Arc::new(id4_builder.finish()),
            Arc::new(id5_builder.finish()),
            Arc::new(v2_builder.finish()),
        ],
    )
    .unwrap();

    Ok(PyArrowType(batch))
}

#[pyfunction]
fn generate_join_rhs_big(
    n: i64,
    seed: i64,
    k1: PyArrowType<ArrayData>,
    k2: PyArrowType<ArrayData>,
    k3: PyArrowType<ArrayData>,
    batch_size: i64,
) -> PyResult<PyArrowType<RecordBatch>> {
    let k1_array: Int64Array = Int64Array::try_from(k1.0)?;
    let k2_array: Int64Array = Int64Array::try_from(k2.0)?;
    let k3_array: Int64Array = Int64Array::try_from(k3.0)?;
    let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);
    let distr_float = Uniform::<f64>::try_from(1.0..=100.0).map_err(|e| UniformError(e))?;
    let item_capacity = batch_size as usize; // validation is on the python side

    assert!(
        k1_array.len() == item_capacity,
        "Internal error: keys size mismatch: {} != {}",
        k1_array.len(),
        item_capacity,
    );
    assert!(
        k2_array.len() == item_capacity,
        "Internal error: keys size mismatch: {} != {}",
        k2_array.len(),
        item_capacity,
    );
    assert!(
        k2_array.len() == item_capacity,
        "Internal error: keys size mismatch: {} != {}",
        k2_array.len(),
        item_capacity,
    );

    let schema = Schema::new(vec![
        Field::new("id1", DataType::Int64, false),
        Field::new("id2", DataType::Int64, false),
        Field::new("id3", DataType::Int64, false),
        Field::new("id4", DataType::Utf8, false),
        Field::new("id5", DataType::Utf8, false),
        Field::new("id6", DataType::Utf8, false),
        Field::new("v2", DataType::Float64, false),
    ]);

    let len_of_max_key = (n * 11 / 10 / 1_000_000).to_string().len() + 2; // id{}, where {} is a number from a vec
    let mut id4_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_key); // utf8
    let mut id5_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_key); // utf8
    let mut id6_builder =
        StringBuilder::with_capacity(item_capacity, item_capacity * 8 * len_of_max_key); // utf8
    let mut v2_builder = Float64Builder::with_capacity(item_capacity);

    for _i in 0..item_capacity {
        id4_builder.append_value(format! {"id{}", k1_array.value(_i)});
        id5_builder.append_value(format! {"id{}", k2_array.value(_i)});
        id6_builder.append_value(format! {"id{}", k3_array.value(_i)});
        v2_builder.append_value(distr_float.sample(&mut rng));
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(k1_array),
            Arc::new(k2_array),
            Arc::new(k3_array),
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
fn native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate_groupby, m)?)?;
    m.add_function(wrap_pyfunction!(generate_join_lhs, m)?)?;
    m.add_function(wrap_pyfunction!(generate_join_rhs_small, m)?)?;
    m.add_function(wrap_pyfunction!(generate_join_rhs_medium, m)?)?;
    m.add_function(wrap_pyfunction!(generate_join_rhs_big, m)?)?;
    Ok(())
}
