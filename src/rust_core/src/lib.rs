use pyo3::prelude::*;

#[pyfunction]
fn rust_ping() -> PyResult<String> {
    Ok("pong из Rust ядра!".to_string())
}

#[pymodule]
fn hydrastream_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rust_ping, m)?)?;
    Ok(())
}
