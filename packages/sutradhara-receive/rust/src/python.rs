//! PyO3 bindings for the Python wheel migration.
//!
//! The Python package keeps its dataclass-shaped public surface while this module
//! exposes Rust-backed contract primitives for thin Python wrappers. Hook-aware
//! write operations are intentionally left for the next binding slice.

use crate::{
    BAG_PROFILE, BAGIT_TEXT, CANONICALIZATION_VERSION, DATA_DIR_NAME, PACKAGE_GLOBS,
    PACKAGE_PROFILE_HASH, PACKAGE_PROFILE_VERSION, RECEIVE_PACKAGE, RECEIVE_PACKAGE_NAME,
    RECEIVE_PACKAGE_VERSION, RECEIVE_VERSION, ReceiveError,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyModule};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[pyfunction(name = "escape_member_name")]
fn py_escape_member_name(raw: &[u8]) -> String {
    crate::escape_member_name(raw)
}

#[pyfunction(name = "unescape_member_name")]
fn py_unescape_member_name<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyBytes>> {
    let raw = crate::unescape_member_name(text).map_err(py_value_error)?;
    Ok(PyBytes::new(py, &raw))
}

#[pyfunction(name = "canonicalize_manifest_path")]
fn py_canonicalize_manifest_path(raw: &str) -> PyResult<String> {
    crate::canonicalize_manifest_path(raw).map_err(py_value_error)
}

#[pyfunction(name = "canonicalize_filesystem_path")]
fn py_canonicalize_filesystem_path(
    path: &Bound<'_, PyAny>,
    root: &Bound<'_, PyAny>,
) -> PyResult<String> {
    let path = py_path_to_pathbuf(path)?;
    let root = py_path_to_pathbuf(root)?;
    crate::canonicalize_filesystem_path(&path, &root).map_err(py_value_error)
}

#[pyfunction(name = "safe_payload_path")]
fn py_safe_payload_path(payload_root: &Bound<'_, PyAny>, relpath: &str) -> PyResult<String> {
    let payload_root = py_path_to_pathbuf(payload_root)?;
    crate::safe_payload_path(&payload_root, relpath)
        .map(|path| path_to_string(&path))
        .map_err(py_value_error)
}

#[pyfunction(name = "sha256_file")]
fn py_sha256_file(path: &Bound<'_, PyAny>) -> PyResult<String> {
    crate::sha256_file(&py_path_to_pathbuf(path)?).map_err(py_runtime_error)
}

#[pyfunction(name = "bagit_manifest_text")]
fn py_bagit_manifest_text(entries: BTreeMap<String, String>) -> PyResult<String> {
    crate::bagit_manifest_text(&entries).map_err(py_value_error)
}

#[pyfunction(name = "bag_info_text")]
fn py_bag_info_text(metadata: BTreeMap<String, String>) -> String {
    crate::bag_info_text(&metadata)
}

#[pyfunction(name = "tagmanifest_text")]
fn py_tagmanifest_text(bag_root: &Bound<'_, PyAny>, tag_files: Vec<String>) -> PyResult<String> {
    let bag_root = py_path_to_pathbuf(bag_root)?;
    crate::tagmanifest_text(&bag_root, &tag_files).map_err(py_runtime_error)
}

#[pyfunction(name = "read_bag_info")]
fn py_read_bag_info(path: &Bound<'_, PyAny>) -> PyResult<BTreeMap<String, String>> {
    crate::read_bag_info(&py_path_to_pathbuf(path)?).map_err(py_runtime_error)
}

#[pyfunction(name = "read_manifest_sha256")]
fn py_read_manifest_sha256(path: &Bound<'_, PyAny>) -> PyResult<BTreeMap<String, String>> {
    crate::read_manifest_sha256(&py_path_to_pathbuf(path)?).map_err(py_runtime_error)
}

#[pyfunction(name = "read_package_index_json")]
fn py_read_package_index_json(path: &Bound<'_, PyAny>) -> PyResult<String> {
    let payload =
        crate::read_package_index(&py_path_to_pathbuf(path)?).map_err(py_runtime_error)?;
    serde_json::to_string(&payload).map_err(py_runtime_error)
}

#[pyfunction(name = "manifest_mismatch_json")]
fn py_manifest_mismatch_json(
    actual: BTreeMap<String, String>,
    expected: BTreeMap<String, String>,
) -> PyResult<String> {
    let payload = crate::manifest_mismatch(&actual, &expected).map_err(py_value_error)?;
    serde_json::to_string(&payload).map_err(py_runtime_error)
}

#[pyfunction(name = "hash_payload_tree_json")]
#[pyo3(signature = (payload_root, *, reject_native_packages = false))]
fn py_hash_payload_tree_json(
    payload_root: &Bound<'_, PyAny>,
    reject_native_packages: bool,
) -> PyResult<String> {
    let payload_root = py_path_to_pathbuf(payload_root)?;
    let records = crate::hash_payload_tree_with_policy(&payload_root, reject_native_packages)
        .map_err(py_runtime_error)?;
    serde_json::to_string(&records).map_err(py_runtime_error)
}

#[pyfunction(name = "validate_bag_json")]
fn py_validate_bag_json(bag_root: &Bound<'_, PyAny>) -> PyResult<String> {
    let bag_root = py_path_to_pathbuf(bag_root)?;
    serde_json::to_string(&crate::validate_bag(&bag_root)).map_err(py_runtime_error)
}

#[pyfunction(name = "slug_operator")]
fn py_slug_operator(operator: &str) -> String {
    crate::slug_operator(operator)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("RECEIVE_VERSION", RECEIVE_VERSION)?;
    m.add("RECEIVE_PACKAGE_NAME", RECEIVE_PACKAGE_NAME)?;
    m.add("RECEIVE_PACKAGE_VERSION", RECEIVE_PACKAGE_VERSION)?;
    m.add("RECEIVE_PACKAGE", RECEIVE_PACKAGE)?;
    m.add("CANONICALIZATION_VERSION", CANONICALIZATION_VERSION)?;
    m.add("PACKAGE_PROFILE_VERSION", PACKAGE_PROFILE_VERSION)?;
    m.add("PACKAGE_PROFILE_HASH", PACKAGE_PROFILE_HASH)?;
    m.add("PACKAGE_GLOBS", PACKAGE_GLOBS.to_vec())?;
    m.add("BAG_PROFILE", BAG_PROFILE)?;
    m.add("BAGIT_TEXT", BAGIT_TEXT)?;
    m.add("DATA_DIR_NAME", DATA_DIR_NAME)?;
    m.add("MANIFEST_NAME", "manifest-sha256.txt")?;
    m.add("BAG_INFO_NAME", "bag-info.txt")?;
    m.add("BAGIT_NAME", "bagit.txt")?;
    m.add("TAGMANIFEST_NAME", "tagmanifest-sha256.txt")?;
    m.add("PACKAGE_INDEX_NAME", "package-index.json")?;
    m.add("SUPPORTED_RECEIVE_PACKAGES", vec![RECEIVE_PACKAGE])?;

    m.add_function(wrap_pyfunction!(py_escape_member_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_unescape_member_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonicalize_manifest_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonicalize_filesystem_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_safe_payload_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_sha256_file, m)?)?;
    m.add_function(wrap_pyfunction!(py_bagit_manifest_text, m)?)?;
    m.add_function(wrap_pyfunction!(py_bag_info_text, m)?)?;
    m.add_function(wrap_pyfunction!(py_tagmanifest_text, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_bag_info, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_manifest_sha256, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_package_index_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_manifest_mismatch_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_hash_payload_tree_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_bag_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_slug_operator, m)?)?;
    Ok(())
}

fn py_runtime_error(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn py_value_error(error: ReceiveError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[cfg(unix)]
fn py_path_to_pathbuf(value: &Bound<'_, PyAny>) -> PyResult<PathBuf> {
    use pyo3::types::PyModule;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let os = PyModule::import(value.py(), "os")?;
    let encoded = os.call_method1("fsencode", (value,))?;
    let bytes = encoded.cast::<PyBytes>()?;
    Ok(PathBuf::from(OsString::from_vec(bytes.as_bytes().to_vec())))
}

#[cfg(not(unix))]
fn py_path_to_pathbuf(value: &Bound<'_, PyAny>) -> PyResult<PathBuf> {
    let os = PyModule::import(value.py(), "os")?;
    let path = os.call_method1("fspath", (value,))?.extract::<String>()?;
    Ok(PathBuf::from(path))
}

fn path_to_string(path: &std::path::Path) -> String {
    path.to_string_lossy().to_string()
}
