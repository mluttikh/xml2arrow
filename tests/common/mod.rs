//! Shared test utilities for integration tests.
//!
//! Provides assertion macros for Arrow arrays and helper functions
//! that reduce boilerplate in test cases.

use std::fs::File;
use std::io::{BufReader, Write};

use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;
use tempfile::NamedTempFile;
use xml2arrow::{Config, parse_xml};

/// Assert that an Arrow array column contains the expected values.
///
/// # Example
/// ```ignore
/// assert_array_values!(batch, "id", &[1, 2, 3], Int32Array);
/// ```
macro_rules! assert_array_values {
    ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty) => {{
        let array = $batch
            .column_by_name($column_name)
            .unwrap_or_else(|| panic!("Column '{}' not found in batch", $column_name))
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Column '{}' could not be downcast to {}",
                    $column_name,
                    stringify!($array_type)
                )
            });
        assert_eq!(
            array.len(),
            $expected_values.len(),
            "Array length mismatch for column '{}'",
            $column_name
        );
        for (i, expected) in $expected_values.iter().enumerate() {
            assert_eq!(
                array.value(i),
                *expected,
                "Value mismatch at index {} for column '{}'",
                i,
                $column_name
            );
        }
    }};
}

#[allow(unused_macros)]
/// Assert that a nullable Arrow array column contains the expected Option values.
///
/// # Example
/// ```ignore
/// assert_array_values_option!(batch, "name", &[Some("a"), None], StringArray);
/// ```
macro_rules! assert_array_values_option {
    ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty) => {{
        let array = $batch
            .column_by_name($column_name)
            .unwrap_or_else(|| panic!("Column '{}' not found in batch", $column_name))
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Column '{}' could not be downcast to {}",
                    $column_name,
                    stringify!($array_type)
                )
            });
        assert_eq!(
            array.len(),
            $expected_values.len(),
            "Array length mismatch for column '{}'",
            $column_name
        );
        for (i, expected) in $expected_values.iter().enumerate() {
            match expected {
                Some(val) => assert_eq!(
                    array.value(i),
                    *val,
                    "Value mismatch at index {} for column '{}'",
                    i,
                    $column_name
                ),
                None => assert!(
                    array.is_null(i),
                    "Expected null at index {} for column '{}'",
                    i,
                    $column_name
                ),
            }
        }
    }};
}

/// Assert that a floating-point Arrow array column contains the expected values
/// within the given tolerance.
///
/// # Example
/// ```ignore
/// assert_array_approx_values!(batch, "value", &[3.14], Float64Array, 1e-10);
/// ```
macro_rules! assert_array_approx_values {
    ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty, $tolerance:expr) => {{
        let array = $batch
            .column_by_name($column_name)
            .unwrap_or_else(|| panic!("Column '{}' not found in batch", $column_name))
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Column '{}' could not be downcast to {}",
                    $column_name,
                    stringify!($array_type)
                )
            });
        assert_eq!(
            array.len(),
            $expected_values.len(),
            "Array length mismatch for column '{}'",
            $column_name
        );
        for (i, expected) in $expected_values.iter().enumerate() {
            assert!(
                (array.value(i) - *expected).abs() < $tolerance,
                "Value mismatch at index {} for column '{}': expected {}, got {}",
                i,
                $column_name,
                expected,
                array.value(i)
            );
        }
    }};
}

#[allow(unused_macros)]
/// Assert that a nullable floating-point Arrow array column contains the expected
/// Option values within the given tolerance.
///
/// # Example
/// ```ignore
/// assert_array_approx_values_option!(batch, "value", &[Some(3.14), None], Float64Array, 1e-10);
/// ```
macro_rules! assert_array_approx_values_option {
    ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty, $tolerance:expr) => {{
        let array = $batch
            .column_by_name($column_name)
            .unwrap_or_else(|| panic!("Column '{}' not found in batch", $column_name))
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Column '{}' could not be downcast to {}",
                    $column_name,
                    stringify!($array_type)
                )
            });
        assert_eq!(
            array.len(),
            $expected_values.len(),
            "Array length mismatch for column '{}'",
            $column_name
        );
        for (i, expected) in $expected_values.iter().enumerate() {
            match expected {
                Some(val) => assert!(
                    (array.value(i) - *val).abs() < $tolerance,
                    "Value mismatch at index {} for column '{}': expected {}, got {}",
                    i,
                    $column_name,
                    val,
                    array.value(i)
                ),
                None => assert!(
                    array.is_null(i),
                    "Expected null at index {} for column '{}'",
                    i,
                    $column_name
                ),
            }
        }
    }};
}

/// Parse XML content from a string using a YAML config string.
///
/// Panics on parse or config errors -- intended for tests where
/// the XML and config are known to be valid.
#[allow(dead_code)]
pub fn parse_xml_str(xml: &str, yaml_config: &str) -> IndexMap<String, RecordBatch> {
    let config: Config =
        serde_yaml::from_str(yaml_config).unwrap_or_else(|e| panic!("Invalid YAML config: {e}"));
    parse_xml(xml.as_bytes(), &config).unwrap_or_else(|e| panic!("XML parsing failed: {e:?}"))
}

/// Parse XML content by writing it to a temporary file first, simulating
/// real file-based parsing.
///
/// Panics on I/O, config, or parse errors -- intended for tests where
/// the inputs are known to be valid.
#[allow(dead_code)]
pub fn parse_xml_file(xml: &str, yaml_config: &str) -> IndexMap<String, RecordBatch> {
    let mut xml_file = NamedTempFile::new().expect("Failed to create temp file");
    write!(xml_file, "{}", xml).expect("Failed to write XML to temp file");

    let config: Config =
        serde_yaml::from_str(yaml_config).unwrap_or_else(|e| panic!("Invalid YAML config: {e}"));

    let file = File::open(xml_file.path()).expect("Failed to open temp file");
    let reader = BufReader::new(file);
    parse_xml(reader, &config).unwrap_or_else(|e| panic!("XML parsing failed: {e:?}"))
}

/// Write XML content to a temporary file and return the file handle.
///
/// Useful for tests that need access to the temp file itself (e.g., to
/// test error paths or check the `Result` directly).
#[allow(dead_code)]
pub fn write_xml_tempfile(xml: &str) -> NamedTempFile {
    let mut xml_file = NamedTempFile::new().expect("Failed to create temp file");
    write!(xml_file, "{}", xml).expect("Failed to write XML to temp file");
    xml_file
}
