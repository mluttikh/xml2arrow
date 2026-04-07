//! Integration tests for xml2arrow
//!
//! These tests focus on file-based concerns that unit tests cannot cover:
//! encoding, BOM handling, large files, config loading from YAML files,
//! and edge cases around empty/whitespace-only files.
//!
//! Parsing logic (dtypes, nesting, transforms, attributes, etc.) is
//! thoroughly covered by the unit tests in `src/xml_parser.rs`.

#[macro_use]
mod common;

use std::fs::File;
use std::io::{BufReader, Write};

use arrow::array::{Array, Float64Array, Int32Array, StringArray, UInt32Array};
use tempfile::NamedTempFile;
use xml2arrow::{Config, parse_xml};

use common::{parse_xml_file, write_xml_tempfile};

// ---------------------------------------------------------------------------
// Large file handling
// ---------------------------------------------------------------------------

#[test]
fn test_large_file_1k_rows_parsed_correctly() {
    let mut xml = String::from(r#"<?xml version="1.0"?><data>"#);
    for i in 0..1000 {
        xml.push_str(&format!(
            r#"<item><id>{}</id><value>{:.2}</value><name>Item{}</name></item>"#,
            i,
            i as f64 * 0.01,
            i
        ));
    }
    xml.push_str("</data>");

    let batches = parse_xml_file(
        &xml,
        r#"
        tables:
          - name: items
            xml_path: /data
            levels: []
            fields:
              - name: id
                xml_path: /data/item/id
                data_type: Int32
              - name: value
                xml_path: /data/item/value
                data_type: Float64
              - name: name
                xml_path: /data/item/name
                data_type: Utf8
        "#,
    );

    let batch = batches.get("items").unwrap();
    assert_eq!(batch.num_rows(), 1000);

    // Verify first and last values
    let id_array = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_array.value(0), 0);
    assert_eq!(id_array.value(999), 999);

    let value_array = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((value_array.value(0) - 0.0).abs() < 1e-10);
    assert!((value_array.value(999) - 9.99).abs() < 1e-10);
}

#[test]
fn test_large_file_10k_rows_parsed_correctly() {
    let mut xml = String::from(r#"<?xml version="1.0"?><data>"#);
    for i in 0..10000 {
        xml.push_str(&format!(
            r#"<item><id>{}</id><value>{:.4}</value></item>"#,
            i,
            (i as f64).sin()
        ));
    }
    xml.push_str("</data>");

    let batches = parse_xml_file(
        &xml,
        r#"
        tables:
          - name: items
            xml_path: /data
            levels: []
            fields:
              - name: id
                xml_path: /data/item/id
                data_type: Int32
              - name: value
                xml_path: /data/item/value
                data_type: Float64
        "#,
    );

    let batch = batches.get("items").unwrap();
    assert_eq!(batch.num_rows(), 10000);
}

// ---------------------------------------------------------------------------
// Config loading from YAML file
// ---------------------------------------------------------------------------

#[test]
fn test_yaml_config_with_transform_applied() {
    let mut yaml_file = NamedTempFile::with_suffix(".yaml").unwrap();
    write!(
        yaml_file,
        r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - name: id
        xml_path: /data/item/id
        data_type: Int32
      - name: value
        xml_path: /data/item/value
        data_type: Float64
        scale: 0.001
        offset: 10.0
"#
    )
    .unwrap();

    let xml_file =
        write_xml_tempfile(r#"<data><item><id>1</id><value>1000</value></item></data>"#);

    // Tests Config::from_yaml_file specifically
    let config = Config::from_yaml_file(yaml_file.path()).unwrap();

    let file = File::open(xml_file.path()).unwrap();
    let reader = BufReader::new(file);
    let batches = parse_xml(reader, &config).unwrap();
    let batch = batches.get("items").unwrap();

    assert_eq!(batch.num_rows(), 1);
    // Value should be: (1000 * 0.001) + 10.0 = 11.0
    assert_array_approx_values!(batch, "value", &[11.0], Float64Array, 1e-10);
}

#[test]
fn test_invalid_yaml_config_returns_error() {
    let mut yaml_file = NamedTempFile::with_suffix(".yaml").unwrap();
    write!(yaml_file, "this is not valid yaml config: [[[").unwrap();

    let result = Config::from_yaml_file(yaml_file.path());
    assert!(result.is_err(), "Invalid YAML should produce an error");
}

#[test]
fn test_missing_yaml_config_returns_error() {
    let result = Config::from_yaml_file("/tmp/nonexistent_xml2arrow_test_config.yaml");
    assert!(result.is_err(), "Missing config file should produce an error");
}

#[test]
fn test_config_reused_across_multiple_files() {
    let config: Config = serde_yaml::from_str(
        r#"
        tables:
          - name: items
            xml_path: /data
            levels: []
            fields:
              - name: value
                xml_path: /data/item/value
                data_type: Int32
        "#,
    )
    .unwrap();

    let xml_a = write_xml_tempfile(r#"<data><item><value>1</value></item></data>"#);
    let xml_b = write_xml_tempfile(
        r#"<data><item><value>10</value></item><item><value>20</value></item></data>"#,
    );

    // Parse first file
    let file_a = File::open(xml_a.path()).unwrap();
    let batches_a = parse_xml(BufReader::new(file_a), &config).unwrap();
    let batch_a = batches_a.get("items").unwrap();
    assert_eq!(batch_a.num_rows(), 1);
    assert_array_values!(batch_a, "value", &[1], Int32Array);

    // Parse second file with the same config
    let file_b = File::open(xml_b.path()).unwrap();
    let batches_b = parse_xml(BufReader::new(file_b), &config).unwrap();
    let batch_b = batches_b.get("items").unwrap();
    assert_eq!(batch_b.num_rows(), 2);
    assert_array_values!(batch_b, "value", &[10, 20], Int32Array);
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

#[test]
fn test_utf8_bom_file_parsed_correctly() {
    // A BOM (Byte Order Mark) is a special Unicode character (U+FEFF) that some
    // editors prepend to files to signal the encoding. In UTF-8 it is the three-byte
    // sequence 0xEF 0xBB 0xBF. The parser must handle files that start with a BOM
    // without treating it as XML content.
    let mut xml_file = NamedTempFile::new().unwrap();
    xml_file.write_all(&[0xEF, 0xBB, 0xBF]).unwrap();
    write!(
        xml_file,
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <data><item><value>42</value></item></data>"#
    )
    .unwrap();

    let config: Config = serde_yaml::from_str(
        r#"
        tables:
          - name: items
            xml_path: /data
            levels: []
            fields:
              - name: value
                xml_path: /data/item/value
                data_type: Int32
        "#,
    )
    .unwrap();

    let file = File::open(xml_file.path()).unwrap();
    let reader = BufReader::new(file);
    let batches = parse_xml(reader, &config).unwrap();
    let batch = batches.get("items").unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_array_values!(batch, "value", &[42], Int32Array);
}

// ---------------------------------------------------------------------------
// Empty / edge-case files
// ---------------------------------------------------------------------------

#[test]
fn test_empty_file_returns_empty_batch() {
    let xml_file = NamedTempFile::new().unwrap();
    // File is empty -- no content written

    let config: Config = serde_yaml::from_str(
        r#"
        tables:
          - name: items
            xml_path: /data
            levels: []
            fields:
              - name: value
                xml_path: /data/item/value
                data_type: Int32
        "#,
    )
    .unwrap();

    let file = File::open(xml_file.path()).unwrap();
    let reader = BufReader::new(file);
    let result = parse_xml(reader, &config);

    assert!(result.is_ok());
    let batches = result.unwrap();
    let batch = batches.get("items").unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_whitespace_only_file_returns_empty_batch() {
    let xml_file = write_xml_tempfile("   \n\t\n   ");

    let config: Config = serde_yaml::from_str(
        r#"
        tables:
          - name: items
            xml_path: /data
            levels: []
            fields:
              - name: value
                xml_path: /data/item/value
                data_type: Int32
        "#,
    )
    .unwrap();

    let file = File::open(xml_file.path()).unwrap();
    let reader = BufReader::new(file);
    let result = parse_xml(reader, &config);

    assert!(result.is_ok());
}

// ---------------------------------------------------------------------------
// Realistic end-to-end scenario
// ---------------------------------------------------------------------------

#[test]
fn test_realistic_sensor_data_parsed_correctly() {
    let batches = parse_xml_file(
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <sensorData>
            <sensors>
                <sensor>
                    <id>S001</id>
                    <type>temperature</type>
                    <unit>celsius</unit>
                    <readings>
                        <reading><time>10:30:00</time><value>23.5</value></reading>
                        <reading><time>10:31:00</time><value>23.7</value></reading>
                        <reading><time>10:32:00</time><value>23.6</value></reading>
                    </readings>
                </sensor>
                <sensor>
                    <id>S002</id>
                    <type>humidity</type>
                    <unit>percent</unit>
                    <readings>
                        <reading><time>10:30:00</time><value>45.2</value></reading>
                        <reading><time>10:31:00</time><value>45.5</value></reading>
                    </readings>
                </sensor>
            </sensors>
        </sensorData>"#,
        r#"
        tables:
          - name: sensors
            xml_path: /sensorData/sensors
            levels: ["sensor"]
            fields:
              - name: id
                xml_path: /sensorData/sensors/sensor/id
                data_type: Utf8
              - name: type
                xml_path: /sensorData/sensors/sensor/type
                data_type: Utf8
              - name: unit
                xml_path: /sensorData/sensors/sensor/unit
                data_type: Utf8
          - name: readings
            xml_path: /sensorData/sensors/sensor/readings
            levels: ["sensor", "reading"]
            fields:
              - name: time
                xml_path: /sensorData/sensors/sensor/readings/reading/time
                data_type: Utf8
              - name: value
                xml_path: /sensorData/sensors/sensor/readings/reading/value
                data_type: Float64
        "#,
    );

    // Check sensors
    let sensors = batches.get("sensors").unwrap();
    assert_eq!(sensors.num_rows(), 2);
    assert_array_values!(sensors, "id", &["S001", "S002"], StringArray);
    assert_array_values!(sensors, "type", &["temperature", "humidity"], StringArray);
    assert_array_values!(sensors, "unit", &["celsius", "percent"], StringArray);

    // Check readings with parent indices
    let readings = batches.get("readings").unwrap();
    assert_eq!(readings.num_rows(), 5);
    assert_array_values!(readings, "<sensor>", &[0, 0, 0, 1, 1], UInt32Array);
    assert_array_values!(
        readings,
        "time",
        &["10:30:00", "10:31:00", "10:32:00", "10:30:00", "10:31:00"],
        StringArray
    );
}
