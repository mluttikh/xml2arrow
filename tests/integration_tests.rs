//! Integration tests for xml2arrow
//!
//! These tests verify end-to-end behavior with realistic scenarios including
//! file-based parsing and large file handling.

#[macro_use]
mod common;

use std::fs::File;
use std::io::{BufReader, Write};

use arrow::array::{Array, Float64Array, Int32Array, StringArray, UInt32Array};
use tempfile::NamedTempFile;
use xml2arrow::{Config, parse_xml};

use common::{parse_xml_file, write_xml_tempfile};

#[test]
fn test_file_parse_basic() {
    let batches = parse_xml_file(
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <data>
            <item><id>1</id><name>First</name></item>
            <item><id>2</id><name>Second</name></item>
            <item><id>3</id><name>Third</name></item>
        </data>"#,
        r#"
        tables:
          - name: items
            xml_path: /data
            levels: []
            fields:
              - name: id
                xml_path: /data/item/id
                data_type: Int32
              - name: name
                xml_path: /data/item/name
                data_type: Utf8
        "#,
    );

    let batch = batches.get("items").unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_array_values!(batch, "id", &[1, 2, 3], Int32Array);
    assert_array_values!(batch, "name", &["First", "Second", "Third"], StringArray);
}

#[test]
fn test_file_parse_large_1k_rows() {
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
fn test_file_parse_large_10k_rows() {
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

#[test]
fn test_file_parse_nested_structure() {
    let batches = parse_xml_file(
        r#"<?xml version="1.0"?>
        <company>
            <departments>
                <department>
                    <name>Engineering</name>
                    <employees>
                        <employee><id>1</id><ename>Alice</ename><role>Developer</role></employee>
                        <employee><id>2</id><ename>Bob</ename><role>Architect</role></employee>
                    </employees>
                </department>
                <department>
                    <name>Sales</name>
                    <employees>
                        <employee><id>3</id><ename>Charlie</ename><role>Manager</role></employee>
                    </employees>
                </department>
            </departments>
        </company>"#,
        r#"
        tables:
          - name: departments
            xml_path: /company/departments
            levels: ["department"]
            fields:
              - name: name
                xml_path: /company/departments/department/name
                data_type: Utf8
          - name: employees
            xml_path: /company/departments/department/employees
            levels: ["department", "employee"]
            fields:
              - name: id
                xml_path: /company/departments/department/employees/employee/id
                data_type: Int32
              - name: name
                xml_path: /company/departments/department/employees/employee/ename
                data_type: Utf8
              - name: role
                xml_path: /company/departments/department/employees/employee/role
                data_type: Utf8
        "#,
    );

    // Check departments
    let dept_batch = batches.get("departments").unwrap();
    assert_eq!(dept_batch.num_rows(), 2);
    assert_array_values!(dept_batch, "name", &["Engineering", "Sales"], StringArray);

    // Check employees
    let emp_batch = batches.get("employees").unwrap();
    assert_eq!(emp_batch.num_rows(), 3);
    assert_array_values!(emp_batch, "id", &[1, 2, 3], Int32Array);
    assert_array_values!(emp_batch, "name", &["Alice", "Bob", "Charlie"], StringArray);
    assert_array_values!(
        emp_batch,
        "role",
        &["Developer", "Architect", "Manager"],
        StringArray
    );

    // Check department indices
    assert_array_values!(emp_batch, "<department>", &[0, 0, 1], UInt32Array);
}

#[test]
fn test_file_config_yaml_with_transform() {
    // Create a temporary YAML config file
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

    let xml_file = write_xml_tempfile(
        r#"<data><item><id>1</id><value>1000</value></item></data>"#,
    );

    // Load config from file (tests Config::from_yaml_file specifically)
    let config = Config::from_yaml_file(yaml_file.path()).unwrap();

    let file = File::open(xml_file.path()).unwrap();
    let reader = BufReader::new(file);
    let batches = parse_xml(reader, &config).unwrap();
    let batch = batches.get("items").unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Value should be: (1000 * 0.001) + 10.0 = 1.0 + 10.0 = 11.0
    assert_array_approx_values!(batch, "value", &[11.0], Float64Array, 1e-10);
}

#[test]
fn test_file_parse_multiple_tables() {
    let batches = parse_xml_file(
        r#"<?xml version="1.0"?>
        <root>
            <header>
                <info>
                    <title>Test Document</title>
                    <version>1.0</version>
                </info>
            </header>
            <records>
                <record><id>1</id><data>A</data></record>
                <record><id>2</id><data>B</data></record>
            </records>
        </root>"#,
        r#"
        tables:
          - name: header
            xml_path: /root/header
            levels: []
            fields:
              - name: title
                xml_path: /root/header/info/title
                data_type: Utf8
              - name: version
                xml_path: /root/header/info/version
                data_type: Utf8
          - name: records
            xml_path: /root/records
            levels: []
            fields:
              - name: id
                xml_path: /root/records/record/id
                data_type: Int32
              - name: data
                xml_path: /root/records/record/data
                data_type: Utf8
        "#,
    );

    // Check header table
    let header_batch = batches.get("header").unwrap();
    assert_eq!(header_batch.num_rows(), 1);
    assert_array_values!(header_batch, "title", &["Test Document"], StringArray);
    assert_array_values!(header_batch, "version", &["1.0"], StringArray);

    // Check records table
    let records_batch = batches.get("records").unwrap();
    assert_eq!(records_batch.num_rows(), 2);
    assert_array_values!(records_batch, "id", &[1, 2], Int32Array);
    assert_array_values!(records_batch, "data", &["A", "B"], StringArray);
}

#[test]
fn test_file_parse_deeply_nested() {
    let batches = parse_xml_file(
        r#"<?xml version="1.0"?>
        <root>
            <level1>
                <level2>
                    <level3>
                        <level4>
                            <level5>
                                <value>deep_value</value>
                            </level5>
                        </level4>
                    </level3>
                </level2>
            </level1>
        </root>"#,
        r#"
        tables:
          - name: deep
            xml_path: /root/level1/level2/level3/level4/level5
            levels: []
            fields:
              - name: value
                xml_path: /root/level1/level2/level3/level4/level5/value
                data_type: Utf8
        "#,
    );

    let batch = batches.get("deep").unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_array_values!(batch, "value", &["deep_value"], StringArray);
}

#[test]
fn test_file_encoding_utf8_bom() {
    // UTF-8 BOM must be written as raw bytes before the XML content
    let mut xml_file = NamedTempFile::new().unwrap();
    xml_file.write_all(&[0xEF, 0xBB, 0xBF]).unwrap(); // UTF-8 BOM
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

#[test]
fn test_file_edge_empty() {
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
fn test_file_edge_whitespace_only() {
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

#[test]
fn test_file_parse_realistic_sensor_data() {
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

    // Check readings
    let readings = batches.get("readings").unwrap();
    assert_eq!(readings.num_rows(), 5);
    // First 3 readings belong to sensor 0, next 2 to sensor 1
    assert_array_values!(readings, "<sensor>", &[0, 0, 0, 1, 1], UInt32Array);
}
