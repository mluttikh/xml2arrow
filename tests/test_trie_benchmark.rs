use xml2arrow::{Config, parse_xml};

#[cfg(feature = "trie_parser")]
#[test]
fn test_benchmark_structure() {
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<document>
    <header>
        <creation_time>2024-11-30T10:15:30Z</creation_time>
        <document_type>TimeSeriesData</document_type>
        <site_id>12345</site_id>
        <comments>
            <comment timestamp="2024-11-30T09:00:00Z" user="operator1">Data collection started</comment>
            <comment timestamp="2024-11-30T09:15:00Z" user="operator1">Sensor calibration verified</comment>
        </comments>
        <sampling_rate_hz>1000</sampling_rate_hz>
    </header>
    <data>
        <sensors>
            <sensor id="SENSOR_0001" type="pressure" unit="pascal" calibration_date="2024-11-01">
                <metadata>
                    <location>Zone-0</location>
                    <manufacturer>SensorCorp</manufacturer>
                    <model>PC-1000</model>
                    <serial_number>SN12345678</serial_number>
                    <calibration_coefficient>1.001000</calibration_coefficient>
                    <offset>0.000000</offset>
                    <last_maintenance_hours>500</last_maintenance_hours>
                </metadata>
                <measurements>
                    <measurement timestamp_ms="0" quality="0">
                        <value>101325.000000</value>
                        <temperature>20.000</temperature>
                    </measurement>
                    <measurement timestamp_ms="1" quality="0">
                        <value>101325.841471</value>
                        <temperature>20.010</temperature>
                    </measurement>
                </measurements>
            </sensor>
            <sensor id="SENSOR_0002" type="pressure" unit="pascal" calibration_date="2024-11-01">
                <metadata>
                    <location>Zone-1</location>
                    <manufacturer>SensorCorp</manufacturer>
                    <model>PC-1100</model>
                    <serial_number>SN12346678</serial_number>
                    <calibration_coefficient>1.002000</calibration_coefficient>
                    <offset>0.010000</offset>
                    <last_maintenance_hours>600</last_maintenance_hours>
                </metadata>
                <measurements>
                    <measurement timestamp_ms="0" quality="0">
                        <value>101425.000000</value>
                        <temperature>20.500</temperature>
                    </measurement>
                </measurements>
            </sensor>
        </sensors>
    </data>
</document>"#;

    let config: Config = serde_yaml::from_str(
        r#"
parser_options:
  trim_text: false
tables:
  - name: root
    xml_path: /document
    levels: []
    fields:
      - name: creation_time
        xml_path: /document/header/creation_time
        data_type: Utf8
        nullable: false
      - name: document_type
        xml_path: /document/header/document_type
        data_type: Utf8
        nullable: false
      - name: site_id
        xml_path: /document/header/site_id
        data_type: Int32
        nullable: false
      - name: sampling_rate_hz
        xml_path: /document/header/sampling_rate_hz
        data_type: Int32
        nullable: false
  - name: comments
    xml_path: /document/header/comments
    levels:
      - comment
    fields:
      - name: timestamp
        xml_path: /document/header/comments/comment/@timestamp
        data_type: Utf8
        nullable: false
      - name: user
        xml_path: /document/header/comments/comment/@user
        data_type: Utf8
        nullable: false
      - name: text
        xml_path: /document/header/comments/comment
        data_type: Utf8
        nullable: false
  - name: sensors
    xml_path: /document/data/sensors
    levels:
      - sensor
    fields:
      - name: sensor_id
        xml_path: /document/data/sensors/sensor/@id
        data_type: Utf8
        nullable: false
      - name: sensor_type
        xml_path: /document/data/sensors/sensor/@type
        data_type: Utf8
        nullable: false
      - name: unit
        xml_path: /document/data/sensors/sensor/@unit
        data_type: Utf8
        nullable: false
      - name: calibration_date
        xml_path: /document/data/sensors/sensor/@calibration_date
        data_type: Utf8
        nullable: false
      - name: location
        xml_path: /document/data/sensors/sensor/metadata/location
        data_type: Utf8
        nullable: false
      - name: manufacturer
        xml_path: /document/data/sensors/sensor/metadata/manufacturer
        data_type: Utf8
        nullable: false
      - name: model
        xml_path: /document/data/sensors/sensor/metadata/model
        data_type: Utf8
        nullable: false
      - name: serial_number
        xml_path: /document/data/sensors/sensor/metadata/serial_number
        data_type: Utf8
        nullable: false
      - name: calibration_coefficient
        xml_path: /document/data/sensors/sensor/metadata/calibration_coefficient
        data_type: Float64
        nullable: false
      - name: offset
        xml_path: /document/data/sensors/sensor/metadata/offset
        data_type: Float64
        nullable: false
      - name: last_maintenance_hours
        xml_path: /document/data/sensors/sensor/metadata/last_maintenance_hours
        data_type: Int32
        nullable: false
  - name: measurements
    xml_path: /document/data/sensors/sensor/measurements
    levels:
      - sensor
      - measurement
    fields:
      - name: timestamp_ms
        xml_path: /document/data/sensors/sensor/measurements/measurement/@timestamp_ms
        data_type: UInt64
        nullable: false
      - name: quality
        xml_path: /document/data/sensors/sensor/measurements/measurement/@quality
        data_type: Int8
        nullable: false
      - name: value
        xml_path: /document/data/sensors/sensor/measurements/measurement/value
        data_type: Float64
        nullable: false
      - name: temperature
        xml_path: /document/data/sensors/sensor/measurements/measurement/temperature
        data_type: Float64
        nullable: false
"#,
    )
    .expect("Failed to parse config");

    let result = parse_xml(xml_content.as_bytes(), &config);

    match &result {
        Ok(batches) => {
            println!("\n=== Successful Parse ===");
            for (name, batch) in batches {
                println!("\nTable: {}", name);
                println!("  Rows: {}", batch.num_rows());
                println!("  Columns: {}", batch.num_columns());
                let schema = batch.schema();
                for i in 0..batch.num_columns() {
                    let col = batch.column(i);
                    let field = schema.field(i);
                    println!(
                        "    {} ({}): {} values",
                        field.name(),
                        field.data_type(),
                        col.len()
                    );
                }
            }
        }
        Err(e) => {
            println!("\n=== Parse Error ===");
            println!("Error: {:?}", e);
        }
    }

    let batches = result.expect("Parse should succeed");

    // Validate root table
    let root = batches.get("root").expect("root table should exist");
    assert_eq!(root.num_rows(), 1);

    // Validate comments table
    let comments = batches
        .get("comments")
        .expect("comments table should exist");
    assert_eq!(comments.num_rows(), 2);

    // Validate sensors table
    let sensors = batches.get("sensors").expect("sensors table should exist");
    assert_eq!(sensors.num_rows(), 2, "Should have 2 sensor rows");

    // Validate measurements table
    let measurements = batches
        .get("measurements")
        .expect("measurements table should exist");
    assert_eq!(
        measurements.num_rows(),
        3,
        "Should have 3 measurement rows total (2 from sensor 1, 1 from sensor 2)"
    );
}
