use std::time::Instant;
use xml2arrow::{Config, parse_xml};

/// Generate a realistic XML file structure matching the user's use case:
/// - Header section with metadata (strings, timestamps, integers)
/// - Deeply nested data structure (3-4 levels)
/// - Millions of floating point measurements
/// - Mixed with some integer and string fields
fn create_realistic_xml(num_measurements: usize, num_sensors: usize) -> String {
    let mut xml = String::with_capacity(num_measurements * 200 + 10000);

    xml.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>
<document>
    <header>
        <creation_time>2024-11-30T10:15:30Z</creation_time>
        <document_type>TimeSeriesData</document_type>
        <document_type_version>2.1.4</document_type_version>
        <creator>DataAcquisitionSystem</creator>
        <creator_version>5.2.1</creator_version>
        <site_id>12345</site_id>
        <facility_name>Manufacturing Plant A</facility_name>
        <comments>
            <comment timestamp="2024-11-30T09:00:00Z" user="operator1">Data collection started</comment>
            <comment timestamp="2024-11-30T09:15:00Z" user="operator1">Sensor calibration verified</comment>
            <comment timestamp="2024-11-30T10:00:00Z" user="supervisor">Quality check passed</comment>
        </comments>
        <sampling_rate_hz>1000</sampling_rate_hz>
        <number_of_sensors>"#);
    xml.push_str(&num_sensors.to_string());
    xml.push_str(
        r#"</number_of_sensors>
        <total_measurements>"#,
    );
    xml.push_str(&(num_measurements * num_sensors).to_string());
    xml.push_str(
        r#"</total_measurements>
    </header>
    <data>
        <sensors>
"#,
    );

    // Generate nested sensor data with millions of float measurements
    for sensor_id in 0..num_sensors {
        xml.push_str(&format!(
            r#"            <sensor id="SENSOR_{:04}" type="pressure" unit="pascal" calibration_date="2024-11-01">
                <metadata>
                    <location>Zone-{}</location>
                    <manufacturer>SensorCorp</manufacturer>
                    <model>PC-{}00</model>
                    <serial_number>SN{:08}</serial_number>
                    <calibration_coefficient>{:.6}</calibration_coefficient>
                    <offset>{:.6}</offset>
                    <last_maintenance_hours>{}</last_maintenance_hours>
                </metadata>
                <measurements>
"#,
            sensor_id,
            sensor_id % 10,
            sensor_id % 50 + 10,
            sensor_id * 1000 + 12345678,
            1.0 + (sensor_id as f64) * 0.001,
            (sensor_id as f64) * 0.01,
            sensor_id * 100 + 500
        ));

        // Generate measurements - this is where the bulk of the floats are
        for measurement_idx in 0..num_measurements {
            let timestamp_ms = measurement_idx as u64;
            let value =
                101325.0 + (measurement_idx as f64).sin() * 5000.0 + (sensor_id as f64) * 100.0;
            let temperature = 20.0 + (measurement_idx as f64 / 100.0).sin() * 5.0;
            let quality = if measurement_idx % 97 == 0 { 1 } else { 0 }; // Occasional quality flag

            xml.push_str(&format!(
                r#"                    <measurement timestamp_ms="{}" quality="{}">
                        <value>{:.6}</value>
                        <temperature>{:.3}</temperature>
                    </measurement>
"#,
                timestamp_ms, quality, value, temperature
            ));
        }

        xml.push_str(
            r#"                </measurements>
            </sensor>
"#,
        );
    }

    xml.push_str(
        r#"        </sensors>
    </data>
</document>"#,
    );

    xml
}

fn create_realistic_config() -> Config {
    serde_yaml::from_str(
        r#"
parser_options:
  trim_text: false
tables:
  - name: root
    xml_path: /
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
      - name: document_type_version
        xml_path: /document/header/document_type_version
        data_type: Utf8
        nullable: false
      - name: creator
        xml_path: /document/header/creator
        data_type: Utf8
        nullable: false
      - name: creator_version
        xml_path: /document/header/creator_version
        data_type: Utf8
        nullable: false
      - name: site_id
        xml_path: /document/header/site_id
        data_type: Int32
        nullable: false
      - name: facility_name
        xml_path: /document/header/facility_name
        data_type: Utf8
        nullable: false
      - name: sampling_rate_hz
        xml_path: /document/header/sampling_rate_hz
        data_type: Int32
        nullable: false
      - name: number_of_sensors
        xml_path: /document/header/number_of_sensors
        data_type: Int32
        nullable: false
      - name: total_measurements
        xml_path: /document/header/total_measurements
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
    .expect("Failed to parse config")
}

fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║      XML2Arrow Realistic Benchmark - High-Volume Float Data             ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Simulating industrial time-series data:");
    println!("  • Deeply nested structure (3-4 levels)");
    println!("  • Header metadata (strings, integers, timestamps)");
    println!("  • Millions of float measurements");
    println!("  • Mixed data types (Float64, Int32, UInt64, Utf8)");
    println!();

    let test_cases = vec![
        ("Small - 1K floats/sensor, 2 sensors", 1_000, 2, 20),
        ("Medium - 10K floats/sensor, 5 sensors", 10_000, 5, 5),
        ("Large - 100K floats/sensor, 10 sensors", 100_000, 10, 2),
        ("X-Large - 500K floats/sensor, 5 sensors", 500_000, 5, 1),
    ];

    for (name, measurements_per_sensor, num_sensors, iterations) in test_cases {
        let total_measurements = measurements_per_sensor * num_sensors;

        println!("┌──────────────────────────────────────────────────────────────────────────┐");
        println!("│ Test case: {:<62} │", name);
        println!("└──────────────────────────────────────────────────────────────────────────┘");

        print!("  Generating XML... ");
        std::io::Write::flush(&mut std::io::stdout()).unwrap();

        let gen_start = Instant::now();
        let xml = create_realistic_xml(measurements_per_sensor, num_sensors);
        let gen_time = gen_start.elapsed();

        println!("done in {:.2}s", gen_time.as_secs_f64());
        println!("  XML size: {}", format_size(xml.len()));
        println!("  Total measurements: {}", total_measurements);
        println!("  Float values: {}", total_measurements * 2); // value + temperature

        let config = create_realistic_config();

        // Warm-up
        print!("  Warming up... ");
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
        for _ in 0..2 {
            let _ = parse_xml(xml.as_bytes(), &config).unwrap();
        }
        println!("done");

        // Actual benchmark
        print!("  Benchmarking ({} iterations)... ", iterations);
        std::io::Write::flush(&mut std::io::stdout()).unwrap();

        let start = Instant::now();
        for _ in 0..iterations {
            let _ = parse_xml(xml.as_bytes(), &config).unwrap();
        }
        let duration = start.elapsed();
        println!("done");

        let avg_ms = duration.as_secs_f64() * 1000.0 / iterations as f64;
        let throughput_floats = (total_measurements as f64 * 2.0 / avg_ms) * 1000.0; // *2 for value+temp
        let throughput_rows = (total_measurements as f64 / avg_ms) * 1000.0;
        let mb_per_sec = (xml.len() as f64 / (1024.0 * 1024.0)) / (avg_ms / 1000.0);

        println!();
        println!("  Results:");
        println!("    Average parse time:   {:.2} ms", avg_ms);
        println!(
            "    Throughput:           {:.0} floats/sec",
            throughput_floats
        );
        println!(
            "    Row throughput:       {:.0} measurements/sec",
            throughput_rows
        );
        println!("    XML throughput:       {:.2} MB/sec", mb_per_sec);
        println!(
            "    Time per float:       {:.2} ns",
            avg_ms * 1_000_000.0 / (total_measurements as f64 * 2.0)
        );
        println!();
    }

    println!("╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║                         Benchmark Complete                               ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Note: These benchmarks use synthetic data. For best results,");
    println!("      profile with your actual production XML files.");
}
