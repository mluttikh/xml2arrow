use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::time::Duration;
use xml2arrow::{Config, parse_xml};

/// Generate realistic XML matching industrial use case
fn generate_realistic_xml(num_measurements: usize, num_sensors: usize) -> String {
    let mut xml = String::with_capacity(num_measurements * 200 + 10000);

    xml.push_str(
        r#"<?xml version="1.0" encoding="UTF-8"?>
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
        <number_of_sensors>"#,
    );
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

        for measurement_idx in 0..num_measurements {
            let timestamp_ms = measurement_idx as u64;
            let value =
                101325.0 + (measurement_idx as f64).sin() * 5000.0 + (sensor_id as f64) * 100.0;
            let temperature = 20.0 + (measurement_idx as f64 / 100.0).sin() * 5.0;
            let quality = if measurement_idx % 97 == 0 { 1 } else { 0 };

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

fn get_config() -> Config {
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

fn bench_parse_small(c: &mut Criterion) {
    let xml = generate_realistic_xml(1_000, 2);
    let config = get_config();
    let size_bytes = xml.len();

    let mut group = c.benchmark_group("parse_small");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(size_bytes as u64));

    group.bench_with_input(
        BenchmarkId::new(
            "1K_measurements_2_sensors",
            format!("{}KB", size_bytes / 1024),
        ),
        &xml,
        |b, xml| {
            b.iter(|| {
                let result = parse_xml(xml.as_bytes(), &config);
                result.unwrap()
            });
        },
    );

    group.finish();
}

fn bench_parse_medium(c: &mut Criterion) {
    let xml = generate_realistic_xml(10_000, 5);
    let config = get_config();
    let size_bytes = xml.len();

    let mut group = c.benchmark_group("parse_medium");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(15));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Bytes(size_bytes as u64));

    group.bench_with_input(
        BenchmarkId::new(
            "10K_measurements_5_sensors",
            format!("{}MB", size_bytes / (1024 * 1024)),
        ),
        &xml,
        |b, xml| {
            b.iter(|| {
                let result = parse_xml(xml.as_bytes(), &config);
                result.unwrap()
            });
        },
    );

    group.finish();
}

fn bench_parse_large(c: &mut Criterion) {
    let xml = generate_realistic_xml(100_000, 10);
    let config = get_config();
    let size_bytes = xml.len();

    let mut group = c.benchmark_group("parse_large");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(10));
    group.throughput(Throughput::Bytes(size_bytes as u64));

    group.bench_with_input(
        BenchmarkId::new(
            "100K_measurements_10_sensors",
            format!("{}MB", size_bytes / (1024 * 1024)),
        ),
        &xml,
        |b, xml| {
            b.iter(|| {
                let result = parse_xml(xml.as_bytes(), &config);
                result.unwrap()
            });
        },
    );

    group.finish();
}

fn bench_parse_xlarge(c: &mut Criterion) {
    let xml = generate_realistic_xml(200_000, 5);
    let config = get_config();
    let size_bytes = xml.len();

    let mut group = c.benchmark_group("parse_xlarge");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(40));
    group.warm_up_time(Duration::from_secs(10));
    group.throughput(Throughput::Bytes(size_bytes as u64));

    group.bench_with_input(
        BenchmarkId::new(
            "200K_measurements_5_sensors",
            format!("{}MB", size_bytes / (1024 * 1024)),
        ),
        &xml,
        |b, xml| {
            b.iter(|| {
                let result = parse_xml(xml.as_bytes(), &config);
                result.unwrap()
            });
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_small,
    bench_parse_medium,
    bench_parse_large,
    bench_parse_xlarge
);
criterion_main!(benches);
