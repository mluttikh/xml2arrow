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

use arrow::array::{
    Array, Float64Array, Int32Array, RecordBatch, RecordBatchReader, StringArray, UInt32Array,
};
use arrow::compute::concat_batches;
use indexmap::IndexMap;
use tempfile::NamedTempFile;
use xml2arrow::{BatchOptions, Config, Parser, TableBatch};

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

    let xml_file = write_xml_tempfile(r#"<data><item><id>1</id><value>1000</value></item></data>"#);

    // Tests Config::from_yaml_file specifically
    let config = Config::from_yaml_file(yaml_file.path()).unwrap();

    let file = File::open(xml_file.path()).unwrap();
    let reader = BufReader::new(file);
    let batches = Parser::new(&config).unwrap().parse(reader).unwrap();
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
    assert!(
        result.is_err(),
        "Missing config file should produce an error"
    );
}

#[test]
fn test_config_reused_across_multiple_files() {
    let config: Config = yaml_serde::from_str(
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
    let batches_a = Parser::new(&config)
        .unwrap()
        .parse(BufReader::new(file_a))
        .unwrap();
    let batch_a = batches_a.get("items").unwrap();
    assert_eq!(batch_a.num_rows(), 1);
    assert_array_values!(batch_a, "value", &[1], Int32Array);

    // Parse second file with the same config
    let file_b = File::open(xml_b.path()).unwrap();
    let batches_b = Parser::new(&config)
        .unwrap()
        .parse(BufReader::new(file_b))
        .unwrap();
    let batch_b = batches_b.get("items").unwrap();
    assert_eq!(batch_b.num_rows(), 2);
    assert_array_values!(batch_b, "value", &[10, 20], Int32Array);
}

#[test]
fn test_reused_parser_isolates_state_across_documents() {
    // A `Parser` compiles the path trie once and is reused for many documents.
    // Each `parse`/`parse_slice` must build fresh state, so results from one
    // document must never leak into the next — including nested tables where a
    // stale builder_stack or row counter would corrupt parent-index foreign
    // keys. This exercises a multi-level config across heterogeneous inputs.
    let config: Config = yaml_serde::from_str(
        r#"
        tables:
          - name: groups
            xml_path: /data
            levels:
              - group
            fields:
              - name: gid
                xml_path: /data/group/@id
                data_type: Int32
          - name: items
            xml_path: /data/group/items
            levels:
              - group
              - item
            fields:
              - name: value
                xml_path: /data/group/items/item/value
                data_type: Int32
        "#,
    )
    .unwrap();

    let parser = Parser::new(&config).unwrap();

    // First document: two groups, with 1 and 2 items respectively.
    let xml_a = r#"<data>
        <group id="1"><items><item><value>10</value></item></items></group>
        <group id="2"><items><item><value>20</value></item><item><value>21</value></item></items></group>
    </data>"#;
    let batches_a = parser.parse_slice(xml_a.as_bytes()).unwrap();
    let groups_a = batches_a.get("groups").unwrap();
    let items_a = batches_a.get("items").unwrap();
    assert_eq!(groups_a.num_rows(), 2);
    assert_array_values!(groups_a, "gid", &[1, 2], Int32Array);
    assert_eq!(items_a.num_rows(), 3);
    assert_array_values!(items_a, "value", &[10, 20, 21], Int32Array);

    // Second, smaller document through the SAME parser. Row counts and parent
    // indices must reflect only this document, proving no carryover.
    let xml_b = r#"<data>
        <group id="7"><items><item><value>99</value></item></items></group>
    </data>"#;
    let batches_b = parser.parse_slice(xml_b.as_bytes()).unwrap();
    let groups_b = batches_b.get("groups").unwrap();
    let items_b = batches_b.get("items").unwrap();
    assert_eq!(groups_b.num_rows(), 1);
    assert_array_values!(groups_b, "gid", &[7], Int32Array);
    assert_eq!(items_b.num_rows(), 1);
    assert_array_values!(items_b, "value", &[99], Int32Array);
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

    let config: Config = yaml_serde::from_str(
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
    let batches = Parser::new(&config).unwrap().parse(reader).unwrap();
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

    let config: Config = yaml_serde::from_str(
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
    let result = Parser::new(&config).unwrap().parse(reader);

    assert!(result.is_ok());
    let batches = result.unwrap();
    let batch = batches.get("items").unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_whitespace_only_file_returns_empty_batch() {
    let xml_file = write_xml_tempfile("   \n\t\n   ");

    let config: Config = yaml_serde::from_str(
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
    let result = Parser::new(&config).unwrap().parse(reader);

    assert!(result.is_ok());
}

// ---------------------------------------------------------------------------
// Streaming (batched) output
// ---------------------------------------------------------------------------
//
// The unit tests in `src/xml_parser.rs` cover the streaming *semantics* over
// in-memory byte slices. What only an integration test reaches is the
// file-based half: a real `File` behind a `BufReader` (the shape the feature
// exists for), BOM handling on the streaming pump, and one compiled `Parser`
// driving several streams — a documented guarantee with no unit coverage.

/// Builds a nested sensor document: `sensors` sensors, each with
/// `readings_per_sensor` readings. Enough rows that small batch thresholds
/// force repeated flushes, and nested enough that the child table's
/// `<sensor>` foreign keys must keep counting across those flush boundaries.
fn sensor_xml(sensors: usize, readings_per_sensor: usize) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?><sensorData><sensors>"#);
    for s in 0..sensors {
        xml.push_str(&format!("<sensor><id>S{s:03}</id><readings>"));
        for r in 0..readings_per_sensor {
            xml.push_str(&format!(
                "<reading><value>{}.5</value></reading>",
                s * 100 + r
            ));
        }
        xml.push_str("</readings></sensor>");
    }
    xml.push_str("</sensors></sensorData>");
    xml
}

const SENSOR_YAML: &str = r#"
    tables:
      - name: sensors
        xml_path: /sensorData/sensors
        levels: [sensor]
        fields:
          - name: id
            xml_path: /sensorData/sensors/sensor/id
            data_type: Utf8
      - name: readings
        xml_path: /sensorData/sensors/sensor/readings
        levels: [sensor, reading]
        fields:
          - name: value
            xml_path: /sensorData/sensors/sensor/readings/reading/value
            data_type: Float64
    "#;

const COUNTER_YAML: &str = r#"
    tables:
      - name: rows
        xml_path: /data
        levels: [row]
        fields:
          - name: v
            xml_path: /data/row/v
            data_type: Int32
    "#;

/// Drives `parse_batches` over a file and groups the batches per table,
/// asserting the "every yielded batch has ≥ 1 row" contract as it goes.
fn stream_file(
    parser: &Parser,
    path: &std::path::Path,
    options: BatchOptions,
) -> IndexMap<String, Vec<RecordBatch>> {
    let reader = BufReader::new(File::open(path).expect("Failed to open temp file"));
    let mut grouped: IndexMap<String, Vec<RecordBatch>> = IndexMap::new();
    for item in parser.parse_batches(reader, options) {
        let TableBatch { table, batch } = item.expect("streamed batch failed");
        assert!(batch.num_rows() > 0, "yielded an empty batch for '{table}'");
        grouped.entry(table.to_string()).or_default().push(batch);
    }
    grouped
}

#[test]
fn test_streaming_from_file_matches_full_parse() {
    // The acceptance property, end-to-end over a real file: concatenating a
    // table's streamed batches reproduces exactly what `parse` returns for it.
    let xml_file = write_xml_tempfile(&sensor_xml(5, 7));
    let config: Config = yaml_serde::from_str(SENSOR_YAML).unwrap();
    let parser = Parser::new(&config).unwrap();

    let full = parser
        .parse(BufReader::new(File::open(xml_file.path()).unwrap()))
        .unwrap();

    let options = BatchOptions::default().with_max_rows_per_batch(3);
    let streamed = stream_file(&parser, xml_file.path(), options);

    // Guard the guard: if the threshold stopped forcing flushes, the
    // equivalence below would hold trivially and prove nothing.
    assert!(
        streamed["readings"].len() > 1,
        "expected several batches, got {}",
        streamed["readings"].len()
    );

    for (name, batches) in &streamed {
        let schema = parser.schema(name).unwrap();
        let concatenated = concat_batches(&schema, batches).unwrap();
        assert_eq!(
            &concatenated,
            full.get(name).unwrap(),
            "streamed batches differ from full parse for table '{name}'"
        );
    }
}

#[test]
fn test_streaming_handles_utf8_bom_file() {
    // BOM stripping is a reader-level concern, so it has to hold on the
    // streaming pump too, not only in `parse`. Same 0xEF 0xBB 0xBF prefix as
    // `test_utf8_bom_file_parsed_correctly`.
    let mut xml_file = NamedTempFile::new().unwrap();
    xml_file.write_all(&[0xEF, 0xBB, 0xBF]).unwrap();
    write!(
        xml_file,
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <data><row><v>42</v></row></data>"#
    )
    .unwrap();

    let config: Config = yaml_serde::from_str(COUNTER_YAML).unwrap();
    let parser = Parser::new(&config).unwrap();
    let streamed = stream_file(&parser, xml_file.path(), BatchOptions::default());

    assert_eq!(streamed["rows"].len(), 1);
    assert_array_values!(&streamed["rows"][0], "v", &[42], Int32Array);
}

#[test]
fn test_parser_reused_across_sequential_streams() {
    // Each stream must build fresh per-parse state. A leaked row counter or
    // builder_stack would show up as wrong `<sensor>` foreign keys on the
    // second document, so assert those and not just row counts.
    let file_a = write_xml_tempfile(&sensor_xml(2, 2));
    let file_b = write_xml_tempfile(&sensor_xml(1, 3));
    let config: Config = yaml_serde::from_str(SENSOR_YAML).unwrap();
    let parser = Parser::new(&config).unwrap();
    let options = BatchOptions::default().with_max_rows_per_batch(2);

    let schema = parser.schema("readings").unwrap();

    let streamed_a = stream_file(&parser, file_a.path(), options);
    let readings_a = concat_batches(&schema, &streamed_a["readings"]).unwrap();
    assert_eq!(readings_a.num_rows(), 4);
    assert_array_values!(&readings_a, "<sensor>", &[0, 0, 1, 1], UInt32Array);

    let streamed_b = stream_file(&parser, file_b.path(), options);
    let readings_b = concat_batches(&schema, &streamed_b["readings"]).unwrap();
    assert_eq!(readings_b.num_rows(), 3);
    assert_array_values!(&readings_b, "<sensor>", &[0, 0, 0], UInt32Array);
    assert_array_values!(&readings_b, "<reading>", &[0, 1, 2], UInt32Array);
}

#[test]
fn test_parser_serves_two_concurrent_streams() {
    // `BatchStream` borrows its `Parser` immutably, which the docs advertise
    // as "one compiled parser can serve many concurrent streams". Two live
    // streams advanced alternately must stay fully independent — each owns its
    // own converter, path tracker and row counters, sharing only the
    // immutable compiled trie.
    let file_a = write_xml_tempfile(
        r#"<data><row><v>1</v></row><row><v>2</v></row><row><v>3</v></row></data>"#,
    );
    let file_b = write_xml_tempfile(r#"<data><row><v>10</v></row><row><v>20</v></row></data>"#);
    let config: Config = yaml_serde::from_str(COUNTER_YAML).unwrap();
    let parser = Parser::new(&config).unwrap();
    // One row per batch, so each `next()` surfaces exactly one row and the
    // interleaving is observable.
    let options = BatchOptions::default().with_max_rows_per_batch(1);

    let mut stream_a =
        parser.parse_batches(BufReader::new(File::open(file_a.path()).unwrap()), options);
    let mut stream_b =
        parser.parse_batches(BufReader::new(File::open(file_b.path()).unwrap()), options);

    let mut values_a = Vec::new();
    let mut values_b = Vec::new();
    loop {
        let next_a = stream_a.next().transpose().unwrap();
        let next_b = stream_b.next().transpose().unwrap();
        if next_a.is_none() && next_b.is_none() {
            break;
        }
        for (batch, sink) in [(next_a, &mut values_a), (next_b, &mut values_b)] {
            if let Some(TableBatch { batch, .. }) = batch {
                let column = batch
                    .column_by_name("v")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                sink.extend(column.values().iter().copied());
            }
        }
    }

    assert_eq!(values_a, vec![1, 2, 3]);
    assert_eq!(values_b, vec![10, 20]);
}

#[test]
fn test_single_table_reader_over_file_exposes_schema_before_parsing() {
    // The `RecordBatchReader` adapter is the integration point for
    // schema-first sinks (`parquet::arrow::ArrowWriter`, DataFusion): the
    // schema must be obtainable before the first batch is read, and be the
    // one every batch then carries.
    let xml_file = write_xml_tempfile(&sensor_xml(3, 4));
    // `sensors` is structural (no fields): it feeds its row counter to the
    // child's `<sensor>` level but produces no output, so this still counts as
    // a single-output-table config — exercised here over a real file.
    let config: Config = yaml_serde::from_str(
        r#"
        tables:
          - name: sensors
            xml_path: /sensorData/sensors
            levels: [sensor]
            fields: []
          - name: readings
            xml_path: /sensorData/sensors/sensor/readings
            levels: [sensor, reading]
            fields:
              - name: value
                xml_path: /sensorData/sensors/sensor/readings/reading/value
                data_type: Float64
        "#,
    )
    .unwrap();
    let parser = Parser::new(&config).unwrap();

    let reader = parser
        .parse_single_table(
            BufReader::new(File::open(xml_file.path()).unwrap()),
            BatchOptions::default().with_max_rows_per_batch(5),
        )
        .unwrap();

    // Read the schema before consuming a single batch, as a writer would.
    let schema = reader.schema();
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["<sensor>", "<reading>", "value"]
    );

    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![5, 5, 2]
    );
    for batch in &batches {
        assert_eq!(batch.schema(), schema);
    }

    let concatenated = concat_batches(&schema, &batches).unwrap();
    let full = parser
        .parse(BufReader::new(File::open(xml_file.path()).unwrap()))
        .unwrap();
    assert_eq!(&concatenated, full.get("readings").unwrap());
}

// ---------------------------------------------------------------------------
// Bug reproductions (fixed defects, pinned end-to-end)
// ---------------------------------------------------------------------------

#[test]
fn test_character_references_in_text_survive_file_roundtrip() {
    // Regression: "&#66;" / "&#x41;" in element text used to resolve to an
    // empty string, silently corrupting extracted values.
    let batches = parse_xml_file(
        r#"<data><item><name>A&#66;C &#x2013; done</name></item></data>"#,
        r#"
        tables:
          - name: items
            xml_path: /data
            levels: []
            fields:
              - name: name
                xml_path: /data/item/name
                data_type: Utf8
        "#,
    );
    let batch = batches.get("items").unwrap();
    assert_array_values!(batch, "name", &["ABC – done"], StringArray);
}

#[test]
fn test_unconfigured_elements_do_not_fabricate_rows() {
    // Regression: an element the config doesn't map, directly under a table
    // path, used to finalize a spurious all-null row — so documents gaining
    // new sibling elements broke row counts (and errored on non-nullable
    // fields).
    let batches = parse_xml_file(
        r#"<data>
            <item><id>1</id></item>
            <schema_v2_extension><stuff>x</stuff></schema_v2_extension>
            <item><id>2</id></item>
        </data>"#,
        r#"
        tables:
          - name: items
            xml_path: /data
            levels: [item]
            fields:
              - name: id
                xml_path: /data/item/id
                data_type: Int32
        "#,
    );
    let batch = batches.get("items").unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_array_values!(batch, "id", &[1, 2], Int32Array);
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

#[test]
fn test_truncated_file_is_rejected_rather_than_silently_short() {
    // Regression: a file cut short mid-element (killed writer, short read,
    // partial download) used to parse to `Ok` with only the rows that had
    // closed before the cut — a partial result indistinguishable from a
    // complete one. File-based because that is how truncation actually
    // reaches users; the buffered reader must surface it just as the
    // zero-copy path does.
    let xml_file =
        write_xml_tempfile(r#"<data><item><id>1</id></item><item><id>2</id></item><item><id>3"#);
    let config: Config = yaml_serde::from_str(
        r#"
        tables:
          - name: items
            xml_path: /data
            levels: [item]
            fields:
              - name: id
                xml_path: /data/item/id
                data_type: Int32
        "#,
    )
    .unwrap();

    let file = File::open(xml_file.path()).unwrap();
    let err = Parser::new(&config)
        .unwrap()
        .parse(BufReader::new(file))
        .unwrap_err();
    assert!(
        matches!(err, xml2arrow::Error::TruncatedInput { .. }),
        "expected TruncatedInput, got: {err}"
    );

    // Opting in returns the two rows that did close, and nothing more.
    let mut lenient = config.clone();
    lenient.parser_options.allow_truncated_input = true;
    let file = File::open(xml_file.path()).unwrap();
    let batches = Parser::new(&lenient)
        .unwrap()
        .parse(BufReader::new(file))
        .unwrap();
    let items = batches.get("items").unwrap();
    assert_eq!(items.num_rows(), 2);
    assert_array_values!(items, "id", &[1, 2], Int32Array);
}

// ---------------------------------------------------------------------------
// Forward compatibility (0.20): builders, deprecated API, config lints
// ---------------------------------------------------------------------------
//
// `Config`, `TableConfig` and `FieldConfig` are `#[non_exhaustive]`, so this
// file — an external crate, like any user's — can only build them through the
// builders or YAML. These tests pin that the builders cover every shape the
// struct literals did, that the deprecated entry points still behave
// identically until 1.0 removes them, and that lints report what the
// transition plan says they report.

#[test]
fn builders_produce_the_same_config_as_yaml() {
    use xml2arrow::config::{DType, FieldConfigBuilder, TableConfig};

    let built = Config::builder()
        .table(
            TableConfig::builder("stations", "/report/stations")
                .field(
                    FieldConfigBuilder::new("id", "/report/stations/station/@id", DType::Int32)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .table(
            TableConfig::builder("measurements", "/report/stations/station/measurements")
                .level("stations")
                .fields([FieldConfigBuilder::new(
                    "value",
                    "/report/stations/station/measurements/measurement/value",
                    DType::Float64,
                )
                .nullable(true)
                .scale(0.001)
                .build()
                .unwrap()])
                .build(),
        )
        .build()
        .unwrap();

    let from_yaml: Config = yaml_serde::from_str(
        r#"
tables:
  - name: stations
    xml_path: /report/stations
    levels: []
    fields:
      - {name: id, path: /report/stations/station/@id, data_type: Int32}
  - name: measurements
    xml_path: /report/stations/station/measurements
    levels: [stations]
    fields:
      - name: value
        path: /report/stations/station/measurements/measurement/value
        data_type: Float64
        nullable: true
        scale: 0.001
"#,
    )
    .unwrap();

    // The YAML uses `path:`, because `FieldConfigBuilder` writes that key.
    // A config spelled with the legacy `xml_path:` parses to an equivalent
    // configuration but not an *equal* one — the two keys are preserved as
    // written, and only path resolution unifies them. `legacy_and_relative_
    // field_paths_agree` covers that equivalence where it matters: the output.
    assert_eq!(built, from_yaml);
}

#[test]
fn config_builder_validates_on_build() {
    use xml2arrow::config::TableConfig;

    // Two tables sharing an xml_path: rejected by `Config::validate`, which
    // `ConfigBuilder::build` runs, so an invalid config is unrepresentable.
    let err = Config::builder()
        .table(TableConfig::builder("a", "/data").build())
        .table(TableConfig::builder("b", "/data").build())
        .build()
        .unwrap_err();
    assert!(
        matches!(err, xml2arrow::Error::InvalidConfig { .. }),
        "expected InvalidConfig, got: {err}"
    );
}

#[test]
#[allow(deprecated)]
fn deprecated_entry_points_still_produce_identical_output() {
    let yaml = r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: value, xml_path: /data/item/value, data_type: Int32}
"#;
    let xml = b"<data><item><value>1</value></item><item><value>2</value></item></data>";
    let config: Config = yaml_serde::from_str(yaml).unwrap();
    let parser = Parser::new(&config).unwrap();

    let expected = parser.parse_slice(xml).unwrap();
    assert_eq!(xml2arrow::parse_xml(&xml[..], &config).unwrap(), expected);
    assert_eq!(xml2arrow::parse_xml_slice(xml, &config).unwrap(), expected);

    // The callback wrapper still drives the stream to completion.
    let mut seen = 0;
    parser
        .parse_streaming(&xml[..], BatchOptions::default(), |table, batch| {
            assert_eq!(table, "items");
            seen += batch.num_rows();
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, 2);
}

#[test]
fn lints_flag_inferred_row_boundaries_without_changing_behavior() {
    use xml2arrow::Lint;

    // A metadata table whose fields sit under two different child elements:
    // each closing child finalizes a row, so this yields two half-filled rows
    // per <header> rather than one row. The lint says so; the parse is
    // unchanged.
    let config: Config = yaml_serde::from_str(
        r#"
tables:
  - name: header
    xml_path: /report/header
    levels: []
    fields:
      - {name: title, xml_path: /report/header/title, data_type: Utf8, nullable: true}
      - {name: created, xml_path: /report/header/created, data_type: Utf8, nullable: true}
"#,
    )
    .unwrap();
    let parser = Parser::new(&config).unwrap();

    let warnings = parser.warnings();
    assert!(
        warnings.iter().any(|lint| matches!(
            lint,
            Lint::InferredRowBoundary { table, child_elements, .. }
                if table == "header" && child_elements.len() == 2
        )),
        "expected an InferredRowBoundary lint, got: {warnings:?}"
    );

    let batches = parser
        .parse_slice(b"<report><header><title>T</title><created>C</created></header></report>")
        .unwrap();
    assert_eq!(
        batches.get("header").unwrap().num_rows(),
        2,
        "the lint is advisory: behavior must be unchanged"
    );
}

// ---------------------------------------------------------------------------
// Diagnostics (0.20): error coordinates, unmatched fields, value size cap
// ---------------------------------------------------------------------------

#[test]
fn value_errors_carry_row_and_byte_coordinates() {
    use xml2arrow::errors::Error;

    let config: Config = yaml_serde::from_str(
        r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: v, xml_path: /data/item/v, data_type: Int32}
"#,
    )
    .unwrap();
    let parser = Parser::new(&config).unwrap();

    // The third row is the bad one: the coordinate must say so, because "which
    // row" is the question a user asks first when a 10-million-row parse fails.
    let xml = b"<data><item><v>1</v></item><item><v>2</v></item><item><v>x</v></item></data>";
    let err = parser.parse_slice(xml).unwrap_err();
    let Error::ParseError { location, .. } = &err else {
        panic!("expected ParseError, got: {err}");
    };
    assert_eq!(location.row, Some(2));
    assert!(location.position.is_some(), "byte offset missing: {err}");
    assert!(err.to_string().contains("row index 2"), "{err}");

    // Every entry point annotates identically — the streaming path reports the
    // same coordinates even though it has already yielded batches.
    let streamed = parser
        .parse_batches_slice(xml, BatchOptions::default().with_max_rows_per_batch(1))
        .find_map(Result::err)
        .expect("stream must surface the error");
    assert_eq!(streamed.to_string(), err.to_string());
}

#[test]
fn unmatched_fields_are_reported_together_when_enabled() {
    use xml2arrow::errors::Error;

    let yaml = r#"
parser_options:
  error_on_unmatched_fields: true
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: v, xml_path: /data/item/v, data_type: Int32}
      - {name: typo, xml_path: /data/item/valeu, data_type: Int32, nullable: true}
"#;
    let config: Config = yaml_serde::from_str(yaml).unwrap();
    let err = Parser::new(&config)
        .unwrap()
        .parse_slice(b"<data><item><v>1</v></item></data>")
        .unwrap_err();
    let Error::UnmatchedFields { fields, .. } = &err else {
        panic!("expected UnmatchedFields, got: {err}");
    };
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].field, "typo");
    assert_eq!(fields[0].xml_path, "/data/item/valeu");

    // Off by default: the same config without the flag parses fine, which is
    // what keeps existing configs working.
    let lenient: Config =
        yaml_serde::from_str(&yaml.replace("  error_on_unmatched_fields: true\n", "")).unwrap();
    assert!(
        Parser::new(&lenient)
            .unwrap()
            .parse_slice(b"<data><item><v>1</v></item></data>")
            .is_ok()
    );
}

#[test]
fn max_value_bytes_bounds_accumulation_across_events() {
    use xml2arrow::errors::Error;

    // One value split across text, entity and CDATA events: the cap applies to
    // the accumulated total, which is the whole point — a single event's size
    // is not the bound that matters.
    let config: Config = yaml_serde::from_str(
        r#"
parser_options:
  max_value_bytes: 16
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: s, xml_path: /data/item/s, data_type: Utf8}
"#,
    )
    .unwrap();
    let parser = Parser::new(&config).unwrap();

    assert!(
        parser
            .parse_slice(b"<data><item><s>short</s></item></data>")
            .is_ok()
    );

    let split = b"<data><item><s>aaaaaaaa&amp;<![CDATA[bbbbbbbbbbbb]]></s></item></data>";
    let err = parser.parse_slice(split).unwrap_err();
    let Error::ValueTooLarge { field, limit, .. } = &err else {
        panic!("expected ValueTooLarge, got: {err}");
    };
    assert_eq!(&**field, "s");
    assert_eq!(*limit, 16);
}

// ---------------------------------------------------------------------------
// Owned streams (0.21): a stream that outlives the scope it was created in
// ---------------------------------------------------------------------------
//
// The borrowing entry points cannot express any of these: a `BatchStream` that
// borrows its parser is confined to the scope holding the parser, which is what
// pushes FFI and async wrappers into a producer thread plus a channel.

#[test]
fn an_owned_stream_can_be_returned_from_the_scope_that_built_it() {
    use std::io::Cursor;

    // The parser is created and dropped inside the function; the stream it
    // produced keeps working, which is the whole point.
    fn build() -> impl Iterator<Item = xml2arrow::Result<TableBatch>> {
        let config: Config = yaml_serde::from_str(
            r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: v, xml_path: /data/item/v, data_type: Int32}
"#,
        )
        .unwrap();
        let xml = b"<data><item><v>1</v></item><item><v>2</v></item></data>".to_vec();
        Parser::new(&config)
            .unwrap()
            .into_batches(Cursor::new(xml), BatchOptions::default())
    }

    let rows: usize = build().map(|b| b.unwrap().batch.num_rows()).sum();
    assert_eq!(rows, 2);
}

#[test]
fn an_owned_stream_can_be_sent_to_another_thread() {
    use std::io::Cursor;

    let config: Config = yaml_serde::from_str(
        r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: v, xml_path: /data/item/v, data_type: Int32}
"#,
    )
    .unwrap();
    let parser = Parser::new(&config).unwrap();
    let xml = b"<data><item><v>1</v></item><item><v>2</v></item><item><v>3</v></item></data>";

    let stream = parser
        .clone()
        .into_batches(Cursor::new(xml.to_vec()), BatchOptions::default());
    let rows = std::thread::spawn(move || {
        stream
            .map(|item| item.unwrap().batch.num_rows())
            .sum::<usize>()
    })
    .join()
    .unwrap();
    assert_eq!(rows, 3);

    // Cloning is a handle copy, so the original parser is untouched.
    assert_eq!(parser.parse_slice(xml).unwrap()["items"].num_rows(), 3);
}

#[test]
fn owned_and_borrowed_streams_produce_identical_output() {
    use std::io::Cursor;

    let config: Config = yaml_serde::from_str(
        r#"
tables:
  - name: stations
    xml_path: /report/stations
    levels: []
    fields:
      - {name: name, xml_path: /report/stations/station/name, data_type: Utf8}
  - name: readings
    xml_path: /report/stations/station/readings
    levels: [station]
    fields:
      - {name: v, xml_path: /report/stations/station/readings/reading/v, data_type: Int32}
"#,
    )
    .unwrap();
    let parser = Parser::new(&config).unwrap();
    let xml = br#"<report><stations>
        <station><name>a</name><readings><reading><v>1</v></reading><reading><v>2</v></reading></readings></station>
        <station><name>b</name><readings><reading><v>3</v></reading></readings></station>
    </stations></report>"#;
    let options = BatchOptions::default().with_max_rows_per_batch(1);

    let borrowed: Vec<(String, usize)> = parser
        .parse_batches(&xml[..], options)
        .map(|item| {
            let b = item.unwrap();
            (b.table.to_string(), b.batch.num_rows())
        })
        .collect();
    let owned: Vec<(String, usize)> = parser
        .clone()
        .into_batches(Cursor::new(xml.to_vec()), options)
        .map(|item| {
            let b = item.unwrap();
            (b.table.to_string(), b.batch.num_rows())
        })
        .collect();

    assert_eq!(borrowed, owned);
    assert!(!borrowed.is_empty());
}

#[test]
fn an_owned_single_table_reader_exposes_its_schema_and_streams() {
    use arrow::array::RecordBatchReader;
    use std::io::Cursor;

    let config: Config = yaml_serde::from_str(
        r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: v, xml_path: /data/item/v, data_type: Int32}
"#,
    )
    .unwrap();
    let parser = Parser::new(&config).unwrap();
    let xml = b"<data><item><v>1</v></item><item><v>2</v></item></data>".to_vec();

    let reader = parser
        .clone()
        .into_single_table(
            Cursor::new(xml),
            BatchOptions::default().with_max_rows_per_batch(1),
        )
        .unwrap();
    assert_eq!(reader.schema(), parser.schema("items").unwrap());

    // A RecordBatchReader is what ArrowWriter and pyarrow's C stream take, and
    // both want to own it.
    let boxed: Box<dyn RecordBatchReader + Send> = Box::new(reader);
    let rows: usize = boxed.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, 2);
}

#[test]
fn a_parser_is_send_and_sync_and_shares_compiled_state() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Parser>();

    let config: Config = yaml_serde::from_str(
        r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: v, xml_path: /data/item/v, data_type: Int32}
"#,
    )
    .unwrap();
    let parser = Parser::new(&config).unwrap();
    let xml = b"<data><item><v>7</v></item></data>";

    // One compiled config, several threads, no rebuild per thread.
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let parser = parser.clone();
            std::thread::spawn(move || parser.parse_slice(xml).unwrap()["items"].num_rows())
        })
        .collect();
    for handle in handles {
        assert_eq!(handle.join().unwrap(), 1);
    }
}

#[test]
fn unmatched_field_detection_explains_itself_when_stopping_early() {
    use xml2arrow::errors::Error;

    // `stop_at_paths` guarantees that everything below the stop path captures
    // nothing, so strict detection reports it. The report is accurate; the
    // default advice ("check the xml_path spellings") is not, so the message
    // has to name the real cause.
    let config: Config = yaml_serde::from_str(
        r#"
parser_options:
  stop_at_paths: [/data/header]
  error_on_unmatched_fields: true
tables:
  - name: header
    xml_path: /data/header
    levels: []
    fields: [{name: title, xml_path: /data/header/title, data_type: Utf8}]
  - name: items
    xml_path: /data/items
    levels: []
    fields: [{name: v, xml_path: /data/items/item/v, data_type: Int32}]
"#,
    )
    .unwrap();
    let xml = b"<data><header><title>t</title></header><items><item><v>1</v></item></items></data>";

    let err = Parser::new(&config).unwrap().parse_slice(xml).unwrap_err();
    let Error::UnmatchedFields {
        fields,
        stop_paths_configured,
    } = &err
    else {
        panic!("expected UnmatchedFields, got: {err}");
    };
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].field, "v");
    assert!(stop_paths_configured);
    assert!(err.to_string().contains("stop_at_paths"), "{err}");

    // Without stop paths the wording stays as it was — the hint is only added
    // where it applies. (Dropping the stop path from *this* config would make
    // the field match, so the comparison needs a genuinely misspelled path.)
    let misspelled: Config = yaml_serde::from_str(
        r#"
parser_options:
  error_on_unmatched_fields: true
tables:
  - name: items
    xml_path: /data
    levels: []
    fields: [{name: v, xml_path: /data/item/valeu, data_type: Int32, nullable: true}]
"#,
    )
    .unwrap();
    let err = Parser::new(&misspelled)
        .unwrap()
        .parse_slice(b"<data><item><value>1</value></item></data>")
        .unwrap_err();
    assert!(
        err.to_string().contains("check the xml_path spellings"),
        "{err}"
    );
    assert!(!err.to_string().contains("stop_at_paths"), "{err}");
}

#[test]
fn the_value_cap_does_not_carry_over_between_rows() {
    // Exceeding the cap is fatal today, so this asserts the reachable half:
    // the row that trips it is the row reported. The reset in `end_row` is what
    // keeps that true if a future value policy makes the error recoverable.
    use xml2arrow::errors::Error;

    let config: Config = yaml_serde::from_str(
        r#"
parser_options:
  max_value_bytes: 4
tables:
  - name: items
    xml_path: /data
    levels: []
    fields: [{name: s, xml_path: /data/item/s, data_type: Utf8}]
"#,
    )
    .unwrap();
    let parser = Parser::new(&config).unwrap();

    // Three small rows then an oversized one: the error names row 3, so no
    // earlier row was affected by the field's state.
    let xml = b"<data><item><s>a</s></item><item><s>b</s></item><item><s>c</s></item><item><s>toolong</s></item></data>";
    let err = parser.parse_slice(xml).unwrap_err();
    let Error::ValueTooLarge { location, .. } = &err else {
        panic!("expected ValueTooLarge, got: {err}");
    };
    assert_eq!(location.row, Some(3));

    // And a document that never trips it parses clean.
    assert_eq!(
        parser
            .parse_slice(b"<data><item><s>a</s></item><item><s>b</s></item></data>")
            .unwrap()["items"]
            .num_rows(),
        2
    );
}
