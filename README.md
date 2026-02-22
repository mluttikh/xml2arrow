[![Build Status](https://github.com/mluttikh/xml2arrow/actions/workflows/ci.yml/badge.svg)](https://github.com/mluttikh/xml2arrow/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/xml2arrow)](https://crates.io/crates/xml2arrow)
[![crates.io](https://img.shields.io/crates/d/xml2arrow)](https://crates.io/crates/xml2arrow)
[![docs.rs](https://docs.rs/xml2arrow/badge.svg)](https://docs.rs/xml2arrow)
[![Python](https://img.shields.io/badge/python-xml2arrow-blue.svg?style=flat&logo=python)](https://github.com/mluttikh/xml2arrow-python)
[![License](https://img.shields.io/crates/l/xml2arrow)](LICENSE)

# xml2arrow

A Rust crate for efficiently converting XML data to Apache Arrow format.

A Python version of this library is also available on GitHub: [https://github.com/mluttikh/xml2arrow-python](https://github.com/mluttikh/xml2arrow-python)

## Overview

`xml2arrow` transforms XML documents into Apache Arrow `RecordBatch`es. It uses
[quick-xml](https://github.com/tafia/quick-xml) for single-pass streaming XML
parsing and the [arrow](https://github.com/apache/arrow-rs) crate for building
columnar data structures. The mapping from XML paths to Arrow fields is defined in
a YAML configuration file, making it straightforward to extract nested or
attribute-heavy XML into flat, typed tables ready for analytics pipelines.

## Installation

Add the following to your `Cargo.toml`:

```toml
[dependencies]
xml2arrow = "0.15.0"
```

## Features

- 🚀 **High-performance** single-pass XML parsing via [quick-xml](https://github.com/tafia/quick-xml)
- 📊 **Declarative mapping** from XML structures to Arrow tables using a YAML config file
- 🔄 **Nested structure support** with parent–child index columns linking related tables
- 🎯 **Type conversion** including automatic scale and offset transforms for float fields
- 💡 **Attribute and element extraction** using `@`-prefixed path segments for attributes
- ⏹️ **Early termination** via `stop_at_paths` for efficiently reading only part of a file

## Usage

### 1. Write a configuration file

The YAML configuration defines which parts of the XML document become tables and
how their fields are typed. The full schema is:

```yaml
parser_options:
  trim_text: <true|false>      # Trim whitespace from text nodes (default: false)
  stop_at_paths: [<xml_path>]  # Stop parsing after these closing tags (optional,
                               # useful for reading only a file header)
tables:
  - name: <table_name>         # Name of the resulting Arrow RecordBatch
    xml_path: <xml_path>       # Path to the element whose children are rows.
                               # Use "/" to treat the whole document as one row.
    levels: [<level>, ...]     # Parent-link index columns — see "Nested tables"
    fields:
      - name: <field_name>     # Arrow column name
        xml_path: <field_path> # Path to the element or attribute holding the value.
                               # Prefix the last segment with @ for attributes
                               # (e.g. /library/book/@id)
        data_type: <type>      # Arrow data type — see supported types below
        nullable: <true|false> # Whether the field can be null (default: false)
                               # If false, missing/empty tags cause a ParseError.
        scale: <number>        # Multiply float values by this factor (optional)
        offset: <number>       # Add this value to float values after scaling (optional)
                               # value = (value * scale) + offset
```

**Supported data types:** `Boolean`, `Int8`, `UInt8`, `Int16`, `UInt16`, `Int32`,
`UInt32`, `Int64`, `UInt64`, `Float32`, `Float64`, `Utf8`

`Boolean` fields accept (case-insensitively): `true`, `false`, `1`, `0`, `yes`,
`no`, `on`, `off`, `t`, `f`, `y`, `n`.

### 2. Nested tables and `levels`

When your XML has a parent–child relationship between tables, `levels` creates the
index columns that link child rows back to their parent rows. Each string in the
list names an element at a nesting boundary above the row element, and generates a
zero-based `UInt32` column named `<level>` in the output.

*Note: If a table is defined purely to establish a structural hierarchy (i.e., it
has levels defined but an empty fields list), it acts only as a boundary and will
be excluded from the final output map.*

For example, given stations that each have multiple measurements:

```xml
<report>
  <monitoring_stations>
    <monitoring_station>   <!-- boundary → produces <station> index -->
      <measurements>
        <measurement>      <!-- row element for the measurements table -->
          ...
        </measurement>
      </measurements>
    </monitoring_station>
  </monitoring_stations>
</report>
```

```yaml
- name: measurements
  xml_path: /report/monitoring_stations/monitoring_station/measurements
  levels: [station, measurement]
  fields: [...]
```

This produces a `<station>` column (which parent station each measurement belongs
to) and a `<measurement>` column (the per-station row counter), letting you join
the `measurements` table back to the `stations` table on `<station>`.

### 3. Parse the XML

```rust
use std::fs::File;
use std::io::BufReader;
use xml2arrow::{Config, parse_xml};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_yaml_file("config.yaml")?;

    let file = File::open("data.xml")?;
    let reader = BufReader::new(file);
    let record_batches = parse_xml(reader, &config)?;

    for (name, batch) in &record_batches {
        println!("Table '{}': {} rows, {} columns", name, batch.num_rows(), batch.num_columns());

        // Access a column by name
        if let Some(col) = batch.column_by_name("temperature") {
            println!("temperature column has {} values", col.len());
        }
    }

    Ok(())
}
```

`parse_xml` returns an `IndexMap<String, RecordBatch>` whose keys are the table
names defined in your config. The `RecordBatch` type is the standard Arrow
in-memory columnar representation and can be passed directly to libraries such as
[DataFusion](https://github.com/apache/datafusion),
[Polars](https://github.com/pola-rs/polars), or written to Parquet via
[parquet](https://crates.io/crates/parquet).

---

## Example

This example extracts meteorological station data from a nested XML document into
three linked Arrow tables.

### XML data (`stations.xml`)

```xml
<report>
  <header>
    <title>Meteorological Station Data</title>
    <created_by>National Weather Service</created_by>
    <creation_time>2024-12-30T13:59:15Z</creation_time>
  </header>
  <monitoring_stations>
    <monitoring_station id="MS001">
      <location>
        <latitude>-61.39110459389277</latitude>
        <longitude>48.08662749089257</longitude>
        <elevation>547.1050788360882</elevation>
      </location>
      <measurements>
        <measurement>
          <timestamp>2024-12-30T12:39:15Z</timestamp>
          <temperature unit="C">35.486545480326114</temperature>
          <pressure unit="hPa">950.439973486407</pressure>
          <humidity unit="%">49.77716576844861</humidity>
        </measurement>
        <measurement>
          <timestamp>2024-12-30T12:44:15Z</timestamp>
          <temperature unit="C">29.095166644493865</temperature>
          <pressure unit="hPa">1049.3215015450517</pressure>
          <humidity unit="%">32.5687148391251</humidity>
        </measurement>
      </measurements>
      <metadata>
        <description>Located in the Arctic Tundra area, used for Scientific Research.</description>
        <install_date>2024-03-31</install_date>
      </metadata>
    </monitoring_station>
    <monitoring_station id="MS002">
      <location>
        <latitude>11.891496388319311</latitude>
        <longitude>135.09336983543022</longitude>
        <elevation>174.53349357280004</elevation>
      </location>
      <measurements>
        <measurement>
          <timestamp>2024-12-30T12:39:15Z</timestamp>
          <temperature unit="C">24.791842953632283</temperature>
          <pressure unit="hPa">989.4054287187706</pressure>
          <humidity unit="%">57.70794884397625</humidity>
        </measurement>
        <measurement>
          <timestamp>2024-12-30T12:44:15Z</timestamp>
          <temperature unit="C">15.153690541845911</temperature>
          <pressure unit="hPa">1001.413052919951</pressure>
          <humidity unit="%">45.45094598045342</humidity>
        </measurement>
        <measurement>
          <timestamp>2024-12-30T12:49:15Z</timestamp>
          <temperature unit="C">-4.022555715139081</temperature>
          <pressure unit="hPa">1000.5225751769922</pressure>
          <humidity unit="%">70.40117458947834</humidity>
        </measurement>
        <measurement>
          <timestamp>2024-12-30T12:54:15Z</timestamp>
          <temperature unit="C">25.852920542644185</temperature>
          <pressure unit="hPa">953.762785698162</pressure>
          <humidity unit="%">42.62088244545566</humidity>
        </measurement>
      </measurements>
      <metadata>
        <description>Located in the Desert area, used for Weather Forecasting.</description>
        <install_date>2024-01-17</install_date>
      </metadata>
    </monitoring_station>
  </monitoring_stations>
</report>
```

### Configuration (`stations.yaml`)

```yaml
tables:
  - name: report
    xml_path: /
    levels: []
    fields:
      - name: title
        xml_path: /report/header/title
        data_type: Utf8
      - name: created_by
        xml_path: /report/header/created_by
        data_type: Utf8
      - name: creation_time
        xml_path: /report/header/creation_time
        data_type: Utf8

  - name: stations
    xml_path: /report/monitoring_stations
    levels:
      - station
    fields:
      - name: id
        xml_path: /report/monitoring_stations/monitoring_station/@id
        data_type: Utf8
      - name: latitude
        xml_path: /report/monitoring_stations/monitoring_station/location/latitude
        data_type: Float32
      - name: longitude
        xml_path: /report/monitoring_stations/monitoring_station/location/longitude
        data_type: Float32
      - name: elevation
        xml_path: /report/monitoring_stations/monitoring_station/location/elevation
        data_type: Float32
      - name: description
        xml_path: /report/monitoring_stations/monitoring_station/metadata/description
        data_type: Utf8
      - name: install_date
        xml_path: /report/monitoring_stations/monitoring_station/metadata/install_date
        data_type: Utf8

  - name: measurements
    xml_path: /report/monitoring_stations/monitoring_station/measurements
    levels:
      - station    # Links each measurement back to its parent station
      - measurement
    fields:
      - name: timestamp
        xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/timestamp
        data_type: Utf8
      - name: temperature
        xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/temperature
        data_type: Float64
        offset: 273.15   # Convert °C → K
      - name: pressure
        xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/pressure
        data_type: Float64
        scale: 100.0     # Convert hPa → Pa
      - name: humidity
        xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/humidity
        data_type: Float64
```

### Output

```
- report:
 ┌─────────────────────────────┬──────────────────────────┬──────────────────────┐
 │ title                       ┆ created_by               ┆ creation_time        │
 │ ---                         ┆ ---                      ┆ ---                  │
 │ str                         ┆ str                      ┆ str                  │
 ╞═════════════════════════════╪══════════════════════════╪══════════════════════╡
 │ Meteorological Station Data ┆ National Weather Service ┆ 2024-12-30T13:59:15Z │
 └─────────────────────────────┴──────────────────────────┴──────────────────────┘

- stations:
 ┌───────────┬───────┬────────────┬────────────┬────────────┬────────────────────────┬──────────────┐
 │ <station> ┆ id    ┆ latitude   ┆ longitude  ┆ elevation  ┆ description            ┆ install_date │
 │ ---       ┆ ---   ┆ ---        ┆ ---        ┆ ---        ┆ ---                    ┆ ---          │
 │ u32       ┆ str   ┆ f32        ┆ f32        ┆ f32        ┆ str                    ┆ str          │
 ╞═══════════╪═══════╪════════════╪════════════╪════════════╪════════════════════════╪══════════════╡
 │ 0         ┆ MS001 ┆ -61.391106 ┆ 48.086628  ┆ 547.105103 ┆ Located in the Arctic  ┆ 2024-03-31   │
 │           ┆       ┆            ┆            ┆            ┆ Tundra a…              ┆              │
 │ 1         ┆ MS002 ┆ 11.891497  ┆ 135.093369 ┆ 174.533493 ┆ Located in the Desert  ┆ 2024-01-17   │
 │           ┆       ┆            ┆            ┆            ┆ area, us…              ┆              │
 └───────────┴───────┴────────────┴────────────┴────────────┴────────────────────────┴──────────────┘

- measurements:
 ┌───────────┬───────────────┬──────────────────────┬─────────────┬───────────────┬───────────┐
 │ <station> ┆ <measurement> ┆ timestamp            ┆ temperature ┆ pressure      ┆ humidity  │
 │ ---       ┆ ---           ┆ ---                  ┆ ---         ┆ ---           ┆ ---       │
 │ u32       ┆ u32           ┆ str                  ┆ f64         ┆ f64           ┆ f64       │
 ╞═══════════╪═══════════════╪══════════════════════╪═════════════╪═══════════════╪═══════════╡
 │ 0         ┆ 0             ┆ 2024-12-30T12:39:15Z ┆ 308.636545  ┆ 95043.997349  ┆ 49.777166 │
 │ 0         ┆ 1             ┆ 2024-12-30T12:44:15Z ┆ 302.245167  ┆ 104932.150155 ┆ 32.568715 │
 │ 1         ┆ 0             ┆ 2024-12-30T12:39:15Z ┆ 297.941843  ┆ 98940.542872  ┆ 57.707949 │
 │ 1         ┆ 1             ┆ 2024-12-30T12:44:15Z ┆ 288.303691  ┆ 100141.305292 ┆ 45.450946 │
 │ 1         ┆ 2             ┆ 2024-12-30T12:49:15Z ┆ 269.127444  ┆ 100052.257518 ┆ 70.401175 │
 │ 1         ┆ 3             ┆ 2024-12-30T12:54:15Z ┆ 299.002921  ┆ 95376.27857   ┆ 42.620882 │
 └───────────┴───────────────┴──────────────────────┴─────────────┴───────────────┴───────────┘
```

The `<station>` index in the `measurements` table links each measurement to its
parent station by row position, enabling a join on `stations.<station> = measurements.<station>`.

---

## ⚡ Performance

`xml2arrow` is designed for single-pass, low-allocation parsing. The core of this
is a trie-based path registry that replaces string comparisons in the hot loop with
direct integer indexing.

Benchmarks were measured on an Apple M1 Pro using [Criterion.rs](https://github.com/bheisler/criterion.rs):

| Benchmark                          | File size | Throughput     |
| :--------------------------------- | --------: | :------------- |
| 1K measurements, 2 sensors (small) |    413 KB | ~298 MiB/s     |
| 10K measurements, 5 sensors (medium) |    10 MB | ~308 MiB/s     |
| 100K measurements, 10 sensors (large) |  202 MB | ~307 MiB/s     |
| 200K measurements, 5 sensors (xlarge) |  203 MB | ~307 MiB/s     |

Throughput stays consistent from sub-megabyte to 200 MB files, reflecting the
predictable cost of the single-pass design.

```bash
# Run all benchmarks
cargo bench

# Save a named baseline before making changes
cargo bench --bench parse_benchmark -- --save-baseline before

# Compare against it after
cargo bench --bench parse_benchmark -- --baseline before

# Open the interactive HTML report
open target/criterion/report/index.html
```
