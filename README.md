[![Build Status](https://github.com/mluttikh/xml2arrow/actions/workflows/ci.yml/badge.svg)](https://github.com/mluttikh/xml2arrow/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/xml2arrow)](https://crates.io/crates/xml2arrow)
[![crates.io](https://img.shields.io/crates/d/xml2arrow)](https://crates.io/crates/xml2arrow)
[![docs.rs](https://docs.rs/xml2arrow/badge.svg)](https://docs.rs/xml2arrow)
[![License](https://img.shields.io/crates/l/xml2arrow)](LICENSE)
# XML2ARROW

A Rust crate for efficiently converting XML data to Apache Arrow format.

## Overview

`xml2arrow` provides a high-performance solution for transforming XML documents into Apache Arrow tables. It leverages the `quick-xml` parser for efficient XML processing and the `arrow` crate for building Arrow data structures. This makes it ideal for handling large XML datasets and integrating them into data processing pipelines that utilize the Arrow ecosystem.

## Usage

1. Create a Configuration File (YAML):

The configuration file (YAML format) defines how your XML structure maps to Arrow tables and fields. Here's a detailed explanation of the configuration structure:

```yaml
tables:
  - name: <table_name>         # The name of the resulting Arrow table
    xml_path: <xml_path>       # The XML path to the *parent* element of the table's row elements
    row_element: <row_element> # The name of the XML element that represents a row
    levels:                    # Index levels for nested XML structures.
    - <level1>
    - <level2> 
    fields:
    - name: <field_name>       # The name of the Arrow field
      xml_path: <field_path>   # The XML path to the field within a row
      data_type: <data_type>   # The Arrow data type (see below)
      nullable: <true|false>   # Whether the field can be null
      scale: <number>          # Optional scaling factor for floats. 
      offset: <number>         # Optional offset for numeric floats
  - name: ...
```

* `tables`: A list of table configurations. Each entry defines a separate Arrow table to be extracted from the XML.
* `name`: The name given to the resulting Arrow *RecordBatch* (which represents a table).
* `xml_path`: An XPath-like string that specifies the XML element that is the parent of the elements representing rows in the table. For example, if your XML contains `<library><book>...</book><book>...</book></library>`, the `xml_path` would be `/library`. The `book` elements are then identified by the `row_element` configuration.
* `row_element`: The element that represents a single row. For example, if the xml_path is `/library/book`, the `row_element` is `book`.
* `levels`: An array of strings that represent parent tables to create an index for nested structures. If the XML structure is `/library/shelfs/shelf/books/book` you should define levels like this: `levels: ["shelfs", "books"]`. This will create indexes named `<shelfs>` and `<books>`.
* `fields`: A list of field configurations for each column in the Arrow table.
  * `name`: The name of the field in the Arrow schema.
  * `xml_path`: An XPath-like string that specifies the XML element containing the field's value.
  * `data_type`: The Arrow data type of the field. Supported types are:
    * `Boolean` (*true* or *false*)
    * `Int16`
    * `UInt16`
    * `Int32`
    * `UInt32`
    * `Int64`
    * `UInt64`
    * `Float32`
    * `Float64`
    * `Utf8` (Strings)
  * `nullable`: A boolean value indicating whether the field can contain null values.
  * `scale` (Optional): A scaling factor for float fields (e.g., to convert units).
  * `offset` (Optional): An offset value for float fields (e.g., to convert units).

2. Parse the XML
```Rust
use std::fs::File;
use std::io::BufReader;
use xml2arrow::(Config, parse_xml};

let config = Config::from_yaml_file("config.yaml").unwrap();

let file = File::open("data.xml").unwrap();
let reader = BufReader::new(file);
let record_batches = parse_xml(reader, &config).unwrap();
```

## Example

Suppose we have the following XML file (`stations.xml`):

```xml
<report>
  <header>
    <title>Meteorological Station Data</title>
    <created_by>National Weather Service</created_by>
    <creation_time>2024-12-30T13:59:15Z</creation_time>
  </header>
  <monitoring_stations>
    <monitoring_station>
      <id>MS001</id>
      <location>
        <latitude>-61.39110459389277</latitude>
        <longitude>48.08662749089257</longitude>
        <elevation unit="m">547.1050788360882</elevation>
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
    <monitoring_station>
      <id>MS002</id>
      <location>
        <latitude>11.891496388319311</latitude>
        <longitude>135.09336983543022</longitude>
        <elevation unit="m">174.53349357280004</elevation>
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

We can define a YAML configuration file (`stations.yaml`) to specify how to convert the XML data to Arrow tables:

```yaml
tables:
  - name: report
    xml_path: /
    row_element: /report
    levels: []
    fields:
    - name: title
      xml_path: /report/header/title
      data_type: Utf8
      nullable: false
    - name: created_by
      xml_path: /report/header/created_by
      data_type: Utf8
      nullable: false
    - name: creation_time
      xml_path: /report/header/creation_time
      data_type: Utf8
      nullable: false
  - name: stations
    xml_path: /report/monitoring_stations
    row_element: /report/monitoring_stations/monitoring_station
    levels:
    - station
    fields:
    - name: id
      xml_path: /report/monitoring_stations/monitoring_station/id
      data_type: Utf8
      nullable: false
    - name: latitude
      xml_path: /report/monitoring_stations/monitoring_station/location/latitude
      data_type: Float32
      nullable: false
    - name: longitude
      xml_path: /report/monitoring_stations/monitoring_station/location/longitude
      data_type: Float32
      nullable: false
    - name: elevation
      xml_path: /report/monitoring_stations/monitoring_station/location/elevation
      data_type: Float32
      nullable: false
    - name: description
      xml_path: report/monitoring_stations/monitoring_station/metadata/description
      data_type: Utf8
      nullable: false
    - name: install_date
      xml_path: report/monitoring_stations/monitoring_station/metadata/install_date
      data_type: Utf8
      nullable: false
  - name: measurements
    xml_path: /report/monitoring_stations/monitoring_station/measurements
    row_element: /report/monitoring_stations/monitoring_station/measurements/measurement
    levels:
    - station
    - measurement
    fields:
    - name: timestamp
      xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/timestamp
      data_type: Utf8
      nullable: false
    - name: temperature
      xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/temperature
      data_type: Float64
      nullable: false
      offset: 273.15  # Convert from Celsius to Kelvin
    - name: pressure
      xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/pressure
      data_type: Float64
      nullable: false
      scale: 100.0    # Convert from hPa to Pa
    - name: humidity
      xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/humidity
      data_type: Float64
      nullable: false
```

Here's how to use `xml2arrow` to parse the XML and YAML files and get the resulting Arrow tables:

```rust
use std::fs::File;
use std::io::BufReader;
use xml2arrow::(Config, parse_xml};

let config = Config::from_yaml_file("stations.yaml").unwrap();

let file = File::open("stations.xml").unwrap();
let reader = BufReader::new(file);
let record_batches = parse_xml(reader, &config).unwrap();
```

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
 │ 1         ┆ 2             ┆ 2024-12-30T12:39:15Z ┆ 297.941843  ┆ 98940.542872  ┆ 57.707949 │
 │ 1         ┆ 3             ┆ 2024-12-30T12:44:15Z ┆ 288.303691  ┆ 100141.305292 ┆ 45.450946 │
 │ 1         ┆ 4             ┆ 2024-12-30T12:49:15Z ┆ 269.127444  ┆ 100052.257518 ┆ 70.401175 │
 │ 1         ┆ 5             ┆ 2024-12-30T12:54:15Z ┆ 299.002921  ┆ 95376.27857   ┆ 42.620882 │
 └───────────┴───────────────┴──────────────────────┴─────────────┴───────────────┴───────────┘
```