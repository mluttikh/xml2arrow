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
