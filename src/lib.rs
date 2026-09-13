//! Convert XML documents into Apache Arrow `RecordBatch`es in a single
//! streaming pass.
//!
//! A YAML [`Config`] maps XML paths to tables and columns. The paths are
//! compiled once into an integer trie, and parsing then walks the document
//! without ever matching a string or allocating in the event loop — so one
//! [`Parser`] can serve many documents at a fixed per-document cost.
//!
//! # Example
//!
//! ```rust
//! use xml2arrow::{Parser, config_from_yaml};
//!
//! let config = config_from_yaml!(r#"
//! version: 2
//! tables:
//!   - name: measurements
//!     xml_path: /report/measurements
//!     row: measurement
//!     fields:
//!       - {name: id,    path: "@id",  data_type: UInt32}
//!       - {name: value, path: value,  data_type: Float64}
//! "#);
//!
//! let xml = r#"
//! <report>
//!   <measurements>
//!     <measurement id="1"><value>1.5</value></measurement>
//!     <measurement id="2"><value>2.5</value></measurement>
//!   </measurements>
//! </report>"#;
//!
//! let parser = Parser::new(&config)?;
//! let batches = parser.parse_slice(xml.as_bytes())?;
//!
//! let measurements = &batches["measurements"];
//! assert_eq!(measurements.num_rows(), 2);
//! assert_eq!(measurements.num_columns(), 2);
//! # Ok::<(), xml2arrow::Error>(())
//! ```
//!
//! # Where to start
//!
//! - [`Parser`] is the entry point: build one from a [`Config`], then call
//!   [`Parser::parse`] (any `BufRead`), [`Parser::parse_slice`] (zero-copy
//!   over bytes), or [`Parser::parse_batches`] to stream batches instead of
//!   collecting them.
//! - The [`config`] module documents the mapping itself — how a table finds
//!   its rows, how a field finds its value, and how nested tables relate.
//! - [`Config::lint`] and [`Parser::warnings`] report configurations that are
//!   valid but commonly surprising. The library never prints; hosts log them.
//! - [`Error`] is the failure surface. Its `Display` output is stable, and
//!   errors carry the row and byte offset where the problem was found.
//!
//! # Configuration formats
//!
//! The config above declares `version: 2`, so it uses configuration format
//! version 2: every table names the element that makes a row with `row:`,
//! fields give a `path:` relative to it, and a table nested inside another
//! declares `links:`. The [configuration reference] documents every key.
//!
//! A config without `version: 2` uses configuration format version 1, the
//! format of every release before 0.20. It is deprecated, and keeps working
//! unchanged until 1.0: its rows are inferred from the configured fields, and
//! nested tables use `levels:` position columns. [`Config::lint`] reports what
//! such a config still needs to change, and the [migration guide] takes it
//! there step by step.
//!
//! [configuration reference]: https://github.com/mluttikh/xml2arrow/blob/main/docs/configuration.md
//! [migration guide]: https://github.com/mluttikh/xml2arrow/blob/main/docs/migrating-to-version-2.md
//!
//! # Compatibility
//!
//! No release removes or changes a public item without a deprecation period
//! first. A version 1 config parses exactly as it did before version 2
//! existed, which the crate holds itself to with a frozen output corpus.
//! [`MIGRATION.md`] covers moving a codebase to this release.
//!
//! [`MIGRATION.md`]: https://github.com/mluttikh/xml2arrow/blob/main/MIGRATION.md

// Every public item carries documentation, and this keeps it that way: the
// config keys and error variants *are* the interface, so an undocumented one
// is a gap in the contract rather than a missing nicety. `warn` rather than
// `deny` so a work-in-progress build is not blocked; CI runs clippy with
// `-D warnings`, which promotes it.
#![warn(missing_docs)]

pub mod config;

pub mod errors;
pub use errors::{Error, Result};

pub mod lint;
pub use lint::Lint;

mod path_registry;
mod xml_parser;
// The two free functions are deprecated (see their notes); re-exporting them
// is not itself a use worth warning about.
#[allow(deprecated)]
pub use xml_parser::{
    BatchOptions, BatchStream, EventSource, Parser, ReaderSource, SingleTableReader, SliceSource,
    TableBatch, parse_xml, parse_xml_slice,
};

pub use config::{
    Config, ConfigBuilder, DType, FieldConfig, FieldConfigBuilder, Link, OnInvalid, OnMissing,
    OnRepeat, ParserOptions, RowId, TableConfig, TableConfigBuilder, ValuePolicies,
};
