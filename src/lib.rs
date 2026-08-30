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
//! # Rows
//!
//! A table's rows come from one repeating element. Say which with `row:`:
//!
//! ```yaml
//! xml_path: /report/measurements   # the container
//! row: measurement                 # one row per <measurement>
//! ```
//!
//! Without `row:`, boundaries are *inferred* — a row ends whenever any
//! configured direct child of `xml_path` closes. That rule depends on which
//! fields happen to be configured, so adding a column can change a table's row
//! count. It is the historical default and still supported; [`Config::lint`]
//! reports tables where it is likely to surprise.
//!
//! # Compatibility
//!
//! No release removes or changes a public item without a deprecation period
//! first. Newer configuration keys are additive: a config that sets none of
//! them parses exactly as it did before they existed, which the crate holds
//! itself to with a frozen output corpus. `MIGRATION.md` covers moving an
//! existing config and codebase forward.

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
