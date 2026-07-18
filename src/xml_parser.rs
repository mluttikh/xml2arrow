//! XML-to-Arrow parsing pipeline.
//!
//! This module is intentionally organized as a top-down narrative of the parsing flow:
//! 1) Build per-table and per-field builders that accumulate values into Arrow arrays.
//! 2) Stream XML events and track the current path using integer IDs (via `PathRegistry`).
//! 3) On element boundaries, push/pop table context and finalize rows deterministically.
//! 4) After streaming, finish builders into `RecordBatch`es and return an ordered map.
//!
//! The guiding goals are: single-pass parsing, predictable O(1) lookups, and minimal
//! allocation in the hot path.
//! This means we front-load configuration validation and path compilation so the
//! event loop can focus on direct indexing and appends.
use std::io::BufRead;
use std::iter::FusedIterator;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder,
    Int64Builder, RecordBatch, RecordBatchReader, StringBuilder, UInt8Builder, UInt16Builder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use indexmap::IndexMap;
use quick_xml::Reader;
use quick_xml::XmlVersion;
use quick_xml::encoding::Decoder;
use quick_xml::escape;
use quick_xml::events::attributes::Attributes;
use quick_xml::events::{BytesRef, Event};

use crate::Config;
use crate::config::{DType, FieldConfig, TableConfig};
use crate::errors::ConfigIssue;
use crate::errors::Error;
use crate::errors::ParseKind;
use crate::errors::Result;
use crate::path_registry::{PathNodeId, PathNodeInfo, PathRegistry, PathTracker};

// === Field-level accumulation ===

/// Enum-based array builder that avoids dynamic dispatch (`Box<dyn ArrayBuilder>`)
/// in the hot path. Each variant holds the concrete Arrow builder type directly.
enum TypedArrayBuilder {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    UInt8(UInt8Builder),
    Int16(Int16Builder),
    UInt16(UInt16Builder),
    Int32(Int32Builder),
    UInt32(UInt32Builder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
}

impl TypedArrayBuilder {
    fn from_dtype(data_type: DType) -> Self {
        match data_type {
            DType::Boolean => Self::Boolean(BooleanBuilder::default()),
            DType::Int8 => Self::Int8(Int8Builder::default()),
            DType::UInt8 => Self::UInt8(UInt8Builder::default()),
            DType::Int16 => Self::Int16(Int16Builder::default()),
            DType::UInt16 => Self::UInt16(UInt16Builder::default()),
            DType::Int32 => Self::Int32(Int32Builder::default()),
            DType::UInt32 => Self::UInt32(UInt32Builder::default()),
            DType::Int64 => Self::Int64(Int64Builder::default()),
            DType::UInt64 => Self::UInt64(UInt64Builder::default()),
            DType::Float32 => Self::Float32(Float32Builder::default()),
            DType::Float64 => Self::Float64(Float64Builder::default()),
            DType::Utf8 => Self::Utf8(StringBuilder::default()),
        }
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Int8(b) => Arc::new(b.finish()),
            Self::UInt8(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::UInt16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::UInt32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::UInt64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Utf8(b) => Arc::new(b.finish()),
        }
    }
}

/// The subset of a `FieldConfig` that the hot path and its error messages need.
///
/// `FieldBuilder` used to hold a full `FieldConfig` clone (including `data_type`,
/// which is consumed once at construction). Storing only what the runtime reads
/// keeps the builder narrower and decouples it from future additions to
/// `FieldConfig`.
struct FieldMeta {
    name: String,
    xml_path: String,
    nullable: bool,
    scale: Option<f64>,
    offset: Option<f64>,
}

impl FieldMeta {
    fn from_config(fc: &FieldConfig) -> Self {
        Self {
            name: fc.name.clone(),
            xml_path: fc.xml_path.clone(),
            nullable: fc.nullable,
            scale: fc.scale,
            offset: fc.offset,
        }
    }
}

/// Builds Arrow arrays for a single field based on parsed XML data.
///
/// This struct manages the accumulation of values from the XML and their conversion
/// to the appropriate Arrow data type. It also handles null values and applies
/// scaling and offset transformations if configured.
struct FieldBuilder {
    /// The runtime-relevant subset of the field's configuration.
    meta: FieldMeta,
    /// The Arrow array builder used to construct the array (enum dispatch, no vtable).
    array_builder: TypedArrayBuilder,
    /// Indicates whether the builder has received any values for the current row.
    ///
    /// Doubles as the repeated-element guard: when a field's element is
    /// *entered* while `has_value` is already set for the current row, the
    /// element appeared twice — historically the raw bytes were silently
    /// concatenated ("1" + "2" → 12 for an Int32), fabricating values; now it
    /// is a `ParseError` (see `check_element_not_repeated`). Text split
    /// across multiple events *within* one element (text + CDATA, entity
    /// references) never trips this: no re-entry happens in between.
    has_value: bool,
    /// Pre-computed `meta.scale.is_some() || meta.offset.is_some()`. The
    /// per-row float append path is hot enough that gating the `Option` loads
    /// behind a single `bool` measurably outperforms unconditionally probing
    /// both `scale` and `offset` on every value.
    has_transform: bool,
    /// Temporary storage for accumulating the current value from potentially
    /// multiple XML text nodes. Held as raw bytes so the hot loop never pays
    /// `str::from_utf8` per event — validation happens once, at append time,
    /// and only for `Utf8` fields. Numeric and boolean parsers consume bytes
    /// directly.
    current_value: Vec<u8>,
}

/// Parses a boolean token from a byte slice, trimming ASCII whitespace first.
/// Returns `Ok(Some(bool))` for valid tokens, `Ok(None)` for empty/whitespace-only input,
/// or `Err(())` for unrecognized values. Operating on bytes lets the caller skip
/// `str::from_utf8` on the hot path; the boolean vocabulary is ASCII-only so
/// `eq_ignore_ascii_case` on bytes is semantically equivalent to the string form.
fn parse_boolean_token(value: &[u8]) -> std::result::Result<Option<bool>, ()> {
    let trimmed = value.trim_ascii();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.eq_ignore_ascii_case(b"false")
        || trimmed == b"0"
        || trimmed.eq_ignore_ascii_case(b"no")
        || trimmed.eq_ignore_ascii_case(b"n")
        || trimmed.eq_ignore_ascii_case(b"f")
        || trimmed.eq_ignore_ascii_case(b"off")
    {
        Ok(Some(false))
    } else if trimmed.eq_ignore_ascii_case(b"true")
        || trimmed == b"1"
        || trimmed.eq_ignore_ascii_case(b"yes")
        || trimmed.eq_ignore_ascii_case(b"y")
        || trimmed.eq_ignore_ascii_case(b"t")
        || trimmed.eq_ignore_ascii_case(b"on")
    {
        Ok(Some(true))
    } else {
        Err(())
    }
}

/// Helper macro to reduce boilerplate for parsing and appending integer values.
/// `$value` is a `&[u8]`; `atoi` already consumes bytes, so this is the natural
/// representation. On failure we fall back to `str::parse` purely to surface
/// the underlying `IntErrorKind` (overflow, invalid digit, etc.) in the error
/// message — this path only runs when parsing fails, so the extra UTF-8 check
/// is free for the hot case.
macro_rules! append_int {
    ($builder:expr, $value:expr, $has_value:expr, $field_config:expr, $ty:ty, $type_name:expr) => {
        if $has_value {
            match atoi::atoi::<$ty>($value) {
                Some(val) => $builder.append_value(val),
                None => {
                    let as_str = std::str::from_utf8($value).unwrap_or("");
                    match as_str.parse::<$ty>() {
                        Ok(val) => $builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError {
                                field: Arc::from($field_config.name.as_str()),
                                path: Arc::from($field_config.xml_path.as_str()),
                                value: String::from_utf8_lossy($value).into_owned(),
                                kind: ParseKind::InvalidNumber {
                                    type_name: $type_name,
                                    reason: e.to_string(),
                                },
                            });
                        }
                    }
                }
            }
        } else if $field_config.nullable {
            $builder.append_null();
        } else {
            return Err(Error::MissingRequiredField {
                field: Arc::from($field_config.name.as_str()),
                path: Arc::from($field_config.xml_path.as_str()),
            });
        }
    };
}

/// Helper macro for parsing and appending float values using `fast_float2`.
///
/// Applies `scale` and `offset` only when `$has_transform` is true. The flag
/// is computed once at builder construction; gating the `Option` loads behind
/// it avoids two memory loads + branches per value in the (very common) case
/// where neither transform is configured. `scale as $ty` is a no-op for `f64`
/// and a truncating cast for `f32`.
macro_rules! append_float {
    ($builder:expr, $value:expr, $has_value:expr, $field_config:expr, $has_transform:expr, $ty:ty, $type_name:expr) => {
        if $has_value {
            match fast_float2::parse::<$ty, _>($value) {
                Ok(mut val) => {
                    if $has_transform {
                        #[allow(clippy::cast_possible_truncation)]
                        if let Some(scale) = $field_config.scale {
                            val *= scale as $ty;
                        }
                        #[allow(clippy::cast_possible_truncation)]
                        if let Some(offset) = $field_config.offset {
                            val += offset as $ty;
                        }
                    }
                    $builder.append_value(val);
                }
                Err(e) => {
                    return Err(Error::ParseError {
                        field: Arc::from($field_config.name.as_str()),
                        path: Arc::from($field_config.xml_path.as_str()),
                        value: String::from_utf8_lossy($value).into_owned(),
                        kind: ParseKind::InvalidNumber {
                            type_name: $type_name,
                            reason: e.to_string(),
                        },
                    });
                }
            }
        } else if $field_config.nullable {
            $builder.append_null();
        } else {
            return Err(Error::MissingRequiredField {
                field: Arc::from($field_config.name.as_str()),
                path: Arc::from($field_config.xml_path.as_str()),
            });
        }
    };
}

impl FieldBuilder {
    fn new(field_config: &FieldConfig) -> Self {
        let array_builder = TypedArrayBuilder::from_dtype(field_config.data_type);
        let has_transform = field_config.scale.is_some() || field_config.offset.is_some();
        Self {
            meta: FieldMeta::from_config(field_config),
            array_builder,
            has_value: false,
            has_transform,
            current_value: Vec::with_capacity(128),
        }
    }

    #[inline]
    fn set_current_value(&mut self, value: &[u8]) {
        self.current_value.extend_from_slice(value);
        self.has_value = true;
    }

    /// Appends the currently accumulated value to the Arrow array builder,
    /// performing type conversion and handling nulls.
    fn append_current_value(&mut self) -> Result<()> {
        let value = self.current_value.as_slice();
        let has_value = self.has_value;
        let has_transform = self.has_transform;
        let fc = &self.meta;

        match &mut self.array_builder {
            TypedArrayBuilder::Utf8(b) => {
                if has_value {
                    // UTF-8 is validated exactly once per row per Utf8 field
                    // (here), rather than once per text event in the hot loop.
                    // Numeric/boolean fields never pay this cost.
                    let s = std::str::from_utf8(value)?;
                    b.append_value(s);
                } else if fc.nullable {
                    b.append_null();
                } else {
                    b.append_value("");
                }
            }
            TypedArrayBuilder::Int8(b) => append_int!(b, value, has_value, fc, i8, "i8"),
            TypedArrayBuilder::UInt8(b) => append_int!(b, value, has_value, fc, u8, "u8"),
            TypedArrayBuilder::Int16(b) => append_int!(b, value, has_value, fc, i16, "i16"),
            TypedArrayBuilder::UInt16(b) => append_int!(b, value, has_value, fc, u16, "u16"),
            TypedArrayBuilder::Int32(b) => append_int!(b, value, has_value, fc, i32, "i32"),
            TypedArrayBuilder::UInt32(b) => append_int!(b, value, has_value, fc, u32, "u32"),
            TypedArrayBuilder::Int64(b) => append_int!(b, value, has_value, fc, i64, "i64"),
            TypedArrayBuilder::UInt64(b) => append_int!(b, value, has_value, fc, u64, "u64"),
            TypedArrayBuilder::Float32(b) => {
                append_float!(b, value, has_value, fc, has_transform, f32, "f32")
            }
            TypedArrayBuilder::Float64(b) => {
                append_float!(b, value, has_value, fc, has_transform, f64, "f64")
            }
            TypedArrayBuilder::Boolean(b) => {
                if has_value {
                    match parse_boolean_token(value) {
                        Ok(Some(val)) => b.append_value(val),
                        Ok(None) if fc.nullable => b.append_null(),
                        Ok(None) => {
                            return Err(Error::MissingRequiredField {
                                field: Arc::from(fc.name.as_str()),
                                path: Arc::from(fc.xml_path.as_str()),
                            });
                        }
                        Err(()) => {
                            return Err(Error::ParseError {
                                field: Arc::from(fc.name.as_str()),
                                path: Arc::from(fc.xml_path.as_str()),
                                value: String::from_utf8_lossy(value).into_owned(),
                                kind: ParseKind::InvalidBoolean,
                            });
                        }
                    }
                } else if fc.nullable {
                    b.append_null();
                } else {
                    return Err(Error::MissingRequiredField {
                        field: Arc::from(fc.name.as_str()),
                        path: Arc::from(fc.xml_path.as_str()),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Arc<dyn Array> {
        self.array_builder.finish()
    }
}

// === Table-level batching ===
// A TableBuilder owns per-field builders plus index builders for nested levels.
// It finalizes rows into a RecordBatch in a single, ordered pass.
///
/// The subset of a `TableConfig` that finalization needs.
///
/// `TableBuilder` used to hold a full `TableConfig` clone (including the
/// `fields` vector, which is consumed once when constructing the per-field
/// builders). Storing only the pieces used at `finish()` / error-reporting
/// time keeps the builder narrower.
struct TableMeta {
    name: String,
    xml_path: String,
}

impl TableMeta {
    fn from_config(tc: &TableConfig) -> Self {
        Self {
            name: tc.name.clone(),
            xml_path: tc.xml_path.clone(),
        }
    }
}

/// Builds an Arrow `RecordBatch` for a single table defined in the configuration.
///
/// This struct manages the building of a single Arrow `RecordBatch` by collecting
/// data for each field defined in the table's configuration. It also handles
/// parent/child relationships between tables through index builders.
struct TableBuilder {
    /// The table's runtime-relevant metadata.
    meta: TableMeta,
    /// The table's output schema (index columns + value columns), precomputed
    /// once in `Parser::new` and shared by every batch this builder emits —
    /// streamed batches of one table must be schema-identical, and consumers
    /// (e.g. `RecordBatchReader::schema`) rely on pointer-shared equality.
    schema: Arc<Schema>,
    // Builders for the parent row indices, used for representing nested tables.
    index_builders: Vec<UInt32Builder>,
    /// Builders for each field in the table, indexed by field position.
    field_builders: Vec<FieldBuilder>,
    /// The current row index for this table.
    ///
    /// This increments every time `save_row` is called. Critically, this value
    /// is also read by any ACTIVE CHILD TABLES on the stack to populate their
    /// parent index columns (the foreign keys defined in `levels`).
    ///
    /// NOT reset by `flush()`: it counts rows within the current table
    /// *scope*, not within the current batch. Resetting it on flush would
    /// corrupt the foreign keys of child rows finalized after the flush.
    row_index: usize,
    /// Rows finalized into the array builders since the last `flush()`.
    /// Always 0 for structural tables (empty `fields`), which never emit.
    rows_in_batch: usize,
    /// Raw value bytes accumulated since the last `flush()`. Approximates the
    /// batch's heap footprint cheaply (one add per field per row) so the
    /// byte-budget flush trigger can protect `StringBuilder`'s i32-offset
    /// 2 GiB ceiling without scanning builder internals.
    bytes_in_batch: usize,
}

impl TableBuilder {
    fn new(table_config: &TableConfig, schema: Arc<Schema>) -> Self {
        let mut index_builders = Vec::with_capacity(table_config.levels.len());
        index_builders.resize_with(table_config.levels.len(), UInt32Builder::default);
        let mut field_builders = Vec::with_capacity(table_config.fields.len());
        for field_config in &table_config.fields {
            field_builders.push(FieldBuilder::new(field_config));
        }
        Self {
            meta: TableMeta::from_config(table_config),
            schema,
            index_builders,
            field_builders,
            row_index: 0,
            rows_in_batch: 0,
            bytes_in_batch: 0,
        }
    }

    fn end_row(&mut self, indices: &[u32]) -> Result<()> {
        // Append the current row's data to the arrays
        self.save_row(indices)?;
        for field_builder in &mut self.field_builders {
            field_builder.has_value = false;
            field_builder.current_value.clear();
        }
        Ok(())
    }

    /// Sets a field value by field index.
    #[inline]
    fn set_field_value_by_index(&mut self, field_idx: usize, value: &[u8]) {
        if let Some(field_builder) = self.field_builders.get_mut(field_idx) {
            field_builder.set_current_value(value);
        }
    }

    fn save_row(&mut self, indices: &[u32]) -> Result<()> {
        // Structural tables (empty `fields`) exist only to feed their
        // `row_index` counter to child tables; they are skipped by every
        // emission path. Appending their index values would grow builders
        // nobody ever reads — under streaming, where all *output* tables
        // flush periodically, that would be the one unbounded allocation.
        // Only the `row_index` advance below is observable for them.
        if !self.field_builders.is_empty() {
            // 1. Write the parent foreign keys.
            // The `indices` slice contains the row_index of each ancestor table,
            // in order of hierarchy. These align 1:1 with the `levels` defined
            // in this table's configuration.
            for (index, index_builder) in indices.iter().zip(&mut self.index_builders) {
                index_builder.append_value(*index);
            }

            // 2. Write the actual field values for this table.
            let mut row_bytes = 0usize;
            for field_builder in &mut self.field_builders {
                row_bytes += field_builder.current_value.len();
                field_builder.append_current_value()?;
            }

            self.rows_in_batch += 1;
            self.bytes_in_batch = self.bytes_in_batch.saturating_add(row_bytes);
        }

        // 3. Advance this table's primary key.
        self.row_index += 1;
        Ok(())
    }

    /// Finishes the accumulated rows into a `RecordBatch` and resets the
    /// array builders (Arrow's `finish()` contract) so accumulation can
    /// continue into the next batch. `row_index` is deliberately untouched —
    /// see its field doc; flushing is *value-transparent*: concatenating all
    /// batches a table emits yields exactly the single batch a non-streaming
    /// parse would have produced.
    fn flush(&mut self) -> Result<RecordBatch> {
        let num_arrays = self.field_builders.len() + self.index_builders.len();
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(num_arrays);
        for index_builder in &mut self.index_builders {
            arrays.push(Arc::new(index_builder.finish()));
        }
        for field_builder in &mut self.field_builders {
            arrays.push(field_builder.finish());
        }
        self.rows_in_batch = 0;
        self.bytes_in_batch = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays).map_err(|e| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "Failed to create RecordBatch for table with name {} and XML path {}: {}",
                self.meta.name, self.meta.xml_path, e
            ))
        })?)
    }
}

/// Entry on the table stack tracking active tables during parsing.
#[derive(Debug, Clone)]
struct TableStackEntry {
    /// The table index in the `table_builders` array.
    table_idx: usize,
    /// The node ID in the path registry.
    node_id: PathNodeId,
}

/// Converts parsed XML events into Arrow `RecordBatch`es.
///
/// This struct maintains a stack of table builders to handle nested XML structures.
/// It uses integer-based path indexing via `PathRegistry` for efficient lookups.
/// The mutable, per-parse half of the conversion.
///
/// Everything here holds parse-specific state (the Arrow builders that
/// accumulate values, the active table stack, scratch buffers) and so must be
/// reconstructed for every document. The immutable, *reusable* half — the
/// validated config and compiled `PathRegistry` — lives in [`Parser`], which
/// this borrows from. Splitting the two is what lets a single `Parser` parse
/// many documents without rebuilding the path trie each time (see [`Parser`]).
struct XmlToArrowConverter<'a> {
    /// Table builders for each table defined in the configuration, indexed by position.
    table_builders: Vec<TableBuilder>,
    /// Stack of active tables representing the current nesting level.
    builder_stack: Vec<TableStackEntry>,
    /// Path registry for efficient path lookups, borrowed from the owning `Parser`.
    registry: &'a PathRegistry,
    /// Reusable buffer for collecting parent row indices, avoiding per-row allocation.
    parent_indices_buffer: Vec<u32>,
    /// Mirror of `ParserOptions::validate_attributes`. When `false`, the
    /// attribute iterator skips quick-xml's per-element duplicate-key check
    /// (and its backing allocation). Cached here so `parse_attributes` reads
    /// a plain `bool` rather than re-deriving it from the config each call.
    validate_attributes: bool,
    /// Mirror of `ParserOptions::strip_namespaces`. When `true`, element and
    /// attribute names are resolved via `local_name()` (stripping any `ns:`
    /// prefix); when `false`, the raw qualified `name()` is used, skipping the
    /// per-name `:` scan. Cached here so the hot path reads a plain `bool`.
    strip_namespaces: bool,
    /// Per-batch flush thresholds (rows / raw value bytes). The
    /// collect-everything entry points pass `usize::MAX`, so the two per-row
    /// comparisons in `end_current_row` are always-false, perfectly
    /// predicted branches there.
    max_rows_per_batch: usize,
    max_bytes_per_batch: usize,
    /// Set by `end_current_row` when the just-finalized row pushed its table
    /// over a batch threshold. The streaming loop takes it and flushes that
    /// table; the collect loops never observe it set (thresholds are MAX).
    /// At most one table can become pending per event, because each event
    /// finalizes at most one row.
    pending_flush: Option<usize>,
}

impl<'a> XmlToArrowConverter<'a> {
    /// Builds the fresh per-parse state from an already-compiled [`Parser`].
    ///
    /// This is the only work that *must* happen on every document: allocating
    /// the Arrow builders that will hold the parsed values. The expensive setup
    /// (config validation, path-trie construction, schema construction) was
    /// already done once in [`Parser::new`], so this stays cheap even for tiny
    /// documents.
    ///
    /// The batch thresholds are clamped to at least 1 so a zero value can
    /// never emit empty batches (a yielded batch always has ≥ 1 row).
    fn new(parser: &'a Parser, max_rows_per_batch: usize, max_bytes_per_batch: usize) -> Self {
        let config = &parser.config;
        let mut table_builders = Vec::with_capacity(config.tables.len());
        for (table_config, schema) in config.tables.iter().zip(&parser.table_schemas) {
            table_builders.push(TableBuilder::new(table_config, schema.clone()));
        }

        let mut converter = Self {
            table_builders,
            builder_stack: Vec::new(),
            registry: &parser.registry,
            parent_indices_buffer: Vec::new(),
            validate_attributes: config.parser_options.validate_attributes,
            strip_namespaces: config.parser_options.strip_namespaces,
            max_rows_per_batch: max_rows_per_batch.max(1),
            max_bytes_per_batch: max_bytes_per_batch.max(1),
            pending_flush: None,
        };

        // Start the root-level table (xml_path: /) if it exists AND has fields
        // defined. We only start it if it has fields, because adding it to the
        // builder_stack would affect parent_row_indices for all nested tables,
        // breaking their level indexing. Tables with xml_path: / and no fields
        // are just used for hierarchy purposes and don't need to be on the stack.
        if let Some(table_idx) = parser.registry.get_table_index(PathNodeId::ROOT)
            && !converter.table_builders[table_idx].field_builders.is_empty()
        {
            converter.start_table(table_idx, PathNodeId::ROOT);
        }
        converter
    }

    /// Sets a field value for the current table using path node information.
    #[inline]
    pub fn set_field_value_for_node(&mut self, node_id: PathNodeId, value: &[u8]) {
        let info = self.registry.get_node_info(node_id);
        if info.field_indices.is_empty() {
            return;
        }

        // Get the current table index from the stack
        if let Some(current_entry) = self.builder_stack.last() {
            let current_table_idx = current_entry.table_idx;

            // Find matching field indices for the current table
            for &(table_idx, field_idx) in &info.field_indices {
                if table_idx == current_table_idx {
                    self.table_builders[table_idx].set_field_value_by_index(field_idx, value);
                }
            }
        }
    }

    /// Rejects a repeated occurrence of a field's element within one row.
    ///
    /// Called from the `Start`/`Empty` arms when the *entered* node carries
    /// field mappings (`info` is the `PathNodeInfo` those arms already hold,
    /// so this adds no registry lookup). If a matching field of the current
    /// table already accumulated a value in this row, the element is
    /// appearing a second time — historically the raw bytes were silently
    /// concatenated ("1" + "2" → 12 for an Int32), fabricating values that
    /// never appeared in the document; now it is a `ParseError`.
    ///
    /// Value-less occurrences (`<v/><v>2</v>`) pass: nothing was captured, so
    /// the rule is "at most one value-bearing occurrence per row". Duplicate
    /// *attributes* (reachable with `validate_attributes: false`) are
    /// intentionally exempt — attribute pseudo-nodes are entered inside
    /// `parse_attributes`, which does not run this check, preserving the
    /// documented concatenation behavior for trusted input.
    #[inline]
    fn check_element_not_repeated(&self, info: &PathNodeInfo) -> Result<()> {
        if let Some(current_entry) = self.builder_stack.last() {
            let current_table_idx = current_entry.table_idx;
            for &(table_idx, field_idx) in &info.field_indices {
                if table_idx == current_table_idx
                    && self.table_builders[table_idx].field_builders[field_idx].has_value
                {
                    return Err(self.duplicate_value_error(table_idx, field_idx));
                }
            }
        }
        Ok(())
    }

    /// Builds the `RowIndexOverflow` error for `end_current_row`. Out of
    /// line and cold, like `duplicate_value_error`, so the checked cast in
    /// the per-row path stays a bare compare.
    #[cold]
    #[inline(never)]
    fn row_index_overflow_error(&self, table_idx: usize) -> Error {
        Error::RowIndexOverflow {
            table: Arc::from(self.table_builders[table_idx].meta.name.as_str()),
        }
    }

    /// Builds the `DuplicateValue` error for `check_element_not_repeated`.
    /// Out of line and cold so the check above stays a bare `has_value`
    /// branch in the per-element hot path; the allocations here only run
    /// when the parse is already failing.
    #[cold]
    #[inline(never)]
    fn duplicate_value_error(&self, table_idx: usize, field_idx: usize) -> Error {
        let field_builder = &self.table_builders[table_idx].field_builders[field_idx];
        Error::ParseError {
            field: Arc::from(field_builder.meta.name.as_str()),
            path: Arc::from(field_builder.meta.xml_path.as_str()),
            value: String::from_utf8_lossy(&field_builder.current_value).into_owned(),
            kind: ParseKind::DuplicateValue,
        }
    }

    /// Returns the metadata of the first field of the *current* table mapped
    /// to `node_id`, or `None` when a value at this node would not be
    /// captured. Used by the entity-reference handler to decide whether an
    /// unresolvable reference is an error (it would corrupt a field value)
    /// or ignorable (nothing is listening at this node).
    fn active_field_meta(&self, node_id: PathNodeId) -> Option<&FieldMeta> {
        let info = self.registry.get_node_info(node_id);
        let current_table_idx = self.builder_stack.last()?.table_idx;
        info.field_indices
            .iter()
            .find(|(table_idx, _)| *table_idx == current_table_idx)
            .map(|&(table_idx, field_idx)| {
                &self.table_builders[table_idx].field_builders[field_idx].meta
            })
    }

    fn end_current_row(&mut self) -> Result<()> {
        // Collect parent indices into reusable buffer to avoid per-row allocation.
        self.parent_indices_buffer.clear();
        for entry in &self.builder_stack {
            // Skip the root table (xml_path: /) when collecting parent indices.
            // The root table is special - it represents the document root and shouldn't
            // contribute to parent indices for child tables.
            if entry.node_id == PathNodeId::ROOT {
                continue;
            }
            // Checked, not `as`: the streaming entry points removed the
            // memory ceiling that used to make >u32::MAX rows per scope
            // unreachable, and a wrapped foreign key would silently link
            // child rows to the wrong parents. The happy path is a single
            // predictable compare per ancestor.
            let row_index = self.table_builders[entry.table_idx].row_index;
            let Ok(index) = u32::try_from(row_index) else {
                return Err(self.row_index_overflow_error(entry.table_idx));
            };
            self.parent_indices_buffer.push(index);
        }
        if let Some(entry) = self.builder_stack.last() {
            let table_idx = entry.table_idx;
            let table = &mut self.table_builders[table_idx];
            table.end_row(&self.parent_indices_buffer)?;
            // Batch-threshold check, on the only path where a row finalizes.
            // Structural tables keep `rows_in_batch == 0`, so they can never
            // trip this even with tiny thresholds.
            if table.rows_in_batch >= self.max_rows_per_batch
                || table.bytes_in_batch >= self.max_bytes_per_batch
            {
                self.pending_flush = Some(table_idx);
            }
        }
        Ok(())
    }

    /// Pushes a new table scope onto the builder stack.
    ///
    /// `table_idx` is taken as a parameter (rather than re-derived from
    /// `node_id` via `registry.get_table_index`) because every hot-path
    /// caller already has the index in hand from the same `PathNodeInfo`
    /// read that decided this node was a table boundary. Avoiding the
    /// re-lookup keeps the per-Start path to a single `node_info[]` access.
    fn start_table(&mut self, table_idx: usize, node_id: PathNodeId) {
        self.builder_stack
            .push(TableStackEntry { table_idx, node_id });
        self.table_builders[table_idx].row_index = 0;
    }

    fn end_table(&mut self) {
        self.builder_stack.pop();
    }

    fn finish(mut self) -> Result<IndexMap<String, arrow::record_batch::RecordBatch>> {
        let mut record_batches = IndexMap::new();
        for table_builder in &mut self.table_builders {
            if !table_builder.field_builders.is_empty() {
                let record_batch = table_builder.flush()?;
                record_batches.insert(table_builder.meta.name.clone(), record_batch);
            }
        }
        Ok(record_batches)
    }
}

/// A reusable, pre-compiled parser for a single [`Config`].
///
/// Constructing a `Parser` does the *one-time* setup work — validating the
/// config and compiling every configured XML path into an integer-indexed
/// `PathRegistry` trie. That work is fixed cost (independent of document size)
/// and, on a small document, can dominate the total parse time. Holding a
/// `Parser` lets you pay it **once** and then parse many documents through the
/// same compiled state — the intended pattern when processing a stream or
/// directory of files that all share one schema.
///
/// The free functions [`parse_xml`] and [`parse_xml_slice`] are thin wrappers
/// that build a throwaway `Parser` per call; reach for `Parser` directly
/// whenever you parse more than one document with the same `Config`.
///
/// # Example
///
/// ```rust
/// use xml2arrow::{Parser, config::{Config, TableConfig, FieldConfigBuilder, DType}};
///
/// let fields = vec![FieldConfigBuilder::new("value", "/data/item/value", DType::Int32).build().unwrap()];
/// let tables = vec![TableConfig::new("items", "/data", vec![], fields)];
/// let config = Config { tables, parser_options: Default::default() };
///
/// // Compile once...
/// let parser = Parser::new(&config).unwrap();
/// // ...parse many.
/// for xml in [b"<data><item><value>1</value></item></data>".as_slice(),
///             b"<data><item><value>2</value></item></data>".as_slice()] {
///     let batches = parser.parse_slice(xml).unwrap();
///     // ... use batches
/// }
/// ```
pub struct Parser {
    /// Owned copy of the configuration, retained so each parse can rebuild its
    /// fresh `TableBuilder`s and re-apply the reader options.
    config: Config,
    /// The compiled path trie — the expensive artifact this type exists to reuse.
    registry: PathRegistry,
    /// Path nodes that trigger early termination after their closing tags.
    /// Resolved against `registry` once at construction.
    stop_node_ids: Vec<PathNodeId>,
    /// Whether any configured field maps to an attribute. Cached so the hot
    /// path can pick the attribute-free event loop without re-scanning config.
    needs_attrs: bool,
    /// Per-table output schemas (index columns + value columns), aligned with
    /// `config.tables`. Computed once here so every batch a table emits —
    /// streamed or collected — shares one `Arc<Schema>`, and so
    /// [`Parser::schema`] can serve consumers (IPC writers, DataFusion
    /// registration) before any document is parsed.
    table_schemas: Vec<Arc<Schema>>,
    /// Table names as shared strings, aligned with `config.tables`. Streamed
    /// [`TableBatch`]es carry a clone; `Arc<str>` keeps that per-batch cost
    /// to a refcount bump.
    table_names: Vec<Arc<str>>,
}

/// Builds a table's output schema exactly as its batches are laid out:
/// one non-nullable `UInt32` index column per `levels` entry (named
/// `<level>`), followed by the configured fields in order.
fn build_table_schema(table_config: &TableConfig) -> Schema {
    let mut fields =
        Vec::with_capacity(table_config.levels.len() + table_config.fields.len());
    for level in &table_config.levels {
        fields.push(Field::new(format!("<{level}>"), DataType::UInt32, false));
    }
    for fc in &table_config.fields {
        fields.push(Field::new(
            &fc.name,
            fc.data_type.as_arrow_type(),
            fc.nullable,
        ));
    }
    Schema::new(fields)
}

impl Parser {
    /// Compiles `config` into a reusable parser, performing all one-time setup.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration validation fails (e.g. an unsupported
    /// scale/offset on a non-numeric field).
    pub fn new(config: &Config) -> Result<Self> {
        // Validate field configurations early to catch unsupported scale/offset.
        config.validate()?;

        // Build the path registry for efficient lookups — the work we want to
        // amortize across every document parsed through this `Parser`.
        let registry = PathRegistry::from_config(config);

        let stop_node_ids = config
            .parser_options
            .stop_at_paths
            .iter()
            .filter_map(|path| registry.resolve_path(path))
            .collect::<Vec<_>>();

        let table_schemas = config
            .tables
            .iter()
            .map(|tc| Arc::new(build_table_schema(tc)))
            .collect();
        let table_names = config
            .tables
            .iter()
            .map(|tc| Arc::from(tc.name.as_str()))
            .collect();

        Ok(Self {
            config: config.clone(),
            registry,
            stop_node_ids,
            needs_attrs: config.requires_attribute_parsing(),
            table_schemas,
            table_names,
        })
    }

    /// Returns the output schema of `table` without parsing any document.
    ///
    /// The schema is fully determined by the [`Config`]: one non-nullable
    /// `UInt32` index column per `levels` entry (named `<level>`), followed by
    /// the configured fields. Every batch the table produces — via
    /// [`Parser::parse`] or the streaming entry points — shares this exact
    /// `Arc<Schema>`.
    ///
    /// Returns `None` for unknown table names and for *structural* tables
    /// (empty `fields`), which never appear in any output.
    #[must_use]
    pub fn schema(&self, table: &str) -> Option<SchemaRef> {
        self.config
            .tables
            .iter()
            .position(|tc| tc.name == table && !tc.fields.is_empty())
            .map(|idx| self.table_schemas[idx].clone())
    }

    /// Returns the schema of the config's unique output table — the table
    /// that [`Parser::parse_single_table`] streams.
    ///
    /// Consumers wiring the single-table stream into schema-first sinks
    /// (Parquet writers, Arrow C-stream exports for Python) need this before
    /// constructing the reader; exposing it here keeps the "exactly one
    /// output table" rule and its error in one place.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the config does not have exactly
    /// one table with fields, exactly like [`Parser::parse_single_table`].
    pub fn single_table_schema(&self) -> Result<SchemaRef> {
        Ok(self.table_schemas[self.single_output_table_index()?].clone())
    }

    /// Parses XML from a streaming reader (e.g. a `File`) into Arrow batches.
    ///
    /// Use [`Parser::parse_slice`] instead when the whole document is already
    /// in memory — it avoids quick-xml's per-event buffer copy.
    ///
    /// # Errors
    ///
    /// Returns an error if XML parsing encounters invalid data, value
    /// conversion fails, or Arrow `RecordBatch` creation fails.
    pub fn parse(&self, reader: impl BufRead) -> Result<IndexMap<String, RecordBatch>> {
        let mut reader = Reader::from_reader(reader);
        self.configure_reader(&mut reader);
        let needs_attrs = self.needs_attrs;
        self.run_parse(&mut reader, |r, t, c, s| {
            if needs_attrs {
                process_xml_events::<_, true>(r, t, c, s)
            } else {
                process_xml_events::<_, false>(r, t, c, s)
            }
        })
    }

    /// Parses XML from an in-memory byte slice into Arrow batches (zero-copy).
    ///
    /// # Errors
    ///
    /// Returns an error if XML parsing encounters invalid data, value
    /// conversion fails, or Arrow `RecordBatch` creation fails.
    pub fn parse_slice(&self, xml: &[u8]) -> Result<IndexMap<String, RecordBatch>> {
        let mut reader = Reader::from_reader(xml);
        self.configure_reader(&mut reader);
        let needs_attrs = self.needs_attrs;
        self.run_parse(&mut reader, |r, t, c, s| {
            if needs_attrs {
                process_xml_events_slice::<true>(r, t, c, s)
            } else {
                process_xml_events_slice::<false>(r, t, c, s)
            }
        })
    }

    /// Parses XML from a streaming reader, yielding Arrow batches
    /// incrementally with bounded memory.
    ///
    /// This is the entry point for documents too large to hold in memory as
    /// one set of Arrow tables (multi-GB data files, Wikipedia-style dumps).
    /// The returned [`BatchStream`] is an
    /// `Iterator<Item = Result<TableBatch>>`: each item names the table it
    /// belongs to and carries a batch of its rows. A table's batch is emitted
    /// whenever it crosses one of the [`BatchOptions`] thresholds, and the
    /// remainder is drained when the document ends (or a
    /// `stop_at_paths` match stops it early).
    ///
    /// Guarantees:
    ///
    /// - **Value transparency**: concatenating a table's batches in yield
    ///   order (e.g. `arrow::compute::concat_batches`) produces exactly the
    ///   `RecordBatch` that [`Parser::parse`] would have returned for it —
    ///   including the `<level>` index columns.
    /// - Batches of one table share one `Arc<Schema>`, equal to
    ///   [`Parser::schema`], and always contain at least one row (tables with
    ///   no rows yield nothing — use [`Parser::schema`] when a sink needs a
    ///   schema up front).
    /// - Within a table, rows appear in document order across batches. No
    ///   ordering promise across tables; note that a parent table's row
    ///   always finalizes *after* its children's rows (its element closes
    ///   last), so a child batch can reference a parent row that arrives in
    ///   a later batch of the parent table.
    /// - On error the stream yields the `Err` once and then fuses;
    ///   already-yielded batches remain valid.
    ///
    /// The `<level>` caveat from the non-streaming API carries over
    /// unchanged: index values are per-scope ordinals, so incremental joins
    /// must match on all level columns (see the `levels` documentation).
    ///
    /// # Example
    ///
    /// ```rust
    /// use xml2arrow::{BatchOptions, Parser, config::{Config, TableConfig, FieldConfigBuilder, DType}};
    ///
    /// let fields = vec![FieldConfigBuilder::new("value", "/data/item/value", DType::Int32).build().unwrap()];
    /// let tables = vec![TableConfig::new("items", "/data", vec![], fields)];
    /// let config = Config { tables, parser_options: Default::default() };
    /// let parser = Parser::new(&config).unwrap();
    ///
    /// let xml: &[u8] = b"<data><item><value>1</value></item><item><value>2</value></item></data>";
    /// let options = BatchOptions::default().with_max_rows_per_batch(1);
    /// for item in parser.parse_batches(xml, options) {
    ///     let batch = item.unwrap();
    ///     assert_eq!(&*batch.table, "items");
    ///     assert_eq!(batch.batch.num_rows(), 1);
    /// }
    /// ```
    pub fn parse_batches<R: BufRead>(
        &self,
        reader: R,
        options: BatchOptions,
    ) -> BatchStream<'_, ReaderSource<R>> {
        let mut reader = Reader::from_reader(reader);
        self.configure_reader(&mut reader);
        BatchStream::new(
            self,
            ReaderSource {
                reader,
                buf: Vec::with_capacity(4096),
            },
            options,
        )
    }

    /// Zero-copy variant of [`Parser::parse_batches`] for XML already in
    /// memory (or memory-mapped): events borrow directly from the slice, no
    /// per-event buffer copy. Combined with a memory-mapped file this parses
    /// arbitrarily large documents with bounded heap — the OS pages the
    /// input, the batch thresholds bound the builders.
    pub fn parse_batches_slice<'a>(
        &'a self,
        xml: &'a [u8],
        options: BatchOptions,
    ) -> BatchStream<'a, SliceSource<'a>> {
        let mut reader = Reader::from_reader(xml);
        self.configure_reader(&mut reader);
        BatchStream::new(self, SliceSource { reader }, options)
    }

    /// Callback-style wrapper over [`Parser::parse_batches`]: drives the
    /// stream to completion, handing each batch to `sink`. A `sink` error
    /// aborts the parse and is returned as-is.
    ///
    /// # Errors
    ///
    /// Returns the first error the underlying stream yields (XML parsing,
    /// value conversion, or `RecordBatch` creation), or the first error
    /// returned by `sink` — whichever occurs first aborts the parse.
    pub fn parse_streaming<R, F>(&self, reader: R, options: BatchOptions, mut sink: F) -> Result<()>
    where
        R: BufRead,
        F: FnMut(&str, RecordBatch) -> Result<()>,
    {
        for item in self.parse_batches(reader, options) {
            let TableBatch { table, batch } = item?;
            sink(&table, batch)?;
        }
        Ok(())
    }

    /// Streams the single output table of this config as an
    /// [`arrow::array::RecordBatchReader`].
    ///
    /// Many configs — and virtually all "huge document" ones — define exactly
    /// one table with fields. For those, this adapter exposes the parse as
    /// the Arrow ecosystem's standard reader abstraction, directly consumable
    /// by `parquet::arrow::ArrowWriter`, DataFusion, or (through the C stream
    /// interface) pyarrow.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the config does not have exactly
    /// one table with fields (structural tables don't count — they produce no
    /// output). Use [`Parser::parse_batches`] for multi-table configs.
    pub fn parse_single_table<R: BufRead>(
        &self,
        reader: R,
        options: BatchOptions,
    ) -> Result<SingleTableReader<'_, ReaderSource<R>>> {
        let table_idx = self.single_output_table_index()?;
        Ok(SingleTableReader {
            schema: self.table_schemas[table_idx].clone(),
            stream: self.parse_batches(reader, options),
        })
    }

    /// Zero-copy variant of [`Parser::parse_single_table`] for in-memory (or
    /// memory-mapped) XML.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the config does not have exactly
    /// one table with fields, exactly like [`Parser::parse_single_table`].
    pub fn parse_single_table_slice<'a>(
        &'a self,
        xml: &'a [u8],
        options: BatchOptions,
    ) -> Result<SingleTableReader<'a, SliceSource<'a>>> {
        let table_idx = self.single_output_table_index()?;
        Ok(SingleTableReader {
            schema: self.table_schemas[table_idx].clone(),
            stream: self.parse_batches_slice(xml, options),
        })
    }

    /// Index of the unique output table (non-empty `fields`), or the
    /// `SingleTableRequired` config error.
    fn single_output_table_index(&self) -> Result<usize> {
        let mut output_tables = self
            .config
            .tables
            .iter()
            .enumerate()
            .filter(|(_, tc)| !tc.fields.is_empty())
            .map(|(idx, _)| idx);
        let first = output_tables.next();
        match (first, output_tables.next()) {
            (Some(idx), None) => Ok(idx),
            _ => Err(Error::InvalidConfig {
                reason: ConfigIssue::SingleTableRequired {
                    // Either no output table at all, or the two found plus
                    // whatever the iterator still holds.
                    output_tables: first.map_or(0, |_| 2 + output_tables.count()),
                },
            }),
        }
    }

    /// Applies `ParserOptions` to the shared `quick_xml::Reader` config.
    ///
    /// Both entry points configure the reader identically; isolating that
    /// here means each option lives in one place. `validate_closing_tags =
    /// false` is the per-event optimization described in
    /// `ParserOptions::validate_closing_tags`.
    fn configure_reader<R>(&self, reader: &mut Reader<R>) {
        let rc = reader.config_mut();
        if self.config.parser_options.trim_text {
            rc.trim_text(true);
        }
        if !self.config.parser_options.validate_closing_tags {
            rc.check_end_names = false;
        }
    }

    /// Shared parser driver used by both `parse` and `parse_slice`.
    ///
    /// The two entry points differ only in how they obtain events from their
    /// reader and whether attribute parsing is required; everything else —
    /// building the fresh per-parse converter, root-table priming, and
    /// finalization — is identical. Accepting the event loop as a closure lets
    /// each caller stay specialized (buffered vs zero-copy, attrs on vs off)
    /// without duplicating the surrounding orchestration.
    fn run_parse<R, F>(
        &self,
        reader: &mut R,
        run_events: F,
    ) -> Result<IndexMap<String, RecordBatch>>
    where
        F: FnOnce(
            &mut R,
            &mut PathTracker,
            &mut XmlToArrowConverter<'_>,
            &[PathNodeId],
        ) -> Result<()>,
    {
        // usize::MAX thresholds: collect everything, never trigger a flush
        // (root-table priming happens inside the converter constructor).
        let mut xml_to_arrow_converter = XmlToArrowConverter::new(self, usize::MAX, usize::MAX);
        let mut path_tracker = PathTracker::new(&self.registry);

        run_events(
            reader,
            &mut path_tracker,
            &mut xml_to_arrow_converter,
            &self.stop_node_ids,
        )?;

        xml_to_arrow_converter.finish()
    }
}

/// Parses XML data from a reader into Arrow record batches based on a provided configuration.
///
/// This is a convenience wrapper that compiles `config` into a [`Parser`] and
/// parses a single document. When parsing **many** documents with the same
/// `Config`, construct a [`Parser`] once and reuse it — that amortizes the
/// one-time path-compilation cost this wrapper pays on every call.
///
/// This function takes a reader implementing the `BufRead` trait (e.g., a `File`, `&[u8]`, or `String`)
/// and a `Config` struct that defines the structure of the XML data and how it should be mapped
/// to Arrow tables.
///
/// # Arguments
///
/// * `reader`: A reader object that provides access to the XML data.
/// * `config`: A `Config` struct that specifies the tables, fields, and data types to extract from the XML.
///
/// # Returns
///
/// A `Result` containing:
///
/// *   `Ok(IndexMap<String, RecordBatch>)`: An `IndexMap` where keys are the XML names of the tables (as defined in the config)
///     and values are the corresponding Arrow `RecordBatch` objects.
/// *   `Err(Error)`: An `Error` value if any error occurs during parsing, configuration, or Arrow table creation.
///
/// # Errors
///
/// Returns an error if configuration validation fails, XML parsing encounters invalid
/// data, value conversion fails, or Arrow `RecordBatch` creation fails.
///
/// # Example
///
/// ```rust
/// use xml2arrow::{parse_xml, config::{Config, TableConfig, FieldConfigBuilder, DType}};
/// use std::fs::File;
/// use std::io::BufReader;
///
/// let xml_content = r#"<data><item><value>123</value></item></data>"#;
/// let fields = vec![FieldConfigBuilder::new("value", "/data/item/value", DType::Int32).build().unwrap()];
/// let tables = vec![TableConfig::new("items", "/data", vec![], fields)];
/// let config = Config { tables, parser_options: Default::default() };
/// let record_batches = parse_xml(xml_content.as_bytes(), &config).unwrap();
/// // ... use record_batches
/// ```
pub fn parse_xml(reader: impl BufRead, config: &Config) -> Result<IndexMap<String, RecordBatch>> {
    Parser::new(config)?.parse(reader)
}

/// Parses XML data from an in-memory byte slice into Arrow record batches.
///
/// This is the zero-copy variant of [`parse_xml`]. When the XML data is already
/// in memory, this function avoids per-event buffer copies by using quick-xml's
/// slice reader, which returns events that borrow directly from the input.
/// For streaming sources (files, network), use [`parse_xml`] instead.
///
/// Like [`parse_xml`], this compiles a throwaway [`Parser`] per call; reuse a
/// [`Parser`] when parsing many slices with the same `Config`.
///
/// # Arguments
///
/// * `xml`: A byte slice containing the XML data.
/// * `config`: A `Config` struct that specifies the tables, fields, and data types to extract.
///
/// # Returns
///
/// An `IndexMap<String, RecordBatch>` where keys are table names and values are Arrow batches.
///
/// # Errors
///
/// Returns an error if configuration validation fails, XML parsing encounters invalid
/// data, value conversion fails, or Arrow `RecordBatch` creation fails.
///
/// # Example
///
/// ```rust
/// use xml2arrow::{parse_xml_slice, config::{Config, TableConfig, FieldConfigBuilder, DType}};
///
/// let xml = b"<data><item><value>123</value></item></data>";
/// let fields = vec![FieldConfigBuilder::new("value", "/data/item/value", DType::Int32).build().unwrap()];
/// let tables = vec![TableConfig::new("items", "/data", vec![], fields)];
/// let config = Config { tables, parser_options: Default::default() };
/// let record_batches = parse_xml_slice(xml, &config).unwrap();
/// ```
pub fn parse_xml_slice(xml: &[u8], config: &Config) -> Result<IndexMap<String, RecordBatch>> {
    Parser::new(config)?.parse_slice(xml)
}

// --- Event loop implementations ---
//
// Two loop variants exist to match how events are read from quick-xml:
//
// 1) `process_xml_events` (buffered): uses `read_event_into(&mut buf)` which
//    copies each event into a reusable buffer. Required for streaming readers.
//
// 2) `process_xml_events_slice` (zero-copy): uses `read_event()` on
//    `Reader<&[u8]>` which returns events that borrow directly from the input
//    slice, eliminating per-event copies.
//
// Both delegate to `handle_event` for the actual event processing so the
// match-arm logic is written exactly once.
//
// A THIRD pump exists in `BatchStream::next` (streaming output): it reads
// through the `EventSource` abstraction and steps one event at a time so a
// batch can be yielded mid-parse. All three share `handle_event`, but the
// read/Break/buffer discipline around it is written three times — any change
// to the loops below must be mirrored there, and vice versa.

/// The result of processing a single XML event, telling the event loop
/// whether to continue reading or stop (on EOF or a stop-path match).
enum LoopAction {
    Continue,
    Break,
}

/// Closes the currently entered element: pops the table (if any), leaves the
/// path tracker, finalizes the parent row (if the parent is a table or we are
/// at the root table), and signals a break when a stop path matches.
///
/// Called by both `Event::End` (capturing the top frame's data first) and
/// `Event::Empty` (which enters and immediately closes). Keeping the closing
/// semantics in one place avoids subtle drift between the two.
///
/// `is_table` is the closing element's own table flag, captured by the
/// caller from the path-tracker frame before this function pops it. After
/// `path_tracker.leave()`, the new top of the stack is the parent — so
/// `top_is_table()` answers "do we need to finalize a parent row?" without
/// any second `node_info` lookup. The implicit root frame carries its own
/// `is_table` (seeded at construction), so the root-table case does not
/// need a special branch.
#[inline]
fn close_element(
    node_id: Option<PathNodeId>,
    is_table: bool,
    path_tracker: &mut PathTracker,
    xml_to_arrow_converter: &mut XmlToArrowConverter<'_>,
    stop_node_ids: &[PathNodeId],
) -> Result<LoopAction> {
    if is_table {
        xml_to_arrow_converter.end_table();
    }
    path_tracker.leave();

    // Only *configured* elements delimit rows (node_id is Some). Letting an
    // unknown element finalize a row would fabricate null/empty rows whenever
    // the document grows siblings the config doesn't know about. Configured
    // children keep the established behavior: each known direct child of a
    // table element closing ends a row.
    if node_id.is_some() && path_tracker.top_is_table() {
        xml_to_arrow_converter.end_current_row()?;
    }

    // Stop after closing the configured path, so header-only reads can exit
    // without scanning the remainder of the XML.
    if let Some(node_id) = node_id
        && stop_node_ids.contains(&node_id)
    {
        return Ok(LoopAction::Break);
    }
    Ok(LoopAction::Continue)
}

/// Resolves a general reference in element text (`&#66;`, `&amp;`,
/// `&custom;`) and appends the resolved bytes to the fields listening at
/// `node_id`.
///
/// Two resolution steps cover the complete vocabulary a non-DTD-aware parser
/// can handle: character references (`&#66;`, `&#x41;`) and the five
/// predefined entities. A 4-byte stack buffer keeps the char-ref path
/// allocation-free. An undeclared/custom entity cannot be resolved without
/// DTD support; silently dropping it — the historical behavior — corrupted
/// the extracted value, so it errors instead, but only when a configured
/// field is actually capturing at this node.
///
/// Kept out of line (like `parse_attributes`): references are rare relative
/// to Start/Text/End events, and inlining this — with its error construction
/// — would grow `handle_event` for no hot-path benefit.
#[inline(never)]
fn handle_general_ref(
    e: BytesRef<'_>,
    node_id: PathNodeId,
    xml_to_arrow_converter: &mut XmlToArrowConverter<'_>,
) -> Result<()> {
    if let Some(ch) = e.resolve_char_ref()? {
        let mut utf8_buf = [0u8; 4];
        let encoded = ch.encode_utf8(&mut utf8_buf);
        xml_to_arrow_converter.set_field_value_for_node(node_id, encoded.as_bytes());
        return Ok(());
    }

    // The predefined-entity lookup requires a `&str`, but the vocabulary
    // (`amp`, `lt`, `gt`, `quot`, `apos`) is ASCII, so invalid UTF-8 simply
    // fails to resolve.
    let text = e.into_inner();
    let resolved = std::str::from_utf8(&text)
        .ok()
        .and_then(escape::resolve_predefined_entity);
    match resolved {
        Some(entity_text) => {
            xml_to_arrow_converter.set_field_value_for_node(node_id, entity_text.as_bytes());
        }
        None => {
            if let Some(meta) = xml_to_arrow_converter.active_field_meta(node_id) {
                return Err(Error::ParseError {
                    field: Arc::from(meta.name.as_str()),
                    path: Arc::from(meta.xml_path.as_str()),
                    value: String::from_utf8_lossy(&text).into_owned(),
                    kind: ParseKind::UnresolvedEntity,
                });
            }
        }
    }
    Ok(())
}

/// Processes a single XML event, updating path tracking, table builders, and field values.
///
/// This function encapsulates the core event-handling logic shared by both the
/// buffered and zero-copy parsing paths. Extracting it avoids duplicating the
/// match arms while keeping each loop wrapper focused solely on how it obtains
/// the next event.
///
/// On every `Event::Start` / `Event::Empty`, `path_tracker.enter()` returns
/// the new node together with a borrow of its `PathNodeInfo`. We extract the
/// few fields the arm actually needs (`table_index`, `has_attribute_children`)
/// before doing any further work on the converter, so the immutable borrow
/// of the registry ends and the converter is free to be mutated. The parent's
/// `is_table` flag — needed when this element later closes — was cached onto
/// the path-tracker frame by `enter()`, so we never re-read `node_info[]` at
/// `Event::End` time.
#[inline]
fn handle_event<const PARSE_ATTRIBUTES: bool>(
    event: Event<'_>,
    decoder: Decoder,
    path_tracker: &mut PathTracker,
    xml_to_arrow_converter: &mut XmlToArrowConverter<'_>,
    stop_node_ids: &[PathNodeId],
    attr_name_buffer: &mut Vec<u8>,
) -> Result<LoopAction> {
    match event {
        Event::Start(e) => {
            // `strip_namespaces` (default) resolves the local name, dropping any
            // `ns:` prefix; disabling it uses the raw qualified name and skips
            // quick-xml's per-name `:` scan — identical output for prefix-free
            // documents (see `ParserOptions::strip_namespaces`).
            let name_bytes = if xml_to_arrow_converter.strip_namespaces {
                e.local_name().into_inner()
            } else {
                e.name().into_inner()
            };

            // Fused lookup: child resolution + node-info read in one pass.
            // The `&PathNodeInfo` borrow is tied to the registry (not the
            // converter), so the repeated-element check can run on it before
            // the bits the rest of the arm needs are extracted into Copy
            // locals.
            let (node_id_opt, table_index, has_attrs) =
                match path_tracker.enter(name_bytes, xml_to_arrow_converter.registry) {
                    Some((id, info)) => {
                        // Re-entering a field element whose row value is
                        // already set means the element appeared twice —
                        // rejected while `info` is in hand (no extra
                        // registry read; see check_element_not_repeated).
                        if !info.field_indices.is_empty() {
                            xml_to_arrow_converter.check_element_not_repeated(info)?;
                        }
                        (Some(id), info.table_index, info.has_attribute_children)
                    }
                    None => (None, None, false),
                };

            if let Some(table_idx) = table_index {
                // node_id is Some when info was returned, so this unwrap holds.
                xml_to_arrow_converter.start_table(table_idx, node_id_opt.unwrap());
            }

            if PARSE_ATTRIBUTES && has_attrs {
                parse_attributes(
                    decoder,
                    e.attributes(),
                    path_tracker,
                    xml_to_arrow_converter,
                    attr_name_buffer,
                )?;
            }
        }
        Event::Empty(e) => {
            // `strip_namespaces` (default) resolves the local name, dropping any
            // `ns:` prefix; disabling it uses the raw qualified name and skips
            // quick-xml's per-name `:` scan — identical output for prefix-free
            // documents (see `ParserOptions::strip_namespaces`).
            let name_bytes = if xml_to_arrow_converter.strip_namespaces {
                e.local_name().into_inner()
            } else {
                e.name().into_inner()
            };

            let (node_id_opt, table_index, has_attrs, is_table) =
                match path_tracker.enter(name_bytes, xml_to_arrow_converter.registry) {
                    Some((id, info)) => {
                        // Same repeated-element guard as Event::Start.
                        if !info.field_indices.is_empty() {
                            xml_to_arrow_converter.check_element_not_repeated(info)?;
                        }
                        (
                            Some(id),
                            info.table_index,
                            info.has_attribute_children,
                            info.is_table(),
                        )
                    }
                    None => (None, None, false, false),
                };

            if let Some(table_idx) = table_index {
                xml_to_arrow_converter.start_table(table_idx, node_id_opt.unwrap());
            }

            if PARSE_ATTRIBUTES && has_attrs {
                parse_attributes(
                    decoder,
                    e.attributes(),
                    path_tracker,
                    xml_to_arrow_converter,
                    attr_name_buffer,
                )?;
            }

            // Empty elements have no children or text; close immediately.
            // `close_element` will pop this element's frame via leave(), so
            // top_is_table() then reads the parent — exactly matching the
            // Event::End behavior below.
            return close_element(
                node_id_opt,
                is_table,
                path_tracker,
                xml_to_arrow_converter,
                stop_node_ids,
            );
        }
        Event::GeneralRef(e) => {
            if let Some(node_id) = path_tracker.current() {
                handle_general_ref(e, node_id, xml_to_arrow_converter)?;
            }
        }
        Event::Text(e) => {
            if let Some(node_id) = path_tracker.current() {
                let text = e.into_inner();
                xml_to_arrow_converter.set_field_value_for_node(node_id, &text);
            }
        }
        Event::CData(e) => {
            if let Some(node_id) = path_tracker.current() {
                let text = e.into_inner();
                xml_to_arrow_converter.set_field_value_for_node(node_id, &text);
            }
        }
        Event::End(_) => {
            // Read this element's own (node_id, is_table) from the top of
            // the merged stack BEFORE close_element pops it via leave().
            // peek_top() returns None when the stack is at root, matching
            // the prior "End with empty element_stack ⇒ no-op" behavior.
            if let Some((node_id_opt, is_table)) = path_tracker.peek_top() {
                return close_element(
                    node_id_opt,
                    is_table,
                    path_tracker,
                    xml_to_arrow_converter,
                    stop_node_ids,
                );
            }
        }
        Event::Eof => {
            return Ok(LoopAction::Break);
        }
        _ => (),
    }
    Ok(LoopAction::Continue)
}

/// Streaming (buffered) XML event loop for readers that implement `BufRead`.
///
/// This path copies each event into a reusable buffer via `read_event_into`.
/// For in-memory byte slices, prefer `process_xml_events_slice` which avoids
/// the copy entirely.
fn process_xml_events<B: BufRead, const PARSE_ATTRIBUTES: bool>(
    reader: &mut Reader<B>,
    path_tracker: &mut PathTracker,
    xml_to_arrow_converter: &mut XmlToArrowConverter<'_>,
    stop_node_ids: &[PathNodeId],
) -> Result<()> {
    let mut buf = Vec::with_capacity(4096);
    let mut attr_name_buffer = Vec::with_capacity(64);
    // Decoder is `Copy` and reflects the reader's encoding, which does not
    // change once parsing begins — read it once outside the hot loop.
    let decoder = reader.decoder();

    loop {
        let event = reader.read_event_into(&mut buf)?;
        let action = handle_event::<PARSE_ATTRIBUTES>(
            event,
            decoder,
            path_tracker,
            xml_to_arrow_converter,
            stop_node_ids,
            &mut attr_name_buffer,
        )?;
        if matches!(action, LoopAction::Break) {
            break;
        }
        buf.clear();
    }
    Ok(())
}

/// Zero-copy XML event loop for in-memory byte slices.
///
/// When the XML input is already in memory, `Reader<&[u8]>::read_event()`
/// returns events that borrow directly from the input slice — no buffer
/// allocation or per-event copy required. This is the fast path for the
/// common case where the caller has the full XML in a `&[u8]` or `&str`.
fn process_xml_events_slice<const PARSE_ATTRIBUTES: bool>(
    reader: &mut Reader<&[u8]>,
    path_tracker: &mut PathTracker,
    xml_to_arrow_converter: &mut XmlToArrowConverter<'_>,
    stop_node_ids: &[PathNodeId],
) -> Result<()> {
    let mut attr_name_buffer = Vec::with_capacity(64);
    // Decoder is `Copy` and reflects the reader's encoding, which does not
    // change once parsing begins — read it once outside the hot loop.
    let decoder = reader.decoder();

    loop {
        let event = reader.read_event()?;
        let action = handle_event::<PARSE_ATTRIBUTES>(
            event,
            decoder,
            path_tracker,
            xml_to_arrow_converter,
            stop_node_ids,
            &mut attr_name_buffer,
        )?;
        if matches!(action, LoopAction::Break) {
            break;
        }
    }
    Ok(())
}

#[inline(never)]
fn parse_attributes(
    decoder: Decoder,
    mut attributes: Attributes,
    path_tracker: &mut PathTracker,
    xml_to_arrow_converter: &mut XmlToArrowConverter<'_>,
    attr_name_buffer: &mut Vec<u8>,
) -> Result<()> {
    // For trusted inputs, skip quick-xml's duplicate-attribute detection.
    // That check is O(n²) in the element's attribute count and, more
    // importantly, allocates a `Vec` per attribute-bearing element to record
    // the seen key ranges — pure overhead when we don't act on duplicates.
    if !xml_to_arrow_converter.validate_attributes {
        attributes.with_checks(false);
    }
    // Hoisted out of the loop: same `:`-scan trade-off as element names, applied
    // to each attribute key (see `ParserOptions::strip_namespaces`).
    let strip_namespaces = xml_to_arrow_converter.strip_namespaces;
    for attribute in attributes {
        let attribute = attribute?;
        let key = if strip_namespaces {
            attribute.key.local_name().into_inner()
        } else {
            attribute.key.into_inner()
        };

        // Reuse buffer to avoid allocation: build "@key" as bytes
        attr_name_buffer.clear();
        attr_name_buffer.push(b'@');
        attr_name_buffer.extend_from_slice(key);

        // Attributes only need the node ID; the `&PathNodeInfo` borrow
        // returned by enter() is dropped immediately so the converter can
        // be mutated below.
        let attr_node_id = path_tracker
            .enter(attr_name_buffer, xml_to_arrow_converter.registry)
            .map(|(id, _)| id);
        if let Some(id) = attr_node_id {
            // Attribute values are almost always plain UTF-8 needing no
            // processing; in that case `decoded_and_normalized_value` is
            // wasted work — it runs a per-attribute decode + unescape, and
            // `Utf8` fields are validated exactly once at row finalization
            // (`append_current_value`) anyway while numeric fields parse
            // straight from bytes. The slow path triggers on exactly the
            // bytes a conforming parser must process: `&` starts an entity
            // or character reference, and literal `\t`, `\r`, `\n` become
            // spaces under XML 1.0 attribute-value normalization
            // (https://www.w3.org/TR/xml/#AVNormalize). Values containing
            // none of the four are byte-identical either way.
            //
            // Element text (`Event::Text`) intentionally stays raw: the XML
            // spec applies this normalization to attribute values only.
            let raw = attribute.value.as_ref();
            if raw
                .iter()
                .any(|b| matches!(b, b'&' | b'\t' | b'\r' | b'\n'))
            {
                // `Implicit1_0` selects plain XML 1.0 normalization (no
                // XML 1.1 extras), the same default the deprecated
                // `decode_and_unescape_value` hardcoded.
                let value =
                    attribute.decoded_and_normalized_value(XmlVersion::Implicit1_0, decoder)?;
                xml_to_arrow_converter.set_field_value_for_node(id, value.as_bytes());
            } else {
                xml_to_arrow_converter.set_field_value_for_node(id, raw);
            }
        }
        path_tracker.leave();
    }
    Ok(())
}

// === Streaming (batched) output ===
//
// The collect-everything entry points above accumulate the whole document
// into one RecordBatch per table; peak memory is proportional to the
// dataset. The types below bound that: `BatchStream` steps the same
// `handle_event` core one event at a time and emits a table's accumulated
// rows whenever the table crosses a `BatchOptions` threshold. Flushing is
// value-transparent (see `TableBuilder::flush`): concatenating a table's
// streamed batches reproduces the collect-everything output exactly.

/// Flush thresholds for the streaming entry points ([`Parser::parse_batches`]
/// and friends). A table's batch is emitted as soon as *either* threshold is
/// reached. Both are checked at row boundaries only, so a batch can exceed
/// `max_bytes_per_batch` by at most one row's bytes.
///
/// Marked `#[non_exhaustive]`; construct via `Default` and adjust fields, or
/// chain the `with_*` setters:
///
/// ```rust
/// use xml2arrow::BatchOptions;
/// let options = BatchOptions::default().with_max_rows_per_batch(1024);
/// ```
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct BatchOptions {
    /// Emit a table's batch once it holds this many rows. Defaults to 8192
    /// (the de-facto Arrow ecosystem working batch size). `0` is treated as
    /// `1` — a yielded batch always has at least one row.
    pub max_rows_per_batch: usize,
    /// Also emit once a table has accumulated this many raw value bytes,
    /// whichever threshold is hit first. Defaults to 128 MiB. This guards
    /// against fat rows (multi-MB text fields) exhausting `StringBuilder`'s
    /// i32-offset 2 GiB ceiling long before the row cap is reached.
    pub max_bytes_per_batch: usize,
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            max_rows_per_batch: 8192,
            max_bytes_per_batch: 128 * 1024 * 1024,
        }
    }
}

impl BatchOptions {
    /// Returns `self` with `max_rows_per_batch` replaced.
    #[must_use]
    pub fn with_max_rows_per_batch(mut self, rows: usize) -> Self {
        self.max_rows_per_batch = rows;
        self
    }

    /// Returns `self` with `max_bytes_per_batch` replaced.
    #[must_use]
    pub fn with_max_bytes_per_batch(mut self, bytes: usize) -> Self {
        self.max_bytes_per_batch = bytes;
        self
    }
}

/// One streamed batch: the table it belongs to and a `RecordBatch` of its
/// rows. Yielded by [`BatchStream`].
#[derive(Debug, Clone)]
pub struct TableBatch {
    /// The configured table name. `Arc<str>`: all batches of one table share
    /// the allocation, cloning is a refcount bump.
    pub table: Arc<str>,
    /// The rows accumulated since this table's previous batch.
    pub batch: RecordBatch,
}

mod sealed {
    pub trait Sealed {}
}

/// A source of XML events for [`BatchStream`] — the streaming counterpart of
/// the buffered/zero-copy split in the collect-everything entry points.
///
/// Sealed: implemented only by [`ReaderSource`] (buffered, any `BufRead`) and
/// [`SliceSource`] (zero-copy, in-memory slice). The trait exists so the
/// streaming machinery is written once; its methods are an internal detail.
pub trait EventSource: sealed::Sealed {
    #[doc(hidden)]
    fn read_event(&mut self) -> std::result::Result<Event<'_>, quick_xml::Error>;
    #[doc(hidden)]
    fn decoder(&self) -> Decoder;
}

/// Buffered [`EventSource`] over any `BufRead` (files, sockets,
/// decompression adapters). Each event is copied into a reusable buffer —
/// required for streaming readers, same trade-off as [`Parser::parse`].
pub struct ReaderSource<R: BufRead> {
    reader: Reader<R>,
    buf: Vec<u8>,
}

impl<R: BufRead> sealed::Sealed for ReaderSource<R> {}

impl<R: BufRead> EventSource for ReaderSource<R> {
    fn read_event(&mut self) -> std::result::Result<Event<'_>, quick_xml::Error> {
        self.buf.clear();
        self.reader.read_event_into(&mut self.buf)
    }

    fn decoder(&self) -> Decoder {
        self.reader.decoder()
    }
}

/// Zero-copy [`EventSource`] over an in-memory byte slice; events borrow
/// directly from the input, same trade-off as [`Parser::parse_slice`].
pub struct SliceSource<'x> {
    reader: Reader<&'x [u8]>,
}

impl sealed::Sealed for SliceSource<'_> {}

impl EventSource for SliceSource<'_> {
    fn read_event(&mut self) -> std::result::Result<Event<'_>, quick_xml::Error> {
        self.reader.read_event()
    }

    fn decoder(&self) -> Decoder {
        self.reader.decoder()
    }
}

/// Where the streaming loop is in its lifecycle.
#[derive(Clone, Copy, Debug)]
enum StreamState {
    /// Reading events; batches are emitted on threshold crossings.
    Running,
    /// Input exhausted (EOF or a stop-path match). Emitting each table's
    /// remaining rows in config order, resuming at `next_table`.
    Draining { next_table: usize },
    /// Everything emitted, or an error was yielded. Only `None` from here.
    Done,
}

/// Incremental XML → Arrow parse: an iterator of [`TableBatch`]es with
/// bounded memory.
///
/// Created by [`Parser::parse_batches`] / [`Parser::parse_batches_slice`];
/// see [`Parser::parse_batches`] for the yielded guarantees. The stream
/// borrows its [`Parser`] immutably, so one compiled parser can serve many
/// concurrent streams.
///
/// The iterator is fused: after `None` — or after yielding an `Err` — it
/// only returns `None`.
pub struct BatchStream<'p, S> {
    parser: &'p Parser,
    source: S,
    /// Copy of the reader's decoder, read once — the encoding cannot change
    /// once parsing begins (mirrors the collect-everything loops).
    decoder: Decoder,
    path_tracker: PathTracker,
    converter: XmlToArrowConverter<'p>,
    attr_name_buffer: Vec<u8>,
    state: StreamState,
}

impl<'p, S: EventSource> BatchStream<'p, S> {
    fn new(parser: &'p Parser, source: S, options: BatchOptions) -> Self {
        let decoder = source.decoder();
        Self {
            parser,
            source,
            decoder,
            path_tracker: PathTracker::new(&parser.registry),
            converter: XmlToArrowConverter::new(
                parser,
                options.max_rows_per_batch,
                options.max_bytes_per_batch,
            ),
            attr_name_buffer: Vec::with_capacity(64),
            state: StreamState::Running,
        }
    }

    /// Flushes `table_idx`'s accumulated rows into a [`TableBatch`].
    fn flush_table(&mut self, table_idx: usize) -> Result<TableBatch> {
        let batch = self.converter.table_builders[table_idx].flush()?;
        Ok(TableBatch {
            table: self.parser.table_names[table_idx].clone(),
            batch,
        })
    }

    /// Yields the next non-empty leftover table during the drain phase,
    /// advancing to `Done` when none remain.
    fn drain_next(&mut self) -> Option<Result<TableBatch>> {
        let StreamState::Draining { next_table } = self.state else {
            return None;
        };
        for table_idx in next_table..self.converter.table_builders.len() {
            let table = &self.converter.table_builders[table_idx];
            // Structural tables never emit; tables whose rows all went out
            // in threshold flushes have nothing left. Skipping them keeps
            // the "a yielded batch always has ≥ 1 row" contract.
            if table.field_builders.is_empty() || table.rows_in_batch == 0 {
                continue;
            }
            self.state = StreamState::Draining {
                next_table: table_idx + 1,
            };
            return match self.flush_table(table_idx) {
                Ok(item) => Some(Ok(item)),
                Err(e) => {
                    self.state = StreamState::Done;
                    Some(Err(e))
                }
            };
        }
        self.state = StreamState::Done;
        None
    }
}

impl<'p, S: EventSource> Iterator for BatchStream<'p, S> {
    type Item = Result<TableBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.state {
                StreamState::Done => return None,
                StreamState::Draining { .. } => return self.drain_next(),
                StreamState::Running => {
                    let event = match self.source.read_event() {
                        Ok(event) => event,
                        Err(e) => {
                            self.state = StreamState::Done;
                            return Some(Err(e.into()));
                        }
                    };
                    // Runtime dispatch on `needs_attrs` where the collect
                    // loops monomorphize: one perfectly-predicted branch per
                    // event buys a single public stream type per source.
                    // Keep the surrounding read/Break discipline in lockstep
                    // with `process_xml_events` / `process_xml_events_slice`.
                    let handled = if self.parser.needs_attrs {
                        handle_event::<true>(
                            event,
                            self.decoder,
                            &mut self.path_tracker,
                            &mut self.converter,
                            &self.parser.stop_node_ids,
                            &mut self.attr_name_buffer,
                        )
                    } else {
                        handle_event::<false>(
                            event,
                            self.decoder,
                            &mut self.path_tracker,
                            &mut self.converter,
                            &self.parser.stop_node_ids,
                            &mut self.attr_name_buffer,
                        )
                    };
                    let action = match handled {
                        Ok(action) => action,
                        Err(e) => {
                            self.state = StreamState::Done;
                            return Some(Err(e));
                        }
                    };
                    // Transition on Break *before* emitting a pending flush:
                    // a stop-path close can both finalize a row (tripping a
                    // threshold) and end the parse. The flushed batch goes
                    // out now; the next call continues draining.
                    if matches!(action, LoopAction::Break) {
                        self.state = StreamState::Draining { next_table: 0 };
                    }
                    if let Some(table_idx) = self.converter.pending_flush.take() {
                        return match self.flush_table(table_idx) {
                            Ok(item) => Some(Ok(item)),
                            Err(e) => {
                                self.state = StreamState::Done;
                                Some(Err(e))
                            }
                        };
                    }
                }
            }
        }
    }
}

impl<'p, S: EventSource> FusedIterator for BatchStream<'p, S> {}

/// Manual `Debug` (rather than derive) so it exists for every source type —
/// `Reader<R>` internals aren't `Debug` — and so users can `unwrap`/`expect`
/// on `Result`s containing the stream in tests.
impl<S> std::fmt::Debug for BatchStream<'_, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchStream")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

/// A [`RecordBatchReader`] over the single output table of a config.
///
/// Created by [`Parser::parse_single_table`] /
/// [`Parser::parse_single_table_slice`]. Yields the table's batches in row
/// order; [`RecordBatchReader::schema`] returns the same `Arc<Schema>` every
/// batch carries, available before the first byte is parsed.
///
/// Errors surface as `ArrowError` (the trait's error type): native Arrow
/// errors pass through, everything else wraps as
/// `ArrowError::ExternalError` with this crate's [`Error`] as the source.
pub struct SingleTableReader<'p, S> {
    schema: SchemaRef,
    stream: BatchStream<'p, S>,
}

impl<'p, S: EventSource> Iterator for SingleTableReader<'p, S> {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // The stream can only ever yield the one output table (structural
        // tables never emit), so no name filtering is needed.
        match self.stream.next()? {
            Ok(table_batch) => Some(Ok(table_batch.batch)),
            Err(Error::Arrow(e)) => Some(Err(e)),
            Err(e) => Some(Err(arrow::error::ArrowError::ExternalError(Box::new(e)))),
        }
    }
}

impl<'p, S: EventSource> FusedIterator for SingleTableReader<'p, S> {}

impl<'p, S: EventSource> RecordBatchReader for SingleTableReader<'p, S> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Manual `Debug` for the same reasons as [`BatchStream`]'s — and because
/// `Result<SingleTableReader, _>::unwrap_err()` in caller tests requires it.
impl<S> std::fmt::Debug for SingleTableReader<'_, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleTableReader")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::config::Config;
    use crate::config_from_yaml;
    use approx::abs_diff_eq;
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use rstest::rstest;

    /// Parse XML from a string using an inline YAML config.
    /// Panics on error -- intended for tests with known-good inputs.
    fn parse(xml: &str, yaml_config: &str) -> IndexMap<String, RecordBatch> {
        let config = config_from_yaml!(yaml_config);
        parse_xml(xml.as_bytes(), &config).unwrap()
    }

    macro_rules! assert_array_values {
        ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty) => {{
            let array = $batch
                .column_by_name($column_name)
                .unwrap_or_else(|| panic!("Column '{}' not found in batch", $column_name))
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap_or_else(|| {
                    panic!(
                        "Column '{}' could not be downcast to {}",
                        $column_name,
                        stringify!($array_type)
                    )
                });
            assert_eq!(
                array.len(),
                $expected_values.len(),
                "Array length mismatch for column '{}'",
                $column_name
            );
            for (i, expected) in $expected_values.iter().enumerate() {
                assert_eq!(
                    array.value(i),
                    *expected,
                    "Value mismatch at index {} for column '{}'",
                    i,
                    $column_name
                );
            }
        }};
    }

    macro_rules! assert_array_values_option {
        ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty) => {{
            let array = $batch
                .column_by_name($column_name)
                .unwrap_or_else(|| panic!("Column '{}' not found in batch", $column_name))
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap_or_else(|| {
                    panic!(
                        "Column '{}' could not be downcast to {}",
                        $column_name,
                        stringify!($array_type)
                    )
                });
            assert_eq!(
                array.len(),
                $expected_values.len(),
                "Array length mismatch for column '{}'",
                $column_name
            );
            for (i, expected) in $expected_values.iter().enumerate() {
                match expected {
                    Some(val) => assert_eq!(
                        array.value(i),
                        *val,
                        "Value mismatch at index {} for column '{}'",
                        i,
                        $column_name
                    ),
                    None => assert!(
                        array.is_null(i),
                        "Expected null at index {} for column '{}'",
                        i,
                        $column_name
                    ),
                }
            }
        }};
    }

    macro_rules! assert_array_approx_values {
        ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty, $tolerance:expr) => {{
            let array = $batch
                .column_by_name($column_name)
                .unwrap_or_else(|| panic!("Column '{}' not found in batch", $column_name))
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap_or_else(|| {
                    panic!(
                        "Column '{}' could not be downcast to {}",
                        $column_name,
                        stringify!($array_type)
                    )
                });
            assert_eq!(
                array.len(),
                $expected_values.len(),
                "Array length mismatch for column '{}'",
                $column_name
            );
            for (i, expected) in $expected_values.iter().enumerate() {
                assert!(
                    abs_diff_eq!(array.value(i), *expected, epsilon = $tolerance),
                    "Value mismatch at index {} for column '{}': expected {}, got {}",
                    i,
                    $column_name,
                    expected,
                    array.value(i)
                );
            }
        }};
    }

    macro_rules! assert_array_approx_values_option {
        ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty, $tolerance:expr) => {{
            let array = $batch
                .column_by_name($column_name)
                .unwrap_or_else(|| panic!("Column '{}' not found in batch", $column_name))
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap_or_else(|| {
                    panic!(
                        "Column '{}' could not be downcast to {}",
                        $column_name,
                        stringify!($array_type)
                    )
                });
            assert_eq!(
                array.len(),
                $expected_values.len(),
                "Array length mismatch for column '{}'",
                $column_name
            );
            for (i, expected) in $expected_values.iter().enumerate() {
                match expected {
                    Some(val) => assert!(
                        abs_diff_eq!(array.value(i), *val, epsilon = $tolerance),
                        "Value mismatch at index {} for column '{}': expected {}, got {}",
                        i,
                        $column_name,
                        val,
                        array.value(i)
                    ),
                    None => assert!(
                        array.is_null(i),
                        "Expected null at index {} for column '{}'",
                        i,
                        $column_name
                    ),
                }
            }
        }};
    }

    #[test]
    fn test_multiple_nested_tables_parsed_correctly() {
        let record_batches = parse(
            r#"
            <?xml version="1.0" encoding="UTF-8"?>
            <data>
              <dataset>
                <table>
                  <item>
                    <name>Item 1</name>
                    <value>10.5</value>
                  </item>
                  <item>
                    <name>Item 2</name>
                    <value>20.5</value>
                  </item>
                </table>
                <table>
                  <item>
                    <name>Item 3</name>
                    <value>30.5</value>
                  </item>
                </table>
              </dataset>
            </data>
            "#,
            r#"
            tables:
                - name: tables
                  xml_path: /data/dataset
                  levels:
                    - table
                  fields: []
                - name: items
                  xml_path: /data/dataset/table
                  levels:
                    - table
                    - item
                  fields:
                    - name: name
                      xml_path: /data/dataset/table/item/name
                      data_type: Utf8
                    - name: value
                      xml_path: /data/dataset/table/item/value
                      data_type: Float64
            "#,
        );

        // Tables table has no fields so not in output
        let items_batch = record_batches.get("items").unwrap();
        assert_eq!(items_batch.num_rows(), 3);
        assert_array_values!(items_batch, "<table>", vec![0u32, 0, 1], UInt32Array);
        assert_array_values!(items_batch, "<item>", vec![0u32, 1, 0], UInt32Array);
        assert_array_values!(
            items_batch,
            "name",
            vec!["Item 1", "Item 2", "Item 3"],
            StringArray
        );
        assert_array_values!(items_batch, "value", vec![10.5, 20.5, 30.5], Float64Array);
    }

    #[test]
    fn test_stop_at_paths_extracts_header_only() {
        let xml_content = r#"
        <report>
            <header>
                <title>Header Title</title>
                <created_by>Unit Test</created_by>
            </header>
            <data>
                <item><value>1</value></item>
                <item><value>2</value></item>
            </data>
        </report>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            parser_options:
                stop_at_paths:
                    - /report/header
            tables:
                - name: header
                  xml_path: /report
                  levels: [header]
                  fields:
                    - name: title
                      xml_path: /report/header/title
                      data_type: Utf8
                    - name: created_by
                      xml_path: /report/header/created_by
                      data_type: Utf8
                - name: data
                  xml_path: /report/data
                  levels: [item]
                  fields:
                    - name: value
                      xml_path: /report/data/item/value
                      data_type: Int32
            "#,
        );

        let header_batch = record_batches.get("header").unwrap();
        assert_eq!(header_batch.num_rows(), 1);
        assert_array_values!(header_batch, "title", vec!["Header Title"], StringArray);
        assert_array_values!(header_batch, "created_by", vec!["Unit Test"], StringArray);

        let data_batch = record_batches.get("data").unwrap();
        assert_eq!(data_batch.num_rows(), 0);
    }

    #[test]
    fn test_multiple_tables_parsed_correctly() {
        let xml_content = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <root>
            <items>
                <item>
                    <name>Item 1</name>
                    <value>10.5</value>
                </item>
                <item>
                    <name>Item 2</name>
                    <value>20.5</value>
                </item>
            </items>
            <metadata>
                <version>1.0</version>
            </metadata>
        </root>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: items
                  xml_path: /root/items
                  levels:
                    - item
                  fields:
                    - name: name
                      xml_path: /root/items/item/name
                      data_type: Utf8
                    - name: value
                      xml_path: /root/items/item/value
                      data_type: Float64
                - name: metadata
                  xml_path: /root/metadata
                  levels: []
                  fields:
                    - name: version
                      xml_path: /root/metadata/version
                      data_type: Utf8
            "#,
        );

        assert_eq!(record_batches.len(), 2);

        // Check items table
        let items_batch = record_batches.get("items").unwrap();
        assert_eq!(items_batch.num_rows(), 2);
        assert_array_values!(items_batch, "<item>", vec![0u32, 1], UInt32Array);
        assert_array_values!(items_batch, "name", vec!["Item 1", "Item 2"], StringArray);
        assert_array_values!(items_batch, "value", vec![10.5, 20.5], Float64Array);

        // Check metadata table
        let metadata_batch = record_batches.get("metadata").unwrap();
        assert_eq!(metadata_batch.num_rows(), 1);
        assert_array_values!(metadata_batch, "version", vec!["1.0"], StringArray);
    }

    #[test]
    fn test_all_numeric_dtypes_parsed_correctly() {
        let xml_content = r#"
        <data>
            <row>
                <int8>-128</int8>
                <uint8>255</uint8>
                <int16>-32768</int16>
                <uint16>65535</uint16>
                <int32>-2147483648</int32>
                <uint32>4294967295</uint32>
                <int64>-9223372036854775808</int64>
                <uint64>18446744073709551615</uint64>
                <float32>3.14</float32>
                <float64>2.718281828459045</float64>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: numbers
                  xml_path: /data
                  levels:
                    - row
                  fields:
                    - name: int8
                      xml_path: /data/row/int8
                      data_type: Int8
                    - name: uint8
                      xml_path: /data/row/uint8
                      data_type: UInt8
                    - name: int16
                      xml_path: /data/row/int16
                      data_type: Int16
                    - name: uint16
                      xml_path: /data/row/uint16
                      data_type: UInt16
                    - name: int32
                      xml_path: /data/row/int32
                      data_type: Int32
                    - name: uint32
                      xml_path: /data/row/uint32
                      data_type: UInt32
                    - name: int64
                      xml_path: /data/row/int64
                      data_type: Int64
                    - name: uint64
                      xml_path: /data/row/uint64
                      data_type: UInt64
                    - name: float32
                      xml_path: /data/row/float32
                      data_type: Float32
                    - name: float64
                      xml_path: /data/row/float64
                      data_type: Float64
            "#,
        );

        let batch = record_batches.get("numbers").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "int8", vec![-128i8], Int8Array);
        assert_array_values!(batch, "uint8", vec![255u8], UInt8Array);
        assert_array_values!(batch, "int16", vec![-32768i16], Int16Array);
        assert_array_values!(batch, "uint16", vec![65535u16], UInt16Array);
        assert_array_values!(batch, "int32", vec![-2147483648i32], Int32Array);
        assert_array_values!(batch, "uint32", vec![4294967295u32], UInt32Array);
        assert_array_values!(batch, "int64", vec![-9223372036854775808i64], Int64Array);
        assert_array_values!(batch, "uint64", vec![18446744073709551615u64], UInt64Array);
        assert_array_approx_values!(batch, "float32", vec![3.14f32], Float32Array, 0.001);
        assert_array_approx_values!(
            batch,
            "float64",
            vec![2.718281828459045f64],
            Float64Array,
            0.0000000001
        );
    }

    #[test]
    fn test_xml_entities_unescaped_in_text() {
        let xml_content = r#"<data><row><text>&lt;hello&gt; &amp; "world"</text></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: text_table
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: text
                      xml_path: /data/row/text
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("text_table").unwrap();
        assert_array_values!(batch, "text", vec!["<hello> & \"world\""], StringArray);
    }

    #[test]
    fn test_empty_input_returns_empty_batch() {
        let xml_content = r#"<data></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: empty_table
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("empty_table").unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_scale_and_offset_applied_to_float64() {
        let xml_content = r#"
        <data>
            <row><value>100</value></row>
            <row><value>200</value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: scaled
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      scale: 0.1
                      offset: 10.0
            "#,
        );
        let batch = record_batches.get("scaled").unwrap();
        // value = value * scale + offset = 100 * 0.1 + 10 = 20, 200 * 0.1 + 10 = 30
        assert_array_approx_values!(batch, "value", vec![20.0, 30.0], Float64Array, 0.001);
    }

    #[test]
    fn test_scale_and_offset_applied_to_float32() {
        let xml_content = r#"
        <data>
            <row><value>100</value></row>
            <row><value>200</value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: scaled
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float32
                      scale: 0.1
                      offset: 10.0
            "#,
        );
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![20.0f32, 30.0f32], Float32Array, 0.001);
    }

    #[test]
    fn test_multiple_attributes_parsed_correctly() {
        let xml_content = r#"
        <data>
            <item id="1" name="First" type="A">Content 1</item>
            <item id="2" name="Second" type="B">Content 2</item>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: items
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: id
                      xml_path: /data/item/@id
                      data_type: Int32
                    - name: name
                      xml_path: /data/item/@name
                      data_type: Utf8
                    - name: type
                      xml_path: /data/item/@type
                      data_type: Utf8
                    - name: content
                      xml_path: /data/item
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values!(batch, "id", vec![1i32, 2], Int32Array);
        assert_array_values!(batch, "name", vec!["First", "Second"], StringArray);
        assert_array_values!(batch, "type", vec!["A", "B"], StringArray);
        assert_array_values!(
            batch,
            "content",
            vec!["Content 1", "Content 2"],
            StringArray
        );
    }

    #[test]
    fn test_validate_attributes_false_still_parses_attributes() {
        // Disabling quick-xml's duplicate-attribute check (the N2 fast path)
        // must not change how well-formed attributes are read.
        let xml_content = r#"
        <data>
            <item id="1" name="First">Content 1</item>
            <item id="2" name="Second">Content 2</item>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            parser_options:
                validate_attributes: false
            tables:
                - name: items
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: id
                      xml_path: /data/item/@id
                      data_type: Int32
                    - name: name
                      xml_path: /data/item/@name
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values!(batch, "id", vec![1i32, 2], Int32Array);
        assert_array_values!(batch, "name", vec!["First", "Second"], StringArray);
    }

    #[test]
    fn test_validate_attributes_false_tolerates_duplicate_attribute() {
        // With the check disabled, a duplicated attribute is no longer a
        // hard error. quick-xml yields both, and our accumulator appends —
        // documenting the (intentionally unguarded) trusted-input behavior.
        let xml_content = r#"<data><item name="a" name="b"/></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            parser_options:
                validate_attributes: false
            tables:
                - name: items
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: name
                      xml_path: /data/item/@name
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 1);
        // Both attribute values are concatenated rather than rejected.
        assert_array_values!(batch, "name", vec!["ab"], StringArray);
    }

    #[test]
    fn test_three_level_nesting_produces_parent_indices() {
        let xml_content = r#"
        <root>
            <group>
                <item><value>1</value></item>
                <item><value>2</value></item>
            </group>
            <group>
                <item><value>3</value></item>
                <item><value>4</value></item>
                <item><value>5</value></item>
            </group>
            <group>
                <item><value>6</value></item>
            </group>
        </root>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: groups
                  xml_path: /root
                  levels: [group]
                  fields: []
                - name: items
                  xml_path: /root/group
                  levels: [group, item]
                  fields:
                    - name: value
                      xml_path: /root/group/item/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("items").unwrap();

        assert_eq!(batch.num_rows(), 6);

        // Each item's <group> index should reflect which group it belongs to,
        // not reset to 0 for every new group.
        assert_array_values!(batch, "<group>", vec![0u32, 0, 1, 1, 1, 2], UInt32Array);
        assert_array_values!(batch, "<item>", vec![0u32, 1, 0, 1, 2, 0], UInt32Array);
        assert_array_values!(batch, "value", vec![1i32, 2, 3, 4, 5, 6], Int32Array);
    }

    #[test]
    fn test_deeply_nested_structure_parsed_correctly() {
        let xml_content = r#"
        <level1>
            <level2>
                <level3>
                    <level4>
                        <level5>
                            <row>
                                <value>42</value>
                            </row>
                        </level5>
                    </level4>
                </level3>
            </level2>
        </level1>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: deep
                  xml_path: /level1/level2/level3/level4/level5
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /level1/level2/level3/level4/level5/row/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("deep").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "value", vec![42i32], Int32Array);
    }

    #[test]
    fn test_deeply_nested_generic_paths_parsed_correctly() {
        let xml_content = r#"
        <level1>
            <level2>
                <level3>
                    <row>
                        <value>1</value>
                    </row>
                    <row>
                        <value>2</value>
                    </row>
                </level3>
                <level3>
                    <row>
                        <value>3</value>
                    </row>
                </level3>
            </level2>
            <level2>
                <level3>
                    <row>
                        <value>4</value>
                    </row>
                    <row>
                        <value>5</value>
                    </row>
                </level3>
            </level2>
        </level1>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: level2s
                  xml_path: /level1
                  levels: [level2]
                  fields: []
                - name: level3s
                  xml_path: /level1/level2
                  levels: [level2, level3]
                  fields: []
                - name: rows
                  xml_path: /level1/level2/level3
                  levels: [level2, level3, row]
                  fields:
                    - name: value
                      xml_path: /level1/level2/level3/row/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("rows").unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_array_values!(batch, "<level2>", vec![0u32, 0, 0, 1, 1], UInt32Array);
        assert_array_values!(batch, "<level3>", vec![0u32, 0, 1, 0, 0], UInt32Array);
        assert_array_values!(batch, "<row>", vec![0u32, 1, 0, 0, 1], UInt32Array);
        assert_array_values!(batch, "value", vec![1i32, 2, 3, 4, 5], Int32Array);
    }

    #[test]
    fn test_nested_rows_indexed_correctly() {
        let xml_content = r#"
        <data>
            <group>
                <item><value>1</value></item>
                <item><value>2</value></item>
            </group>
            <group>
                <item><value>3</value></item>
            </group>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: groups
                  xml_path: /data
                  levels: [group]
                  fields: []
                - name: items
                  xml_path: /data/group
                  levels: [group, item]
                  fields:
                    - name: value
                      xml_path: /data/group/item/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_array_values!(batch, "<group>", vec![0u32, 0, 1], UInt32Array);
        assert_array_values!(batch, "<item>", vec![0u32, 1, 0], UInt32Array);
        assert_array_values!(batch, "value", vec![1i32, 2, 3], Int32Array);
    }

    #[test]
    fn test_empty_tags_produce_empty_strings() {
        let xml_content = r#"
        <data>
            <row>
                <value>1</value>
                <empty/>
            </row>
            <row>
                <value/>
                <empty>not empty</empty>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
                      nullable: true
                    - name: empty
                      xml_path: /data/row/empty
                      data_type: Utf8
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values_option!(batch, "value", vec![Some("1"), None], StringArray);
        assert_array_values_option!(batch, "empty", vec![None, Some("not empty")], StringArray);
    }

    #[test]
    fn test_boolean_valid_values_parsed() {
        let xml_content = r#"
        <data>
            <row><bool>true</bool></row>
            <row><bool>false</bool></row>
            <row><bool>1</bool></row>
            <row><bool>0</bool></row>
            <row><bool>yes</bool></row>
            <row><bool>no</bool></row>
            <row><bool>on</bool></row>
            <row><bool>off</bool></row>
            <row><bool>t</bool></row>
            <row><bool>f</bool></row>
            <row><bool>y</bool></row>
            <row><bool>n</bool></row>
            <row><bool> TrUe </bool></row>
            <row><bool> OFF </bool></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: bools
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: bool
                      xml_path: /data/row/bool
                      data_type: Boolean
            "#,
        );
        let batch = record_batches.get("bools").unwrap();
        assert_eq!(batch.num_rows(), 14);
        assert_array_values!(
            batch,
            "bool",
            vec![
                true, false, true, false, true, false, true, false, true, false, true, false, true,
                false
            ],
            BooleanArray
        );
    }

    #[test]
    fn test_boolean_invalid_value_returns_error() -> Result<()> {
        let xml_content = r#"
        <data>
            <row><bool>maybe</bool></row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: bools
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: bool
                      xml_path: /data/row/bool
                      data_type: Boolean
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match &err {
            Error::ParseError {
                value,
                kind: ParseKind::InvalidBoolean,
                ..
            } => {
                assert_eq!(value, "maybe");
                assert!(err.to_string().contains("boolean"));
            }
            other => panic!("Expected ParseError with InvalidBoolean, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_boolean_missing_value_returns_error() -> Result<()> {
        let xml_content = r#"
        <data>
            <row><bool></bool></row>
            <row><bool>   </bool></row>
        </data>
        "#;

        // Non-nullable should fail
        let config = config_from_yaml!(
            r#"
            tables:
                - name: bools
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: bool
                      xml_path: /data/row/bool
                      data_type: Boolean
                      nullable: false
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());

        // Nullable should succeed with nulls
        let config = config_from_yaml!(
            r#"
            tables:
                - name: bools
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: bool
                      xml_path: /data/row/bool
                      data_type: Boolean
                      nullable: true
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("bools").unwrap();
        assert_array_values_option!(
            batch,
            "bool",
            vec![None::<bool>, None::<bool>],
            BooleanArray
        );

        Ok(())
    }

    #[rstest]
    #[case("Int32", "scale: 0.1")]
    #[case("Int16", "offset: 10.0")]
    #[case("Boolean", "scale: 0.1")]
    #[case("Utf8", "offset: 10.0")]
    fn test_unsupported_dtype_transform_returns_error(
        #[case] dtype: &str,
        #[case] transform: &str,
    ) {
        let yaml = format!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: {dtype}
                      {transform}
            "#
        );
        let config: Config = yaml_serde::from_str(&yaml).unwrap();
        assert!(
            config.validate().is_err(),
            "Expected validation error for {transform} on {dtype}"
        );
    }

    #[test]
    fn test_non_utf8_bytes_return_error() {
        let xml_content = r#"<data><row><value>Hello 世界 🌍</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: unicode
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("unicode").unwrap();
        assert_array_values!(batch, "value", vec!["Hello 世界 🌍"], StringArray);
    }

    #[test]
    fn test_attributes_on_empty_elements_parsed() {
        let xml_content = r#"
        <data>
            <item id="1" />
            <item id="2"></item>
            <item id="3">content</item>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: items
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: id
                      xml_path: /data/item/@id
                      data_type: Int32
                    - name: content
                      xml_path: /data/item
                      data_type: Utf8
                      nullable: true
            "#,
        );
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_array_values!(batch, "id", vec![1i32, 2, 3], Int32Array);
        assert_array_values_option!(
            batch,
            "content",
            vec![None, None, Some("content")],
            StringArray
        );
    }

    #[test]
    fn test_malformed_xml_returns_error() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#
        );

        // Invalid value for Int32 should error
        let result = parse_xml(
            "<data><row><value>not_a_number</value></row></data>".as_bytes(),
            &config,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            e @ Error::ParseError { .. } => assert!(e.to_string().contains("not_a_number")),
            e => panic!("Expected ParseError, got {:?}", e),
        }
    }

    #[test]
    fn test_trim_text_option_strips_whitespace() -> Result<()> {
        let xml_content = r#"
        <data>
            <row>
                <value>   trimmed   </value>
            </row>
        </data>
        "#;

        // Without trim
        let config_no_trim = config_from_yaml!(
            r#"
            parser_options:
                trim_text: false
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config_no_trim)?;
        let batch = record_batches.get("test").unwrap();
        assert_array_values!(batch, "value", vec!["   trimmed   "], StringArray);

        // With trim
        let config_trim = config_from_yaml!(
            r#"
            parser_options:
                trim_text: true
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config_trim)?;
        let batch = record_batches.get("test").unwrap();
        assert_array_values!(batch, "value", vec!["trimmed"], StringArray);

        Ok(())
    }

    #[rstest]
    #[case("128", "Int8")]
    #[case("256", "UInt8")]
    #[case("32768", "Int16")]
    #[case("-32769", "Int16")]
    #[case("65536", "UInt16")]
    #[case("4294967296", "UInt32")]
    #[case("9223372036854775808", "Int64")]
    #[case("18446744073709551616", "UInt64")]
    fn test_numeric_overflow_returns_error(#[case] value: &str, #[case] dtype: &str) {
        let xml_content = format!("<data><row><value>{value}</value></row></data>");
        let yaml_config = format!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: {dtype}
            "#
        );
        let config: Config = yaml_serde::from_str(&yaml_config).unwrap();
        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(
            result.is_err(),
            "Expected overflow error for {value} as {dtype}"
        );
    }

    #[test]
    fn test_int64_max_boundary_parsed_correctly() -> Result<()> {
        let xml_content = r#"<data><row><value>9223372036854775807</value></row></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int64
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("test").unwrap();
        assert_array_values!(batch, "value", vec![i64::MAX], Int64Array);

        Ok(())
    }

    #[test]
    fn test_scale_and_offset_with_negative_values() {
        let xml_content = r#"
        <data>
            <row><value>-100</value></row>
            <row><value>0</value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: scaled
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      scale: -0.5
                      offset: -10.0
            "#,
        );
        let batch = record_batches.get("scaled").unwrap();
        // value = value * scale + offset = -100 * -0.5 + -10 = 40, 0 * -0.5 + -10 = -10
        assert_array_approx_values!(batch, "value", vec![40.0, -10.0], Float64Array, 0.001);
    }

    #[test]
    fn test_all_null_values_produce_null_arrays() {
        let xml_content = r#"
        <data>
            <row></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: nulls
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: int_val
                      xml_path: /data/row/int_val
                      data_type: Int32
                      nullable: true
                    - name: float_val
                      xml_path: /data/row/float_val
                      data_type: Float64
                      nullable: true
                    - name: str_val
                      xml_path: /data/row/str_val
                      data_type: Utf8
                      nullable: true
                    - name: bool_val
                      xml_path: /data/row/bool_val
                      data_type: Boolean
                      nullable: true
            "#,
        );
        let batch = record_batches.get("nulls").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values_option!(batch, "int_val", vec![None::<i32>], Int32Array);
        assert_array_approx_values_option!(
            batch,
            "float_val",
            vec![None::<f64>],
            Float64Array,
            0.001
        );
        assert_array_values_option!(batch, "str_val", vec![None::<&str>], StringArray);
        assert_array_values_option!(batch, "bool_val", vec![None::<bool>], BooleanArray);
    }

    #[test]
    fn test_mixed_null_and_valid_values_parsed() {
        let xml_content = r#"
        <data>
            <row><value>10</value></row>
            <row></row>
            <row><value>30</value></row>
            <row></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: mixed
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
                      nullable: true
            "#,
        );
        let batch = record_batches.get("mixed").unwrap();
        assert_eq!(batch.num_rows(), 4);
        assert_array_values_option!(
            batch,
            "value",
            vec![Some(10i32), None, Some(30), None],
            Int32Array
        );
    }

    #[test]
    fn test_all_nullable_types_with_null_values() {
        let xml_content = r#"
        <data>
            <row>
                <int8></int8>
                <uint8></uint8>
                <int16></int16>
                <uint16></uint16>
                <int32></int32>
                <uint32></uint32>
                <int64></int64>
                <uint64></uint64>
                <float32></float32>
                <float64></float64>
                <bool></bool>
                <str></str>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: all_nulls
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: int8
                      xml_path: /data/row/int8
                      data_type: Int8
                      nullable: true
                    - name: uint8
                      xml_path: /data/row/uint8
                      data_type: UInt8
                      nullable: true
                    - name: int16
                      xml_path: /data/row/int16
                      data_type: Int16
                      nullable: true
                    - name: uint16
                      xml_path: /data/row/uint16
                      data_type: UInt16
                      nullable: true
                    - name: int32
                      xml_path: /data/row/int32
                      data_type: Int32
                      nullable: true
                    - name: uint32
                      xml_path: /data/row/uint32
                      data_type: UInt32
                      nullable: true
                    - name: int64
                      xml_path: /data/row/int64
                      data_type: Int64
                      nullable: true
                    - name: uint64
                      xml_path: /data/row/uint64
                      data_type: UInt64
                      nullable: true
                    - name: float32
                      xml_path: /data/row/float32
                      data_type: Float32
                      nullable: true
                    - name: float64
                      xml_path: /data/row/float64
                      data_type: Float64
                      nullable: true
                    - name: bool
                      xml_path: /data/row/bool
                      data_type: Boolean
                      nullable: true
                    - name: str
                      xml_path: /data/row/str
                      data_type: Utf8
                      nullable: true
            "#,
        );
        let batch = record_batches.get("all_nulls").unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_empty_vs_missing_nullable_element() {
        let xml_content = r#"
        <data>
            <row><value></value></row>
            <row></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Both empty element and missing element should be null
        assert_array_values_option!(batch, "value", vec![None::<&str>, None], StringArray);
    }

    #[test]
    fn test_cdata_basic_text_extracted() {
        let xml_content = r#"
        <data>
            <row>
                <value><![CDATA[Hello World]]></value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: cdata
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("cdata").unwrap();
        assert_array_values!(batch, "value", vec!["Hello World"], StringArray);
    }

    #[test]
    fn test_cdata_special_characters_preserved() {
        let xml_content = r#"
        <data>
            <row>
                <value><![CDATA[<script>alert('XSS')</script>]]></value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: cdata
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("cdata").unwrap();
        assert_array_values!(
            batch,
            "value",
            vec!["<script>alert('XSS')</script>"],
            StringArray
        );
    }

    #[test]
    fn test_cdata_mixed_with_regular_text() {
        let xml_content = r#"
        <data>
            <row>
                <value>Before <![CDATA[<CDATA>]]> After</value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: cdata
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("cdata").unwrap();
        assert_array_values!(batch, "value", vec!["Before <CDATA> After"], StringArray);
    }

    #[test]
    fn test_cdata_multiple_sections_concatenated() {
        let xml_content = r#"
        <data>
            <row>
                <value><![CDATA[Part1]]><![CDATA[Part2]]></value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: cdata
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("cdata").unwrap();
        assert_array_values!(batch, "value", vec!["Part1Part2"], StringArray);
    }

    #[test]
    fn test_cdata_numeric_values_converted() {
        let xml_content = r#"
        <data>
            <row>
                <value><![CDATA[42]]></value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: cdata
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("cdata").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);
    }

    #[test]
    fn test_default_namespace_stripped() {
        let xml_content = r#"
        <data xmlns="http://example.com/ns">
            <row>
                <value>42</value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: ns_test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("ns_test").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);
    }

    #[test]
    fn test_prefixed_namespace_stripped() {
        let xml_content = r#"
        <ns:data xmlns:ns="http://example.com/ns">
            <ns:row>
                <ns:value>42</ns:value>
            </ns:row>
        </ns:data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: ns_test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("ns_test").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);
    }

    #[test]
    fn test_multiple_namespaces_stripped() {
        let xml_content = r#"
        <data xmlns="http://example.com/default" xmlns:other="http://example.com/other">
            <row>
                <value>42</value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: ns_test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("ns_test").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);
    }

    #[test]
    fn test_missing_string_defaults_to_empty() {
        // Test that non-nullable strings get empty string, not error
        let xml_content = r#"
        <data>
            <row>
                <other>exists</other>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
                      nullable: false
                    - name: other
                      xml_path: /data/row/other
                      data_type: Utf8
                      nullable: false
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 1);
        // Non-nullable string with missing element should be empty string
        assert_array_values!(batch, "value", vec![""], StringArray);
        assert_array_values!(batch, "other", vec!["exists"], StringArray);
    }

    #[test]
    fn test_missing_numeric_returns_error() -> Result<()> {
        // Test that non-nullable numeric fields error on missing
        let xml_content = r#"
        <data>
            <row>
                <other>42</other>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
                      nullable: false
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());
        match result.unwrap_err() {
            e @ Error::MissingRequiredField { .. } => {
                assert!(e.to_string().contains("Missing value"));
            }
            e => panic!("Expected MissingRequiredField, got {:?}", e),
        }

        Ok(())
    }

    #[test]
    fn test_negative_unsigned_returns_error() {
        let xml_content = r#"<data><row><value>-1</value></row></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: UInt32
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_all_type_boundaries_parsed_correctly() -> Result<()> {
        let xml_content = r#"
        <data>
            <row>
                <int8_min>-128</int8_min>
                <int8_max>127</int8_max>
                <uint8_max>255</uint8_max>
                <int16_min>-32768</int16_min>
                <int16_max>32767</int16_max>
                <uint16_max>65535</uint16_max>
                <int32_min>-2147483648</int32_min>
                <int32_max>2147483647</int32_max>
                <uint32_max>4294967295</uint32_max>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: boundaries
                  xml_path: /data
                  levels: [row]
                  fields:
                    - { name: int8_min, xml_path: /data/row/int8_min, data_type: Int8 }
                    - { name: int8_max, xml_path: /data/row/int8_max, data_type: Int8 }
                    - { name: uint8_max, xml_path: /data/row/uint8_max, data_type: UInt8 }
                    - { name: int16_min, xml_path: /data/row/int16_min, data_type: Int16 }
                    - { name: int16_max, xml_path: /data/row/int16_max, data_type: Int16 }
                    - { name: uint16_max, xml_path: /data/row/uint16_max, data_type: UInt16 }
                    - { name: int32_min, xml_path: /data/row/int32_min, data_type: Int32 }
                    - { name: int32_max, xml_path: /data/row/int32_max, data_type: Int32 }
                    - { name: uint32_max, xml_path: /data/row/uint32_max, data_type: UInt32 }
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("boundaries").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "int8_min", vec![i8::MIN], Int8Array);
        assert_array_values!(batch, "int8_max", vec![i8::MAX], Int8Array);
        assert_array_values!(batch, "uint8_max", vec![u8::MAX], UInt8Array);
        assert_array_values!(batch, "int16_min", vec![i16::MIN], Int16Array);
        assert_array_values!(batch, "int16_max", vec![i16::MAX], Int16Array);
        assert_array_values!(batch, "uint16_max", vec![u16::MAX], UInt16Array);
        assert_array_values!(batch, "int32_min", vec![i32::MIN], Int32Array);
        assert_array_values!(batch, "int32_max", vec![i32::MAX], Int32Array);
        assert_array_values!(batch, "uint32_max", vec![u32::MAX], UInt32Array);

        Ok(())
    }

    #[test]
    fn test_scale_only_applied_to_float64() {
        let xml_content = r#"<data><row><value>100</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: scaled
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      scale: 0.5
            "#,
        );
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![50.0], Float64Array, 0.001);
    }

    #[test]
    fn test_scale_only_applied_to_float32() {
        let xml_content = r#"<data><row><value>100</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: scaled
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float32
                      scale: 0.5
            "#,
        );
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![50.0f32], Float32Array, 0.001);
    }

    #[test]
    fn test_offset_only_applied_to_float64() {
        let xml_content = r#"<data><row><value>100</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: offset
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      offset: 50.0
            "#,
        );
        let batch = record_batches.get("offset").unwrap();
        assert_array_approx_values!(batch, "value", vec![150.0], Float64Array, 0.001);
    }

    #[test]
    fn test_offset_only_applied_to_float32() {
        let xml_content = r#"<data><row><value>100</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: offset
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float32
                      offset: 50.0
            "#,
        );
        let batch = record_batches.get("offset").unwrap();
        assert_array_approx_values!(batch, "value", vec![150.0f32], Float32Array, 0.001);
    }

    #[test]
    fn test_negative_scale_applied_correctly() {
        let xml_content = r#"<data><row><value>-100</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: scaled
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      scale: 2.0
            "#,
        );
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![-200.0], Float64Array, 0.001);
    }

    #[test]
    fn test_negative_offset_applied_correctly() {
        let xml_content = r#"<data><row><value>-100</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: offset
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      offset: -50.0
            "#,
        );
        let batch = record_batches.get("offset").unwrap();
        assert_array_approx_values!(batch, "value", vec![-150.0], Float64Array, 0.001);
    }

    #[test]
    fn test_zero_scale_produces_zeros() {
        let xml_content = r#"<data><row><value>0</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: scaled
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      scale: 100.0
                      offset: 50.0
            "#,
        );
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![50.0], Float64Array, 0.001);
    }

    #[test]
    fn test_very_small_scale_applied_correctly() {
        let xml_content = r#"<data><row><value>1000000</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: scaled
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      scale: 0.000001
            "#,
        );
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![1.0], Float64Array, 0.0001);
    }

    #[test]
    fn test_very_large_scale_applied_correctly() {
        let xml_content = r#"<data><row><value>0.001</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: scaled
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      scale: 1000000.0
            "#,
        );
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![1000.0], Float64Array, 0.1);
    }

    #[test]
    fn test_float_scientific_notation_parsed() {
        let xml_content = r#"
        <data>
            <row>
                <float32>1.5e10</float32>
                <float64>2.5e-10</float64>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: scientific
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: float32
                      xml_path: /data/row/float32
                      data_type: Float32
                    - name: float64
                      xml_path: /data/row/float64
                      data_type: Float64
            "#,
        );
        let batch = record_batches.get("scientific").unwrap();
        assert_array_approx_values!(batch, "float32", vec![1.5e10f32], Float32Array, 1e6);
        assert_array_approx_values!(batch, "float64", vec![2.5e-10f64], Float64Array, 1e-15);
    }

    #[test]
    fn test_very_small_float_parsed_correctly() {
        let xml_content = r#"
        <data>
            <row>
                <value>0.00000000000000000001</value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: small
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
            "#,
        );
        let batch = record_batches.get("small").unwrap();
        assert_array_approx_values!(batch, "value", vec![1e-20f64], Float64Array, 1e-25);
    }

    #[test]
    fn test_very_large_float_parsed_correctly() {
        let xml_content = r#"
        <data>
            <row>
                <value>100000000000000000000</value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: large
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
            "#,
        );
        let batch = record_batches.get("large").unwrap();
        assert_array_approx_values!(batch, "value", vec![1e20f64], Float64Array, 1e15);
    }

    #[test]
    fn test_unicode_scripts_in_attributes() {
        let xml_content = r#"
        <data>
            <item name="日本語">Japanese</item>
            <item name="العربية">Arabic</item>
            <item name="עברית">Hebrew</item>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: unicode
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: name
                      xml_path: /data/item/@name
                      data_type: Utf8
                    - name: content
                      xml_path: /data/item
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("unicode").unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_array_values!(
            batch,
            "name",
            vec!["日本語", "العربية", "עברית"],
            StringArray
        );
    }

    #[test]
    fn test_emoji_text_parsed_correctly() {
        let xml_content = r#"
        <data>
            <row><value>Hello 🌍 World 🚀</value></row>
            <row><value>😀😃😄😁</value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: emoji
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("emoji").unwrap();
        assert_array_values!(
            batch,
            "value",
            vec!["Hello 🌍 World 🚀", "😀😃😄😁"],
            StringArray
        );
    }

    #[test]
    fn test_empty_attribute_value_preserved() {
        let xml_content = r#"
        <data>
            <item id="" name="test">content</item>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: attrs
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: id
                      xml_path: /data/item/@id
                      data_type: Utf8
                    - name: name
                      xml_path: /data/item/@name
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("attrs").unwrap();
        assert_array_values!(batch, "id", vec![""], StringArray);
        assert_array_values!(batch, "name", vec!["test"], StringArray);
    }

    #[test]
    fn test_attribute_whitespace_preserved() {
        let xml_content = r#"
        <data>
            <item name="  spaced  ">content</item>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: attrs
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: name
                      xml_path: /data/item/@name
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("attrs").unwrap();
        assert_array_values!(batch, "name", vec!["  spaced  "], StringArray);
    }

    #[test]
    fn test_multiple_attributes_on_single_element() {
        let xml_content = r#"
        <data>
            <item a="1" b="2" c="3" d="4" e="5">content</item>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: attrs
                  xml_path: /data
                  levels: [item]
                  fields:
                    - { name: a, xml_path: /data/item/@a, data_type: Int32 }
                    - { name: b, xml_path: /data/item/@b, data_type: Int32 }
                    - { name: c, xml_path: /data/item/@c, data_type: Int32 }
                    - { name: d, xml_path: /data/item/@d, data_type: Int32 }
                    - { name: e, xml_path: /data/item/@e, data_type: Int32 }
            "#,
        );
        let batch = record_batches.get("attrs").unwrap();
        assert_array_values!(batch, "a", vec![1i32], Int32Array);
        assert_array_values!(batch, "b", vec![2i32], Int32Array);
        assert_array_values!(batch, "c", vec![3i32], Int32Array);
        assert_array_values!(batch, "d", vec![4i32], Int32Array);
        assert_array_values!(batch, "e", vec![5i32], Int32Array);
    }

    #[test]
    fn test_xml_comments_ignored_during_parse() {
        let xml_content = r#"
        <!-- This is a comment -->
        <data>
            <!-- Another comment -->
            <row>
                <!-- Comment inside -->
                <value>42</value>
            </row>
        </data>
        <!-- Final comment -->
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);
    }

    #[test]
    fn test_xml_declaration_handled_correctly() {
        let xml_content = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <data>
            <row><value>42</value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);
    }

    #[test]
    fn test_index_only_table_excluded_from_output() {
        // Tables with no fields (only indices) should not appear in output
        let xml_content = r#"
        <data>
            <group>
                <item><value>1</value></item>
                <item><value>2</value></item>
            </group>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: groups
                  xml_path: /data
                  levels: [group]
                  fields: []
                - name: items
                  xml_path: /data/group
                  levels: [group, item]
                  fields:
                    - name: value
                      xml_path: /data/group/item/value
                      data_type: Int32
            "#,
        );
        // "groups" table should not be in output (no fields)
        assert!(!record_batches.contains_key("groups"));
        // "items" table should be in output
        assert!(record_batches.contains_key("items"));
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values!(batch, "<group>", vec![0u32, 0], UInt32Array);
        assert_array_values!(batch, "<item>", vec![0u32, 1], UInt32Array);
        assert_array_values!(batch, "value", vec![1i32, 2], Int32Array);
    }

    #[test]
    fn test_fields_at_root_level_parsed_correctly() {
        // Test fields defined directly at root level (xml_path: /)
        let xml_content = r#"
        <root>
            <name>Test Document</name>
            <version>1.0</version>
        </root>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: document
                  xml_path: /
                  levels: []
                  fields:
                    - name: name
                      xml_path: /root/name
                      data_type: Utf8
                    - name: version
                      xml_path: /root/version
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("document").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "name", vec!["Test Document"], StringArray);
        assert_array_values!(batch, "version", vec!["1.0"], StringArray);
    }

    #[test]
    fn test_root_attributes_parsed_correctly() {
        let xml_content = r#"<root version="2.0" encoding="utf-8"><data>content</data></root>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: document
                  xml_path: /
                  levels: []
                  fields:
                    - name: version
                      xml_path: /root/@version
                      data_type: Utf8
                    - name: encoding
                      xml_path: /root/@encoding
                      data_type: Utf8
                    - name: data
                      xml_path: /root/data
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("document").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "version", vec!["2.0"], StringArray);
        assert_array_values!(batch, "encoding", vec!["utf-8"], StringArray);
        assert_array_values!(batch, "data", vec!["content"], StringArray);
    }

    #[test]
    fn test_xml_entities_unescaped_in_attributes() {
        let xml_content = r#"
        <items>
            <item id="AT&amp;T" label="2 &lt; 3" note="&quot;quoted&quot;" path="a&apos;b"/>
        </items>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: items
                  xml_path: /items
                  levels: [item]
                  fields:
                    - name: id
                      xml_path: /items/item/@id
                      data_type: Utf8
                    - name: label
                      xml_path: /items/item/@label
                      data_type: Utf8
                    - name: note
                      xml_path: /items/item/@note
                      data_type: Utf8
                    - name: path
                      xml_path: /items/item/@path
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("items").unwrap();

        assert_array_values!(batch, "id", vec!["AT&T"], StringArray);
        assert_array_values!(batch, "label", vec!["2 < 3"], StringArray);
        assert_array_values!(batch, "note", vec!["\"quoted\""], StringArray);
        assert_array_values!(batch, "path", vec!["a'b"], StringArray);
    }

    #[test]
    fn test_numeric_entities_unescaped_in_attributes() {
        let xml_content = r#"<items><item symbol="&#65;&#66;&#67;" euro="&#x20AC;"/></items>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: items
                  xml_path: /items
                  levels: [item]
                  fields:
                    - name: symbol
                      xml_path: /items/item/@symbol
                      data_type: Utf8
                    - name: euro
                      xml_path: /items/item/@euro
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("items").unwrap();

        assert_array_values!(batch, "symbol", vec!["ABC"], StringArray); // decimal refs
        assert_array_values!(batch, "euro", vec!["€"], StringArray); // hex ref
    }

    #[test]
    fn test_attribute_whitespace_normalized_per_xml_spec() {
        // XML 1.0 §3.3.3: literal tab/CR/LF in attribute values become
        // spaces, but *character references* to those characters survive —
        // that is the spec's escape hatch for intentional whitespace. Both
        // dispatch arms of the attribute fast path must agree on this:
        // `literal` takes the slow path only because of its whitespace,
        // `mixed` because of the entity, and both must normalize alike.
        let xml_content = "<items><item literal=\"a\tb\nc\" referenced=\"a&#x9;b&#xA;c\" mixed=\"x&amp;\ty\"/></items>";

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: items
                  xml_path: /items
                  levels: [item]
                  fields:
                    - name: literal
                      xml_path: /items/item/@literal
                      data_type: Utf8
                    - name: referenced
                      xml_path: /items/item/@referenced
                      data_type: Utf8
                    - name: mixed
                      xml_path: /items/item/@mixed
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("items").unwrap();

        assert_array_values!(batch, "literal", vec!["a b c"], StringArray);
        assert_array_values!(batch, "referenced", vec!["a\tb\nc"], StringArray);
        assert_array_values!(batch, "mixed", vec!["x& y"], StringArray);
    }

    #[test]
    fn test_element_text_whitespace_not_normalized() {
        // Attribute-value normalization applies to attributes only; tabs and
        // newlines inside element text are data and must pass through raw.
        let xml_content = "<items><item><value>a\tb\nc</value></item></items>";

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: items
                  xml_path: /items
                  levels: [item]
                  fields:
                    - name: value
                      xml_path: /items/item/value
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("items").unwrap();

        assert_array_values!(batch, "value", vec!["a\tb\nc"], StringArray);
    }

    #[test]
    fn test_root_with_nested_tables_parsed_correctly() {
        let xml_content = r#"
        <document>
            <meta>
                <title>Test Doc</title>
                <author>John</author>
            </meta>
            <items>
                <item>
                    <name>Item 1</name>
                    <price>10.5</price>
                </item>
                <item>
                    <name>Item 2</name>
                    <price>20.5</price>
                </item>
            </items>
        </document>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: document
                  xml_path: /
                  levels: []
                  fields:
                    - name: title
                      xml_path: /document/meta/title
                      data_type: Utf8
                    - name: author
                      xml_path: /document/meta/author
                      data_type: Utf8
                - name: items
                  xml_path: /document/items
                  levels: [item]
                  fields:
                    - name: name
                      xml_path: /document/items/item/name
                      data_type: Utf8
                    - name: price
                      xml_path: /document/items/item/price
                      data_type: Float64
            "#,
        );

        // Check document table
        let doc_batch = record_batches.get("document").unwrap();
        assert_eq!(doc_batch.num_rows(), 1);
        assert_array_values!(doc_batch, "title", vec!["Test Doc"], StringArray);
        assert_array_values!(doc_batch, "author", vec!["John"], StringArray);

        // Check items table
        let items_batch = record_batches.get("items").unwrap();
        assert_eq!(items_batch.num_rows(), 2);
        assert_array_values!(items_batch, "name", vec!["Item 1", "Item 2"], StringArray);
        assert_array_values!(items_batch, "price", vec![10.5, 20.5], Float64Array);
    }

    // =========================================================================
    // Float overflow with scale/offset
    // =========================================================================

    #[test]
    fn test_f32_overflow_with_scale_produces_infinity() {
        let xml_content = r#"<data><row><value>3.4e38</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float32
                      scale: 10.0
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert!(
            array.value(0).is_infinite(),
            "Expected infinity for f32 overflow, got {}",
            array.value(0)
        );
    }

    #[test]
    fn test_f64_large_value_with_scale_no_overflow() {
        let xml_content = r#"<data><row><value>1.7e308</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      scale: 0.5
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            array.value(0).is_finite(),
            "Expected finite value, got {}",
            array.value(0)
        );
    }

    #[test]
    fn test_f64_overflow_with_scale_produces_infinity() {
        let xml_content = r#"<data><row><value>1.7e308</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      scale: 10.0
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            array.value(0).is_infinite(),
            "Expected infinity for f64 overflow, got {}",
            array.value(0)
        );
    }

    #[test]
    fn test_f32_nan_with_transform_returns_error() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float32
                      scale: 2.0
            "#
        );

        let result = parse_xml(
            "<data><row><value>not_a_float</value></row></data>".as_bytes(),
            &config,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            e @ Error::ParseError { .. } => assert!(e.to_string().contains("not_a_float")),
            e => panic!("Expected ParseError, got {:?}", e),
        }
    }

    #[test]
    fn test_f64_nan_with_transform_returns_error() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      offset: 1.0
            "#
        );

        let result = parse_xml(
            "<data><row><value>abc</value></row></data>".as_bytes(),
            &config,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            e @ Error::ParseError { .. } => assert!(e.to_string().contains("abc")),
            e => panic!("Expected ParseError, got {:?}", e),
        }
    }

    #[test]
    fn test_nullable_f32_with_transform_missing_value() {
        let xml_content = r#"<data><row></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float32
                      scale: 2.0
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 1);
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert!(array.is_null(0));
    }

    #[test]
    fn test_non_nullable_f64_with_transform_missing_value_errors() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      offset: 5.0
                      nullable: false
            "#
        );

        let result = parse_xml("<data><row></row></data>".as_bytes(), &config);
        assert!(result.is_err());
        match result.unwrap_err() {
            e @ Error::MissingRequiredField { .. } => {
                assert!(e.to_string().contains("Missing value"));
            }
            e => panic!("Expected MissingRequiredField, got {:?}", e),
        }
    }

    // =========================================================================
    // Sibling tables at the same nesting level
    // =========================================================================

    #[test]
    fn test_sibling_tables_at_same_level() {
        let xml_content = r#"
        <data>
            <orders>
                <order><id>1</id><total>100</total></order>
                <order><id>2</id><total>200</total></order>
            </orders>
            <customers>
                <customer><name>Alice</name></customer>
                <customer><name>Bob</name></customer>
                <customer><name>Charlie</name></customer>
            </customers>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: orders
                  xml_path: /data/orders
                  levels: [order]
                  fields:
                    - name: id
                      xml_path: /data/orders/order/id
                      data_type: Int32
                    - name: total
                      xml_path: /data/orders/order/total
                      data_type: Int32
                - name: customers
                  xml_path: /data/customers
                  levels: [customer]
                  fields:
                    - name: name
                      xml_path: /data/customers/customer/name
                      data_type: Utf8
            "#,
        );

        assert_eq!(record_batches.len(), 2);

        let orders = record_batches.get("orders").unwrap();
        assert_eq!(orders.num_rows(), 2);
        assert_array_values!(orders, "id", vec![1i32, 2], Int32Array);
        assert_array_values!(orders, "total", vec![100i32, 200], Int32Array);

        let customers = record_batches.get("customers").unwrap();
        assert_eq!(customers.num_rows(), 3);
        assert_array_values!(
            customers,
            "name",
            vec!["Alice", "Bob", "Charlie"],
            StringArray
        );
    }

    #[test]
    fn test_sibling_tables_with_shared_parent_indices() {
        let xml_content = r#"
        <data>
            <group>
                <items>
                    <item><value>1</value></item>
                    <item><value>2</value></item>
                </items>
                <tags>
                    <tag><label>A</label></tag>
                </tags>
            </group>
            <group>
                <items>
                    <item><value>3</value></item>
                </items>
                <tags>
                    <tag><label>B</label></tag>
                    <tag><label>C</label></tag>
                </tags>
            </group>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: groups
                  xml_path: /data
                  levels: [group]
                  fields: []
                - name: items
                  xml_path: /data/group/items
                  levels: [group, item]
                  fields:
                    - name: value
                      xml_path: /data/group/items/item/value
                      data_type: Int32
                - name: tags
                  xml_path: /data/group/tags
                  levels: [group, tag]
                  fields:
                    - name: label
                      xml_path: /data/group/tags/tag/label
                      data_type: Utf8
            "#,
        );

        let items = record_batches.get("items").unwrap();
        assert_eq!(items.num_rows(), 3);
        assert_array_values!(items, "<group>", vec![0u32, 0, 1], UInt32Array);
        assert_array_values!(items, "value", vec![1i32, 2, 3], Int32Array);

        let tags = record_batches.get("tags").unwrap();
        assert_eq!(tags.num_rows(), 3);
        assert_array_values!(tags, "<group>", vec![0u32, 1, 1], UInt32Array);
        assert_array_values!(tags, "label", vec!["A", "B", "C"], StringArray);
    }

    // =========================================================================
    // Empty element vs missing element for non-string types
    // =========================================================================

    #[test]
    fn test_empty_element_int32_nullable_produces_null() {
        let xml_content = r#"
        <data>
            <row><value></value></row>
            <row><value>42</value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values_option!(batch, "value", vec![None, Some(42i32)], Int32Array);
    }

    #[test]
    fn test_empty_element_int32_non_nullable_returns_error() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
                      nullable: false
            "#
        );

        // Empty element <value></value> with has_value=false for non-nullable Int32
        let result = parse_xml(
            "<data><row><value></value></row></data>".as_bytes(),
            &config,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_element_float64_nullable_produces_null() {
        let xml_content = r#"
        <data>
            <row><value></value></row>
            <row><value>3.14</value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_approx_values_option!(
            batch,
            "value",
            vec![None, Some(3.14f64)],
            Float64Array,
            0.001
        );
    }

    #[test]
    fn test_empty_element_boolean_nullable_produces_null() {
        let xml_content = r#"
        <data>
            <row><flag></flag></row>
            <row><flag>true</flag></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: flag
                      xml_path: /data/row/flag
                      data_type: Boolean
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values_option!(batch, "flag", vec![None, Some(true)], BooleanArray);
    }

    #[test]
    fn test_missing_element_vs_empty_element_for_int32() {
        // Both missing tag and empty tag should behave the same for nullable numeric fields
        let xml_content = r#"
        <data>
            <row><value>10</value></row>
            <row></row>
            <row><value></value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_array_values_option!(batch, "value", vec![Some(10i32), None, None], Int32Array);
    }

    // =========================================================================
    // Non-nullable boolean with whitespace-only value
    // =========================================================================

    #[test]
    fn test_non_nullable_boolean_whitespace_only_returns_error() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: flag
                      xml_path: /data/row/flag
                      data_type: Boolean
                      nullable: false
            "#
        );

        let result = parse_xml(
            "<data><row><flag>   </flag></row></data>".as_bytes(),
            &config,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            e @ Error::MissingRequiredField { .. } => {
                assert!(e.to_string().contains("Missing value"));
            }
            e => panic!("Expected MissingRequiredField, got {:?}", e),
        }
    }

    #[test]
    fn test_nullable_boolean_whitespace_only_produces_null() {
        let xml_content = r#"<data><row><flag>   </flag></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: flag
                      xml_path: /data/row/flag
                      data_type: Boolean
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values_option!(batch, "flag", vec![None::<bool>], BooleanArray);
    }

    // =========================================================================
    // Stop paths mid-nested-data
    // =========================================================================

    #[test]
    fn test_stop_at_path_mid_nested_table() {
        let xml_content = r#"
        <root>
            <items>
                <item><value>1</value></item>
                <item><value>2</value></item>
            </items>
            <more_items>
                <item><value>3</value></item>
                <item><value>4</value></item>
            </more_items>
        </root>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            parser_options:
                stop_at_paths:
                    - /root/items
            tables:
                - name: items
                  xml_path: /root/items
                  levels: [item]
                  fields:
                    - name: value
                      xml_path: /root/items/item/value
                      data_type: Int32
                - name: more
                  xml_path: /root/more_items
                  levels: [item]
                  fields:
                    - name: value
                      xml_path: /root/more_items/item/value
                      data_type: Int32
            "#,
        );

        // First table should have data (stop happens after its closing tag)
        let items = record_batches.get("items").unwrap();
        assert_eq!(items.num_rows(), 2);
        assert_array_values!(items, "value", vec![1i32, 2], Int32Array);

        // Second table should be empty (parsing stopped before it)
        let more = record_batches.get("more").unwrap();
        assert_eq!(more.num_rows(), 0);
    }

    #[test]
    fn test_multiple_stop_at_paths() {
        let xml_content = r#"
        <root>
            <meta><title>Report</title></meta>
            <summary><count>5</count></summary>
            <data>
                <item><value>1</value></item>
            </data>
        </root>
        "#;

        // Stop after /root/summary, so /root/data is never reached
        let record_batches = parse(
            xml_content,
            r#"
            parser_options:
                stop_at_paths:
                    - /root/meta
                    - /root/summary
            tables:
                - name: meta
                  xml_path: /root
                  levels: [meta]
                  fields:
                    - name: title
                      xml_path: /root/meta/title
                      data_type: Utf8
                - name: data
                  xml_path: /root/data
                  levels: [item]
                  fields:
                    - name: value
                      xml_path: /root/data/item/value
                      data_type: Int32
            "#,
        );

        // Meta should be parsed (stop occurs at first matching stop_at_path)
        let meta = record_batches.get("meta").unwrap();
        assert_eq!(meta.num_rows(), 1);
        assert_array_values!(meta, "title", vec!["Report"], StringArray);

        // Data should be empty (parsing stopped before it)
        let data = record_batches.get("data").unwrap();
        assert_eq!(data.num_rows(), 0);
    }

    // =========================================================================
    // Root table without fields (hierarchy-only)
    // =========================================================================

    #[test]
    fn test_root_table_without_fields_does_not_affect_child_indexing() {
        let xml_content = r#"
        <data>
            <item><value>1</value></item>
            <item><value>2</value></item>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: root
                  xml_path: /
                  levels: []
                  fields: []
                - name: items
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: value
                      xml_path: /data/item/value
                      data_type: Int32
            "#,
        );

        // Root table has no fields, should not appear in output
        assert!(!record_batches.contains_key("root"));

        // Items should work normally without root table interfering with indices
        let items = record_batches.get("items").unwrap();
        assert_eq!(items.num_rows(), 2);
        assert_array_values!(items, "<item>", vec![0u32, 1], UInt32Array);
        assert_array_values!(items, "value", vec![1i32, 2], Int32Array);
    }

    #[test]
    fn test_root_table_with_fields_does_not_break_child_indices() {
        // Root table with xml_path: / and fields should not affect child table indexing
        let xml_content = r#"
        <doc>
            <title>Report</title>
            <items>
                <item><value>1</value></item>
                <item><value>2</value></item>
            </items>
        </doc>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: doc
                  xml_path: /
                  levels: []
                  fields:
                    - name: title
                      xml_path: /doc/title
                      data_type: Utf8
                - name: items
                  xml_path: /doc/items
                  levels: [item]
                  fields:
                    - name: value
                      xml_path: /doc/items/item/value
                      data_type: Int32
            "#,
        );

        let doc = record_batches.get("doc").unwrap();
        assert_eq!(doc.num_rows(), 1);
        assert_array_values!(doc, "title", vec!["Report"], StringArray);

        // Child table indices should start from 0, unaffected by root table
        let items = record_batches.get("items").unwrap();
        assert_eq!(items.num_rows(), 2);
        assert_array_values!(items, "<item>", vec![0u32, 1], UInt32Array);
        assert_array_values!(items, "value", vec![1i32, 2], Int32Array);
    }

    // =========================================================================
    // 4+ level nesting
    // =========================================================================

    #[test]
    fn test_four_level_nesting_with_indices() {
        let xml_content = r#"
        <root>
            <a>
                <b>
                    <c>
                        <d><value>1</value></d>
                        <d><value>2</value></d>
                    </c>
                    <c>
                        <d><value>3</value></d>
                    </c>
                </b>
                <b>
                    <c>
                        <d><value>4</value></d>
                    </c>
                </b>
            </a>
        </root>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: as
                  xml_path: /root
                  levels: [a]
                  fields: []
                - name: bs
                  xml_path: /root/a
                  levels: [a, b]
                  fields: []
                - name: cs
                  xml_path: /root/a/b
                  levels: [a, b, c]
                  fields: []
                - name: ds
                  xml_path: /root/a/b/c
                  levels: [a, b, c, d]
                  fields:
                    - name: value
                      xml_path: /root/a/b/c/d/value
                      data_type: Int32
            "#,
        );

        let ds = record_batches.get("ds").unwrap();
        assert_eq!(ds.num_rows(), 4);
        assert_array_values!(ds, "<a>", vec![0u32, 0, 0, 0], UInt32Array);
        assert_array_values!(ds, "<b>", vec![0u32, 0, 0, 1], UInt32Array);
        assert_array_values!(ds, "<c>", vec![0u32, 0, 1, 0], UInt32Array);
        assert_array_values!(ds, "<d>", vec![0u32, 1, 0, 0], UInt32Array);
        assert_array_values!(ds, "value", vec![1i32, 2, 3, 4], Int32Array);
    }

    // =========================================================================
    // Non-nullable missing values across all numeric types
    // =========================================================================

    #[rstest]
    #[case("Int8")]
    #[case("UInt8")]
    #[case("Int16")]
    #[case("UInt16")]
    #[case("Int32")]
    #[case("UInt32")]
    #[case("Int64")]
    #[case("UInt64")]
    #[case("Float32")]
    #[case("Float64")]
    #[case("Boolean")]
    fn test_non_nullable_missing_value_returns_error_all_types(#[case] dtype: &str) {
        let yaml_config = format!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: {dtype}
                      nullable: false
            "#
        );
        let config: Config = yaml_serde::from_str(&yaml_config).unwrap();
        let result = parse_xml(
            "<data><row><other>x</other></row></data>".as_bytes(),
            &config,
        );
        assert!(
            result.is_err(),
            "Expected error for missing non-nullable {dtype}"
        );
    }

    // =========================================================================
    // Malformed XML returns error
    // =========================================================================

    #[test]
    fn test_mismatched_xml_tags_returns_error() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#
        );

        // Mismatched closing tag should produce an error
        let result = parse_xml(
            "<data><row><value>42</wrong></row></data>".as_bytes(),
            &config,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_input_returns_error() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#
        );

        let garbage: &[u8] = &[0xFF, 0xFE, 0x00, 0x01, 0x80, 0x90, 0xAB];
        let result = parse_xml(garbage, &config);
        // Binary input should either error or produce an empty batch
        // (quick_xml may treat it as text with no matching elements)
        if let Ok(batches) = &result {
            let batch = batches.get("test").unwrap();
            assert_eq!(batch.num_rows(), 0);
        }
    }

    // =========================================================================
    // Attribute entity unescaping
    // =========================================================================

    #[test]
    fn test_attribute_with_xml_entities() {
        let xml_content = r#"<data><item name="A &amp; B" desc="&lt;tag&gt;">text</item></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: items
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: name
                      xml_path: /data/item/@name
                      data_type: Utf8
                    - name: desc
                      xml_path: /data/item/@desc
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "name", vec!["A & B"], StringArray);
        assert_array_values!(batch, "desc", vec!["<tag>"], StringArray);
    }

    // =========================================================================
    // Self-closing elements in table context
    // =========================================================================

    #[test]
    fn test_self_closing_row_element_produces_nulls() {
        let xml_content = r#"
        <data>
            <row><value>1</value></row>
            <row/>
            <row><value>3</value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_array_values_option!(batch, "value", vec![Some(1i32), None, Some(3)], Int32Array);
    }

    // =========================================================================
    // Attributes with numeric types and missing attributes
    // =========================================================================

    #[test]
    fn test_nullable_missing_attribute_produces_null() {
        let xml_content = r#"
        <data>
            <item id="1" name="first">text1</item>
            <item id="2">text2</item>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: items
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: id
                      xml_path: /data/item/@id
                      data_type: Int32
                    - name: name
                      xml_path: /data/item/@name
                      data_type: Utf8
                      nullable: true
            "#,
        );
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values!(batch, "id", vec![1i32, 2], Int32Array);
        assert_array_values_option!(batch, "name", vec![Some("first"), None], StringArray);
    }

    // =========================================================================
    // Mixed content with ignored child nodes
    // =========================================================================

    #[test]
    fn test_mixed_content_with_ignored_child_elements() {
        // Text around child elements that aren't configured as fields should
        // be concatenated: "Start " + " End" = "Start  End"
        let xml_content = r#"
        <data>
            <row>
                <value>Start <ignored/> End</value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "value", vec!["Start  End"], StringArray);
    }

    #[test]
    fn test_mixed_content_with_nested_child_elements() {
        // More complex: text around a child element with its own text content
        let xml_content = r#"
        <data>
            <row>
                <value>Before <child>inner</child> After</value>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 1);
        // Only text directly under <value> should be accumulated, not the child's text
        assert_array_values!(batch, "value", vec!["Before  After"], StringArray);
    }

    // =========================================================================
    // Schema nullability verification
    // =========================================================================

    #[test]
    fn test_schema_nullability_matches_config() {
        let xml_content = r#"
        <data>
            <row>
                <required>1</required>
                <optional>2</optional>
            </row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: required
                      xml_path: /data/row/required
                      data_type: Int32
                      nullable: false
                    - name: optional
                      xml_path: /data/row/optional
                      data_type: Int32
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        let schema = batch.schema();

        let required_field = schema.field_with_name("required").unwrap();
        assert!(
            !required_field.is_nullable(),
            "Field 'required' should not be nullable in schema"
        );

        let optional_field = schema.field_with_name("optional").unwrap();
        assert!(
            optional_field.is_nullable(),
            "Field 'optional' should be nullable in schema"
        );
    }

    #[test]
    fn test_schema_nullability_defaults_to_non_nullable() {
        // Fields without explicit nullable should default to non-nullable
        let xml_content = r#"<data><row><value>1</value></row></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        let schema = batch.schema();
        let field = schema.field_with_name("value").unwrap();
        assert!(!field.is_nullable(), "Default nullability should be false");
    }

    // =========================================================================
    // Duplicate attributes
    // =========================================================================

    #[test]
    fn test_duplicate_attributes_returns_error() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: id
                      xml_path: /data/item/@id
                      data_type: Int32
            "#
        );

        // Duplicate attribute "id" is invalid XML
        let result = parse_xml(
            r#"<data><item id="1" id="2">text</item></data>"#.as_bytes(),
            &config,
        );
        assert!(
            result.is_err(),
            "Duplicate attributes should produce an error"
        );
    }

    // =========================================================================
    // Transform on empty-string nullable float
    // =========================================================================

    #[test]
    fn test_nullable_f32_empty_element_with_transform_produces_null() {
        // An empty element <value></value> for a nullable Float32 with offset
        // should produce null, NOT 0.0 + offset
        let xml_content = r#"
        <data>
            <row><value></value></row>
            <row><value>10.0</value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float32
                      scale: 2.0
                      offset: 100.0
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 2);
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert!(
            array.is_null(0),
            "Empty element should be null, not 0.0 + offset"
        );
        // 10.0 * 2.0 + 100.0 = 120.0
        assert!(abs_diff_eq!(array.value(1), 120.0f32, epsilon = 0.001));
    }

    #[test]
    fn test_nullable_f64_empty_element_with_transform_produces_null() {
        let xml_content = r#"
        <data>
            <row><value></value></row>
            <row><value>5.0</value></row>
        </data>
        "#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
                      offset: 50.0
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 2);
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            array.is_null(0),
            "Empty element should be null, not 0.0 + offset"
        );
        assert!(abs_diff_eq!(array.value(1), 55.0f64, epsilon = 0.001));
    }

    // =========================================================================
    // Namespace prefixes in attributes
    // =========================================================================

    #[test]
    fn test_prefixed_namespace_attribute_stripped() {
        // Attributes with namespace prefixes should have the prefix stripped
        // via local_name(), so ns:id matches @id in the config
        let xml_content =
            r#"<data xmlns:ns="http://example.com"><item ns:id="42">text</item></data>"#;

        let record_batches = parse(
            xml_content,
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: id
                      xml_path: /data/item/@id
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "id", vec![42i32], Int32Array);
    }

    #[test]
    fn test_strip_namespaces_false_prefix_free_matches_local_names() {
        // For prefix-free input, disabling strip_namespaces is free: the raw
        // name() and local_name() are byte-identical, so existing configs work
        // unchanged. This is the common-case fast path.
        let xml_content = r#"<data><item id="7"><value>42</value></item></data>"#;
        let record_batches = parse(
            xml_content,
            r#"
            parser_options:
              strip_namespaces: false
            tables:
                - name: test
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: id
                      xml_path: /data/item/@id
                      data_type: Int32
                    - name: value
                      xml_path: /data/item/value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "id", vec![7i32], Int32Array);
        assert_array_values!(batch, "value", vec![42i32], Int32Array);
    }

    #[test]
    fn test_strip_namespaces_false_matches_qualified_names() {
        // With strip_namespaces=false, configured paths must spell out the
        // prefix exactly as it appears in the document — for both elements
        // (levels + field paths) and attributes.
        let xml_content = r#"<data xmlns:ns="http://example.com"><ns:item ns:id="7"><ns:value>42</ns:value></ns:item></data>"#;
        let record_batches = parse(
            xml_content,
            r#"
            parser_options:
              strip_namespaces: false
            tables:
                - name: test
                  xml_path: /data
                  levels: ["ns:item"]
                  fields:
                    - name: id
                      xml_path: /data/ns:item/@ns:id
                      data_type: Int32
                    - name: value
                      xml_path: /data/ns:item/ns:value
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "id", vec![7i32], Int32Array);
        assert_array_values!(batch, "value", vec![42i32], Int32Array);
    }

    #[test]
    fn test_strip_namespaces_false_does_not_strip_prefix() {
        // The trade-off of the fast path: a prefixed element is matched by its
        // raw qualified name, so an unqualified config path no longer matches
        // the prefixed element (contrast with
        // test_prefixed_namespace_attribute_stripped, where the default strips
        // the prefix). The `<ns:value>` text never reaches the `value` field, so
        // it stays null.
        let xml_content = r#"<data xmlns:ns="http://example.com"><ns:item><ns:value>42</ns:value></ns:item></data>"#;
        let record_batches = parse(
            xml_content,
            r#"
            parser_options:
              strip_namespaces: false
            tables:
                - name: test
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: value
                      xml_path: /data/item/value
                      data_type: Int32
                      nullable: true
            "#,
        );
        let batch = record_batches.get("test").unwrap();
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(
            array.iter().all(|v| v.is_none()),
            "unqualified path must not match the prefixed element"
        );
    }

    // =========================================================================
    // Character / entity references in element text
    // =========================================================================

    #[test]
    fn test_decimal_char_ref_in_text_resolved() {
        // Regression: character references used to resolve to an empty
        // string, silently dropping data ("A&#66;C" became "AC").
        let record_batches = parse(
            r#"<data><row><v>A&#66;C</v></row></data>"#,
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("t").unwrap();
        assert_array_values!(batch, "v", vec!["ABC"], StringArray);
    }

    #[test]
    fn test_hex_char_ref_in_text_resolved() {
        let record_batches = parse(
            r#"<data><row><v>X&#x41;Y</v></row></data>"#,
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("t").unwrap();
        assert_array_values!(batch, "v", vec!["XAY"], StringArray);
    }

    #[test]
    fn test_multibyte_char_ref_in_text_resolved() {
        // A code point above ASCII exercises the full 4-byte UTF-8 encode
        // path (U+1F30D 🌍).
        let record_batches = parse(
            r#"<data><row><v>&#127757;</v></row></data>"#,
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Utf8
            "#,
        );
        let batch = record_batches.get("t").unwrap();
        assert_array_values!(batch, "v", vec!["🌍"], StringArray);
    }

    #[test]
    fn test_char_ref_in_numeric_field_parsed() {
        // A char ref can form part of a numeric value; the resolved bytes
        // must flow into the numeric parser like ordinary text.
        let record_batches = parse(
            r#"<data><row><v>4&#50;</v></row></data>"#,
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("t").unwrap();
        assert_array_values!(batch, "v", vec![42i32], Int32Array);
    }

    #[test]
    fn test_unknown_entity_in_captured_field_returns_error() {
        // An undeclared entity cannot be resolved without DTD support.
        // Dropping it silently (the old behavior) corrupted the value, so it
        // must surface as a ParseError naming the entity.
        let config = config_from_yaml!(
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Utf8
            "#
        );
        let result = parse_xml(
            r#"<data><row><v>a&nbsp;b</v></row></data>"#.as_bytes(),
            &config,
        );
        match result.unwrap_err() {
            e @ Error::ParseError {
                kind: ParseKind::UnresolvedEntity,
                ..
            } => assert!(e.to_string().contains("nbsp")),
            e => panic!("Expected UnresolvedEntity ParseError, got {e:?}"),
        }
    }

    #[test]
    fn test_unknown_entity_outside_captured_fields_ignored() {
        // The same unresolvable entity in a part of the document no field
        // captures must not fail the parse — nothing is listening there.
        let record_batches = parse(
            r#"<data><notes>a&nbsp;b</notes><row><v>1</v></row></data>"#,
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("t").unwrap();
        assert_array_values!(batch, "v", vec![1i32], Int32Array);
    }

    #[test]
    fn test_malformed_char_ref_returns_error() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Utf8
            "#
        );
        let result = parse_xml(
            r#"<data><row><v>&#xZZ;</v></row></data>"#.as_bytes(),
            &config,
        );
        assert!(result.is_err(), "invalid char ref digits must error");
    }

    // =========================================================================
    // Row finalization: only configured elements delimit rows
    // =========================================================================

    #[test]
    fn test_unknown_sibling_under_table_does_not_create_row() {
        // Regression: an element the config doesn't know about, sitting
        // directly under a table path, used to finalize a spurious all-null
        // row. Unknown elements must be invisible to row accounting.
        let record_batches = parse(
            r#"
            <data>
                <item><v>1</v></item>
                <unrelated>junk</unrelated>
                <item><v>2</v></item>
            </data>
            "#,
            r#"
            tables:
                - name: items
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: v
                      xml_path: /data/item/v
                      data_type: Int32
                      nullable: true
            "#,
        );
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values!(batch, "v", vec![1i32, 2], Int32Array);
        assert_array_values!(batch, "<item>", vec![0u32, 1], UInt32Array);
    }

    #[test]
    fn test_unknown_self_closing_sibling_under_table_does_not_create_row() {
        // Same rule for the Event::Empty path.
        let record_batches = parse(
            r#"<data><item><v>1</v></item><unrelated/></data>"#,
            r#"
            tables:
                - name: items
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: v
                      xml_path: /data/item/v
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "v", vec![1i32], Int32Array);
    }

    #[test]
    fn test_unknown_sibling_with_non_nullable_fields_does_not_error() {
        // Before the fix the spurious row also *failed* the whole parse when
        // any field was non-nullable (the null row violated nullability).
        // Robustness against schema evolution of the input demands this
        // parses cleanly.
        let record_batches = parse(
            r#"<data><item><v>1</v></item><added_in_v2>x</added_in_v2></data>"#,
            r#"
            tables:
                - name: items
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: v
                      xml_path: /data/item/v
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    // =========================================================================
    // Repeated field elements within one row
    // =========================================================================

    #[test]
    fn test_repeated_element_value_returns_error() {
        // Regression: two occurrences of a field's element in one row used to
        // concatenate raw bytes — "1" + "2" parsed as 12 for Int32,
        // fabricating a value that never appeared in the document.
        let config = config_from_yaml!(
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Int32
            "#
        );
        let result = parse_xml(
            r#"<data><row><v>1</v><v>2</v></row></data>"#.as_bytes(),
            &config,
        );
        match result.unwrap_err() {
            e @ Error::ParseError {
                kind: ParseKind::DuplicateValue,
                ..
            } => assert!(e.to_string().contains("more than once")),
            e => panic!("Expected DuplicateValue ParseError, got {e:?}"),
        }
    }

    #[test]
    fn test_valueless_occurrence_then_value_accepted() {
        // The guard fires when an element is *re-entered with a value
        // already captured*: an empty first occurrence contributed nothing,
        // so a later value is not a conflict.
        let record_batches = parse(
            r#"<data><row><v/><v>2</v></row></data>"#,
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("t").unwrap();
        assert_array_values!(batch, "v", vec![2i32], Int32Array);
    }

    #[test]
    fn test_value_then_repeated_element_returns_error() {
        // Once a value is captured, any re-occurrence of the element in the
        // same row — even a self-closing one — is rejected: the document is
        // presenting multiple candidates for a scalar column.
        let config = config_from_yaml!(
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Int32
            "#
        );
        let result = parse_xml(
            r#"<data><row><v>1</v><v/></row></data>"#.as_bytes(),
            &config,
        );
        assert!(matches!(
            result.unwrap_err(),
            Error::ParseError {
                kind: ParseKind::DuplicateValue,
                ..
            }
        ));
    }

    #[test]
    fn test_same_field_element_across_rows_unaffected_by_sealing() {
        // Sealing is per row: the same element reappearing in the *next* row
        // is the normal case and must keep working.
        let record_batches = parse(
            r#"<data><row><v>1</v></row><row><v>2</v></row></data>"#,
            r#"
            tables:
                - name: t
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: v
                      xml_path: /data/row/v
                      data_type: Int32
            "#,
        );
        let batch = record_batches.get("t").unwrap();
        assert_array_values!(batch, "v", vec![1i32, 2], Int32Array);
    }

    mod streaming {
        //! Tests for the batched/streaming output path.
        //!
        //! The load-bearing check is the *value-transparency equivalence*:
        //! for any document, config, and thresholds, concatenating a table's
        //! streamed batches must reproduce the collect-everything
        //! `RecordBatch` exactly — including `<level>` foreign keys, whose
        //! continuity across flushes is the one thing a buggy flush would
        //! silently corrupt.

        use super::*;
        use crate::{BatchOptions, TableBatch};
        use arrow::compute::concat_batches;

        /// A nested fixture exercising everything at once: a small header
        /// table, a parent table, and a child table with two `<level>`
        /// columns. Station S2's measurements ensure FK values (station 1)
        /// span flush boundaries at small batch sizes.
        const NESTED_XML: &str = r#"<report>
            <header><title>Weather</title></header>
            <stations>
                <station>
                    <id>S1</id>
                    <measurements>
                        <measurement><v>1.5</v></measurement>
                        <measurement><v>2.5</v></measurement>
                        <measurement><v>3.5</v></measurement>
                    </measurements>
                </station>
                <station>
                    <id>S2</id>
                    <measurements>
                        <measurement><v>4.5</v></measurement>
                        <measurement><v>5.5</v></measurement>
                    </measurements>
                </station>
            </stations>
        </report>"#;

        const NESTED_YAML: &str = r#"
            parser_options:
              trim_text: true
            tables:
              - name: header
                xml_path: /report/header
                levels: []
                fields:
                  - name: title
                    xml_path: /report/header/title
                    data_type: Utf8
              - name: stations
                xml_path: /report/stations
                levels: [station]
                fields:
                  - name: id
                    xml_path: /report/stations/station/id
                    data_type: Utf8
              - name: measurements
                xml_path: /report/stations/station/measurements
                levels: [station, measurement]
                fields:
                  - name: v
                    xml_path: /report/stations/station/measurements/measurement/v
                    data_type: Float64
            "#;

        /// Runs `parse_batches`, groups the yielded batches per table
        /// (asserting the ≥ 1 row contract on each), and returns them in
        /// yield order.
        fn collect_streamed(
            parser: &Parser,
            xml: &str,
            options: BatchOptions,
        ) -> IndexMap<String, Vec<RecordBatch>> {
            let mut grouped: IndexMap<String, Vec<RecordBatch>> = IndexMap::new();
            for item in parser.parse_batches(xml.as_bytes(), options) {
                let TableBatch { table, batch } = item.unwrap();
                assert!(batch.num_rows() > 0, "yielded an empty batch for '{table}'");
                grouped.entry(table.to_string()).or_default().push(batch);
            }
            grouped
        }

        /// The §5.1 acceptance property: concat(streamed) == parse().
        fn assert_stream_equals_full_parse(xml: &str, yaml: &str, options: BatchOptions) {
            let config = config_from_yaml!(yaml);
            let parser = Parser::new(&config).unwrap();
            let full = parser.parse(xml.as_bytes()).unwrap();
            let streamed = collect_streamed(&parser, xml, options);

            for (name, batches) in &streamed {
                let schema = parser
                    .schema(name)
                    .unwrap_or_else(|| panic!("no schema for streamed table '{name}'"));
                let concatenated = concat_batches(&schema, batches).unwrap();
                assert_eq!(
                    &concatenated,
                    full.get(name).unwrap(),
                    "concatenated stream differs from full parse for table '{name}'"
                );
            }
            // Tables absent from the stream must be exactly the empty ones.
            for (name, batch) in &full {
                if !streamed.contains_key(name) {
                    assert_eq!(
                        batch.num_rows(),
                        0,
                        "table '{name}' has rows but streamed no batches"
                    );
                }
            }
        }

        #[rstest]
        fn streamed_batches_concat_to_full_parse_result(
            #[values(1, 2, 3, 7, 8192)] max_rows: usize,
        ) {
            assert_stream_equals_full_parse(
                NESTED_XML,
                NESTED_YAML,
                BatchOptions::default().with_max_rows_per_batch(max_rows),
            );
        }

        #[rstest]
        fn byte_budget_triggers_flushes_transparently(#[values(1, 10, 40)] max_bytes: usize) {
            // Tiny byte budgets force flushes at varying row counts; the
            // equivalence must hold regardless of which threshold fires.
            assert_stream_equals_full_parse(
                NESTED_XML,
                NESTED_YAML,
                BatchOptions::default().with_max_bytes_per_batch(max_bytes),
            );
        }

        #[test]
        fn zero_thresholds_are_clamped_to_one_row() {
            // 0 must not emit empty batches — clamped to 1, so every batch
            // carries exactly one row (asserted inside collect_streamed).
            let options = BatchOptions::default()
                .with_max_rows_per_batch(0)
                .with_max_bytes_per_batch(0);
            assert_stream_equals_full_parse(NESTED_XML, NESTED_YAML, options);
        }

        #[test]
        fn foreign_keys_continue_across_flush_boundaries() {
            // The §5.2 trap, asserted directly rather than via equivalence:
            // with one-row batches, the measurement batches' <station> FK
            // must keep counting (0,0,0,1,1) across five separate flushes —
            // a flush that reset `row_index` would restart it at 0.
            let config = config_from_yaml!(NESTED_YAML);
            let parser = Parser::new(&config).unwrap();
            let options = BatchOptions::default().with_max_rows_per_batch(1);

            let mut station_fks = Vec::new();
            for item in parser.parse_batches(NESTED_XML.as_bytes(), options) {
                let TableBatch { table, batch } = item.unwrap();
                if &*table == "measurements" {
                    let fk = batch
                        .column_by_name("<station>")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap();
                    station_fks.extend(fk.values().iter().copied());
                }
            }
            assert_eq!(station_fks, vec![0, 0, 0, 1, 1]);
        }

        #[test]
        fn batches_share_one_schema_with_parser_schema() {
            let config = config_from_yaml!(NESTED_YAML);
            let parser = Parser::new(&config).unwrap();
            let options = BatchOptions::default().with_max_rows_per_batch(1);

            for item in parser.parse_batches(NESTED_XML.as_bytes(), options) {
                let TableBatch { table, batch } = item.unwrap();
                let expected = parser.schema(&table).unwrap();
                assert!(
                    Arc::ptr_eq(&batch.schema(), &expected),
                    "batch schema for '{table}' is not the parser's shared Arc"
                );
            }
        }

        #[test]
        fn parse_batches_slice_matches_buffered_stream() {
            let config = config_from_yaml!(NESTED_YAML);
            let parser = Parser::new(&config).unwrap();
            let options = BatchOptions::default().with_max_rows_per_batch(2);

            let buffered: Vec<_> = parser
                .parse_batches(NESTED_XML.as_bytes(), options)
                .map(|item| item.unwrap())
                .collect();
            let zero_copy: Vec<_> = parser
                .parse_batches_slice(NESTED_XML.as_bytes(), options)
                .map(|item| item.unwrap())
                .collect();

            assert_eq!(buffered.len(), zero_copy.len());
            for (a, b) in buffered.iter().zip(&zero_copy) {
                assert_eq!(a.table, b.table);
                assert_eq!(a.batch, b.batch);
            }
        }

        #[test]
        fn structural_table_feeds_fks_but_never_emits() {
            // A fields-less table exists only to feed its row counter to
            // child `levels`; it must not appear in the stream (and its
            // builders must not grow — the drain would otherwise yield it).
            let xml = r#"<data>
                <group><items><item><v>1</v></item><item><v>2</v></item></items></group>
                <group><items><item><v>3</v></item></items></group>
            </data>"#;
            let yaml = r#"
                parser_options:
                  trim_text: true
                tables:
                  - name: groups
                    xml_path: /data
                    levels: [group]
                    fields: []
                  - name: items
                    xml_path: /data/group/items
                    levels: [group, item]
                    fields:
                      - name: v
                        xml_path: /data/group/items/item/v
                        data_type: Int32
                "#;
            assert_stream_equals_full_parse(
                xml,
                yaml,
                BatchOptions::default().with_max_rows_per_batch(1),
            );

            let config = config_from_yaml!(yaml);
            let parser = Parser::new(&config).unwrap();
            let streamed = collect_streamed(
                &parser,
                xml,
                BatchOptions::default().with_max_rows_per_batch(1),
            );
            assert!(!streamed.contains_key("groups"));
            let group_fks: Vec<u32> = streamed["items"]
                .iter()
                .flat_map(|b| {
                    b.column_by_name("<group>")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(group_fks, vec![0, 0, 1]);
        }

        #[test]
        fn root_table_streams_its_single_row() {
            let xml = r#"<data><count>42</count></data>"#;
            let yaml = r#"
                tables:
                  - name: root
                    xml_path: /
                    levels: []
                    fields:
                      - name: count
                        xml_path: /data/count
                        data_type: Int32
                "#;
            assert_stream_equals_full_parse(xml, yaml, BatchOptions::default());
        }

        #[test]
        fn stop_at_paths_drains_accumulated_rows() {
            let yaml_with_stop = r#"
                parser_options:
                  trim_text: true
                  stop_at_paths: [/report/header]
                tables:
                  - name: header
                    xml_path: /report/header
                    levels: []
                    fields:
                      - name: title
                        xml_path: /report/header/title
                        data_type: Utf8
                  - name: stations
                    xml_path: /report/stations
                    levels: [station]
                    fields:
                      - name: id
                        xml_path: /report/stations/station/id
                        data_type: Utf8
                "#;
            // Equivalence holds (both sides stop early)...
            assert_stream_equals_full_parse(NESTED_XML, yaml_with_stop, BatchOptions::default());

            // ...and the streamed output contains the pre-stop rows only.
            let config = config_from_yaml!(yaml_with_stop);
            let parser = Parser::new(&config).unwrap();
            let streamed = collect_streamed(&parser, NESTED_XML, BatchOptions::default());
            assert_eq!(streamed["header"].len(), 1);
            assert!(!streamed.contains_key("stations"));
        }

        #[test]
        fn error_is_yielded_once_then_stream_fuses() {
            // Row 3 is malformed; with one-row batches the first two rows
            // stream out as valid batches, then the error, then only None.
            let xml = r#"<data><row><v>1</v></row><row><v>2</v></row><row><v>oops</v></row><row><v>4</v></row></data>"#;
            let yaml = r#"
                tables:
                  - name: t
                    xml_path: /data
                    levels: [row]
                    fields:
                      - name: v
                        xml_path: /data/row/v
                        data_type: Int32
                "#;
            let config = config_from_yaml!(yaml);
            let parser = Parser::new(&config).unwrap();
            let mut stream = parser.parse_batches(
                xml.as_bytes(),
                BatchOptions::default().with_max_rows_per_batch(1),
            );

            assert!(stream.next().unwrap().is_ok());
            assert!(stream.next().unwrap().is_ok());
            let err = stream.next().unwrap().unwrap_err();
            assert!(matches!(err, Error::ParseError { .. }));
            assert!(stream.next().is_none(), "stream must fuse after an error");
            assert!(stream.next().is_none());
        }

        #[test]
        fn parse_streaming_callback_receives_all_batches() {
            let config = config_from_yaml!(NESTED_YAML);
            let parser = Parser::new(&config).unwrap();
            let full = parser.parse(NESTED_XML.as_bytes()).unwrap();

            let mut grouped: IndexMap<String, Vec<RecordBatch>> = IndexMap::new();
            parser
                .parse_streaming(
                    NESTED_XML.as_bytes(),
                    BatchOptions::default().with_max_rows_per_batch(2),
                    |table, batch| {
                        grouped.entry(table.to_string()).or_default().push(batch);
                        Ok(())
                    },
                )
                .unwrap();

            let schema = parser.schema("measurements").unwrap();
            let concatenated = concat_batches(&schema, &grouped["measurements"]).unwrap();
            assert_eq!(&concatenated, full.get("measurements").unwrap());
        }

        #[test]
        fn parse_streaming_sink_error_aborts() {
            let config = config_from_yaml!(NESTED_YAML);
            let parser = Parser::new(&config).unwrap();
            let mut calls = 0;
            let result = parser.parse_streaming(
                NESTED_XML.as_bytes(),
                BatchOptions::default().with_max_rows_per_batch(1),
                |_, _| {
                    calls += 1;
                    Err(Error::Io(std::io::Error::other("sink full")))
                },
            );
            assert!(matches!(result.unwrap_err(), Error::Io(_)));
            assert_eq!(calls, 1, "parse must stop after the sink errors");
        }

        #[test]
        fn single_table_reader_streams_with_stable_schema() {
            let xml = r#"<data><row><v>1</v></row><row><v>2</v></row><row><v>3</v></row></data>"#;
            let yaml = r#"
                tables:
                  - name: t
                    xml_path: /data
                    levels: [row]
                    fields:
                      - name: v
                        xml_path: /data/row/v
                        data_type: Int32
                "#;
            let config = config_from_yaml!(yaml);
            let parser = Parser::new(&config).unwrap();
            let reader = parser
                .parse_single_table(
                    xml.as_bytes(),
                    BatchOptions::default().with_max_rows_per_batch(2),
                )
                .unwrap();

            // Schema is available before any parsing and matches the batches.
            let schema = reader.schema();
            assert!(Arc::ptr_eq(&schema, &parser.schema("t").unwrap()));

            let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
                vec![2, 1]
            );
            let concatenated = concat_batches(&schema, &batches).unwrap();
            let full = parser.parse(xml.as_bytes()).unwrap();
            assert_eq!(&concatenated, full.get("t").unwrap());
        }

        #[test]
        fn single_table_reader_rejects_multi_table_configs() {
            let config = config_from_yaml!(NESTED_YAML);
            let parser = Parser::new(&config).unwrap();
            // unwrap_err doubles as a regression test that SingleTableReader
            // implements Debug (required for unwrap_err's bound).
            let err = parser
                .parse_single_table(&b"<report/>"[..], BatchOptions::default())
                .unwrap_err();
            assert!(matches!(err, Error::InvalidConfig { .. }));
            assert!(err.to_string().contains("exactly one table"));
            assert!(err.to_string().contains('3'));
        }

        #[test]
        fn single_table_reader_ignores_structural_tables() {
            // Structural (fields-less) tables produce no output, so a config
            // with one output table plus structural helpers still qualifies.
            let xml = r#"<data><group><items><item><v>7</v></item></items></group></data>"#;
            let yaml = r#"
                tables:
                  - name: groups
                    xml_path: /data
                    levels: [group]
                    fields: []
                  - name: items
                    xml_path: /data/group/items
                    levels: [group, item]
                    fields:
                      - name: v
                        xml_path: /data/group/items/item/v
                        data_type: Int32
                "#;
            let config = config_from_yaml!(yaml);
            let parser = Parser::new(&config).unwrap();
            let reader = parser
                .parse_single_table(xml.as_bytes(), BatchOptions::default())
                .unwrap();
            let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
            assert_eq!(batches.len(), 1);
            assert_array_values!(&batches[0], "v", vec![7i32], Int32Array);
        }

        #[test]
        fn single_table_schema_matches_schema_and_validates() {
            let xml_yaml = r#"
                tables:
                  - name: t
                    xml_path: /data
                    levels: [row]
                    fields:
                      - name: v
                        xml_path: /data/row/v
                        data_type: Int32
                "#;
            let config = config_from_yaml!(xml_yaml);
            let parser = Parser::new(&config).unwrap();
            let schema = parser.single_table_schema().unwrap();
            assert!(Arc::ptr_eq(&schema, &parser.schema("t").unwrap()));

            // Multi-table configs are rejected with the same error as
            // parse_single_table.
            let config = config_from_yaml!(NESTED_YAML);
            let parser = Parser::new(&config).unwrap();
            let err = parser.single_table_schema().unwrap_err();
            assert!(matches!(err, Error::InvalidConfig { .. }));
            assert!(err.to_string().contains("exactly one table"));
        }

        #[test]
        fn schema_reports_output_tables_only() {
            let yaml = r#"
                tables:
                  - name: structural
                    xml_path: /data
                    levels: [group]
                    fields: []
                  - name: items
                    xml_path: /data/group
                    levels: [group, item]
                    fields:
                      - name: v
                        xml_path: /data/group/item/v
                        data_type: Int32
                      - name: label
                        xml_path: /data/group/item/label
                        data_type: Utf8
                        nullable: true
                "#;
            let config = config_from_yaml!(yaml);
            let parser = Parser::new(&config).unwrap();

            let schema = parser.schema("items").unwrap();
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(names, vec!["<group>", "<item>", "v", "label"]);
            assert_eq!(schema.field(0).data_type(), &DataType::UInt32);
            assert!(!schema.field(0).is_nullable());
            assert_eq!(schema.field(2).data_type(), &DataType::Int32);
            assert!(schema.field(3).is_nullable());

            assert!(parser.schema("structural").is_none());
            assert!(parser.schema("nonexistent").is_none());
        }

        #[test]
        fn empty_table_streams_nothing_but_full_parse_returns_empty_batch() {
            // The documented contract difference: parse() includes a 0-row
            // batch for a table that matched nothing; the stream yields no
            // batch for it (consumers get the schema from Parser::schema).
            let xml = r#"<data><row><v>1</v></row></data>"#;
            let yaml = r#"
                tables:
                  - name: t
                    xml_path: /data
                    levels: [row]
                    fields:
                      - name: v
                        xml_path: /data/row/v
                        data_type: Int32
                  - name: missing
                    xml_path: /data/other
                    levels: []
                    fields:
                      - name: x
                        xml_path: /data/other/x
                        data_type: Int32
                        nullable: true
                "#;
            let config = config_from_yaml!(yaml);
            let parser = Parser::new(&config).unwrap();

            let full = parser.parse(xml.as_bytes()).unwrap();
            assert_eq!(full.get("missing").unwrap().num_rows(), 0);

            let streamed = collect_streamed(&parser, xml, BatchOptions::default());
            assert!(!streamed.contains_key("missing"));
            assert_eq!(streamed["t"].len(), 1);
        }

        #[test]
        #[cfg(target_pointer_width = "64")] // u32::MAX + 1 needs a 64-bit usize
        fn row_index_past_u32_max_errors_instead_of_wrapping_fks() {
            // 2^32 real rows aren't testable, so drive the mechanism
            // directly: place a table on the stack with its (private)
            // row_index just past what a UInt32 <level> column can hold and
            // finalize a row. Before the checked conversion this wrapped
            // silently, mis-linking every subsequent child row.
            let config = config_from_yaml!(
                r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: [item]
                    fields:
                      - name: v
                        xml_path: /data/item/v
                        data_type: Int32
                "#
            );
            let parser = Parser::new(&config).unwrap();
            let mut converter = XmlToArrowConverter::new(&parser, usize::MAX, usize::MAX);
            let node_id = parser.registry.resolve_path("/data").unwrap();
            converter.start_table(0, node_id);

            // The last representable index is fine...
            converter.table_builders[0].row_index = u32::MAX as usize;
            converter.table_builders[0].field_builders[0].set_current_value(b"1");
            converter.end_current_row().unwrap();

            // ...one past it must error, naming the table.
            converter.table_builders[0].row_index = u32::MAX as usize + 1;
            converter.table_builders[0].field_builders[0].set_current_value(b"2");
            let err = converter.end_current_row().unwrap_err();
            assert!(matches!(err, Error::RowIndexOverflow { .. }));
            assert!(err.to_string().contains("items"));
        }

        #[test]
        fn large_document_flushes_at_default_row_threshold() {
            // 20_000 rows with default options must arrive as 8192 + 8192 +
            // 3616 — the drain carrying the remainder.
            let mut xml = String::from("<data>");
            for i in 0..20_000 {
                xml.push_str(&format!("<row><v>{i}</v></row>"));
            }
            xml.push_str("</data>");
            let yaml = r#"
                tables:
                  - name: t
                    xml_path: /data
                    levels: [row]
                    fields:
                      - name: v
                        xml_path: /data/row/v
                        data_type: Int64
                "#;
            let config = config_from_yaml!(yaml);
            let parser = Parser::new(&config).unwrap();
            let row_counts: Vec<usize> = parser
                .parse_batches(xml.as_bytes(), BatchOptions::default())
                .map(|item| item.unwrap().batch.num_rows())
                .collect();
            assert_eq!(row_counts, vec![8192, 8192, 3616]);

            assert_stream_equals_full_parse(&xml, yaml, BatchOptions::default());
        }
    }
}
