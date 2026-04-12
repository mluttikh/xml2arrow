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
use std::num::IntErrorKind;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder,
    Int64Builder, RecordBatch, StringBuilder, UInt8Builder, UInt16Builder, UInt32Builder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use indexmap::IndexMap;
use quick_xml::Reader;
use quick_xml::encoding::Decoder;
use quick_xml::escape;
use quick_xml::events::Event;
use quick_xml::events::attributes::Attributes;

use crate::Config;
use crate::config::{DType, FieldConfig, TableConfig};
use crate::errors::Error;
use crate::errors::Result;
use crate::path_registry::{PathNodeId, PathRegistry, PathTracker};

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

/// Builds Arrow arrays for a single field based on parsed XML data.
///
/// This struct manages the accumulation of values from the XML and their conversion
/// to the appropriate Arrow data type. It also handles null values and applies
/// scaling and offset transformations if configured.
struct FieldBuilder {
    /// Configuration of the field, including name, data type, nullability, scaling, and offset.
    field_config: FieldConfig,
    /// The Arrow field description
    field: Field,
    /// The Arrow array builder used to construct the array (enum dispatch, no vtable).
    array_builder: TypedArrayBuilder,
    /// Indicates whether the builder has received any values for the current row.
    has_value: bool,
    /// Temporary storage for accumulating the current value from potentially multiple XML text nodes.
    current_value: String,
    /// Whether this field has scale or offset transforms that should be applied inline.
    has_transform: bool,
}

/// Parses a boolean token from a string, trimming whitespace first.
/// Returns `Ok(Some(bool))` for valid tokens, `Ok(None)` for empty/whitespace-only input,
/// or `Err(())` for unrecognized values.
fn parse_boolean_token(value: &str) -> std::result::Result<Option<bool>, ()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.eq_ignore_ascii_case("false")
        || trimmed == "0"
        || trimmed.eq_ignore_ascii_case("no")
        || trimmed.eq_ignore_ascii_case("n")
        || trimmed.eq_ignore_ascii_case("f")
        || trimmed.eq_ignore_ascii_case("off")
    {
        Ok(Some(false))
    } else if trimmed.eq_ignore_ascii_case("true")
        || trimmed == "1"
        || trimmed.eq_ignore_ascii_case("yes")
        || trimmed.eq_ignore_ascii_case("y")
        || trimmed.eq_ignore_ascii_case("t")
        || trimmed.eq_ignore_ascii_case("on")
    {
        Ok(Some(true))
    } else {
        Err(())
    }
}

/// Helper macro to reduce boilerplate for parsing and appending integer values.
/// Uses `atoi` for fast byte-level integer parsing.
macro_rules! append_int {
    ($builder:expr, $value:expr, $has_value:expr, $field_config:expr, $ty:ty, $type_name:expr) => {
        if $has_value {
            match atoi::atoi::<$ty>($value.as_bytes()) {
                Some(val) => $builder.append_value(val),
                None => {
                    // Fall back to std parse for better error messages
                    match $value.parse::<$ty>() {
                        Ok(val) => $builder.append_value(val),
                        Err(e) => {
                            let msg = if *e.kind() == IntErrorKind::PosOverflow
                                || *e.kind() == IntErrorKind::NegOverflow
                            {
                                format!(
                                    "Failed to parse value '{}' as {} for field '{}' at path {}: {}",
                                    $value, $type_name, $field_config.name, $field_config.xml_path, e
                                )
                            } else {
                                format!(
                                    "Failed to parse value '{}' as {} for field '{}' at path {}: {}",
                                    $value, $type_name, $field_config.name, $field_config.xml_path, e
                                )
                            };
                            return Err(Error::ParseError(msg));
                        }
                    }
                }
            }
        } else if $field_config.nullable {
            $builder.append_null();
        } else {
            return Err(Error::ParseError(format!(
                "Missing value for non-nullable field '{}' at path {}",
                $field_config.name, $field_config.xml_path
            )));
        }
    };
}

/// Helper macro for parsing and appending float values using `fast_float2`.
macro_rules! append_float {
    ($builder:expr, $value:expr, $has_value:expr, $field_config:expr, $ty:ty, $type_name:expr) => {
        if $has_value {
            match fast_float2::parse::<$ty, _>($value) {
                Ok(val) => $builder.append_value(val),
                Err(e) => {
                    return Err(Error::ParseError(format!(
                        "Failed to parse value '{}' as {} for field '{}' at path {}: {}",
                        $value, $type_name, $field_config.name, $field_config.xml_path, e
                    )));
                }
            }
        } else if $field_config.nullable {
            $builder.append_null();
        } else {
            return Err(Error::ParseError(format!(
                "Missing value for non-nullable field '{}' at path {}",
                $field_config.name, $field_config.xml_path
            )));
        }
    };
}

impl FieldBuilder {
    fn new(field_config: &FieldConfig) -> Self {
        let array_builder = TypedArrayBuilder::from_dtype(field_config.data_type);
        let field = Field::new(
            &field_config.name,
            field_config.data_type.as_arrow_type(),
            field_config.nullable,
        );
        let has_transform = field_config.scale.is_some() || field_config.offset.is_some();
        Self {
            field_config: field_config.clone(),
            field,
            array_builder,
            has_value: false,
            has_transform,
            current_value: String::with_capacity(128),
        }
    }

    #[inline]
    fn set_current_value(&mut self, value: &str) {
        self.current_value.push_str(value);
        self.has_value = true;
    }

    /// Appends the currently accumulated value to the Arrow array builder,
    /// performing type conversion and handling nulls.
    #[allow(clippy::too_many_lines)]
    fn append_current_value(&mut self) -> Result<()> {
        let value = self.current_value.as_str();
        let has_value = self.has_value;
        let fc = &self.field_config;

        match &mut self.array_builder {
            TypedArrayBuilder::Utf8(b) => {
                if has_value {
                    b.append_value(value);
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
                if self.has_transform {
                    if has_value {
                        match fast_float2::parse::<f32, _>(value) {
                            #[allow(clippy::cast_possible_truncation)]
                            Ok(mut val) => {
                                if let Some(scale) = fc.scale {
                                    val *= scale as f32;
                                }
                                if let Some(offset) = fc.offset {
                                    val += offset as f32;
                                }
                                b.append_value(val);
                            }
                            Err(e) => {
                                return Err(Error::ParseError(format!(
                                    "Failed to parse value '{}' as f32 for field '{}' at path {}: {}",
                                    value, fc.name, fc.xml_path, e
                                )));
                            }
                        }
                    } else if fc.nullable {
                        b.append_null();
                    } else {
                        return Err(Error::ParseError(format!(
                            "Missing value for non-nullable field '{}' at path {}",
                            fc.name, fc.xml_path
                        )));
                    }
                } else {
                    append_float!(b, value, has_value, fc, f32, "f32");
                }
            }
            TypedArrayBuilder::Float64(b) => {
                if self.has_transform {
                    if has_value {
                        match fast_float2::parse::<f64, _>(value) {
                            Ok(mut val) => {
                                if let Some(scale) = fc.scale {
                                    val *= scale;
                                }
                                if let Some(offset) = fc.offset {
                                    val += offset;
                                }
                                b.append_value(val);
                            }
                            Err(e) => {
                                return Err(Error::ParseError(format!(
                                    "Failed to parse value '{}' as f64 for field '{}' at path {}: {}",
                                    value, fc.name, fc.xml_path, e
                                )));
                            }
                        }
                    } else if fc.nullable {
                        b.append_null();
                    } else {
                        return Err(Error::ParseError(format!(
                            "Missing value for non-nullable field '{}' at path {}",
                            fc.name, fc.xml_path
                        )));
                    }
                } else {
                    append_float!(b, value, has_value, fc, f64, "f64");
                }
            }
            TypedArrayBuilder::Boolean(b) => {
                if has_value {
                    match parse_boolean_token(value) {
                        Ok(Some(val)) => b.append_value(val),
                        Ok(None) if fc.nullable => b.append_null(),
                        Ok(None) => {
                            return Err(Error::ParseError(format!(
                                "Missing value for non-nullable field '{}' at path {}",
                                fc.name, fc.xml_path
                            )));
                        }
                        Err(()) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as boolean for field '{}' at path {}: expected one of 'true', 'false', '1', '0', 'yes', 'no', 'on', 'off', 't', 'f', 'y', or 'n'",
                                value, fc.name, fc.xml_path
                            )));
                        }
                    }
                } else if fc.nullable {
                    b.append_null();
                } else {
                    return Err(Error::ParseError(format!(
                        "Missing value for non-nullable field '{}' at path {}",
                        fc.name, fc.xml_path
                    )));
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
/// Builds an Arrow `RecordBatch` for a single table defined in the configuration.
///
/// This struct manages the building of a single Arrow `RecordBatch` by collecting
/// data for each field defined in the table's configuration. It also handles
/// parent/child relationships between tables through index builders.
struct TableBuilder {
    /// The table's configuration.
    table_config: TableConfig,
    // Builders for the parent row indices, used for representing nested tables.
    index_builders: Vec<UInt32Builder>,
    /// Builders for each field in the table, indexed by field position.
    field_builders: Vec<FieldBuilder>,
    /// The current row index for this table.
    ///
    /// This increments every time `save_row` is called. Critically, this value
    /// is also read by any ACTIVE CHILD TABLES on the stack to populate their
    /// parent index columns (the foreign keys defined in `levels`).
    row_index: usize,
}

impl TableBuilder {
    fn new(table_config: &TableConfig) -> Self {
        let mut index_builders = Vec::with_capacity(table_config.levels.len());
        index_builders.resize_with(table_config.levels.len(), UInt32Builder::default);
        let mut field_builders = Vec::with_capacity(table_config.fields.len());
        for field_config in &table_config.fields {
            field_builders.push(FieldBuilder::new(field_config));
        }
        Self {
            table_config: table_config.clone(),
            index_builders,
            field_builders,
            row_index: 0,
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
    fn set_field_value_by_index(&mut self, field_idx: usize, value: &str) {
        if let Some(field_builder) = self.field_builders.get_mut(field_idx) {
            field_builder.set_current_value(value);
        }
    }

    fn save_row(&mut self, indices: &[u32]) -> Result<()> {
        // 1. Write the parent foreign keys.
        // The `indices` slice contains the row_index of each ancestor table,
        // in order of hierarchy. These align 1:1 with the `levels` defined
        // in this table's configuration.
        for (index, index_builder) in indices.iter().zip(&mut self.index_builders) {
            index_builder.append_value(*index);
        }

        // 2. Write the actual field values for this table.
        for field_builder in &mut self.field_builders {
            field_builder.append_current_value()?;
        }

        // 3. Advance this table's primary key.
        self.row_index += 1;
        Ok(())
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let num_arrays = self.field_builders.len() + self.index_builders.len();
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(num_arrays);
        let mut fields: Vec<Field> = Vec::with_capacity(num_arrays);
        for (level, index_builder) in self
            .table_config
            .levels
            .iter()
            .zip(&mut self.index_builders)
        {
            arrays.push(Arc::new(index_builder.finish()));
            fields.push(Field::new(format!("<{level}>"), DataType::UInt32, false));
        }
        for field_builder in &mut self.field_builders {
            let array = field_builder.finish();
            arrays.push(array);
            fields.push(field_builder.field.clone());
        }
        let schema = Schema::new(fields);
        Ok(RecordBatch::try_new(Arc::new(schema), arrays).map_err(|e| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "Failed to create RecordBatch for table with name {} and XML path {}: {}",
                self.table_config.name, self.table_config.xml_path, e
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
struct XmlToArrowConverter {
    /// Table builders for each table defined in the configuration, indexed by position.
    table_builders: Vec<TableBuilder>,
    /// Stack of active tables representing the current nesting level.
    builder_stack: Vec<TableStackEntry>,
    /// Path registry for efficient path lookups.
    registry: PathRegistry,
    /// Optional path nodes that trigger early termination after their closing tags.
    stop_node_ids: Vec<PathNodeId>,
    /// Reusable buffer for collecting parent row indices, avoiding per-row allocation.
    parent_indices_buffer: Vec<u32>,
}

impl XmlToArrowConverter {
    fn from_config(config: &Config) -> Result<Self> {
        // Validate field configurations early to catch unsupported scale/offset
        config.validate()?;

        // Build path registry for efficient lookups
        let registry = PathRegistry::from_config(config);

        let stop_node_ids = config
            .parser_options
            .stop_at_paths
            .iter()
            .filter_map(|path| registry.resolve_path(path))
            .collect::<Vec<_>>();

        let mut table_builders = Vec::with_capacity(config.tables.len());
        for table_config in &config.tables {
            table_builders.push(TableBuilder::new(table_config));
        }

        let builder_stack = Vec::new();

        Ok(Self {
            table_builders,
            builder_stack,
            registry,
            stop_node_ids,
            parent_indices_buffer: Vec::new(),
        })
    }

    /// Check if the given node represents a table path.
    #[inline]
    fn is_table_path(&self, node_id: PathNodeId) -> bool {
        self.registry.is_table_path(node_id)
    }

    /// Check if there's a root-level table (`xml_path`: /) that has fields defined.
    fn has_root_table_with_fields(&self) -> bool {
        if let Some(table_idx) = self.registry.get_table_index(PathNodeId::ROOT) {
            !self.table_builders[table_idx].field_builders.is_empty()
        } else {
            false
        }
    }

    /// Sets a field value for the current table using path node information.
    #[inline]
    pub fn set_field_value_for_node(&mut self, node_id: PathNodeId, value: &str) {
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
            #[allow(clippy::cast_possible_truncation)] // Row count won't exceed u32::MAX
            self.parent_indices_buffer
                .push(self.table_builders[entry.table_idx].row_index as u32);
        }
        if let Some(entry) = self.builder_stack.last() {
            self.table_builders[entry.table_idx].end_row(&self.parent_indices_buffer)?;
        }
        Ok(())
    }

    fn start_table(&mut self, node_id: PathNodeId) {
        if let Some(table_idx) = self.registry.get_table_index(node_id) {
            self.builder_stack
                .push(TableStackEntry { table_idx, node_id });
            self.table_builders[table_idx].row_index = 0;
        }
    }

    fn end_table(&mut self) {
        self.builder_stack.pop();
    }

    fn finish(mut self) -> Result<IndexMap<String, arrow::record_batch::RecordBatch>> {
        let mut record_batches = IndexMap::new();
        for table_builder in &mut self.table_builders {
            if !table_builder.field_builders.is_empty() {
                let record_batch = table_builder.finish()?;
                record_batches.insert(table_builder.table_config.name.clone(), record_batch);
            }
        }
        Ok(record_batches)
    }
}

/// Parses XML data from a reader into Arrow record batches based on a provided configuration.
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
    let mut reader = Reader::from_reader(reader);
    if config.parser_options.trim_text {
        reader.config_mut().trim_text(true);
    }
    let mut xml_to_arrow_converter = XmlToArrowConverter::from_config(config)?;
    let mut path_tracker = PathTracker::new();

    // Start the root-level table (xml_path: /) if it exists AND has fields defined.
    // We only start it if it has fields, because adding it to the builder_stack
    // would affect parent_row_indices for all nested tables, breaking their
    // level indexing. Tables with xml_path: / and no fields are just used for
    // hierarchy purposes and don't need to be on the stack.
    if xml_to_arrow_converter.has_root_table_with_fields() {
        xml_to_arrow_converter.start_table(PathNodeId::ROOT);
    }

    // Use specialized parsing logic based on whether attribute parsing is required.
    // This avoids unnecessary attribute processing and Empty event handling
    // when attributes are not needed, improving performance.
    let stop_node_ids = std::mem::take(&mut xml_to_arrow_converter.stop_node_ids);
    if config.requires_attribute_parsing() {
        process_xml_events::<_, true>(
            &mut reader,
            &mut path_tracker,
            &mut xml_to_arrow_converter,
            &stop_node_ids,
        )?;
    } else {
        process_xml_events::<_, false>(
            &mut reader,
            &mut path_tracker,
            &mut xml_to_arrow_converter,
            &stop_node_ids,
        )?;
    }

    let batches = xml_to_arrow_converter.finish()?;
    Ok(batches)
}

#[allow(clippy::too_many_lines)]
fn process_xml_events<B: BufRead, const PARSE_ATTRIBUTES: bool>(
    reader: &mut Reader<B>,
    path_tracker: &mut PathTracker,
    xml_to_arrow_converter: &mut XmlToArrowConverter,
    stop_node_ids: &[PathNodeId],
) -> Result<()> {
    let mut buf = Vec::with_capacity(4096);
    let mut attr_name_buffer = Vec::with_capacity(64);

    // Stack to track (node_id, is_table) for each entered element
    // This avoids redundant lookups on End events
    let mut element_stack: Vec<(Option<PathNodeId>, bool)> = Vec::with_capacity(32);

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let name_bytes = e.local_name().into_inner();
                let node_id = path_tracker.enter(name_bytes, &xml_to_arrow_converter.registry);

                let is_table = node_id
                    .is_some_and(|id| xml_to_arrow_converter.is_table_path(id));

                if is_table {
                    xml_to_arrow_converter.start_table(node_id.unwrap());
                }

                element_stack.push((node_id, is_table));

                if PARSE_ATTRIBUTES
                    && let Some(id) = node_id
                    && xml_to_arrow_converter.registry.has_attribute_children(id)
                {
                    parse_attributes(
                        reader.decoder(),
                        e.attributes(),
                        path_tracker,
                        xml_to_arrow_converter,
                        &mut attr_name_buffer,
                    )?;
                }
            }
            Event::Empty(e) => {
                let name_bytes = e.local_name().into_inner();
                let node_id = path_tracker.enter(name_bytes, &xml_to_arrow_converter.registry);

                let is_table = node_id
                    .is_some_and(|id| xml_to_arrow_converter.is_table_path(id));

                if is_table {
                    xml_to_arrow_converter.start_table(node_id.unwrap());
                }

                if PARSE_ATTRIBUTES
                    && let Some(id) = node_id
                    && xml_to_arrow_converter.registry.has_attribute_children(id)
                {
                    parse_attributes(
                        reader.decoder(),
                        e.attributes(),
                        path_tracker,
                        xml_to_arrow_converter,
                        &mut attr_name_buffer,
                    )?;
                }

                // Immediately close: empty elements have no children or text
                if is_table {
                    xml_to_arrow_converter.end_table();
                }
                path_tracker.leave();

                // Check if parent is a table - need to end row
                if let Some(&(_parent_node_id, parent_is_table)) = element_stack.last() {
                    if parent_is_table {
                        xml_to_arrow_converter.end_current_row()?;
                    }
                } else if xml_to_arrow_converter
                    .registry
                    .is_table_path(PathNodeId::ROOT)
                    && path_tracker.current() == Some(PathNodeId::ROOT)
                {
                    xml_to_arrow_converter.end_current_row()?;
                }

                // Check stop paths
                if let Some(node_id) = node_id
                    && stop_node_ids.contains(&node_id)
                {
                    break;
                }
            }
            Event::GeneralRef(e) => {
                if let Some(node_id) = path_tracker.current() {
                    let text = e.into_inner();
                    let text = std::str::from_utf8(&text)?;
                    let resolved = escape::resolve_predefined_entity(text).unwrap_or_default();
                    xml_to_arrow_converter.set_field_value_for_node(node_id, resolved);
                }
            }
            Event::Text(e) => {
                if let Some(node_id) = path_tracker.current() {
                    let text = e.into_inner();
                    let text = std::str::from_utf8(&text)?;
                    xml_to_arrow_converter.set_field_value_for_node(node_id, text);
                }
            }
            Event::CData(e) => {
                if let Some(node_id) = path_tracker.current() {
                    let text = e.into_inner();
                    let text = std::str::from_utf8(&text)?;
                    xml_to_arrow_converter.set_field_value_for_node(node_id, text);
                }
            }
            Event::End(_) => {
                // Pop from our element stack
                if let Some((node_id, is_table)) = element_stack.pop() {
                    if is_table {
                        xml_to_arrow_converter.end_table();
                    }

                    // Leave the current path
                    path_tracker.leave();

                    // Check if parent is a table - need to end row
                    if let Some(&(_parent_node_id, parent_is_table)) = element_stack.last() {
                        if parent_is_table {
                            xml_to_arrow_converter.end_current_row()?;
                        }
                    } else if xml_to_arrow_converter
                        .registry
                        .is_table_path(PathNodeId::ROOT)
                        && path_tracker.current() == Some(PathNodeId::ROOT)
                    {
                        // Check root table case
                        xml_to_arrow_converter.end_current_row()?;
                    }

                    // Stop after closing the configured path, so header-only reads
                    // can exit without scanning the remainder of the XML.
                    if let Some(node_id) = node_id
                        && stop_node_ids.contains(&node_id)
                    {
                        break;
                    }
                }
            }
            Event::Eof => {
                break;
            }
            _ => (),
        }
        buf.clear();
    }
    Ok(())
}

#[inline]
fn parse_attributes(
    decoder: Decoder,
    attributes: Attributes,
    path_tracker: &mut PathTracker,
    xml_to_arrow_converter: &mut XmlToArrowConverter,
    attr_name_buffer: &mut Vec<u8>,
) -> Result<()> {
    for attribute in attributes {
        let attribute = attribute?;
        let key = attribute.key.local_name().into_inner();

        // Reuse buffer to avoid allocation: build "@key" as bytes
        attr_name_buffer.clear();
        attr_name_buffer.push(b'@');
        attr_name_buffer.extend_from_slice(key);

        if let Some(attr_node_id) =
            path_tracker.enter(attr_name_buffer, &xml_to_arrow_converter.registry)
        {
            let value = attribute.decode_and_unescape_value(decoder)?;
            xml_to_arrow_converter.set_field_value_for_node(attr_node_id, &value);
        }
        path_tracker.leave();
    }
    Ok(())
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
        match err {
            Error::ParseError(msg) => {
                assert!(msg.contains("maybe"));
                assert!(msg.contains("boolean"));
            }
            _ => panic!("Expected ParseError, got {:?}", err),
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
        let config: Config = serde_yaml::from_str(&yaml).unwrap();
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
            Error::ParseError(msg) => assert!(msg.contains("not_a_number")),
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
        let config: Config = serde_yaml::from_str(&yaml_config).unwrap();
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
            Error::ParseError(msg) => assert!(msg.contains("Missing value")),
            e => panic!("Expected ParseError, got {:?}", e),
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
            Error::ParseError(msg) => assert!(msg.contains("not_a_float")),
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
            Error::ParseError(msg) => assert!(msg.contains("abc")),
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
            Error::ParseError(msg) => assert!(msg.contains("Missing value")),
            e => panic!("Expected ParseError, got {:?}", e),
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
            Error::ParseError(msg) => assert!(msg.contains("Missing value")),
            e => panic!("Expected ParseError, got {:?}", e),
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
        let config: Config = serde_yaml::from_str(&yaml_config).unwrap();
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
}
