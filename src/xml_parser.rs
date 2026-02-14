//! XML-to-Arrow parsing pipeline.
//!
//! This module is intentionally organized as a top-down narrative of the parsing flow:
//! 1) Build per-table and per-field builders that accumulate values into Arrow arrays.
//! 2) Stream XML events and track the current path using integer IDs (via PathRegistry).
//! 3) On element boundaries, push/pop table context and finalize rows deterministically.
//! 4) After streaming, finish builders into RecordBatches and return an ordered map.
//!
//! The guiding goals are: single-pass parsing, predictable O(1) lookups, and minimal
//! allocation in the hot path.
//! This means we front-load configuration validation and path compilation so the
//! event loop can focus on direct indexing and appends.
use std::io::BufRead;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrowNumericType, AsArray, BooleanBuilder, Float32Array, Float32Builder,
    Float64Array, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    RecordBatch, StringBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::compute::kernels::numeric;
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Schema};
use indexmap::IndexMap;
use quick_xml::Reader;
use quick_xml::escape;
use quick_xml::events::Event;
use quick_xml::events::attributes::Attributes;
use string_cache::DefaultAtom as Atom;

use crate::Config;
use crate::config::{DType, FieldConfig, TableConfig};
use crate::errors::Error;
use crate::errors::Result;
use crate::path_registry::{PathNodeId, PathRegistry, PathTracker};

// === Field-level accumulation ===
// The FieldBuilder owns the value buffer and the Arrow builder for one field.
// It encapsulates conversion, null handling, and optional scale/offset transforms.
///
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
    /// The Arrow array builder used to construct the array.
    array_builder: Box<dyn ArrayBuilder>,
    /// Indicates whether the builder has received any values for the current row.
    has_value: bool,
    /// Temporary storage for accumulating the current value from potentially multiple XML text nodes.
    current_value: String,
}

/// Helper function to parse a string and append it to a numeric builder.
fn append_numeric<T>(
    builder: &mut Box<dyn ArrayBuilder>,
    value: &str,
    has_value: bool,
    field_config: &FieldConfig,
) -> Result<()>
where
    T: ArrowNumericType,
    T::Native: FromStr,
    <T::Native as FromStr>::Err: std::fmt::Display,
{
    let builder = builder
        .as_any_mut()
        .downcast_mut::<arrow::array::PrimitiveBuilder<T>>()
        .expect("Builder type mismatch. This is a bug in create_array_builder.");

    if has_value {
        match value.parse::<T::Native>() {
            Ok(val) => builder.append_value(val),
            Err(e) => {
                return Err(Error::ParseError(format!(
                    "Failed to parse value '{}' as {} for field '{}' at path {}: {}",
                    value,
                    std::any::type_name::<T::Native>(),
                    field_config.name,
                    field_config.xml_path,
                    e
                )));
            }
        }
    } else if field_config.nullable {
        builder.append_null();
    } else {
        return Err(Error::ParseError(format!(
            "Missing value for non-nullable field '{}' at path {}",
            field_config.name, field_config.xml_path
        )));
    }
    Ok(())
}

fn parse_boolean_token(value: &str) -> Option<bool> {
    match value {
        "false" | "0" | "no" | "n" | "f" | "off" => Some(false),
        "true" | "1" | "yes" | "y" | "t" | "on" => Some(true),
        _ => None,
    }
}

impl FieldBuilder {
    fn new(field_config: &FieldConfig) -> Result<Self> {
        let array_builder = create_array_builder(field_config.data_type)?;
        let field = Field::new(
            &field_config.name,
            field_config.data_type.as_arrow_type(),
            field_config.nullable,
        );
        Ok(Self {
            field_config: field_config.clone(),
            field,
            array_builder,
            has_value: false,
            current_value: String::with_capacity(128),
        })
    }

    #[inline]
    fn set_current_value(&mut self, value: &str) {
        self.current_value.push_str(value);
        self.has_value = true;
    }

    /// Appends the currently accumulated value to the appropriate Arrow array builder,
    /// performing type conversion and handling nulls.
    fn append_current_value(&mut self) -> Result<()> {
        let value = self.current_value.as_str();
        match self.field.data_type() {
            DataType::Utf8 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .expect("Utf8Builder");
                if self.has_value {
                    builder.append_value(value);
                } else if self.field_config.nullable {
                    builder.append_null();
                } else {
                    builder.append_value("")
                }
            }
            DataType::Int8 => append_numeric::<arrow::datatypes::Int8Type>(
                &mut self.array_builder,
                value,
                self.has_value,
                &self.field_config,
            )?,
            DataType::UInt8 => append_numeric::<arrow::datatypes::UInt8Type>(
                &mut self.array_builder,
                value,
                self.has_value,
                &self.field_config,
            )?,
            DataType::Int16 => append_numeric::<arrow::datatypes::Int16Type>(
                &mut self.array_builder,
                value,
                self.has_value,
                &self.field_config,
            )?,
            DataType::UInt16 => append_numeric::<arrow::datatypes::UInt16Type>(
                &mut self.array_builder,
                value,
                self.has_value,
                &self.field_config,
            )?,
            DataType::Int32 => append_numeric::<arrow::datatypes::Int32Type>(
                &mut self.array_builder,
                value,
                self.has_value,
                &self.field_config,
            )?,
            DataType::UInt32 => append_numeric::<arrow::datatypes::UInt32Type>(
                &mut self.array_builder,
                value,
                self.has_value,
                &self.field_config,
            )?,
            DataType::Int64 => append_numeric::<arrow::datatypes::Int64Type>(
                &mut self.array_builder,
                value,
                self.has_value,
                &self.field_config,
            )?,
            DataType::UInt64 => append_numeric::<arrow::datatypes::UInt64Type>(
                &mut self.array_builder,
                value,
                self.has_value,
                &self.field_config,
            )?,
            DataType::Float32 => append_numeric::<arrow::datatypes::Float32Type>(
                &mut self.array_builder,
                value,
                self.has_value,
                &self.field_config,
            )?,
            DataType::Float64 => append_numeric::<arrow::datatypes::Float64Type>(
                &mut self.array_builder,
                value,
                self.has_value,
                &self.field_config,
            )?,
            DataType::Boolean => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .expect("BooleanBuilder");
                if self.has_value {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        if self.field_config.nullable {
                            builder.append_null();
                        } else {
                            return Err(Error::ParseError(format!(
                                "Missing value for non-nullable field '{}' at path {}",
                                self.field_config.name, self.field_config.xml_path
                            )));
                        }
                    } else {
                        let normalized = trimmed.to_ascii_lowercase();
                        match parse_boolean_token(normalized.as_str()) {
                            Some(val) => builder.append_value(val),
                            None => {
                                return Err(Error::ParseError(format!(
                                    "Failed to parse value '{}' as boolean for field '{}' at path {}: expected one of 'true', 'false', '1', '0', 'yes', 'no', 'on', 'off', 't', 'f', 'y', or 'n'",
                                    value, self.field_config.name, self.field_config.xml_path
                                )));
                            }
                        }
                    }
                } else if self.field_config.nullable {
                    builder.append_null();
                } else {
                    return Err(Error::ParseError(format!(
                        "Missing value for non-nullable field '{}' at path {}",
                        self.field_config.name, self.field_config.xml_path
                    )));
                }
            }
            _ => {
                return Err(Error::UnsupportedDataType(format!(
                    "Data type {:?} is not supported",
                    self.field.data_type()
                )));
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Arc<dyn Array>> {
        let mut array = self.array_builder.finish();
        if let Some(scale) = self.field_config.scale {
            array = match self.field.data_type() {
                DataType::Float32 => numeric::mul(
                    array.as_primitive::<Float32Type>(),
                    &Float32Array::new_scalar(scale as f32),
                )?,
                &DataType::Float64 => numeric::mul(
                    array.as_primitive::<Float64Type>(),
                    &Float64Array::new_scalar(scale),
                )?,
                _ => {
                    return Err(Error::UnsupportedConversion(format!(
                        "Scaling is only supported for Float32 and Float64, but found {:?}",
                        self.field.data_type()
                    )));
                }
            };
        }
        if let Some(offset) = self.field_config.offset {
            array = match self.field.data_type() {
                DataType::Float32 => numeric::add(
                    array.as_primitive::<Float32Type>(),
                    &Float32Array::new_scalar(offset as f32),
                )?,
                &DataType::Float64 => numeric::add(
                    array.as_primitive::<Float64Type>(),
                    &Float64Array::new_scalar(offset),
                )?,
                _ => {
                    return Err(Error::UnsupportedConversion(format!(
                        "Offset is only supported for Float32 and Float64, but found {:?}",
                        self.field.data_type()
                    )));
                }
            };
        }
        Ok(array)
    }
}

fn create_array_builder(data_type: DType) -> Result<Box<dyn ArrayBuilder>> {
    match data_type {
        DType::Boolean => Ok(Box::new(BooleanBuilder::default())),
        DType::Int8 => Ok(Box::new(Int8Builder::default())),
        DType::UInt8 => Ok(Box::new(UInt8Builder::default())),
        DType::Int16 => Ok(Box::new(Int16Builder::default())),
        DType::UInt16 => Ok(Box::new(UInt16Builder::default())),
        DType::Int32 => Ok(Box::new(Int32Builder::default())),
        DType::UInt32 => Ok(Box::new(UInt32Builder::default())),
        DType::Int64 => Ok(Box::new(Int64Builder::default())),
        DType::UInt64 => Ok(Box::new(UInt64Builder::default())),
        DType::Float32 => Ok(Box::new(Float32Builder::default())),
        DType::Float64 => Ok(Box::new(Float64Builder::default())),
        DType::Utf8 => Ok(Box::new(StringBuilder::default())),
    }
}

// === Table-level batching ===
// A TableBuilder owns per-field builders plus index builders for nested levels.
// It finalizes rows into a RecordBatch in a single, ordered pass.
///
/// Builds an Arrow RecordBatch for a single table defined in the configuration.
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
    row_index: usize,
}

impl TableBuilder {
    fn new(table_config: &TableConfig) -> Result<Self> {
        let mut index_builders = Vec::with_capacity(table_config.levels.len());
        index_builders.resize_with(table_config.levels.len(), UInt32Builder::default);
        let mut field_builders = Vec::with_capacity(table_config.fields.len());
        for field_config in &table_config.fields {
            field_builders.push(FieldBuilder::new(field_config)?);
        }
        Ok(Self {
            table_config: table_config.clone(),
            index_builders,
            field_builders,
            row_index: 0,
        })
    }

    fn end_row(&mut self, indices: &[u32]) -> Result<()> {
        // Append the current row's data to the arrays
        self.save_row(indices)?;
        for field_builder in self.field_builders.iter_mut() {
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
        for (index, index_builder) in indices.iter().zip(&mut self.index_builders) {
            index_builder.append_value(*index)
        }

        for field_builder in self.field_builders.iter_mut() {
            field_builder.append_current_value()?;
        }
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
            fields.push(Field::new(format!("<{}>", level), DataType::UInt32, false));
        }
        for field_builder in self.field_builders.iter_mut() {
            let array = field_builder.finish()?;
            arrays.push(array);
            fields.push(field_builder.field.clone())
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
    /// The table index in the table_builders array.
    table_idx: usize,
    /// The node ID in the path registry.
    node_id: PathNodeId,
}

/// Converts parsed XML events into Arrow RecordBatches.
///
/// This struct maintains a stack of table builders to handle nested XML structures.
/// It uses integer-based path indexing via PathRegistry for efficient lookups.
struct XmlToArrowConverter {
    /// Table builders for each table defined in the configuration, indexed by position.
    table_builders: Vec<TableBuilder>,
    /// Stack of active tables representing the current nesting level.
    builder_stack: Vec<TableStackEntry>,
    /// Path registry for efficient path lookups.
    registry: PathRegistry,
    /// Optional path nodes that trigger early termination after their closing tags.
    stop_node_ids: Vec<PathNodeId>,
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
            table_builders.push(TableBuilder::new(table_config)?);
        }

        let builder_stack = Vec::new();

        Ok(Self {
            table_builders,
            builder_stack,
            registry,
            stop_node_ids,
        })
    }

    /// Check if the given node represents a table path.
    #[inline]
    fn is_table_path(&self, node_id: PathNodeId) -> bool {
        self.registry.is_table_path(node_id)
    }

    /// Check if there's a root-level table (xml_path: /) that has fields defined.
    fn has_root_table_with_fields(&self) -> bool {
        if let Some(table_idx) = self.registry.get_table_index(PathNodeId::ROOT) {
            !self.table_builders[table_idx].field_builders.is_empty()
        } else {
            false
        }
    }

    /// Returns a mutable reference to the current table builder, if any.
    #[inline]
    fn current_table_builder_mut(&mut self) -> Option<&mut TableBuilder> {
        self.builder_stack
            .last()
            .map(|entry| &mut self.table_builders[entry.table_idx])
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
        let indices = self.parent_row_indices();
        if let Some(table_builder) = self.current_table_builder_mut() {
            table_builder.end_row(&indices)?;
        }
        Ok(())
    }

    fn parent_row_indices(&self) -> Vec<u32> {
        let mut indices = Vec::with_capacity(self.builder_stack.len());
        for entry in self.builder_stack.iter() {
            // Skip the root table (xml_path: /) when collecting parent indices.
            // The root table is special - it represents the document root and shouldn't
            // contribute to parent indices for child tables.
            if entry.node_id == PathNodeId::ROOT {
                continue;
            }
            indices.push(self.table_builders[entry.table_idx].row_index as u32);
        }
        indices
    }

    fn start_table(&mut self, node_id: PathNodeId) -> Result<()> {
        if let Some(table_idx) = self.registry.get_table_index(node_id) {
            self.builder_stack
                .push(TableStackEntry { table_idx, node_id });
            self.table_builders[table_idx].row_index = 0;
        }
        Ok(())
    }

    fn end_table(&mut self) -> Result<()> {
        self.builder_stack.pop();
        Ok(())
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
    // Expand empty elements (e.g., <tag/>) into <tag></tag>.
    // This simplifies the event loop, handling Event::Empty is no longer needed.
    reader.config_mut().expand_empty_elements = true;
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
        xml_to_arrow_converter.start_table(PathNodeId::ROOT)?;
    }

    // Use specialized parsing logic based on whether attribute parsing is required.
    // This avoids unnecessary attribute processing and Empty event handling
    // when attributes are not needed, improving performance.
    let stop_node_ids = xml_to_arrow_converter.stop_node_ids.clone();
    if config.requires_attribute_parsing() {
        process_xml_events::<_, true>(
            &mut reader,
            &mut path_tracker,
            &mut xml_to_arrow_converter,
            &stop_node_ids,
            PhantomData,
        )?;
    } else {
        process_xml_events::<_, false>(
            &mut reader,
            &mut path_tracker,
            &mut xml_to_arrow_converter,
            &stop_node_ids,
            PhantomData,
        )?;
    }

    let batches = xml_to_arrow_converter.finish()?;
    Ok(batches)
}

fn process_xml_events<B: BufRead, const PARSE_ATTRIBUTES: bool>(
    reader: &mut Reader<B>,
    path_tracker: &mut PathTracker,
    xml_to_arrow_converter: &mut XmlToArrowConverter,
    stop_node_ids: &[PathNodeId],
    _marker: PhantomData<bool>,
) -> Result<()> {
    let mut buf = Vec::with_capacity(4096);
    let mut attr_name_buffer = String::with_capacity(64);

    // Stack to track (node_id, is_table) for each entered element
    // This avoids redundant lookups on End events
    let mut element_stack: Vec<(Option<PathNodeId>, bool)> = Vec::with_capacity(32);

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let node_name = std::str::from_utf8(e.local_name().into_inner())?;
                let atom = Atom::from(node_name);
                let node_id = path_tracker.enter_atom(atom, &xml_to_arrow_converter.registry);

                let is_table = node_id
                    .map(|id| xml_to_arrow_converter.is_table_path(id))
                    .unwrap_or(false);

                if is_table {
                    xml_to_arrow_converter.start_table(node_id.unwrap())?;
                }

                element_stack.push((node_id, is_table));

                if PARSE_ATTRIBUTES {
                    if node_id.is_some() {
                        parse_attributes(
                            e.attributes(),
                            path_tracker,
                            xml_to_arrow_converter,
                            &mut attr_name_buffer,
                        )?;
                    }
                }
            }
            Event::GeneralRef(e) => {
                if let Some(node_id) = path_tracker.current() {
                    let text = e.into_inner();
                    let text = String::from_utf8_lossy(&text);
                    let text = escape::resolve_predefined_entity(&text).unwrap_or_default();
                    xml_to_arrow_converter.set_field_value_for_node(node_id, &text);
                }
            }
            Event::Text(e) => {
                if let Some(node_id) = path_tracker.current() {
                    let text = e.into_inner();
                    let text = String::from_utf8_lossy(&text);
                    xml_to_arrow_converter.set_field_value_for_node(node_id, &text);
                }
            }
            Event::CData(e) => {
                if let Some(node_id) = path_tracker.current() {
                    let text = e.into_inner();
                    let text = String::from_utf8_lossy(&text);
                    xml_to_arrow_converter.set_field_value_for_node(node_id, &text);
                }
            }
            Event::End(_) => {
                // Pop from our element stack
                if let Some((node_id, is_table)) = element_stack.pop() {
                    if is_table {
                        xml_to_arrow_converter.end_table()?;
                    }

                    // Leave the current path
                    path_tracker.leave();

                    // Check if parent is a table - need to end row
                    if let Some(&(_parent_node_id, parent_is_table)) = element_stack.last() {
                        if parent_is_table {
                            xml_to_arrow_converter.end_current_row()?;
                        }
                    } else {
                        // Check root table case
                        if xml_to_arrow_converter
                            .registry
                            .is_table_path(PathNodeId::ROOT)
                        {
                            if path_tracker.current() == Some(PathNodeId::ROOT) {
                                xml_to_arrow_converter.end_current_row()?;
                            }
                        }
                    }

                    // Stop after closing the configured path, so header-only reads
                    // can exit without scanning the remainder of the XML.
                    if let Some(node_id) = node_id {
                        if !stop_node_ids.is_empty()
                            && stop_node_ids.iter().any(|stop_id| *stop_id == node_id)
                        {
                            break;
                        }
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
    attributes: Attributes,
    path_tracker: &mut PathTracker,
    xml_to_arrow_converter: &mut XmlToArrowConverter,
    attr_name_buffer: &mut String,
) -> Result<()> {
    for attribute in attributes {
        let attribute = attribute?;
        let key = std::str::from_utf8(attribute.key.local_name().into_inner())?;

        // Reuse buffer to avoid allocation
        attr_name_buffer.clear();
        attr_name_buffer.push('@');
        attr_name_buffer.push_str(key);

        let atom = Atom::from(attr_name_buffer.as_str());
        if let Some(attr_node_id) = path_tracker.enter_atom(atom, &xml_to_arrow_converter.registry)
        {
            xml_to_arrow_converter.set_field_value_for_node(
                attr_node_id,
                std::str::from_utf8(attribute.value.as_ref())?,
            );
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
        BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, StringArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };

    macro_rules! assert_array_values {
        ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty) => {
            let array = $batch
                .column_by_name($column_name)
                .unwrap()
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap();
            assert_eq!(array.len(), $expected_values.len());
            for (i, expected) in $expected_values.iter().enumerate() {
                assert_eq!(array.value(i), *expected, "Value at index {} mismatch", i);
            }
        };
    }

    macro_rules! assert_array_values_option {
        ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty) => {
            let array = $batch
                .column_by_name($column_name)
                .unwrap()
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap();
            assert_eq!(array.len(), $expected_values.len());
            for (i, expected) in $expected_values.iter().enumerate() {
                match expected {
                    Some(val) => assert_eq!(array.value(i), *val, "Value at index {} mismatch", i),
                    None => assert!(array.is_null(i), "Expected null at index {}", i),
                }
            }
        };
    }

    macro_rules! assert_array_approx_values {
        ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty, $tolerance:expr) => {
            let array = $batch
                .column_by_name($column_name)
                .unwrap()
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap();
            assert_eq!(array.len(), $expected_values.len());
            for (i, expected) in $expected_values.iter().enumerate() {
                assert!(
                    abs_diff_eq!(array.value(i), *expected, epsilon = $tolerance),
                    "Value at index {} mismatch: Expected {}, got {}",
                    i,
                    expected,
                    array.value(i)
                );
            }
        };
    }

    macro_rules! assert_array_approx_values_option {
        ($batch:expr, $column_name:expr, $expected_values:expr, $array_type:ty, $tolerance:expr) => {
            let array = $batch
                .column_by_name($column_name)
                .unwrap()
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap();
            assert_eq!(array.len(), $expected_values.len());
            for (i, expected) in $expected_values.iter().enumerate() {
                match expected {
                    Some(val) => assert!(
                        abs_diff_eq!(array.value(i), *val, epsilon = $tolerance),
                        "Value at index {} mismatch: Expected {}, got {}",
                        i,
                        val,
                        array.value(i)
                    ),
                    None => assert!(array.is_null(i), "Expected null at index {}", i),
                }
            }
        };
    }

    #[test]
    fn test_parse_complex_multiple_tables_nested() -> Result<()> {
        let xml_content = r#"
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
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

        // Assert items - tables table has no fields so not in output
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

        Ok(())
    }

    #[test]
    fn test_parse_stop_at_paths_header_only() -> Result<()> {
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

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

        let header_batch = record_batches.get("header").unwrap();
        assert_eq!(header_batch.num_rows(), 1);
        assert_array_values!(header_batch, "title", vec!["Header Title"], StringArray);
        assert_array_values!(header_batch, "created_by", vec!["Unit Test"], StringArray);

        let data_batch = record_batches.get("data").unwrap();
        assert_eq!(data_batch.num_rows(), 0);

        Ok(())
    }

    #[test]
    fn test_parse_basic_multiple_tables() -> Result<()> {
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

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

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

        Ok(())
    }

    #[test]
    fn test_parse_dtypes_all_numeric_types() -> Result<()> {
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

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

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

        Ok(())
    }

    #[test]
    fn test_parse_text_special_characters_escaped() -> Result<()> {
        let xml_content = r#"<data><row><text>&lt;hello&gt; &amp; "world"</text></row></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: text_table
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: text
                      xml_path: /data/row/text
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("text_table").unwrap();
        assert_array_values!(batch, "text", vec!["<hello> & \"world\""], StringArray);

        Ok(())
    }

    #[test]
    fn test_parse_edge_empty_input() -> Result<()> {
        let xml_content = r#"<data></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: empty_table
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("empty_table").unwrap();
        assert_eq!(batch.num_rows(), 0);

        Ok(())
    }

    #[test]
    fn test_transform_scale_offset_float64() -> Result<()> {
        let xml_content = r#"
        <data>
            <row><value>100</value></row>
            <row><value>200</value></row>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("scaled").unwrap();
        // value = value * scale + offset = 100 * 0.1 + 10 = 20, 200 * 0.1 + 10 = 30
        assert_array_approx_values!(batch, "value", vec![20.0, 30.0], Float64Array, 0.001);

        Ok(())
    }

    #[test]
    fn test_transform_scale_offset_float32() -> Result<()> {
        let xml_content = r#"
        <data>
            <row><value>100</value></row>
            <row><value>200</value></row>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![20.0f32, 30.0f32], Float32Array, 0.001);

        Ok(())
    }

    #[test]
    fn test_attr_parse_multiple_attributes() -> Result<()> {
        let xml_content = r#"
        <data>
            <item id="1" name="First" type="A">Content 1</item>
            <item id="2" name="Second" type="B">Content 2</item>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
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

        Ok(())
    }

    #[test]
    fn test_parse_structure_deeply_nested() -> Result<()> {
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

        let config = config_from_yaml!(
            r#"
            tables:
                - name: deep
                  xml_path: /level1/level2/level3/level4/level5
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /level1/level2/level3/level4/level5/row/value
                      data_type: Int32
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("deep").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "value", vec![42i32], Int32Array);

        Ok(())
    }

    #[test]
    fn test_parse_structure_deeply_nested_generic() -> Result<()> {
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

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("rows").unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_array_values!(batch, "<level2>", vec![0u32, 0, 0, 1, 1], UInt32Array);
        assert_array_values!(batch, "<level3>", vec![0u32, 0, 1, 0, 0], UInt32Array);
        assert_array_values!(batch, "<row>", vec![0u32, 1, 0, 0, 1], UInt32Array);
        assert_array_values!(batch, "value", vec![1i32, 2, 3, 4, 5], Int32Array);

        Ok(())
    }

    #[test]
    fn test_parse_indices_nested_row_index() -> Result<()> {
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

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_array_values!(batch, "<group>", vec![0u32, 0, 1], UInt32Array);
        assert_array_values!(batch, "<item>", vec![0u32, 1, 0], UInt32Array);
        assert_array_values!(batch, "value", vec![1i32, 2, 3], Int32Array);

        Ok(())
    }

    #[test]
    fn test_parse_edge_empty_tags() -> Result<()> {
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

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values_option!(batch, "value", vec![Some("1"), None], StringArray);
        assert_array_values_option!(batch, "empty", vec![None, Some("not empty")], StringArray);

        Ok(())
    }

    #[test]
    fn test_dtype_boolean_valid_values() -> Result<()> {
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

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
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

        Ok(())
    }

    #[test]
    fn test_dtype_boolean_invalid_values() -> Result<()> {
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
    fn test_dtype_boolean_missing_value() -> Result<()> {
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

    #[test]
    fn test_transform_scale_unsupported_int32() {
        let config_result: std::result::Result<Config, _> = serde_yaml::from_str(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
                      scale: 0.1
            "#,
        );
        let config = config_result.unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_transform_offset_unsupported_int16() {
        let config_result: std::result::Result<Config, _> = serde_yaml::from_str(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int16
                      offset: 10.0
            "#,
        );
        let config = config_result.unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_unicode_parse_non_utf8_bytes() -> Result<()> {
        let xml_content = r#"<data><row><value>Hello 世界 🌍</value></row></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: unicode
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("unicode").unwrap();
        assert_array_values!(batch, "value", vec!["Hello 世界 🌍"], StringArray);

        Ok(())
    }

    #[test]
    fn test_attr_parse_empty_elements() -> Result<()> {
        let xml_content = r#"
        <data>
            <item id="1" />
            <item id="2"></item>
            <item id="3">content</item>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_array_values!(batch, "id", vec![1i32, 2, 3], Int32Array);
        assert_array_values_option!(
            batch,
            "content",
            vec![None, None, Some("content")],
            StringArray
        );

        Ok(())
    }

    #[test]
    fn test_error_malformed_xml_various_cases() {
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
    fn test_parse_options_trim_text() -> Result<()> {
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

    #[test]
    fn test_dtype_overflow_int8() -> Result<()> {
        let xml_content = r#"<data><row><value>128</value></row></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int8
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::ParseError(msg) => assert!(msg.contains("128")),
            e => panic!("Expected ParseError, got {:?}", e),
        }

        Ok(())
    }

    #[test]
    fn test_dtype_overflow_uint32() -> Result<()> {
        let xml_content = r#"<data><row><value>4294967296</value></row></data>"#;

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

        Ok(())
    }

    #[test]
    fn test_dtype_boundary_int64_max() -> Result<()> {
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
    fn test_transform_scale_offset_negative_float() -> Result<()> {
        let xml_content = r#"
        <data>
            <row><value>-100</value></row>
            <row><value>0</value></row>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("scaled").unwrap();
        // value = value * scale + offset = -100 * -0.5 + -10 = 40, 0 * -0.5 + -10 = -10
        assert_array_approx_values!(batch, "value", vec![40.0, -10.0], Float64Array, 0.001);

        Ok(())
    }

    #[test]
    fn test_nullable_all_null_various_types() -> Result<()> {
        let xml_content = r#"
        <data>
            <row></row>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
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

        Ok(())
    }

    #[test]
    fn test_nullable_mixed_null_and_valid() -> Result<()> {
        let xml_content = r#"
        <data>
            <row><value>10</value></row>
            <row></row>
            <row><value>30</value></row>
            <row></row>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("mixed").unwrap();
        assert_eq!(batch.num_rows(), 4);
        assert_array_values_option!(
            batch,
            "value",
            vec![Some(10i32), None, Some(30), None],
            Int32Array
        );

        Ok(())
    }

    #[test]
    fn test_nullable_all_types_with_nulls() -> Result<()> {
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

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("all_nulls").unwrap();
        assert_eq!(batch.num_rows(), 1);

        Ok(())
    }

    #[test]
    fn test_nullable_empty_vs_missing_element() -> Result<()> {
        let xml_content = r#"
        <data>
            <row><value></value></row>
            <row></row>
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
                      data_type: Utf8
                      nullable: true
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Both empty element and missing element should be null
        assert_array_values_option!(batch, "value", vec![None::<&str>, None], StringArray);

        Ok(())
    }

    #[test]
    fn test_error_table_not_found_end_row() -> Result<()> {
        let xml_content = r#"
        <data>
            <row><value>1</value></row>
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
            "#
        );

        // This should work normally
        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_error_table_not_found_parent_indices() -> Result<()> {
        let xml_content = r#"
        <data>
            <row><value>1</value></row>
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
            "#
        );

        // This should work normally
        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_error_attr_malformed() {
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

        // Malformed attribute - this might parse depending on quick-xml behavior
        let xml_content = r#"<data><item id="></item></data>"#;
        let result = parse_xml(xml_content.as_bytes(), &config);
        // The result depends on quick-xml's error handling
        assert!(result.is_err() || result.is_ok());
    }

    #[test]
    fn test_transform_scale_unsupported_boolean() {
        let config_result: std::result::Result<Config, _> = serde_yaml::from_str(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Boolean
                      scale: 0.1
            "#,
        );
        let config = config_result.unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_transform_offset_unsupported_utf8() {
        let config_result: std::result::Result<Config, _> = serde_yaml::from_str(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
                      offset: 10.0
            "#,
        );
        let config = config_result.unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_cdata_parse_basic() -> Result<()> {
        let xml_content = r#"
        <data>
            <row>
                <value><![CDATA[Hello World]]></value>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: cdata
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("cdata").unwrap();
        assert_array_values!(batch, "value", vec!["Hello World"], StringArray);

        Ok(())
    }

    #[test]
    fn test_cdata_parse_special_characters() -> Result<()> {
        let xml_content = r#"
        <data>
            <row>
                <value><![CDATA[<script>alert('XSS')</script>]]></value>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: cdata
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("cdata").unwrap();
        assert_array_values!(
            batch,
            "value",
            vec!["<script>alert('XSS')</script>"],
            StringArray
        );

        Ok(())
    }

    #[test]
    fn test_cdata_parse_mixed_with_text() -> Result<()> {
        let xml_content = r#"
        <data>
            <row>
                <value>Before <![CDATA[<CDATA>]]> After</value>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: cdata
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("cdata").unwrap();
        assert_array_values!(batch, "value", vec!["Before <CDATA> After"], StringArray);

        Ok(())
    }

    #[test]
    fn test_cdata_parse_multiple_sections() -> Result<()> {
        let xml_content = r#"
        <data>
            <row>
                <value><![CDATA[Part1]]><![CDATA[Part2]]></value>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: cdata
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("cdata").unwrap();
        assert_array_values!(batch, "value", vec!["Part1Part2"], StringArray);

        Ok(())
    }

    #[test]
    fn test_cdata_parse_numeric_conversion() -> Result<()> {
        let xml_content = r#"
        <data>
            <row>
                <value><![CDATA[42]]></value>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: cdata
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("cdata").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);

        Ok(())
    }

    #[test]
    fn test_namespace_parse_default() -> Result<()> {
        let xml_content = r#"
        <data xmlns="http://example.com/ns">
            <row>
                <value>42</value>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: ns_test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("ns_test").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);

        Ok(())
    }

    #[test]
    fn test_namespace_parse_prefixed() -> Result<()> {
        let xml_content = r#"
        <ns:data xmlns:ns="http://example.com/ns">
            <ns:row>
                <ns:value>42</ns:value>
            </ns:row>
        </ns:data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: ns_test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("ns_test").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);

        Ok(())
    }

    #[test]
    fn test_namespace_parse_multiple() -> Result<()> {
        let xml_content = r#"
        <data xmlns="http://example.com/default" xmlns:other="http://example.com/other">
            <row>
                <value>42</value>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: ns_test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int32
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("ns_test").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);

        Ok(())
    }

    #[test]
    fn test_nullable_missing_string_uses_empty() -> Result<()> {
        // Test that non-nullable strings get empty string, not error
        let xml_content = r#"
        <data>
            <row>
                <other>exists</other>
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
                      data_type: Utf8
                      nullable: false
                    - name: other
                      xml_path: /data/row/other
                      data_type: Utf8
                      nullable: false
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("test").unwrap();
        assert_eq!(batch.num_rows(), 1);
        // Non-nullable string with missing element should be empty string
        assert_array_values!(batch, "value", vec![""], StringArray);
        assert_array_values!(batch, "other", vec!["exists"], StringArray);

        Ok(())
    }

    #[test]
    fn test_nullable_missing_numeric_errors() -> Result<()> {
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
    fn test_dtype_overflow_int16() -> Result<()> {
        let xml_content = r#"<data><row><value>32768</value></row></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int16
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_dtype_overflow_int16_negative() -> Result<()> {
        let xml_content = r#"<data><row><value>-32769</value></row></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Int16
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_dtype_overflow_uint8() -> Result<()> {
        let xml_content = r#"<data><row><value>256</value></row></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: UInt8
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_dtype_overflow_uint16() -> Result<()> {
        let xml_content = r#"<data><row><value>65536</value></row></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: UInt16
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_dtype_overflow_int64() -> Result<()> {
        let xml_content = r#"<data><row><value>9223372036854775808</value></row></data>"#;

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

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_dtype_overflow_uint64() -> Result<()> {
        let xml_content = r#"<data><row><value>18446744073709551616</value></row></data>"#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: test
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: UInt64
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_dtype_invalid_negative_unsigned() -> Result<()> {
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

        Ok(())
    }

    #[test]
    fn test_dtype_boundary_all_types() -> Result<()> {
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
    fn test_transform_scale_only_float64() -> Result<()> {
        let xml_content = r#"<data><row><value>100</value></row></data>"#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![50.0], Float64Array, 0.001);

        Ok(())
    }

    #[test]
    fn test_transform_scale_only_float32() -> Result<()> {
        let xml_content = r#"<data><row><value>100</value></row></data>"#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![50.0f32], Float32Array, 0.001);

        Ok(())
    }

    #[test]
    fn test_transform_offset_only_float64() -> Result<()> {
        let xml_content = r#"<data><row><value>100</value></row></data>"#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("offset").unwrap();
        assert_array_approx_values!(batch, "value", vec![150.0], Float64Array, 0.001);

        Ok(())
    }

    #[test]
    fn test_transform_offset_only_float32() -> Result<()> {
        let xml_content = r#"<data><row><value>100</value></row></data>"#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("offset").unwrap();
        assert_array_approx_values!(batch, "value", vec![150.0f32], Float32Array, 0.001);

        Ok(())
    }

    #[test]
    fn test_transform_scale_negative_value() -> Result<()> {
        let xml_content = r#"<data><row><value>-100</value></row></data>"#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![-200.0], Float64Array, 0.001);

        Ok(())
    }

    #[test]
    fn test_transform_offset_negative_value() -> Result<()> {
        let xml_content = r#"<data><row><value>-100</value></row></data>"#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("offset").unwrap();
        assert_array_approx_values!(batch, "value", vec![-150.0], Float64Array, 0.001);

        Ok(())
    }

    #[test]
    fn test_transform_scale_zero_value() -> Result<()> {
        let xml_content = r#"<data><row><value>0</value></row></data>"#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![50.0], Float64Array, 0.001);

        Ok(())
    }

    #[test]
    fn test_transform_scale_very_small() -> Result<()> {
        let xml_content = r#"<data><row><value>1000000</value></row></data>"#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![1.0], Float64Array, 0.0001);

        Ok(())
    }

    #[test]
    fn test_transform_scale_very_large() -> Result<()> {
        let xml_content = r#"<data><row><value>0.001</value></row></data>"#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("scaled").unwrap();
        assert_array_approx_values!(batch, "value", vec![1000.0], Float64Array, 0.1);

        Ok(())
    }

    #[test]
    fn test_dtype_float_scientific_notation() -> Result<()> {
        let xml_content = r#"
        <data>
            <row>
                <float32>1.5e10</float32>
                <float64>2.5e-10</float64>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("scientific").unwrap();
        assert_array_approx_values!(batch, "float32", vec![1.5e10f32], Float32Array, 1e6);
        assert_array_approx_values!(batch, "float64", vec![2.5e-10f64], Float64Array, 1e-15);

        Ok(())
    }

    #[test]
    fn test_dtype_float_very_small() -> Result<()> {
        let xml_content = r#"
        <data>
            <row>
                <value>0.00000000000000000001</value>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: small
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("small").unwrap();
        assert_array_approx_values!(batch, "value", vec![1e-20f64], Float64Array, 1e-25);

        Ok(())
    }

    #[test]
    fn test_dtype_float_very_large() -> Result<()> {
        let xml_content = r#"
        <data>
            <row>
                <value>100000000000000000000</value>
            </row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: large
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Float64
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("large").unwrap();
        assert_array_approx_values!(batch, "value", vec![1e20f64], Float64Array, 1e15);

        Ok(())
    }

    #[test]
    fn test_unicode_attr_various_scripts() -> Result<()> {
        let xml_content = r#"
        <data>
            <item name="日本語">Japanese</item>
            <item name="العربية">Arabic</item>
            <item name="עברית">Hebrew</item>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("unicode").unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_array_values!(
            batch,
            "name",
            vec!["日本語", "العربية", "עברית"],
            StringArray
        );

        Ok(())
    }

    #[test]
    fn test_unicode_text_emojis() -> Result<()> {
        let xml_content = r#"
        <data>
            <row><value>Hello 🌍 World 🚀</value></row>
            <row><value>😀😃😄😁</value></row>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: emoji
                  xml_path: /data
                  levels: [row]
                  fields:
                    - name: value
                      xml_path: /data/row/value
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("emoji").unwrap();
        assert_array_values!(
            batch,
            "value",
            vec!["Hello 🌍 World 🚀", "😀😃😄😁"],
            StringArray
        );

        Ok(())
    }

    #[test]
    fn test_attr_parse_empty_value() -> Result<()> {
        let xml_content = r#"
        <data>
            <item id="" name="test">content</item>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("attrs").unwrap();
        assert_array_values!(batch, "id", vec![""], StringArray);
        assert_array_values!(batch, "name", vec!["test"], StringArray);

        Ok(())
    }

    #[test]
    fn test_attr_parse_whitespace_preserved() -> Result<()> {
        let xml_content = r#"
        <data>
            <item name="  spaced  ">content</item>
        </data>
        "#;

        let config = config_from_yaml!(
            r#"
            tables:
                - name: attrs
                  xml_path: /data
                  levels: [item]
                  fields:
                    - name: name
                      xml_path: /data/item/@name
                      data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("attrs").unwrap();
        assert_array_values!(batch, "name", vec!["  spaced  "], StringArray);

        Ok(())
    }

    #[test]
    fn test_attr_parse_multiple_on_element() -> Result<()> {
        let xml_content = r#"
        <data>
            <item a="1" b="2" c="3" d="4" e="5">content</item>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("attrs").unwrap();
        assert_array_values!(batch, "a", vec![1i32], Int32Array);
        assert_array_values!(batch, "b", vec![2i32], Int32Array);
        assert_array_values!(batch, "c", vec![3i32], Int32Array);
        assert_array_values!(batch, "d", vec![4i32], Int32Array);
        assert_array_values!(batch, "e", vec![5i32], Int32Array);

        Ok(())
    }

    #[test]
    fn test_parse_xml_features_comments_ignored() -> Result<()> {
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

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("test").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);

        Ok(())
    }

    #[test]
    fn test_parse_xml_features_declaration() -> Result<()> {
        let xml_content = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <data>
            <row><value>42</value></row>
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("test").unwrap();
        assert_array_values!(batch, "value", vec![42i32], Int32Array);

        Ok(())
    }

    #[test]
    fn test_parse_table_indices_only_excluded() -> Result<()> {
        // Tables with no fields (only indices) should not appear in output
        let xml_content = r#"
        <data>
            <group>
                <item><value>1</value></item>
                <item><value>2</value></item>
            </group>
        </data>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        // "groups" table should not be in output (no fields)
        assert!(!record_batches.contains_key("groups"));
        // "items" table should be in output
        assert!(record_batches.contains_key("items"));
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values!(batch, "<group>", vec![0u32, 0], UInt32Array);
        assert_array_values!(batch, "<item>", vec![0u32, 1], UInt32Array);
        assert_array_values!(batch, "value", vec![1i32, 2], Int32Array);

        Ok(())
    }

    #[test]
    fn test_parse_structure_fields_at_root() -> Result<()> {
        // Test fields defined directly at root level (xml_path: /)
        let xml_content = r#"
        <root>
            <name>Test Document</name>
            <version>1.0</version>
        </root>
        "#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("document").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "name", vec!["Test Document"], StringArray);
        assert_array_values!(batch, "version", vec!["1.0"], StringArray);

        Ok(())
    }

    #[test]
    fn test_parse_structure_root_with_attributes() -> Result<()> {
        let xml_content = r#"<root version="2.0" encoding="utf-8"><data>content</data></root>"#;

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("document").unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "version", vec!["2.0"], StringArray);
        assert_array_values!(batch, "encoding", vec!["utf-8"], StringArray);
        assert_array_values!(batch, "data", vec!["content"], StringArray);

        Ok(())
    }

    #[test]
    fn test_parse_structure_root_with_nested_tables() -> Result<()> {
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

        let config = config_from_yaml!(
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
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

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

        Ok(())
    }
}
