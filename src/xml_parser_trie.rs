//! Trie-based XML parser implementation for high-performance path matching.
//!
//! This module provides an optimized XML parser that uses the PathTrie structure
//! for O(1) path lookups instead of hash-based XmlPath matching. This results in
//! significantly better performance, especially for large XML files with deep nesting.

use std::io::BufRead;

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
use crate::errors::{Error, Result};
use crate::path_trie::{FieldId, PathTrie, PathTrieBuilder, StateId, TableId, UNMATCHED_STATE};

/// Builds Arrow arrays for a single field based on parsed XML data.
struct FieldBuilder {
    field_config: FieldConfig,
    field: Field,
    array_builder: Box<dyn ArrayBuilder>,
    has_value: bool,
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
        .expect("Builder type mismatch");

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
    } else {
        builder.append_null();
    }
    Ok(())
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

    fn set_current_value(&mut self, value: &str) {
        self.current_value.push_str(value);
        self.has_value = true;
    }

    fn append_current_value(&mut self) -> Result<()> {
        let value = self.current_value.as_str();
        match self.field.data_type() {
            DataType::Utf8 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .expect("StringBuilder");
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
                    match value {
                        "false" | "0" => builder.append_value(false),
                        "true" | "1" => builder.append_value(true),
                        _ => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as boolean for field '{}' at path {}: expected 'true', 'false', '1' or '0'",
                                value, self.field_config.name, self.field_config.xml_path
                            )));
                        }
                    }
                } else {
                    builder.append_null();
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

    fn finish(&mut self) -> Result<Arc<dyn Array>> {
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

/// Builds an Arrow RecordBatch for a single table.
struct TableBuilder {
    table_config: TableConfig,
    index_builders: Vec<UInt32Builder>,
    field_builders: Vec<FieldBuilder>,
    row_index: usize,
}

impl TableBuilder {
    /// Check if any field has a value for the current row
    fn has_any_field_value(&self) -> bool {
        self.field_builders.iter().any(|fb| fb.has_value)
    }
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
        self.save_row(indices)?;
        for field_builder in &mut self.field_builders {
            field_builder.has_value = false;
            field_builder.current_value.clear();
        }
        Ok(())
    }

    fn set_field_value(&mut self, _field_id: FieldId, local_field_idx: usize, value: &str) {
        if let Some(field_builder) = self.field_builders.get_mut(local_field_idx) {
            field_builder.set_current_value(value);
        }
    }

    fn save_row(&mut self, indices: &[u32]) -> Result<()> {
        for (index, index_builder) in indices.iter().zip(&mut self.index_builders) {
            index_builder.append_value(*index)
        }

        for field_builder in &mut self.field_builders {
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

        for field_builder in &mut self.field_builders {
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

/// Context for tracking active table state during parsing.
struct TableContext {
    table_id: TableId,
    row_index: u32,
}

impl TableContext {
    fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            row_index: 0,
        }
    }
}

/// Trie-based XML to Arrow converter.
struct XmlToArrowConverter {
    trie: PathTrie,
    table_builders: Vec<TableBuilder>,
    table_stack: Vec<TableContext>,
    /// Reusable buffer for row indices to avoid allocations
    indices_buffer: Vec<u32>,
}

impl XmlToArrowConverter {
    fn from_config(config: &Config) -> Result<Self> {
        let trie = PathTrieBuilder::from_config(config)?;

        let mut table_builders = Vec::with_capacity(trie.table_configs.len());
        for table_config in &trie.table_configs {
            table_builders.push(TableBuilder::new(table_config)?);
        }

        Ok(Self {
            trie,
            table_builders,
            table_stack: Vec::new(),
            indices_buffer: Vec::with_capacity(8), // Most configs have < 8 nested levels
        })
    }

    fn set_field_value(&mut self, field_id: FieldId, value: &str) -> Result<()> {
        // Find which table owns this field
        if let Some(table_id) = self.trie.get_field_table(field_id) {
            // Calculate the local field index within that table
            // Count how many fields belong to tables before this one
            let mut local_field_idx = field_id as usize;
            for tid in 0..table_id {
                local_field_idx -= self.trie.table_configs[tid as usize].fields.len();
            }

            if let Some(table_builder) = self.table_builders.get_mut(table_id as usize) {
                table_builder.set_field_value(field_id, local_field_idx, value);
            }
        }
        Ok(())
    }

    fn start_table(&mut self, table_id: TableId) {
        let table_builder = &mut self.table_builders[table_id as usize];
        table_builder.row_index = 0;
        self.table_stack.push(TableContext::new(table_id));
    }

    fn end_row(&mut self) -> Result<()> {
        if let Some(ctx) = self.table_stack.last() {
            let table_id = ctx.table_id;
            let num_levels = self.trie.table_configs[table_id as usize].levels.len();

            // Reuse buffer for indices to avoid allocations
            self.indices_buffer.clear();

            // Only use the last N table contexts, where N is the number of levels
            let start_idx = self.table_stack.len().saturating_sub(num_levels);
            for table_ctx in &self.table_stack[start_idx..] {
                self.indices_buffer.push(table_ctx.row_index);
            }

            // End the current row
            if let Some(table_builder) = self.table_builders.get_mut(table_id as usize) {
                table_builder.end_row(&self.indices_buffer)?;
            }

            // Increment row index for next row
            if let Some(ctx) = self.table_stack.last_mut() {
                ctx.row_index += 1;
            }
        }
        Ok(())
    }

    fn end_table(&mut self) {
        self.table_stack.pop();
    }

    fn finish(mut self) -> Result<IndexMap<String, RecordBatch>> {
        let mut record_batches = IndexMap::new();
        for (idx, table_builder) in self.table_builders.iter_mut().enumerate() {
            // Create batch if there are fields (even if no rows - empty table)
            if !table_builder.field_builders.is_empty() {
                let record_batch = table_builder.finish()?;
                record_batches.insert(self.trie.table_configs[idx].name.clone(), record_batch);
            }
        }
        Ok(record_batches)
    }
}

/// Parse XML using trie-based path matching.
pub fn parse_xml(reader: impl BufRead, config: &Config) -> Result<IndexMap<String, RecordBatch>> {
    let mut reader = Reader::from_reader(reader);
    reader.config_mut().expand_empty_elements = true;
    if config.parser_options.trim_text {
        reader.config_mut().trim_text(true);
    }

    let mut converter = XmlToArrowConverter::from_config(config)?;
    let mut state_stack: Vec<StateId> = vec![converter.trie.root_id()];

    // Handle root table (xml_path: /) - start it immediately
    let root_state = converter.trie.root_id();
    if converter.trie.is_table_root(root_state) {
        if let Some(table_id) = converter.trie.get_table_id(root_state) {
            converter.start_table(table_id);
        }
    }

    if config.requires_attribute_parsing() {
        process_xml_events::<_, true>(&mut reader, &mut state_stack, &mut converter)?;
    } else {
        process_xml_events::<_, false>(&mut reader, &mut state_stack, &mut converter)?;
    }

    // Handle root table (xml_path: /) - end its row if it has field values
    if converter.trie.is_table_root(root_state) {
        if let Some(table_id) = converter.trie.get_table_id(root_state) {
            let table_config = &converter.trie.table_configs[table_id as usize];
            if table_config.levels.is_empty()
                && converter.table_builders[table_id as usize].has_any_field_value()
            {
                converter.end_row()?;
            }
            converter.end_table();
        }
    }

    converter.finish()
}

fn process_xml_events<B: BufRead, const PARSE_ATTRIBUTES: bool>(
    reader: &mut Reader<B>,
    state_stack: &mut Vec<StateId>,
    converter: &mut XmlToArrowConverter,
) -> Result<()> {
    let mut buf = Vec::with_capacity(4096);
    let mut attr_name_buffer = String::with_capacity(64);

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let node = std::str::from_utf8(e.local_name().into_inner())?;
                let atom = Atom::from(node);

                let current_state = *state_stack.last().unwrap();
                let next_state = converter.trie.transition_element(current_state, &atom);
                state_stack.push(next_state);

                if next_state != UNMATCHED_STATE {
                    // Check if this is a table root
                    if let Some(table_id) = converter.trie.get_table_id(next_state) {
                        converter.start_table(table_id);
                    }

                    // Handle attributes
                    if PARSE_ATTRIBUTES && converter.trie.has_attributes(next_state) {
                        parse_attributes(
                            e.attributes(),
                            next_state,
                            converter,
                            &mut attr_name_buffer,
                        )?;
                    }
                }
            }
            Event::GeneralRef(e) => {
                let current_state = *state_stack.last().unwrap();
                if current_state != UNMATCHED_STATE {
                    if let Some(field_id) = converter.trie.get_field_id(current_state) {
                        let text = e.into_inner();
                        let text = String::from_utf8_lossy(&text);
                        let text = escape::resolve_predefined_entity(&text).unwrap_or_default();
                        converter.set_field_value(field_id, &text)?;
                    }
                }
            }
            Event::Text(e) => {
                let current_state = *state_stack.last().unwrap();
                if current_state != UNMATCHED_STATE {
                    if let Some(field_id) = converter.trie.get_field_id(current_state) {
                        let text = e.into_inner();
                        let text = String::from_utf8_lossy(&text);
                        converter.set_field_value(field_id, &text)?;
                    }
                }
            }
            Event::End(_) => {
                let ending_state = state_stack.pop().unwrap();

                if ending_state != UNMATCHED_STATE {
                    // Check if we're ending a table
                    if converter.trie.is_table_root(ending_state) {
                        converter.end_table();
                    }
                }

                // After popping, check if parent is a table root
                // If so, we may need to end a row depending on the table's level configuration
                if let Some(&parent_state) = state_stack.last() {
                    if parent_state != UNMATCHED_STATE && converter.trie.is_table_root(parent_state)
                    {
                        // Get the table to check if it has levels
                        if let Some(table_id) = converter.trie.get_table_id(parent_state) {
                            let table_config = &converter.trie.table_configs[table_id as usize];

                            // End row if:
                            // - table has no levels AND at least one field has a value, OR
                            // - table has levels (any direct child of table root ends a row)
                            // Note: The levels array is only for naming index columns, not filtering
                            if table_config.levels.is_empty() {
                                if converter.table_builders[table_id as usize].has_any_field_value()
                                {
                                    converter.end_row()?;
                                }
                            } else {
                                // Table has levels - any child of table root ends a row
                                converter.end_row()?;
                            }
                        }
                    }
                }
            }
            Event::Eof => break,
            _ => (),
        }
        buf.clear();
    }
    Ok(())
}

#[inline]
fn parse_attributes(
    attributes: Attributes,
    current_state: StateId,
    converter: &mut XmlToArrowConverter,
    _attr_name_buffer: &mut String,
) -> Result<()> {
    for attribute in attributes {
        let attribute = attribute?;
        let key = std::str::from_utf8(attribute.key.local_name().into_inner())?;

        let attr_atom = Atom::from(key);
        let attr_state = converter
            .trie
            .transition_attribute(current_state, &attr_atom);

        if attr_state != UNMATCHED_STATE {
            if let Some(field_id) = converter.trie.get_field_id(attr_state) {
                let value = std::str::from_utf8(attribute.value.as_ref())?;
                converter.set_field_value(field_id, value)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, FieldConfigBuilder, TableConfig};
    use arrow::array::{Int32Array, StringArray};

    #[test]
    fn test_trie_parse_simple() -> Result<()> {
        let xml_content = r#"<data><item><value>123</value></item></data>"#;
        let config = Config {
            tables: vec![TableConfig::new(
                "items",
                "/data",
                vec![],
                vec![FieldConfigBuilder::new("value", "/data/item/value", DType::Int32).build()],
            )],
            parser_options: Default::default(),
        };

        let batches = parse_xml(xml_content.as_bytes(), &config)?;
        assert_eq!(batches.len(), 1);

        let batch = batches.get("items").unwrap();
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(array.value(0), 123);

        Ok(())
    }

    #[test]
    fn test_trie_parse_with_attributes() -> Result<()> {
        let xml_content = r#"<data><item id="test123"><value>456</value></item></data>"#;
        let config = Config {
            tables: vec![TableConfig::new(
                "items",
                "/data",
                vec![],
                vec![
                    FieldConfigBuilder::new("id", "/data/item/@id", DType::Utf8).build(),
                    FieldConfigBuilder::new("value", "/data/item/value", DType::Int32).build(),
                ],
            )],
            parser_options: Default::default(),
        };

        let batches = parse_xml(xml_content.as_bytes(), &config)?;
        assert_eq!(batches.len(), 1);

        let batch = batches.get("items").unwrap();

        let id_array = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_array.value(0), "test123");

        let value_array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(value_array.value(0), 456);

        Ok(())
    }

    #[test]
    fn test_trie_parse_multiple_items() -> Result<()> {
        let xml_content = r#"
            <data>
                <item><value>10</value></item>
                <item><value>20</value></item>
                <item><value>30</value></item>
            </data>
        "#;

        let config = Config {
            tables: vec![TableConfig::new(
                "items",
                "/data",
                vec![],
                vec![FieldConfigBuilder::new("value", "/data/item/value", DType::Int32).build()],
            )],
            parser_options: Default::default(),
        };

        let batches = parse_xml(xml_content.as_bytes(), &config)?;
        assert_eq!(batches.len(), 1);

        let batch = batches.get("items").unwrap();
        let value_array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(value_array.len(), 3);
        assert_eq!(value_array.value(0), 10);
        assert_eq!(value_array.value(1), 20);
        assert_eq!(value_array.value(2), 30);

        Ok(())
    }

    #[test]
    fn test_trie_parse_nested_with_levels() -> Result<()> {
        let xml_content = r#"
            <document>
                <header>
                    <comments>
                        <comment timestamp="t1" user="u1">text1</comment>
                        <comment timestamp="t2" user="u2">text2</comment>
                        <comment timestamp="t3" user="u3">text3</comment>
                    </comments>
                </header>
            </document>
        "#;

        let config = Config {
            tables: vec![
                TableConfig::new("/", "/", vec![], vec![]),
                TableConfig::new(
                    "comments",
                    "/document/header/comments",
                    vec!["comment".to_string()],
                    vec![
                        FieldConfigBuilder::new(
                            "timestamp",
                            "/document/header/comments/comment/@timestamp",
                            DType::Utf8,
                        )
                        .build(),
                        FieldConfigBuilder::new(
                            "user",
                            "/document/header/comments/comment/@user",
                            DType::Utf8,
                        )
                        .build(),
                        FieldConfigBuilder::new(
                            "text",
                            "/document/header/comments/comment",
                            DType::Utf8,
                        )
                        .build(),
                    ],
                ),
            ],
            parser_options: Default::default(),
        };

        let batches = parse_xml(xml_content.as_bytes(), &config)?;

        println!("Batches: {:?}", batches.keys());

        let comment_batch = batches.get("comments").unwrap();
        println!("Comment batch rows: {}", comment_batch.num_rows());
        println!("Comment batch columns: {}", comment_batch.num_columns());

        for i in 0..comment_batch.num_columns() {
            let col = comment_batch.column(i);
            println!("Column {} length: {}", i, col.len());
        }

        let timestamp_array = comment_batch
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(timestamp_array.len(), 3);
        assert_eq!(timestamp_array.value(0), "t1");
        assert_eq!(timestamp_array.value(1), "t2");
        assert_eq!(timestamp_array.value(2), "t3");

        Ok(())
    }

    #[test]
    fn test_trie_parse_with_nested_metadata() -> Result<()> {
        let xml_content = r#"
            <document>
                <data>
                    <sensors>
                        <sensor id="S001" type="temp">
                            <metadata>
                                <location>Zone-1</location>
                                <calibration>1.5</calibration>
                            </metadata>
                        </sensor>
                        <sensor id="S002" type="pressure">
                            <metadata>
                                <location>Zone-2</location>
                                <calibration>2.5</calibration>
                            </metadata>
                        </sensor>
                    </sensors>
                </data>
            </document>
        "#;

        let config = Config {
            tables: vec![
                TableConfig::new("root", "/document", vec![], vec![]),
                TableConfig::new(
                    "sensors",
                    "/document/data/sensors",
                    vec!["sensor".to_string()],
                    vec![
                        FieldConfigBuilder::new(
                            "sensor_id",
                            "/document/data/sensors/sensor/@id",
                            DType::Utf8,
                        )
                        .build(),
                        FieldConfigBuilder::new(
                            "sensor_type",
                            "/document/data/sensors/sensor/@type",
                            DType::Utf8,
                        )
                        .build(),
                        FieldConfigBuilder::new(
                            "location",
                            "/document/data/sensors/sensor/metadata/location",
                            DType::Utf8,
                        )
                        .build(),
                        FieldConfigBuilder::new(
                            "calibration",
                            "/document/data/sensors/sensor/metadata/calibration",
                            DType::Float64,
                        )
                        .build(),
                    ],
                ),
            ],
            parser_options: Default::default(),
        };

        let batches = parse_xml(xml_content.as_bytes(), &config)?;

        let sensor_batch = batches.get("sensors").unwrap();
        assert_eq!(sensor_batch.num_rows(), 2);

        let id_array = sensor_batch
            .column_by_name("sensor_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_array.value(0), "S001");
        assert_eq!(id_array.value(1), "S002");

        let location_array = sensor_batch
            .column_by_name("location")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(location_array.value(0), "Zone-1");
        assert_eq!(location_array.value(1), "Zone-2");

        Ok(())
    }
}
