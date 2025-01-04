use std::collections::VecDeque;
use std::io::BufRead;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, AsArray, BooleanBuilder, Float32Array, Float32Builder, Float64Array,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, RecordBatch,
    StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::compute::kernels::numeric;
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Schema};
use fxhash::FxBuildHasher;
use indexmap::IndexMap;
use quick_xml::events::attributes::Attributes;
use quick_xml::events::Event;
use quick_xml::Reader;

use crate::config::{DType, FieldConfig, TableConfig};
use crate::errors::Error;
use crate::errors::Result;
use crate::xml_path::XmlPath;
use crate::Config;

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
            current_value: String::with_capacity(32),
        })
    }

    fn set_current_value(&mut self, value: &str) {
        self.current_value.push_str(value);
        self.has_value = true;
    }

    /// Appends the currently accumulated value to the appropriate Arrow array builder,
    /// performing type conversion and handling nulls.
    fn append_current_value(&mut self) -> Result<()> {
        let value = &self.current_value;
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
            DataType::Int8 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<Int8Builder>()
                    .expect("Int8Builder");
                if self.has_value {
                    match value.parse::<i8>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as Int8: {}",
                                value, e
                            )));
                        }
                    }
                } else {
                    builder.append_null();
                }
            }
            DataType::UInt8 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<UInt8Builder>()
                    .expect("UInt8Builder");
                if self.has_value {
                    match value.parse::<u8>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as UInt8: {}",
                                value, e
                            )));
                        }
                    }
                } else {
                    builder.append_null();
                }
            }
            DataType::Int16 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<Int16Builder>()
                    .expect("Int16Builder");
                if self.has_value {
                    match value.parse::<i16>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as Int16: {}",
                                value, e
                            )));
                        }
                    }
                } else {
                    builder.append_null();
                }
            }
            DataType::UInt16 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<UInt16Builder>()
                    .expect("UInt16Builder");
                if self.has_value {
                    match value.parse::<u16>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as UInt16: {}",
                                value, e
                            )));
                        }
                    }
                } else {
                    builder.append_null();
                }
            }
            DataType::Int32 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .expect("Int32Builder");
                if self.has_value {
                    match value.parse::<i32>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as Int32: {}",
                                value, e
                            )));
                        }
                    }
                } else {
                    builder.append_null();
                }
            }
            DataType::UInt32 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<UInt32Builder>()
                    .expect("UInt32Builder");
                if self.has_value {
                    match value.parse::<u32>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as UInt32: {}",
                                value, e
                            )));
                        }
                    }
                } else {
                    builder.append_null();
                }
            }
            DataType::Int64 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<Int64Builder>()
                    .expect("Int64Builder");
                if self.has_value {
                    match value.parse::<i64>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as Int64: {}",
                                value, e
                            )));
                        }
                    }
                } else {
                    builder.append_null();
                }
            }
            DataType::UInt64 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<UInt64Builder>()
                    .expect("UInt64Builder");
                if self.has_value {
                    match value.parse::<u64>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as UInt64: {}",
                                value, e
                            )));
                        }
                    }
                } else {
                    builder.append_null();
                }
            }
            DataType::Float32 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<Float32Builder>()
                    .expect("Float32Builder");
                if self.has_value {
                    match value.parse::<f32>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as Float32: {}",
                                value, e
                            )));
                        }
                    }
                } else {
                    builder.append_null();
                }
            }
            DataType::Float64 => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .expect("Float64Builder");
                if self.has_value {
                    match value.parse::<f64>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as Float64: {}",
                                value, e
                            )));
                        }
                    }
                } else {
                    builder.append_null();
                }
            }
            DataType::Boolean => {
                let builder = self
                    .array_builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .expect("BooleanBuilder");
                if self.has_value {
                    match value.as_str() {
                        "false" | "0" => builder.append_value(false),
                        "true" | "1" => builder.append_value(true),
                        _ => {
                            return Err(Error::ParseError(format!(
                                "Failed to parse value '{}' as boolean, expected 'true', 'false', '1' or '0'",
                                value
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
                )))
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
                _ => unimplemented!(),
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
                _ => unimplemented!(),
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
        DType::Float32 => Ok(Box::new(Float32Builder::new())),
        DType::Float64 => Ok(Box::new(Float64Builder::new())),
        DType::Utf8 => Ok(Box::new(StringBuilder::default())),
    }
}

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
    /// Builders for each field in the table, mapped by their XML path.
    field_builders: IndexMap<XmlPath, FieldBuilder, FxBuildHasher>,
    /// The current row index for this table.
    row_index: usize,
}

impl TableBuilder {
    fn new(table_config: &TableConfig) -> Result<Self> {
        let mut index_builders = Vec::with_capacity(table_config.levels.len());
        index_builders.resize_with(table_config.levels.len(), UInt32Builder::default);
        let mut builder = Self {
            table_config: table_config.clone(),
            index_builders,
            field_builders: IndexMap::with_capacity_and_hasher(
                table_config.fields.len(),
                FxBuildHasher::default(),
            ),
            row_index: 0,
        };
        for field_config in &table_config.fields {
            builder.add_column(field_config)?;
        }
        Ok(builder)
    }

    fn end_row(&mut self, indices: &[u32]) -> Result<()> {
        // Append the current row's data to the arrays
        self.save_row(indices)?;
        for field_builder in self.field_builders.values_mut() {
            field_builder.has_value = false;
            field_builder.current_value.clear();
        }
        Ok(())
    }

    fn add_column(&mut self, field_config: &FieldConfig) -> Result<()> {
        self.field_builders.insert(
            XmlPath::new(&field_config.xml_path),
            FieldBuilder::new(field_config)?,
        );
        Ok(())
    }

    fn set_field_value(&mut self, field_path: &XmlPath, value: &str) {
        if let Some(field_builder) = self.field_builders.get_mut(field_path) {
            field_builder.set_current_value(value);
        }
    }

    fn save_row(&mut self, indices: &[u32]) -> Result<()> {
        for (index, index_builder) in indices.iter().zip(&mut self.index_builders) {
            index_builder.append_value(*index)
        }

        for field_builder in self.field_builders.values_mut() {
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
        for field_builder in self.field_builders.values_mut() {
            let array = field_builder.finish()?;
            arrays.push(array);
            fields.push(field_builder.field.clone())
        }
        let schema = Schema::new(fields);
        Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
    }
}

/// Converts parsed XML events into Arrow RecordBatches.
///
/// This struct maintains a stack of table builders to handle nested XML structures.
/// It uses the provided `Config` to determine which XML elements represent tables
/// and which elements represent fields.
struct XmlToArrowConverter {
    /// Table builders for each table defined in the configuration.
    table_builders: IndexMap<XmlPath, TableBuilder, FxBuildHasher>,
    /// Stack of XML paths representing the current nesting level of tables.
    builder_stack: VecDeque<XmlPath>,
}

impl XmlToArrowConverter {
    fn from_config(config: &Config) -> Result<Self> {
        let mut table_builders =
            IndexMap::with_capacity_and_hasher(config.tables.len(), FxBuildHasher::default());

        for table_config in &config.tables {
            let table_path = XmlPath::new(&table_config.xml_path);
            let table_builder = TableBuilder::new(table_config)?;
            table_builders.insert(table_path, table_builder);
        }
        let mut builder_stack = VecDeque::new();
        builder_stack.push_back(XmlPath::new("/"));

        Ok(Self {
            table_builders,
            builder_stack,
        })
    }

    fn is_table_path(&self, xml_path: &XmlPath) -> bool {
        self.table_builders.contains_key(xml_path)
    }

    fn current_table_builder_mut(&mut self) -> Result<&mut TableBuilder> {
        let table_path = self.builder_stack.back().ok_or(Error::NoTableOnStack)?;
        self.table_builders
            .get_mut(table_path)
            .ok_or_else(|| Error::TableNotFound(table_path.to_string()))
    }

    fn parent_row_indices(&self) -> Result<Vec<u32>> {
        let mut indices = Vec::with_capacity(self.builder_stack.len() - 1);
        for table_path in self.builder_stack.iter().skip(1) {
            let table_builder = self
                .table_builders
                .get(table_path)
                .ok_or_else(|| Error::TableNotFound(table_path.to_string()))?;
            indices.push(table_builder.row_index as u32);
        }
        Ok(indices)
    }

    fn start_table(&mut self, table_path: &XmlPath) -> Result<()> {
        self.builder_stack.push_back(table_path.clone());
        let table_builder = self
            .table_builders
            .get_mut(table_path)
            .ok_or_else(|| Error::TableNotFound(table_path.to_string()))?;
        table_builder.row_index = 0;
        Ok(())
    }

    fn end_table(&mut self) -> Result<()> {
        self.builder_stack.pop_back();
        Ok(())
    }

    fn finish(mut self) -> Result<IndexMap<String, arrow::record_batch::RecordBatch>> {
        let mut record_batches = IndexMap::new();
        for table_builder in &mut self.table_builders.values_mut() {
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
///       and values are the corresponding Arrow `RecordBatch` objects.
/// *   `Err(Error)`: An `Error` value if any error occurs during parsing, configuration, or Arrow table creation.
///
/// # Example
///
/// ```rust
/// use xml2arrow::{parse_xml, config::{Config, TableConfig, FieldConfig, DType}};
/// use std::fs::File;
/// use std::io::BufReader;
///
/// let xml_content = r#"<data><item><value>123</value></item></data>"#;
/// let config = Config {
///     tables: vec![TableConfig {
///         name: "items".to_string(),
///         xml_path: "/data".to_string(),
///         levels: vec![],
///         fields: vec![FieldConfig {
///             name: "value".to_string(),
///             xml_path: "/data/item/value".to_string(),
///             data_type: DType::Int32,
///             nullable: false,
///             scale: None,
///             offset: None,
///         }],
///     }],
/// };
/// let record_batches = parse_xml(xml_content.as_bytes(), &config).unwrap();
/// // ... use record_batches
/// ```
pub fn parse_xml(reader: impl BufRead, config: &Config) -> Result<IndexMap<String, RecordBatch>> {
    let mut reader = Reader::from_reader(reader);
    reader.config_mut().trim_text(true);
    let mut xml_path = XmlPath::new("/");
    let mut xml_to_arrow_converter = XmlToArrowConverter::from_config(config)?;

    // Use specialized parsing logic based on whether attribute parsing is required.
    // This avoids unnecessary attribute processing and Empty event handling
    // when attributes are not needed, improving performance.
    if config.requires_attribute_parsing() {
        process_xml_events::<_, true>(
            &mut reader,
            &mut xml_path,
            &mut xml_to_arrow_converter,
            PhantomData,
        )?;
    } else {
        process_xml_events::<_, false>(
            &mut reader,
            &mut xml_path,
            &mut xml_to_arrow_converter,
            PhantomData,
        )?;
    }

    let batches = xml_to_arrow_converter.finish()?;
    Ok(batches)
}

fn process_xml_events<B: BufRead, const PARSE_ATTRIBUTES: bool>(
    reader: &mut Reader<B>,
    xml_path: &mut XmlPath,
    xml_to_arrow_converter: &mut XmlToArrowConverter,
    _marker: PhantomData<bool>,
) -> Result<()> {
    let mut buf = Vec::with_capacity(256);
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let node = std::str::from_utf8(e.local_name().into_inner())?;
                xml_path.append_node(node);
                if xml_to_arrow_converter.is_table_path(xml_path) {
                    xml_to_arrow_converter.start_table(xml_path)?;
                }
                if PARSE_ATTRIBUTES {
                    parse_attributes(e.attributes(), xml_path, xml_to_arrow_converter)?;
                }
            }
            Event::Empty(e) => {
                if PARSE_ATTRIBUTES {
                    let node = std::str::from_utf8(e.local_name().into_inner())?;
                    xml_path.append_node(node);
                    parse_attributes(e.attributes(), xml_path, xml_to_arrow_converter)?;
                    xml_path.remove_node();
                    if xml_to_arrow_converter.is_table_path(xml_path) {
                        // This is the root element of the table
                        let indices = xml_to_arrow_converter.parent_row_indices()?;
                        xml_to_arrow_converter
                            .current_table_builder_mut()?
                            .end_row(&indices)?;
                    }
                }
            }
            Event::Text(e) => {
                xml_to_arrow_converter
                    .current_table_builder_mut()?
                    .set_field_value(xml_path, &e.unescape()?);
            }
            Event::End(_) => {
                if xml_to_arrow_converter.is_table_path(xml_path) {
                    xml_to_arrow_converter.end_table()?;
                }
                xml_path.remove_node();
                if xml_to_arrow_converter.is_table_path(xml_path) {
                    // This is the root element of the table
                    let indices = xml_to_arrow_converter.parent_row_indices()?;
                    xml_to_arrow_converter
                        .current_table_builder_mut()?
                        .end_row(&indices)?;
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
    xml_path: &mut XmlPath,
    xml_to_arrow_converter: &mut XmlToArrowConverter,
) -> Result<()> {
    for attribute in attributes {
        let attribute = attribute?;
        let key = std::str::from_utf8(attribute.key.local_name().into_inner())?;
        let node = "@".to_string() + key;
        let table_builder = xml_to_arrow_converter.current_table_builder_mut()?;
        xml_path.append_node(&node);
        table_builder.set_field_value(xml_path, std::str::from_utf8(attribute.value.as_ref())?);
        xml_path.remove_node();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DType, FieldConfig, TableConfig};
    use arrow::array::{
        BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    };

    fn approx_equal(a: f64, b: f64, abs: f64) -> bool {
        let abs_difference = (a - b).abs();
        abs_difference < abs
    }

    #[test]
    fn test_parse_xml_complex() -> Result<()> {
        let xml_content = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <data>
          <dataset>
            <table>
              <item>
                <id>1</id>
                <name>Laptop</name>
                <price>1200.50</price>
                <category>Electronics</category>
                <in_stock>true</in_stock>
                <count>4294967290</count>
                <big_count>18446744073709551610</big_count>
                <big_int>9223372036854775807</big_int>
                <properties>
                  <property>
                    <key>CPU</key>
                    <value>Intel i7</value>
                  </property>
                  <property>
                    <key>RAM</key>
                    <value>16GB</value>
                  </property>
                </properties>
              </item>
              <item>
                <id>2</id>
                <name>Book</name>
                <price>25.99</price>
                <category>Literature</category>
                <in_stock>false</in_stock>
                <count>12345</count>
                <big_count>67890</big_count>
                <big_int>-9223372036854775808</big_int>
              </item>
            </table>
            <other_items>
                <other_item>
                    <value>123</value>
                </other_item>
                <other_item>
                    <value>456</value>
                </other_item>
            </other_items>
          </dataset>
        </data>
        "#;

        let config = Config {
            tables: vec![
                TableConfig {
                    name: "items".to_string(),
                    xml_path: "/data/dataset/table".to_string(),
                    levels: vec!["table".to_string()],
                    fields: vec![
                        FieldConfig {
                            name: "id".to_string(),
                            xml_path: "/data/dataset/table/item/id".to_string(),
                            data_type: DType::UInt32,
                            nullable: false,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "name".to_string(),
                            xml_path: "/data/dataset/table/item/name".to_string(),
                            data_type: DType::Utf8,
                            nullable: false,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "price".to_string(),
                            xml_path: "/data/dataset/table/item/price".to_string(),
                            data_type: DType::Float64,
                            nullable: false,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "category".to_string(),
                            xml_path: "/data/dataset/table/item/category".to_string(),
                            data_type: DType::Utf8,
                            nullable: true,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "in_stock".to_string(),
                            xml_path: "/data/dataset/table/item/in_stock".to_string(),
                            data_type: DType::Boolean,
                            nullable: true,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "count".to_string(),
                            xml_path: "/data/dataset/table/item/count".to_string(),
                            data_type: DType::UInt32,
                            nullable: true,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "big_count".to_string(),
                            xml_path: "/data/dataset/table/item/big_count".to_string(),
                            data_type: DType::UInt64,
                            nullable: true,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "big_int".to_string(),
                            xml_path: "/data/dataset/table/item/big_int".to_string(),
                            data_type: DType::Int64,
                            nullable: true,
                            scale: None,
                            offset: None,
                        },
                    ],
                },
                TableConfig {
                    name: "properties".to_string(),
                    xml_path: "/data/dataset/table/item/properties".to_string(),
                    levels: vec!["table".to_string(), "properties".to_string()],
                    fields: vec![
                        FieldConfig {
                            name: "key".to_string(),
                            xml_path: "/data/dataset/table/item/properties/property/key"
                                .to_string(),
                            data_type: DType::Utf8,
                            nullable: true,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "value".to_string(),
                            xml_path: "/data/dataset/table/item/properties/property/value"
                                .to_string(),
                            data_type: DType::Utf8,
                            nullable: true,
                            scale: None,
                            offset: None,
                        },
                    ],
                },
                TableConfig {
                    name: "other_items".to_string(),
                    xml_path: "/data/dataset/other_items".to_string(),
                    levels: vec!["table".to_string()],
                    fields: vec![FieldConfig {
                        name: "value".to_string(),
                        xml_path: "/data/dataset/other_items/other_item/value".to_string(),
                        data_type: DType::Int16,
                        nullable: false,
                        scale: None,
                        offset: None,
                    }],
                },
            ],
        };

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

        // Assertions for "items" table
        let items_batch = record_batches.get("items").unwrap();
        assert_eq!(items_batch.num_rows(), 2);

        let id_array = items_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        let name_array = items_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Laptop");
        assert_eq!(name_array.value(1), "Book");

        let price_array = items_batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(approx_equal(price_array.value(0), 1200.50, 1e-10));
        assert!(approx_equal(price_array.value(1), 25.99, 1e-10));

        let in_stock_array = items_batch
            .column_by_name("in_stock")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(in_stock_array.value(0));
        assert!(!in_stock_array.value(1));

        let count_array = items_batch
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(count_array.value(0), 4294967290);
        assert_eq!(count_array.value(1), 12345);

        let big_count_array = items_batch
            .column_by_name("big_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(big_count_array.value(0), 18446744073709551610);
        assert_eq!(big_count_array.value(1), 67890);

        let big_int_array = items_batch
            .column_by_name("big_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(big_int_array.value(0), 9223372036854775807);
        assert_eq!(big_int_array.value(1), -9223372036854775808);

        // Assertions for "properties" table
        let properties_batch = record_batches.get("properties").unwrap();
        assert_eq!(properties_batch.num_rows(), 2);

        let key_array = properties_batch
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key_array.value(0), "CPU");
        assert_eq!(key_array.value(1), "RAM");

        let value_array = properties_batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(value_array.value(0), "Intel i7");
        assert_eq!(value_array.value(1), "16GB");

        // Assertions for "other_items" table
        let other_items_batch = record_batches.get("other_items").unwrap();
        assert_eq!(other_items_batch.num_rows(), 2);

        let other_value_array = other_items_batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(other_value_array.value(0), 123);
        assert_eq!(other_value_array.value(1), 456);

        Ok(())
    }

    #[test]
    fn test_parse_xml() -> Result<()> {
        let xml_content = r#"
            <data>
              <product_list>
                <product>
                  <id>1</id>
                  <name>Laptop</name>
                  <price>100</price>
                  <items>
                    <item>Item1</item>
                    <item>Item2</item>
                    <item>Item4</item>
                  </items>
                </product>
                  <product>
                  <id>2</id>
                  <name>Mouse</name>
                  <items>
                    <item>Item5</item>
                  </items>
                </product>
                <product>
                  <id>3</id>
                  <price>3140.3</price>
                </product>
              </product_list>
              <producer_list>
                <producer>
                  <name>Producer1</name>
                </producer>
              </producer_list>
            </data>
        "#;

        let config = Config {
            tables: vec![
                TableConfig {
                    name: "/".to_string(),
                    xml_path: "/".to_string(),
                    levels: vec![],
                    fields: vec![],
                },
                TableConfig {
                    name: "products".to_string(),
                    xml_path: "/data/product_list".to_string(),
                    levels: vec!["product".to_string()],
                    fields: vec![
                        FieldConfig {
                            name: "id".to_string(),
                            xml_path: "/data/product_list/product/id".to_string(),
                            data_type: DType::Int16,
                            nullable: true,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "price".to_string(),
                            xml_path: "/data/product_list/product/price".to_string(),
                            data_type: DType::Float64,
                            nullable: true,
                            scale: Some(1e-2),
                            offset: Some(0.1),
                        },
                        FieldConfig {
                            name: "name".to_string(),
                            xml_path: "/data/product_list/product/name".to_string(),
                            data_type: DType::Utf8,
                            nullable: true,
                            scale: None,
                            offset: None,
                        },
                    ],
                },
                TableConfig {
                    name: "items".to_string(),
                    xml_path: "/data/product_list/product/items".to_string(),
                    levels: vec!["product".to_string(), "item".to_string()],
                    fields: vec![FieldConfig {
                        name: "item".to_string(),
                        xml_path: "/data/product_list/product/items/item".to_string(),
                        data_type: DType::Utf8,
                        nullable: true,
                        scale: None,
                        offset: None,
                    }],
                },
            ],
        };

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

        // Assertions for "products" table
        let products_batch = record_batches.get("products").unwrap();
        assert_eq!(products_batch.num_rows(), 3);

        let id_array = products_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);

        let name_array = products_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Laptop");
        assert_eq!(name_array.value(1), "Mouse");
        assert!(name_array.is_null(2)); // Check for null value

        let price_array = products_batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(price_array.value(0), 1.1);
        assert!(price_array.is_null(1));
        assert!(approx_equal(price_array.value(2), 31.503, 1e-12));

        // Assertions for "items" table
        let items_batch = record_batches.get("items").unwrap();
        assert_eq!(items_batch.num_rows(), 4);

        let product_index_array = items_batch
            .column_by_name("<product>")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(product_index_array.value(0), 0);
        assert_eq!(product_index_array.value(1), 0);
        assert_eq!(product_index_array.value(2), 0);
        assert_eq!(product_index_array.value(3), 1);

        let item_array = items_batch
            .column_by_name("item")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(item_array.value(0), "Item1");
        assert_eq!(item_array.value(1), "Item2");
        assert_eq!(item_array.value(2), "Item4");
        assert_eq!(item_array.value(3), "Item5");

        Ok(())
    }

    #[test]
    fn test_parse_xml_different_data_types() -> Result<()> {
        let xml_content = r#"
            <data>
              <item>
                <float32>3.17</float32>
                <float64>0.123456789</float64>
                <bool>true</bool>
                <uint8>252</uint8>
                <int8>-124</int8>
                <uint16>62535</uint16>
                <int16>-23452</int16>
                <uint32>4294967290</uint32>
                <int32>-55769</int32>
                <uint64>18446744073709551610</uint64>
                <int64>9223372036854775807</int64>
                <utf8>Héllo你😊</utf8>
              </item>
            </data>"#;
        let config = Config {
            tables: vec![TableConfig {
                name: "items".to_string(),
                xml_path: "/data".to_string(),
                levels: vec![],
                fields: vec![
                    FieldConfig {
                        name: "float32".to_string(),
                        xml_path: "/data/item/float32".to_string(),
                        data_type: DType::Float32,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "float64".to_string(),
                        xml_path: "/data/item/float64".to_string(),
                        data_type: DType::Float64,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "bool".to_string(),
                        xml_path: "/data/item/bool".to_string(),
                        data_type: DType::Boolean,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "uint8".to_string(),
                        xml_path: "/data/item/uint8".to_string(),
                        data_type: DType::UInt8,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "int8".to_string(),
                        xml_path: "/data/item/int8".to_string(),
                        data_type: DType::Int8,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "uint16".to_string(),
                        xml_path: "/data/item/uint16".to_string(),
                        data_type: DType::UInt16,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "int16".to_string(),
                        xml_path: "/data/item/int16".to_string(),
                        data_type: DType::Int16,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "uint32".to_string(),
                        xml_path: "/data/item/uint32".to_string(),
                        data_type: DType::UInt32,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "int32".to_string(),
                        xml_path: "/data/item/int32".to_string(),
                        data_type: DType::Int32,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "uint64".to_string(),
                        xml_path: "/data/item/uint64".to_string(),
                        data_type: DType::UInt64,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "int64".to_string(),
                        xml_path: "/data/item/int64".to_string(),
                        data_type: DType::Int64,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "utf8".to_string(),
                        xml_path: "/data/item/utf8".to_string(),
                        data_type: DType::Utf8,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                ],
            }],
        };
        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();

        let float32_array = items_batch
            .column_by_name("float32")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(float32_array.value(0), 3.17);

        let float64_array = items_batch
            .column_by_name("float64")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(float64_array.value(0), 0.123456789);

        let bool_array = items_batch
            .column_by_name("bool")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(bool_array.value(0));

        let uint8_array = items_batch
            .column_by_name("uint8")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(uint8_array.value(0), 252);

        let int8_array = items_batch
            .column_by_name("int8")
            .unwrap()
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert_eq!(int8_array.value(0), -124);

        let uint16_array = items_batch
            .column_by_name("uint16")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(uint16_array.value(0), 62535);

        let int16_array = items_batch
            .column_by_name("int16")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(int16_array.value(0), -23452);

        let uint32_array = items_batch
            .column_by_name("uint32")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(uint32_array.value(0), 4294967290);

        let int32_array = items_batch
            .column_by_name("int32")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int32_array.value(0), -55769);

        let uint64_array = items_batch
            .column_by_name("uint64")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(uint64_array.value(0), 18446744073709551610);

        let int64_array = items_batch
            .column_by_name("int64")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(int64_array.value(0), 9223372036854775807);

        let utf8_array = items_batch
            .column_by_name("utf8")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(utf8_array.value(0), "Héllo你😊");

        Ok(())
    }

    #[test]
    fn test_parse_xml_with_special_characters() -> Result<()> {
        let xml_content = r#"<data><item><text>&lt; &gt; &amp; &quot; &apos;</text></item></data>"#;
        let config = Config {
            tables: vec![TableConfig {
                name: "items".to_string(),
                xml_path: "/data".to_string(),
                levels: vec![],
                fields: vec![FieldConfig {
                    name: "text".to_string(),
                    xml_path: "/data/item/text".to_string(),
                    data_type: DType::Utf8,
                    nullable: true,
                    scale: None,
                    offset: None,
                }],
            }],
        };
        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();
        let text_array = items_batch
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text_array.value(0), "< > & \" '");
        Ok(())
    }

    #[test]
    fn test_parse_xml_empty() -> Result<()> {
        let xml_content = "";
        let config = Config { tables: vec![] };
        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        assert!(record_batches.is_empty());
        Ok(())
    }

    #[test]
    fn test_parse_xml_with_scale_and_offset() -> Result<()> {
        let xml_content =
            r#"<data><item><value>123.45</value></item><item><value>67.89</value></item></data>"#;
        let config = Config {
            tables: vec![TableConfig {
                name: "items".to_string(),
                xml_path: "/data".to_string(),
                levels: vec![],
                fields: vec![FieldConfig {
                    name: "value".to_string(),
                    xml_path: "/data/item/value".to_string(),
                    data_type: DType::Float64, // Or DType::Float32
                    nullable: true,
                    scale: Some(0.01),  // Example scale: multiply by 0.01
                    offset: Some(10.0), // Example offset: add 10.0
                }],
            }],
        };

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();
        let value_array = items_batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>() // Or Float32Array if using DType::Float32
            .unwrap();

        // Expected values: (raw_value * scale) + offset
        assert!(approx_equal(
            value_array.value(0),
            (123.45 * 0.01) + 10.0,
            1e-10
        )); // 11.2345
        assert!(approx_equal(
            value_array.value(1),
            (67.89 * 0.01) + 10.0,
            1e-10
        )); // 10.6789

        Ok(())
    }

    #[test]
    fn test_parse_xml_with_scale_and_offset_float32() -> Result<()> {
        let xml_content =
            r#"<data><item><value>123.45</value></item><item><value>67.89</value></item></data>"#;
        let config = Config {
            tables: vec![TableConfig {
                name: "items".to_string(),
                xml_path: "/data".to_string(),
                levels: vec![],
                fields: vec![FieldConfig {
                    name: "value".to_string(),
                    xml_path: "/data/item/value".to_string(),
                    data_type: DType::Float32,
                    nullable: true,
                    scale: Some(0.01),  // Example scale: multiply by 0.01
                    offset: Some(10.0), // Example offset: add 10.0
                }],
            }],
        };

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();
        let value_array = items_batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        // Expected values: (raw_value * scale) + offset
        assert!(approx_equal(
            value_array.value(0) as f64,
            (123.45 * 0.01) + 10.0,
            1e-6
        )); // 11.2345
        assert!(approx_equal(
            value_array.value(1) as f64,
            (67.89 * 0.01) + 10.0,
            1e-6
        )); // 10.6789

        Ok(())
    }

    #[test]
    fn test_parse_xml_with_attributes() -> Result<()> {
        let xml_data = r#"
            <data>
                <items>
                    <item id="1" value="10" type="A" valid="true">
                        <name>Item One</name>
                    </item>
                    <item id="2" value="20" valid="false">
                        <name>Item Two</name>
                    </item>
                </items>
            </data>
        "#;

        let config = Config {
            tables: vec![TableConfig {
                name: "items".to_string(),
                xml_path: "/data/items".to_string(),
                levels: vec![],
                fields: vec![
                    FieldConfig {
                        name: "id".to_string(),
                        xml_path: "/data/items/item/@id".to_string(),
                        data_type: DType::Utf8,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "value".to_string(),
                        xml_path: "/data/items/item/@value".to_string(),
                        data_type: DType::Int32,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "type".to_string(),
                        xml_path: "/data/items/item/@type".to_string(),
                        data_type: DType::Utf8,
                        nullable: true,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "valid".to_string(),
                        xml_path: "/data/items/item/@valid".to_string(),
                        data_type: DType::Boolean,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "name".to_string(),
                        xml_path: "/data/items/item/name".to_string(),
                        data_type: DType::Utf8,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                ],
            }],
        };

        let record_batches = parse_xml(xml_data.as_bytes(), &config)?;

        assert!(record_batches.contains_key("items"));
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 2);

        let id_array = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_array.value(0), "1");
        assert_eq!(id_array.value(1), "2");

        let value_array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(value_array.value(0), 10);
        assert_eq!(value_array.value(1), 20);

        let type_array = batch
            .column_by_name("type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(type_array.value(0), "A");
        assert!(type_array.is_null(1));

        let valid_array = batch
            .column_by_name("valid")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(valid_array.value(0));
        assert!(!valid_array.value(1));

        let name_array = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Item One");
        assert_eq!(name_array.value(1), "Item Two");

        Ok(())
    }

    #[test]
    fn test_nested_row_index() -> Result<()> {
        let xml_content = r#"
            <data>
                <dataset>
                    <table>
                        <group>
                            <item id="1"></item>
                            <item id="2"></item>
                        </group>
                        <group>
                            <item id="3"></item>
                        </group>
                    </table>
                </dataset>
            </data>
        "#;

        let config = Config {
            tables: vec![
                TableConfig {
                    name: "groups".to_string(),
                    xml_path: "/data/dataset/table".to_string(),
                    levels: vec!["table".to_string()],
                    fields: vec![],
                },
                TableConfig {
                    name: "items".to_string(),
                    xml_path: "/data/dataset/table/group".to_string(),
                    levels: vec!["table".to_string(), "group".to_string()], // Two levels
                    fields: vec![FieldConfig {
                        name: "id".to_string(),
                        xml_path: "/data/dataset/table/group/item/@id".to_string(),
                        data_type: DType::UInt32,
                        nullable: false,
                        scale: None,
                        offset: None,
                    }],
                },
            ],
        };

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();

        assert_eq!(items_batch.num_rows(), 3);

        let table_index_array = items_batch
            .column_by_name("<table>")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(table_index_array.value(0), 0);
        assert_eq!(table_index_array.value(1), 0);
        assert_eq!(table_index_array.value(2), 1);

        let group_index_array = items_batch
            .column_by_name("<group>")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(group_index_array.value(0), 0);
        assert_eq!(group_index_array.value(1), 1);
        assert_eq!(group_index_array.value(2), 0);

        let id_array = items_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);

        Ok(())
    }

    #[test]
    fn test_empty_tags() -> Result<()> {
        let xml_content = r#"
            <data>
                <dataset>
                    <table>
                        <item id="1" name="Item 1" />
                        <item id="2" />
                        <item id="3" name="Item 3" />
                    </table>
                </dataset>
            </data>
        "#;

        let config = Config {
            tables: vec![TableConfig {
                name: "items".to_string(),
                xml_path: "/data/dataset/table".to_string(),
                levels: vec!["table".to_string()],
                fields: vec![
                    FieldConfig {
                        name: "id".to_string(),
                        xml_path: "/data/dataset/table/item/@id".to_string(),
                        data_type: DType::UInt8,
                        nullable: false,
                        scale: None,
                        offset: None,
                    },
                    FieldConfig {
                        name: "name".to_string(),
                        xml_path: "/data/dataset/table/item/@name".to_string(),
                        data_type: DType::Utf8,
                        nullable: true, // Name is nullable because of the empty tag
                        scale: None,
                        offset: None,
                    },
                ],
            }],
        };

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();

        assert_eq!(items_batch.num_rows(), 3);

        let table_index_array = items_batch
            .column_by_name("<table>")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(table_index_array.value(0), 0);
        assert_eq!(table_index_array.value(1), 1);
        assert_eq!(table_index_array.value(2), 2);

        let id_array = items_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);

        let name_array = items_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Item 1");
        assert!(name_array.is_null(1)); // Check for null value
        assert_eq!(name_array.value(2), "Item 3");

        Ok(())
    }

    #[test]
    fn test_boolean_parsing() -> Result<()> {
        let test_cases = [
            ("true", Some(true)),
            ("false", Some(false)),
            ("1", Some(true)),
            ("0", Some(false)),
        ];

        for (input, expected) in test_cases {
            // Test with nullable fields
            let config = Config {
                tables: vec![TableConfig {
                    name: "test_table".to_string(),
                    xml_path: "/root".to_string(),
                    levels: vec![],
                    fields: vec![FieldConfig {
                        name: "bool_field".to_string(),
                        xml_path: "/root/value".to_string(),
                        data_type: DType::Boolean,
                        nullable: true,
                        scale: None,
                        offset: None,
                    }],
                }],
            };
            let xml_content = format!("<root><value>{}</value></root>", input);
            let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
            let record_batch = record_batches.get("test_table").unwrap();

            let expected_array = match expected {
                Some(value) => Arc::new(BooleanArray::from(vec![Some(value)])) as Arc<dyn Array>,
                None => Arc::new(BooleanArray::from(vec![None])) as Arc<dyn Array>,
            };
            let schema = Schema::new(vec![Field::new("bool_field", DataType::Boolean, true)]);
            let expected_record_batch =
                RecordBatch::try_new(Arc::new(schema), vec![expected_array])?;
            assert_eq!(
                record_batch, &expected_record_batch,
                "Input: '{}' (nullable)",
                input
            );

            // Test with non-nullable fields
            let config = Config {
                tables: vec![TableConfig {
                    name: "test_table".to_string(),
                    xml_path: "/root".to_string(),
                    levels: vec![],
                    fields: vec![FieldConfig {
                        name: "bool_field".to_string(),
                        xml_path: "/root/value".to_string(),
                        data_type: DType::Boolean,
                        nullable: false,
                        scale: None,
                        offset: None,
                    }],
                }],
            };
            let xml_content = format!("<root><value>{}</value></root>", input);
            let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
            let record_batch = record_batches.get("test_table").unwrap();

            let expected_array = match expected {
                Some(value) => Arc::new(BooleanArray::from(vec![Some(value)])) as Arc<dyn Array>,
                None => Arc::new(BooleanArray::from(vec![None])) as Arc<dyn Array>,
            };
            let schema = Schema::new(vec![Field::new("bool_field", DataType::Boolean, false)]);
            let expected_record_batch =
                RecordBatch::try_new(Arc::new(schema), vec![expected_array])?;
            assert_eq!(
                record_batch, &expected_record_batch,
                "Input: '{}' (non-nullable)",
                input
            );
        }

        Ok(())
    }

    #[test]
    fn test_boolean_parsing_invalid_input() -> Result<()> {
        let test_cases = [
            ("TRUE"),  // Case-sensitive - should be error
            ("FALSE"), // Case-sensitive - should be error
            ("2"),     // Invalid - should be error
            ("-1"),    // Invalid - should be error
            ("abc"),   // Invalid - should be error
        ];

        for input in test_cases {
            // Test with nullable fields
            let config = Config {
                tables: vec![TableConfig {
                    name: "test_table".to_string(),
                    xml_path: "/root".to_string(),
                    levels: vec![],
                    fields: vec![FieldConfig {
                        name: "bool_field".to_string(),
                        xml_path: "/root/value".to_string(),
                        data_type: DType::Boolean,
                        nullable: true,
                        scale: None,
                        offset: None,
                    }],
                }],
            };
            let xml_content = format!("<root><value>{}</value></root>", input);
            let result = parse_xml(xml_content.as_bytes(), &config);
            assert!(
                result.is_err(),
                "Input '{}' (nullable) should have resulted in an error",
                input
            );

            // Test with non-nullable fields
            let config = Config {
                tables: vec![TableConfig {
                    name: "test_table".to_string(),
                    xml_path: "/root".to_string(),
                    levels: vec![],
                    fields: vec![FieldConfig {
                        name: "bool_field".to_string(),
                        xml_path: "/root/value".to_string(),
                        data_type: DType::Boolean,
                        nullable: false,
                        scale: None,
                        offset: None,
                    }],
                }],
            };
            let xml_content = format!("<root><value>{}</value></root>", input);
            let result = parse_xml(xml_content.as_bytes(), &config);
            assert!(
                result.is_err(),
                "Input '{}' (non-nullable) should have resulted in an error",
                input
            );
        }

        Ok(())
    }

    #[test]
    fn test_boolean_parsing_no_value() -> Result<()> {
        let config_nullable = Config {
            tables: vec![TableConfig {
                name: "test_table".to_string(),
                xml_path: "/root".to_string(),
                levels: vec![],
                fields: vec![FieldConfig {
                    name: "bool_field".to_string(),
                    xml_path: "/root/value".to_string(),
                    data_type: DType::Boolean,
                    nullable: true,
                    scale: None,
                    offset: None,
                }],
            }],
        };
        let config_not_nullable = Config {
            tables: vec![TableConfig {
                name: "test_table".to_string(),
                xml_path: "/root".to_string(),
                levels: vec![],
                fields: vec![FieldConfig {
                    name: "bool_field".to_string(),
                    xml_path: "/root/value".to_string(),
                    data_type: DType::Boolean,
                    nullable: false,
                    scale: None,
                    offset: None,
                }],
            }],
        };
        let xml_content_empty = "<root><value></value></root>";

        // Empty value, nullable: should be null
        let record_batches = parse_xml(xml_content_empty.as_bytes(), &config_nullable)?;
        let record_batch = record_batches.get("test_table").unwrap();
        let expected_array = Arc::new(BooleanArray::from(vec![None])) as Arc<dyn Array>;
        let schema = Schema::new(vec![Field::new("bool_field", DataType::Boolean, true)]);
        let expected_record_batch = RecordBatch::try_new(Arc::new(schema), vec![expected_array])?;
        assert_eq!(
            record_batch, &expected_record_batch,
            "Empty value, nullable"
        );

        // Empty value, non-nullable: should be error
        let result = parse_xml(xml_content_empty.as_bytes(), &config_not_nullable);
        assert!(result.is_err(), "Empty value, non-nullable should error");

        Ok(())
    }
}
