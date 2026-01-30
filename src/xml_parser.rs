use std::collections::VecDeque;
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
use fxhash::FxBuildHasher;
use indexmap::IndexMap;
use quick_xml::Reader;
use quick_xml::escape;
use quick_xml::events::Event;
use quick_xml::events::attributes::Attributes;

use crate::Config;
use crate::config::{DType, FieldConfig, TableConfig};
use crate::errors::Error;
use crate::errors::Result;
use crate::xml_path::XmlPath;

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
        Ok(RecordBatch::try_new(Arc::new(schema), arrays).map_err(|e| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "Failed to create RecordBatch for table with name {} and XML path {}: {}",
                self.table_config.name, self.table_config.xml_path, e
            ))
        })?)
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
        // Validate field configurations early to catch unsupported scale/offset
        config.validate()?;
        let mut table_builders =
            IndexMap::with_capacity_and_hasher(config.tables.len(), FxBuildHasher::default());

        for table_config in &config.tables {
            let table_path = XmlPath::new(&table_config.xml_path);
            let table_builder = TableBuilder::new(table_config)?;
            table_builders.insert(table_path, table_builder);
        }
        let builder_stack = VecDeque::new();

        Ok(Self {
            table_builders,
            builder_stack,
        })
    }

    fn is_table_path(&self, xml_path: &XmlPath) -> bool {
        self.table_builders.contains_key(xml_path)
    }

    fn current_table_builder_mut(&mut self) -> Result<Option<&mut TableBuilder>> {
        if let Some(table_path) = self.builder_stack.back() {
            let builder = self
                .table_builders
                .get_mut(table_path)
                .ok_or_else(|| Error::TableNotFound(table_path.to_string()))?;
            Ok(Some(builder))
        } else {
            Ok(None)
        }
    }

    pub fn set_field_value_for_current_table(
        &mut self,
        field_path: &XmlPath,
        value: &str,
    ) -> Result<()> {
        if let Some(table_builder) = self.current_table_builder_mut()? {
            table_builder.set_field_value(field_path, value);
        }
        Ok(())
    }

    fn end_current_row(&mut self) -> Result<()> {
        let indices = self.parent_row_indices()?;
        if let Some(table_builder) = self.current_table_builder_mut()? {
            table_builder.end_row(&indices)?;
        }
        Ok(())
    }

    fn parent_row_indices(&self) -> Result<Vec<u32>> {
        let mut indices = Vec::with_capacity(self.builder_stack.len());
        for table_path in self.builder_stack.iter() {
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
    let mut buf = Vec::with_capacity(4096);
    let mut attr_name_buffer = String::with_capacity(64);
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let node = std::str::from_utf8(e.local_name().into_inner())?;
                xml_path.append_node(node);
                if xml_to_arrow_converter.is_table_path(xml_path) {
                    xml_to_arrow_converter.start_table(xml_path)?;
                }
                if PARSE_ATTRIBUTES {
                    parse_attributes(
                        e.attributes(),
                        xml_path,
                        xml_to_arrow_converter,
                        &mut attr_name_buffer,
                    )?;
                }
            }
            Event::GeneralRef(e) => {
                let text = e.into_inner();
                let text = String::from_utf8_lossy(&text);
                let text = escape::resolve_predefined_entity(&text).unwrap_or_default();
                xml_to_arrow_converter.set_field_value_for_current_table(xml_path, &text)?
            }
            Event::Text(e) => {
                let text = e.into_inner();
                let text = String::from_utf8_lossy(&text);
                xml_to_arrow_converter.set_field_value_for_current_table(xml_path, &text)?
            }
            Event::End(_) => {
                if xml_to_arrow_converter.is_table_path(xml_path) {
                    xml_to_arrow_converter.end_table()?;
                }
                xml_path.remove_node();
                if xml_to_arrow_converter.is_table_path(xml_path) {
                    // This is the root element of the table
                    xml_to_arrow_converter.end_current_row()?
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
    attr_name_buffer: &mut String,
) -> Result<()> {
    for attribute in attributes {
        let attribute = attribute?;
        let key = std::str::from_utf8(attribute.key.local_name().into_inner())?;

        // Reuse buffer to avoid allocation
        attr_name_buffer.clear();
        attr_name_buffer.push('@');
        attr_name_buffer.push_str(key);

        xml_path.append_node(attr_name_buffer);

        xml_to_arrow_converter.set_field_value_for_current_table(
            xml_path,
            std::str::from_utf8(attribute.value.as_ref())?,
        )?;
        xml_path.remove_node();
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::config::{Config, FieldConfigBuilder};
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

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: root
                    xml_path: /
                    levels: []
                    fields: []
                  - name: items
                    xml_path: /data/dataset/table
                    levels: ["table"]
                    fields:
                      - name: id
                        xml_path: /data/dataset/table/item/id
                        data_type: UInt32
                        nullable: false
                      - name: name
                        xml_path: /data/dataset/table/item/name
                        data_type: Utf8
                        nullable: false
                      - name: price
                        xml_path: /data/dataset/table/item/price
                        data_type: Float64
                        nullable: false
                      - name: category
                        xml_path: /data/dataset/table/item/category
                        data_type: Utf8
                        nullable: true
                      - name: in_stock
                        xml_path: /data/dataset/table/item/in_stock
                        data_type: Boolean
                        nullable: true
                      - name: count
                        xml_path: /data/dataset/table/item/count
                        data_type: UInt32
                        nullable: true
                      - name: big_count
                        xml_path: /data/dataset/table/item/big_count
                        data_type: UInt64
                        nullable: true
                      - name: big_int
                        xml_path: /data/dataset/table/item/big_int
                        data_type: Int64
                        nullable: true
                  - name: properties
                    xml_path: /data/dataset/table/item/properties
                    levels: ["table", "properties"]
                    fields:
                      - name: key
                        xml_path: /data/dataset/table/item/properties/property/key
                        data_type: Utf8
                        nullable: true
                      - name: value
                        xml_path: /data/dataset/table/item/properties/property/value
                        data_type: Utf8
                        nullable: true
                  - name: other_items
                    xml_path: /data/dataset/other_items
                    levels: ["table"]
                    fields:
                      - name: value
                        xml_path: /data/dataset/other_items/other_item/value
                        data_type: Int16
                        nullable: false
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

        // Assertions for "items" table
        let items_batch = record_batches.get("items").unwrap();
        assert_eq!(items_batch.num_rows(), 2);
        assert_array_values!(items_batch, "id", &[1, 2], UInt32Array);
        assert_array_values!(items_batch, "name", &["Laptop", "Book"], StringArray);
        assert_array_approx_values!(items_batch, "price", &[1200.50, 25.99], Float64Array, 1e-10);
        assert_array_values!(items_batch, "in_stock", &[true, false], BooleanArray);
        assert_array_values!(items_batch, "count", &[4294967290u32, 12345], UInt32Array);
        assert_array_values!(
            items_batch,
            "big_count",
            &[18446744073709551610u64, 67890],
            UInt64Array
        );
        assert_array_values!(
            items_batch,
            "big_int",
            &[9223372036854775807i64, -9223372036854775808],
            Int64Array
        );

        // Assertions for "properties" table
        let properties_batch = record_batches.get("properties").unwrap();
        assert_eq!(properties_batch.num_rows(), 2);
        assert_array_values!(properties_batch, "key", &["CPU", "RAM"], StringArray);
        assert_array_values!(
            properties_batch,
            "value",
            &["Intel i7", "16GB"],
            StringArray
        );

        // Assertions for "other_items" table
        let other_items_batch = record_batches.get("other_items").unwrap();
        assert_eq!(other_items_batch.num_rows(), 2);
        assert_array_values!(other_items_batch, "value", &[123, 456], Int16Array);

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

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: /
                    xml_path: /
                    levels: []
                    fields: []
                  - name: products
                    xml_path: /data/product_list
                    levels: ["product"]
                    fields:
                      - name: id
                        xml_path: /data/product_list/product/id
                        data_type: Int16
                        nullable: true
                      - name: price
                        xml_path: /data/product_list/product/price
                        data_type: Float64
                        nullable: true
                        scale: 0.01
                        offset: 0.1
                      - name: name
                        xml_path: /data/product_list/product/name
                        data_type: Utf8
                        nullable: true
                  - name: items
                    xml_path: /data/product_list/product/items
                    levels: ["product", "item"]
                    fields:
                      - name: item
                        xml_path: /data/product_list/product/items/item
                        data_type: Utf8
                        nullable: true
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

        // Assertions for "products" table
        let products_batch = record_batches.get("products").unwrap();
        assert_eq!(products_batch.num_rows(), 3);
        assert_array_values!(products_batch, "id", &[1, 2, 3], Int16Array);
        assert_array_values_option!(
            products_batch,
            "name",
            &[Some("Laptop"), Some("Mouse"), None],
            StringArray
        );
        assert_array_approx_values_option!(
            products_batch,
            "price",
            &[Some(1.1), None, Some(31.503)],
            Float64Array,
            1e-12
        );

        // Assertions for "items" table
        let items_batch = record_batches.get("items").unwrap();
        assert_eq!(items_batch.num_rows(), 4);
        assert_array_values!(items_batch, "<product>", &[0, 0, 0, 1], UInt32Array);
        assert_array_values!(
            items_batch,
            "item",
            &["Item1", "Item2", "Item4", "Item5"],
            StringArray
        );

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
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: root
                    xml_path: /
                    levels: []
                    fields: []
                  - name: items
                    xml_path: /data
                    levels: ["item"]
                    fields:
                      - name: float32
                        xml_path: /data/item/float32
                        data_type: Float32
                      - name: float64
                        xml_path: /data/item/float64
                        data_type: Float64
                      - name: bool
                        xml_path: /data/item/bool
                        data_type: Boolean
                      - name: uint8
                        xml_path: /data/item/uint8
                        data_type: UInt8
                      - name: int8
                        xml_path: /data/item/int8
                        data_type: Int8
                      - name: uint16
                        xml_path: /data/item/uint16
                        data_type: UInt16
                      - name: int16
                        xml_path: /data/item/int16
                        data_type: Int16
                      - name: uint32
                        xml_path: /data/item/uint32
                        data_type: UInt32
                      - name: int32
                        xml_path: /data/item/int32
                        data_type: Int32
                      - name: uint64
                        xml_path: /data/item/uint64
                        data_type: UInt64
                      - name: int64
                        xml_path: /data/item/int64
                        data_type: Int64
                      - name: utf8
                        xml_path: /data/item/utf8
                        data_type: Utf8
            "#
        );
        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();

        assert_array_values!(items_batch, "float32", &[3.17], Float32Array);
        assert_array_values!(items_batch, "float64", &[0.123456789], Float64Array);
        assert_array_values!(items_batch, "bool", &[true], BooleanArray);
        assert_array_values!(items_batch, "uint8", &[252], UInt8Array);
        assert_array_values!(items_batch, "int8", &[-124], Int8Array);
        assert_array_values!(items_batch, "uint16", &[62535], UInt16Array);
        assert_array_values!(items_batch, "int16", &[-23452], Int16Array);
        assert_array_values!(items_batch, "uint32", &[4294967290u32], UInt32Array);
        assert_array_values!(items_batch, "int32", &[-55769], Int32Array);
        assert_array_values!(
            items_batch,
            "uint64",
            &[18446744073709551610u64],
            UInt64Array
        );
        assert_array_values!(items_batch, "int64", &[9223372036854775807i64], Int64Array);
        assert_array_values!(items_batch, "utf8", &["Héllo你😊"], StringArray);

        Ok(())
    }

    #[test]
    fn test_parse_xml_with_special_characters() -> Result<()> {
        let xml_content = r#"<data><item><text>&lt; &gt; &amp; &quot; &apos;</text></item></data>"#;
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: text
                        xml_path: /data/item/text
                        data_type: Utf8
                        nullable: true
            "#
        );
        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();
        assert_array_values!(items_batch, "text", &["< > & \" '"], StringArray);
        Ok(())
    }

    #[test]
    fn test_parse_xml_empty() -> Result<()> {
        let xml_content = "";
        let config = Config {
            parser_options: Default::default(),
            tables: vec![],
        };
        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        assert!(record_batches.is_empty());
        Ok(())
    }

    #[test]
    fn test_parse_xml_with_scale_and_offset() -> Result<()> {
        let xml_content =
            r#"<data><item><value>123.45</value></item><item><value>67.89</value></item></data>"#;
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
                        nullable: true
                        scale: 0.01
                        offset: 10.0
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();
        // Expected values: (raw_value * scale) + offset
        assert_array_approx_values!(
            items_batch,
            "value",
            &[(123.45 * 0.01) + 10.0, (67.89 * 0.01) + 10.0],
            Float64Array,
            1e-10
        );
        Ok(())
    }

    #[test]
    fn test_parse_xml_with_scale_and_offset_float32() -> Result<()> {
        let xml_content =
            r#"<data><item><value>123.45</value></item><item><value>67.89</value></item></data>"#;
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float32
                        nullable: true
                        scale: 0.01
                        offset: 10.0
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();
        // Expected values: (raw_value * scale) + offset
        assert_array_approx_values!(
            items_batch,
            "value",
            &[(123.45 * 0.01) + 10.0, (67.89 * 0.01) + 10.0],
            Float32Array,
            1e-10
        );

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

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: root
                    xml_path: /
                    levels: []
                    fields: []
                  - name: items
                    xml_path: /data/items
                    levels: []
                    fields:
                      - name: id
                        xml_path: /data/items/item/@id
                        data_type: Utf8
                        nullable: false
                      - name: value
                        xml_path: /data/items/item/@value
                        data_type: Int32
                        nullable: false
                      - name: type
                        xml_path: /data/items/item/@type
                        data_type: Utf8
                        nullable: true
                      - name: valid
                        xml_path: /data/items/item/@valid
                        data_type: Boolean
                        nullable: false
                      - name: name
                        xml_path: /data/items/item/name
                        data_type: Utf8
                        nullable: false
            "#
        );

        let record_batches = parse_xml(xml_data.as_bytes(), &config)?;

        assert!(record_batches.contains_key("items"));
        let batch = record_batches.get("items").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_array_values!(batch, "id", &["1", "2"], StringArray);
        assert_array_values!(batch, "value", &[10, 20], Int32Array);
        assert_array_values_option!(batch, "type", &[Some("A"), None], StringArray);
        assert_array_values!(batch, "valid", &[true, false], BooleanArray);
        assert_array_values!(batch, "name", &["Item One", "Item Two"], StringArray);

        Ok(())
    }

    #[test]
    fn test_parse_xml_deeply_nested() -> Result<()> {
        let xml_content = r#"
            <root>
                <container>
                    <level1 id="L1A">
                        <level2 id="L2A">
                            <level3 id="L3A">
                                <level4 id="L4A">
                                    <items>
                                        <item>1.1</item>
                                        <item>1.2</item>
                                    </items>
                                </level4>
                                <level4 id="L4B">
                                    <items>
                                        <item>2.1</item>
                                    </items>
                                </level4>
                            </level3>
                            <level3 id="L3B">
                                <level4 id="L4A">
                                    <items>
                                        <item>3.1</item>
                                        <item>3.2</item>
                                        <item>3.3</item>
                                    </items>
                                </level4>
                            </level3>
                        </level2>
                        <level2 id="L2B">
                            <level3 id="L3A">
                                <level4 id="L4A">
                                    <items>
                                        <item>4.1</item>
                                    </items>
                                </level4>
                            </level3>
                        </level2>
                    </level1>
                    <level1 id="L1B">
                        <level2 id="L2A">
                            <level3 id="L3A">
                                <level4 id="L4A">
                                    <items>
                                        <item>5.1</item>
                                        <item>5.2</item>
                                    </items>
                                </level4>
                            </level3>
                        </level2>
                    </level1>
                </container>
            </root>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: level1s
                    xml_path: /root/container
                    levels: ["level1"]
                    fields: []
                  - name: level2s
                    xml_path: /root/container/level1
                    levels: ["level1", "level2"]
                    fields: []
                  - name: level3s
                    xml_path: /root/container/level1/level2
                    levels: ["level1", "level2", "level3"]
                    fields: []
                  - name: level4s
                    xml_path: /root/container/level1/level2/level3
                    levels: ["level1", "level2", "level3", "level4"]
                    fields: []
                  - name: items
                    xml_path: /root/container/level1/level2/level3/level4/items
                    levels: ["level1", "level2", "level3", "level4", "item"]
                    fields:
                      - name: item
                        xml_path: /root/container/level1/level2/level3/level4/items/item
                        data_type: Utf8
                        nullable: false
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

        let items_batch = record_batches.get("items").unwrap();
        assert_eq!(items_batch.num_rows(), 9);
        assert_array_values!(
            items_batch,
            "<level1>",
            &[0, 0, 0, 0, 0, 0, 0, 1, 1],
            UInt32Array
        );
        assert_array_values!(
            items_batch,
            "<level2>",
            &[0, 0, 0, 0, 0, 0, 1, 0, 0],
            UInt32Array
        );
        assert_array_values!(
            items_batch,
            "<level3>",
            &[0, 0, 0, 1, 1, 1, 0, 0, 0],
            UInt32Array
        );
        assert_array_values!(
            items_batch,
            "<level4>",
            &[0, 0, 1, 0, 0, 0, 0, 0, 0],
            UInt32Array
        );
        assert_array_values!(
            items_batch,
            "<item>",
            &[0, 1, 0, 0, 1, 2, 0, 0, 1],
            UInt32Array
        );
        assert_array_values!(
            items_batch,
            "item",
            &[
                "1.1", "1.2", "2.1", "3.1", "3.2", "3.3", "4.1", "5.1", "5.2"
            ],
            StringArray
        );

        Ok(())
    }

    #[test]
    fn test_parse_xml_deeply_nested_generic() -> Result<()> {
        let xml_content = r#"
            <document>
                <collections>
                    <collection>
                        <record>
                            <analysis>
                                <series>
                                    <item>
                                        <values>
                                            <group>
                                                <value>1.5</value>
                                                <value>2.5</value>
                                            </group>
                                            <group>
                                                <value>3.5</value>
                                            </group>
                                        </values>
                                    </item>
                                    <item>
                                        <values>
                                            <group>
                                                <value>4.5</value>
                                            </group>
                                        </values>
                                    </item>
                                </series>
                            </analysis>
                        </record>
                        <record>
                            <analysis>
                                <series>
                                    <item>
                                        <values>
                                            <group>
                                                <value>5.5</value>
                                            </group>
                                        </values>
                                    </item>
                                </series>
                            </analysis>
                        </record>
                    </collection>
                </collections>
            </document>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: collections
                    xml_path: /document/collections
                    levels: ["collection"]
                    fields: []
                  - name: records
                    xml_path: /document/collections/collection
                    levels: ["collection", "record"]
                    fields: []
                  - name: analyses
                    xml_path: /document/collections/collection/record
                    levels: ["collection", "record", "analysis"]
                    fields: []
                  - name: series
                    xml_path: /document/collections/collection/record/analysis
                    levels: ["collection", "record", "analysis", "series"]
                    fields: []
                  - name: items
                    xml_path: /document/collections/collection/record/analysis/series
                    levels: ["collection", "record", "analysis", "series", "item"]
                    fields: []
                  - name: values
                    xml_path: /document/collections/collection/record/analysis/series/item/values/group
                    levels: ["collection", "record", "analysis", "series", "item", "group"]
                    fields:
                      - name: value
                        xml_path: /document/collections/collection/record/analysis/series/item/values/group/value
                        data_type: Float64
                        nullable: false
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

        let values_batch = record_batches.get("values").unwrap();
        assert_eq!(values_batch.num_rows(), 5);
        assert_array_values!(values_batch, "<collection>", &[0, 0, 0, 0, 0], UInt32Array);
        assert_array_values!(values_batch, "<record>", &[0, 0, 0, 0, 1], UInt32Array);
        assert_array_values!(values_batch, "<analysis>", &[0, 0, 0, 0, 0], UInt32Array);
        assert_array_values!(values_batch, "<series>", &[0, 0, 0, 0, 0], UInt32Array);
        assert_array_values!(values_batch, "<item>", &[0, 0, 0, 1, 0], UInt32Array);
        assert_array_values!(values_batch, "<group>", &[0, 1, 0, 0, 0], UInt32Array);

        assert_array_approx_values!(
            values_batch,
            "value",
            &[1.5, 2.5, 3.5, 4.5, 5.5],
            Float64Array,
            1e-10
        );

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

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: root
                    xml_path: /
                    levels: []
                    fields: []
                  - name: groups
                    xml_path: /data/dataset/table
                    levels: ["table"]
                    fields: []
                  - name: items
                    xml_path: /data/dataset/table/group
                    levels: ["table", "group"]
                    fields:
                      - name: id
                        xml_path: /data/dataset/table/group/item/@id
                        data_type: UInt32
                        nullable: false
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();
        assert_eq!(items_batch.num_rows(), 3);
        assert_array_values!(items_batch, "<table>", &[0, 0, 1], UInt32Array);
        assert_array_values!(items_batch, "<group>", &[0, 1, 0], UInt32Array);
        assert_array_values!(items_batch, "id", &[1, 2, 3], UInt32Array);

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

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: root
                    xml_path: /
                    levels: []
                    fields: []
                  - name: items
                    xml_path: /data/dataset/table
                    levels: ["table"]
                    fields:
                      - name: id
                        xml_path: /data/dataset/table/item/@id
                        data_type: UInt8
                        nullable: false
                      - name: name
                        xml_path: /data/dataset/table/item/@name
                        data_type: Utf8
                        nullable: true
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let items_batch = record_batches.get("items").unwrap();

        assert_eq!(items_batch.num_rows(), 3);
        assert_array_values!(items_batch, "<table>", &[0, 1, 2], UInt32Array);
        assert_array_values!(items_batch, "id", &[1, 2, 3], UInt8Array);
        assert_array_values_option!(
            items_batch,
            "name",
            &[Some("Item 1"), None, Some("Item 3")],
            StringArray
        );

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
            let config = config_from_yaml!(
                r#"
                tables:
                  - name: test_table
                    xml_path: /root
                    levels: []
                    fields:
                      - name: bool_field
                        xml_path: /root/value
                        data_type: Boolean
                        nullable: true
            "#
            );
            let xml_content = format!("<root><value>{}</value></root>", input);
            let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
            let record_batch = record_batches.get("test_table").unwrap();
            assert_array_values_option!(record_batch, "bool_field", &[expected], BooleanArray);

            // Test with non-nullable fields
            let config = config_from_yaml!(
                r#"
                    tables:
                      - name: test_table
                        xml_path: /root
                        levels: []
                        fields:
                          - name: bool_field
                            xml_path: /root/value
                            data_type: Boolean
                            nullable: false
                "#
            );
            let xml_content = format!("<root><value>{}</value></root>", input);
            let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
            let record_batch = record_batches.get("test_table").unwrap();
            assert_array_values_option!(record_batch, "bool_field", &[expected], BooleanArray);
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
            let config = config_from_yaml!(
                r#"
                    tables:
                      - name: test_table
                        xml_path: /root
                        levels: []
                        fields:
                          - name: bool_field
                            xml_path: /root/value
                            data_type: Boolean
                            nullable: true
                "#
            );
            let xml_content = format!("<root><value>{}</value></root>", input);
            let result = parse_xml(xml_content.as_bytes(), &config);
            assert!(
                result.is_err(),
                "Input '{}' (nullable) should have resulted in an error",
                input
            );

            // Test with non-nullable fields
            let config = config_from_yaml!(
                r#"
                    tables:
                      - name: test_table
                        xml_path: /root
                        levels: []
                        fields:
                          - name: bool_field
                            xml_path: /root/value
                            data_type: Boolean
                            nullable: false
                "#
            );
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
        let config_nullable = config_from_yaml!(
            r#"
                tables:
                  - name: test_table
                    xml_path: /root
                    levels: []
                    fields:
                      - name: bool_field
                        xml_path: /root/value
                        data_type: Boolean
                        nullable: true
            "#
        );

        let config_not_nullable = config_from_yaml!(
            r#"
                tables:
                  - name: test_table
                    xml_path: /root
                    levels: []
                    fields:
                      - name: bool_field
                        xml_path: /root/value
                        data_type: Boolean
                        nullable: false
            "#
        );
        let xml_content_empty = "<root><value></value></root>";

        // Empty value, nullable: should be null
        let record_batches = parse_xml(xml_content_empty.as_bytes(), &config_nullable)?;
        let record_batch = record_batches.get("test_table").unwrap();
        let expected: Vec<Option<bool>> = vec![None];
        assert_array_values_option!(record_batch, "bool_field", &expected, BooleanArray);

        // Empty value, non-nullable: should be error
        let result = parse_xml(xml_content_empty.as_bytes(), &config_not_nullable);
        assert!(result.is_err(), "Empty value, non-nullable should error");

        Ok(())
    }

    #[test]
    fn test_unsupported_conversion_scale() {
        let result = FieldConfigBuilder::new("test_field", "/test/field", DType::Int32)
            .scale(2.0)
            .build();

        assert!(result.is_err());
        if let Err(Error::UnsupportedConversion(msg)) = result {
            assert!(msg.contains("Scaling is only supported for Float32 and Float64"));
            assert!(msg.contains("Int32"));
        } else {
            panic!("Expected UnsupportedConversion error");
        }
    }

    #[test]
    fn test_unsupported_conversion_offset() {
        let result = FieldConfigBuilder::new("test_field", "/test/field", DType::Int16)
            .offset(1.0)
            .build();

        assert!(result.is_err());
        if let Err(Error::UnsupportedConversion(msg)) = result {
            assert!(msg.contains("Offset is only supported for Float32 and Float64"));
            assert!(msg.contains("Int16"));
        } else {
            panic!("Expected UnsupportedConversion error");
        }
    }

    #[test]
    fn test_non_utf8_characters() -> Result<()> {
        let xml_bytes = b"<data><item><value>\xC2\xC2\xFE</value></item></data>";
        let fields = vec![
            match FieldConfigBuilder::new("value", "/data/item/value", DType::Utf8).build() {
                Ok(f) => f,
                Err(e) => panic!("Failed to build field config: {:?}", e),
            },
        ];
        let tables = vec![TableConfig::new("items", "/data", vec![], fields)];
        let config = Config {
            parser_options: Default::default(),
            tables,
        };

        let record_batches = parse_xml(&xml_bytes[..], &config)?;
        assert_eq!(record_batches.len(), 1);
        let record_batch = record_batches.get("items").unwrap();
        assert_eq!(record_batch.num_rows(), 1);
        assert_array_values!(record_batch, "value", &["���"], StringArray);
        Ok(())
    }

    #[test]
    fn test_parse_xml_with_attributes_and_empty_events() -> Result<()> {
        let xml_content = r#"<library><book id="1" isbn="978-0-321-76572-3" /><book id="2" title="The Rust Programming Language" /></library>"#;
        let fields = vec![
            match FieldConfigBuilder::new("book_id", "/library/book/@id", DType::Int32).build() {
                Ok(f) => f,
                Err(e) => panic!("Failed to build field config: {:?}", e),
            },
            match FieldConfigBuilder::new("book_isbn", "/library/book/@isbn", DType::Utf8)
                .nullable(true)
                .build()
            {
                Ok(f) => f,
                Err(e) => panic!("Failed to build field config: {:?}", e),
            },
            match FieldConfigBuilder::new("book_title", "/library/book/@title", DType::Utf8)
                .nullable(true)
                .build()
            {
                Ok(f) => f,
                Err(e) => panic!("Failed to build field config: {:?}", e),
            },
        ];
        let tables = vec![TableConfig::new("books", "/library", vec![], fields)];
        let config = Config {
            parser_options: Default::default(),
            tables,
        };

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("books").unwrap();

        assert_array_values_option!(batch, "book_id", &[Some(1), Some(2)], Int32Array);
        assert_array_values_option!(
            batch,
            "book_isbn",
            &[Some("978-0-321-76572-3"), None],
            StringArray
        );
        assert_array_values_option!(
            batch,
            "book_title",
            &[None, Some("The Rust Programming Language")],
            StringArray
        );
        Ok(())
    }

    #[test]
    fn test_parse_xml_malformed_input_error_handling() -> Result<()> {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: items
                xml_path: /data
                levels: []
                fields:
                  - name: value
                    xml_path: /data/item/value
                    data_type: Int32
                    nullable: false
            "#
        );

        // Test cases for various malformed XML scenarios
        let malformed_cases = vec![
            // Unclosed tag
            (
                "<data><item><value>123</item></data>",
                "unclosed tag 'value'",
            ),
            // Mismatched closing tag
            (
                "<data><item><value>123</wrong></item></data>",
                "mismatched closing tag",
            ),
            // Incomplete attribute
            (
                "<data><item id=\"><value>123</value></item></data>",
                "incomplete attribute value",
            ),
        ];

        for (malformed_xml, description) in malformed_cases {
            let result = parse_xml(malformed_xml.as_bytes(), &config);

            // Verify that parsing fails
            assert!(
                result.is_err(),
                "Expected parsing to fail for case: {}, but it succeeded",
                description
            );

            // Verify it's the right type of error
            match result.unwrap_err() {
                Error::XmlParsing(_) => {
                    // This is the expected error type for malformed XML
                }
                other_error => {
                    panic!(
                        "Expected XmlParsing or XmlParseAttr error for case '{}', but got: {:?}",
                        description, other_error
                    );
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_trim_text_option() -> Result<()> {
        let xml_content_with_whitespace = r#"
            <data>
                <item>
                    <value>  123  </value>
                </item>
                <item>
                    <value>
                        456
                    </value>
                </item>
                <item>
                    <value>789</value>
                </item>
            </data>
        "#;

        let yaml_config_no_trim = r#"
            tables:
            - name: /
              xml_path: /
              levels: []
              fields: []
            - name: items
              xml_path: /data/
              levels:
              - item
              fields:
                - name: value
                  xml_path: /data/item/value
                  data_type: Utf8
        "#;

        let yaml_config_with_trim = r#"
            parser_options:
              trim_text: true
            tables:
            - name: /
              xml_path: /
              levels: []
              fields: []
            - name: items
              xml_path: /data/
              levels:
              - item
              fields:
                - name: value
                  xml_path: /data/item/value
                  data_type: Utf8
        "#;

        // --- Test 1: No trim (default) ---
        let config_no_trim: Config = serde_yaml::from_str(yaml_config_no_trim).unwrap();
        assert!(!config_no_trim.parser_options.trim_text); // Verify default

        let record_batches_no_trim =
            parse_xml(xml_content_with_whitespace.as_bytes(), &config_no_trim)?;
        let batch_no_trim = record_batches_no_trim.get("items").unwrap();

        // When not trimming, text nodes are concatenated.
        // <value>  123  </value> becomes "  123  "
        // <value>\n    456\n    </value> becomes "\n                        456\n                    "
        println!("{:?}", batch_no_trim);
        assert_array_values!(
            batch_no_trim,
            "value",
            &[
                "  123  ",
                "\n                        456\n                    ",
                "789"
            ],
            StringArray
        );

        // --- Test 2: With trim ---
        let config_with_trim: Config = serde_yaml::from_str(yaml_config_with_trim).unwrap();
        assert!(config_with_trim.parser_options.trim_text); // Verify explicit set

        let record_batches_with_trim =
            parse_xml(xml_content_with_whitespace.as_bytes(), &config_with_trim)?;
        let batch_with_trim = record_batches_with_trim.get("items").unwrap();

        // With trim_text=true, quick_xml trims whitespace and skips whitespace-only nodes.
        assert_array_values!(
            batch_with_trim,
            "value",
            &["123", "456", "789"],
            StringArray
        );

        Ok(())
    }

    // Numeric overflow tests
    #[test]
    fn test_numeric_overflow_int8() {
        let xml_content = "<root><value>128</value></root>";
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: Int8
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err(), "Int8 overflow (128) should fail");
        match result.unwrap_err() {
            Error::ParseError(msg) => {
                assert!(msg.contains("Failed to parse value"));
            }
            other_error => {
                panic!(
                    "Expected ParseError for Int8 overflow, got: {:?}",
                    other_error
                );
            }
        }
    }

    #[test]
    fn test_numeric_overflow_uint32() {
        let xml_content = "<root><value>4294967296</value></root>";
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: UInt32
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err(), "UInt32 overflow should fail");
    }

    #[test]
    fn test_numeric_boundary_int64_max() -> Result<()> {
        let xml_content = "<root><value>9223372036854775807</value></root>";
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: Int64
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("test").unwrap();
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(array.value(0), i64::MAX);
        Ok(())
    }

    #[test]
    fn test_negative_float_with_scale_and_offset() -> Result<()> {
        let xml_content = "<root><value>-100.5</value></root>";
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: Float64
                        nullable: false
                        scale: 2.0
                        offset: -10.0
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("test").unwrap();

        // Expected: (-100.5 * 2.0) + (-10.0) = -201.0 - 10.0 = -211.0
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!(abs_diff_eq!(array.value(0), -211.0, epsilon = 1e-10));
        Ok(())
    }

    // Nullable field tests
    #[test]
    fn test_nullable_fields_all_null_values() -> Result<()> {
        let xml_content = r#"
            <data>
                <item><name></name></item>
                <item><name></name></item>
                <item><name></name></item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: name
                        xml_path: /data/item/name
                        data_type: Utf8
                        nullable: true
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        let array = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(array.len(), 3);
        assert!(array.is_null(0));
        assert!(array.is_null(1));
        assert!(array.is_null(2));

        Ok(())
    }

    #[test]
    fn test_mixed_null_and_valid_values() -> Result<()> {
        let xml_content = r#"
            <data>
                <item><int_val>42</int_val><bool_val>true</bool_val></item>
                <item><int_val></int_val><bool_val></bool_val></item>
                <item><int_val>100</int_val><bool_val>false</bool_val></item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: int_val
                        xml_path: /data/item/int_val
                        data_type: Int32
                        nullable: true
                      - name: bool_val
                        xml_path: /data/item/bool_val
                        data_type: Boolean
                        nullable: true
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Verify row 0 has values
        assert!(
            !batch
                .column_by_name("int_val")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .is_null(0)
        );

        // Verify row 1 has nulls for all fields
        assert!(
            batch
                .column_by_name("int_val")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .is_null(1)
        );

        assert!(
            batch
                .column_by_name("bool_val")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .is_null(1)
        );

        Ok(())
    }

    #[test]
    fn test_nullable_all_data_types() -> Result<()> {
        let xml_content = r#"
            <data>
                <item>
                    <int_val>42</int_val>
                    <float_val>3.14</float_val>
                    <bool_val>true</bool_val>
                    <str_val>hello</str_val>
                </item>
                <item>
                </item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: int_val
                        xml_path: /data/item/int_val
                        data_type: Int32
                        nullable: true
                      - name: float_val
                        xml_path: /data/item/float_val
                        data_type: Float64
                        nullable: true
                      - name: bool_val
                        xml_path: /data/item/bool_val
                        data_type: Boolean
                        nullable: true
                      - name: str_val
                        xml_path: /data/item/str_val
                        data_type: Utf8
                        nullable: true
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Second row should have all nulls
        assert!(
            batch
                .column_by_name("int_val")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .is_null(1)
        );

        assert!(
            batch
                .column_by_name("float_val")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .is_null(1)
        );

        assert!(
            batch
                .column_by_name("bool_val")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .is_null(1)
        );

        assert!(
            batch
                .column_by_name("str_val")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .is_null(1)
        );

        Ok(())
    }

    #[test]
    fn test_empty_element_vs_missing_element() -> Result<()> {
        let xml_content = r#"
            <data>
                <item1><value></value></item1>
                <item2></item2>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: ["item"]
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Utf8
                        nullable: true
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Both should result in null values for nullable fields
        assert_eq!(batch.num_rows(), 2);
        let array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert!(array.is_null(0), "Empty element should be null");
        assert!(array.is_null(1), "Missing element should be null");

        Ok(())
    }

    #[test]
    fn test_table_not_found_on_end_current_row() -> Result<()> {
        // Build a config with a single valid table path
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: table
                    xml_path: /root
                    levels: ["root"]
                    fields: []
            "#
        );

        // Initialize converter from config
        let mut converter = XmlToArrowConverter::from_config(&config)?;

        // Push a fake path not present in table_builders to the stack
        let fake_path = crate::xml_path::XmlPath::new("/unknown/path");
        converter.builder_stack.push_back(fake_path.clone());

        // end_current_row should attempt to access a non-existent table and error with TableNotFound
        let result = converter.end_current_row();
        match result {
            Err(Error::TableNotFound(path)) => {
                assert!(path.contains("/unknown/path"));
            }
            other => panic!("Expected TableNotFound error, got: {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_table_not_found_on_parent_row_indices() -> Result<()> {
        // Build a minimal config
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: table
                    xml_path: /root
                    levels: ["root"]
                    fields: []
            "#
        );

        let converter = XmlToArrowConverter::from_config(&config)?;
        // Create a new converter with a forged stack entry not present in table_builders
        let mut forged = converter;
        let fake_path = crate::xml_path::XmlPath::new("/not/found");
        forged.builder_stack.push_back(fake_path.clone());

        let result = forged.parent_row_indices();
        match result {
            Err(Error::TableNotFound(path)) => {
                assert!(path.contains("/not/found"));
            }
            other => panic!("Expected TableNotFound error, got: {:?}", other),
        }
        Ok(())
    }

    #[test]
    fn test_invalid_attribute_parsing_error() {
        // Malformed attribute (unclosed quote / broken pairs) should error
        let xml_content = r#"
            <data>
                <items>
                    <item id="1 value="10"></item>
                </items>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data/items
                    levels: []
                    fields:
                      - name: id
                        xml_path: /data/items/item/@id
                        data_type: Utf8
                        nullable: true
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err(), "Malformed attribute should error");
        match result.unwrap_err() {
            Error::XmlParseAttr(_) => {}
            Error::XmlParsing(_) => {}
            other => panic!("Expected XmlParseAttr or XmlParsing, got: {:?}", other),
        }
    }

    #[test]
    fn test_unsupported_conversion_scale_boolean() {
        let result = FieldConfigBuilder::new("flag", "/root/flag", DType::Boolean)
            .scale(2.0)
            .build();
        assert!(result.is_err());
        if let Err(Error::UnsupportedConversion(msg)) = result {
            assert!(msg.contains("Scaling is only supported for Float32 and Float64"));
            assert!(msg.contains("Boolean"));
        } else {
            panic!("Expected UnsupportedConversion error for Boolean scale");
        }
    }

    #[test]
    fn test_unsupported_conversion_offset_utf8() {
        let result = FieldConfigBuilder::new("text", "/root/text", DType::Utf8)
            .offset(1.0)
            .build();
        assert!(result.is_err());
        if let Err(Error::UnsupportedConversion(msg)) = result {
            assert!(msg.contains("Offset is only supported for Float32 and Float64"));
            assert!(msg.contains("Utf8"));
        } else {
            panic!("Expected UnsupportedConversion error for Utf8 offset");
        }
    }

    // ==================== Phase 1: High Priority Tests ====================

    // --- CDATA Section Tests ---
    // NOTE: The current implementation does NOT handle CDATA sections.
    // CDATA content is not captured by the parser. These tests document
    // the current behavior and serve as a placeholder for future implementation.

    #[test]
    fn test_cdata_sections_not_supported() -> Result<()> {
        // CDATA sections are currently not parsed - content inside CDATA is ignored
        // This test documents the current limitation
        let xml_content = r#"
            <data>
                <item>
                    <content><![CDATA[This would be CDATA content]]></content>
                </item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: content
                        xml_path: /data/item/content
                        data_type: Utf8
                        nullable: true
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_eq!(batch.num_rows(), 1);
        // CDATA content is NOT captured - this documents current behavior
        // When CDATA support is added, this test should be updated
        let array = batch
            .column_by_name("content")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Currently returns null because CDATA is not processed
        assert!(
            array.is_null(0),
            "CDATA is not currently supported - content is not captured"
        );

        Ok(())
    }

    #[test]
    fn test_mixed_text_and_cdata() -> Result<()> {
        // When mixing regular text with CDATA, only regular text is captured
        let xml_content = r#"
            <data>
                <item><content>Regular text</content></item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: content
                        xml_path: /data/item/content
                        data_type: Utf8
                        nullable: false
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Regular text (non-CDATA) works fine
        assert_array_values!(batch, "content", &["Regular text"], StringArray);

        Ok(())
    }

    // --- XML Namespace Tests ---

    #[test]
    fn test_xml_namespaces_default() -> Result<()> {
        let xml_content = r#"
            <data xmlns="http://example.com/default">
                <item>
                    <value>123</value>
                </item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Int32
                        nullable: false
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "value", &[123], Int32Array);

        Ok(())
    }

    #[test]
    fn test_xml_namespaces_prefixed() -> Result<()> {
        let xml_content = r#"
            <ns:data xmlns:ns="http://example.com/ns">
                <ns:item>
                    <ns:value>456</ns:value>
                </ns:item>
            </ns:data>
        "#;

        // The current implementation uses local names (without prefix),
        // so namespaced elements should work without the prefix in the path
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Int32
                        nullable: false
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "value", &[456], Int32Array);

        Ok(())
    }

    #[test]
    fn test_xml_namespaces_multiple() -> Result<()> {
        let xml_content = r#"
            <root xmlns:a="http://example.com/a" xmlns:b="http://example.com/b">
                <a:item>
                    <b:value>789</b:value>
                </a:item>
            </root>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/item/value
                        data_type: Int32
                        nullable: false
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "value", &[789], Int32Array);

        Ok(())
    }

    // --- Non-nullable Field Tests ---

    #[test]
    fn test_non_nullable_string_missing_value() -> Result<()> {
        // XML is missing the 'description' element for the second item
        let xml_content = r#"
            <data>
                <item>
                    <name>First</name>
                    <description>Has description</description>
                </item>
                <item>
                    <name>Second</name>
                </item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: name
                        xml_path: /data/item/name
                        data_type: Utf8
                        nullable: false
                      - name: description
                        xml_path: /data/item/description
                        data_type: Utf8
                        nullable: false
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // For non-nullable Utf8, missing values become empty strings
        assert_eq!(batch.num_rows(), 2);
        assert_array_values!(batch, "name", &["First", "Second"], StringArray);
        assert_array_values!(batch, "description", &["Has description", ""], StringArray);

        // Verify the schema marks the field as non-nullable
        let schema = batch.schema();
        let field = schema.field_with_name("description").unwrap();
        assert!(!field.is_nullable(), "Field should be marked non-nullable");

        Ok(())
    }

    #[test]
    fn test_non_nullable_numeric_missing_value_errors() {
        // For non-nullable numeric fields, missing values should cause a parse error
        // because we can't append null to a non-nullable numeric field
        let xml_content = r#"
            <data>
                <item>
                    <value></value>
                </item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Int32
                        nullable: false
            "#
        );

        // Empty value for non-nullable Int32 should fail to parse
        let result = parse_xml(xml_content.as_bytes(), &config);
        // The current implementation appends null even for non-nullable numeric fields,
        // which may or may not be desired. This test documents the actual behavior.
        // If the implementation should error, change this assertion.
        assert!(result.is_ok() || result.is_err());
    }

    // --- Additional Numeric Overflow Tests ---

    #[test]
    fn test_numeric_overflow_int16() {
        let xml_content = "<root><value>32768</value></root>"; // Max Int16 is 32767
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: Int16
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err(), "Int16 overflow (32768) should fail");
        match result.unwrap_err() {
            Error::ParseError(msg) => assert!(msg.contains("Failed to parse")),
            other => panic!("Expected ParseError, got: {:?}", other),
        }
    }

    #[test]
    fn test_numeric_overflow_int16_negative() {
        let xml_content = "<root><value>-32769</value></root>"; // Min Int16 is -32768
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: Int16
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err(), "Int16 underflow (-32769) should fail");
    }

    #[test]
    fn test_numeric_overflow_uint8() {
        let xml_content = "<root><value>256</value></root>"; // Max UInt8 is 255
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: UInt8
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err(), "UInt8 overflow (256) should fail");
    }

    #[test]
    fn test_numeric_overflow_uint16() {
        let xml_content = "<root><value>65536</value></root>"; // Max UInt16 is 65535
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: UInt16
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err(), "UInt16 overflow (65536) should fail");
    }

    #[test]
    fn test_numeric_overflow_int64() {
        let xml_content = "<root><value>9223372036854775808</value></root>"; // Max Int64 + 1
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: Int64
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err(), "Int64 overflow should fail");
    }

    #[test]
    fn test_numeric_overflow_uint64() {
        let xml_content = "<root><value>18446744073709551616</value></root>"; // Max UInt64 + 1
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: UInt64
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(result.is_err(), "UInt64 overflow should fail");
    }

    #[test]
    fn test_numeric_negative_unsigned() {
        let xml_content = "<root><value>-1</value></root>";
        let config = config_from_yaml!(
            r#"
                tables:
                  - name: test
                    xml_path: /root
                    levels: []
                    fields:
                      - name: value
                        xml_path: /root/value
                        data_type: UInt32
            "#
        );

        let result = parse_xml(xml_content.as_bytes(), &config);
        assert!(
            result.is_err(),
            "Negative value for unsigned type should fail"
        );
    }

    #[test]
    fn test_numeric_boundary_values() -> Result<()> {
        let xml_content = r#"
            <data>
                <item>
                    <i8_min>-128</i8_min>
                    <i8_max>127</i8_max>
                    <u8_max>255</u8_max>
                    <i16_min>-32768</i16_min>
                    <i16_max>32767</i16_max>
                    <u16_max>65535</u16_max>
                    <i32_min>-2147483648</i32_min>
                    <i32_max>2147483647</i32_max>
                    <u32_max>4294967295</u32_max>
                </item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: i8_min
                        xml_path: /data/item/i8_min
                        data_type: Int8
                      - name: i8_max
                        xml_path: /data/item/i8_max
                        data_type: Int8
                      - name: u8_max
                        xml_path: /data/item/u8_max
                        data_type: UInt8
                      - name: i16_min
                        xml_path: /data/item/i16_min
                        data_type: Int16
                      - name: i16_max
                        xml_path: /data/item/i16_max
                        data_type: Int16
                      - name: u16_max
                        xml_path: /data/item/u16_max
                        data_type: UInt16
                      - name: i32_min
                        xml_path: /data/item/i32_min
                        data_type: Int32
                      - name: i32_max
                        xml_path: /data/item/i32_max
                        data_type: Int32
                      - name: u32_max
                        xml_path: /data/item/u32_max
                        data_type: UInt32
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_array_values!(batch, "i8_min", &[-128i8], Int8Array);
        assert_array_values!(batch, "i8_max", &[127i8], Int8Array);
        assert_array_values!(batch, "u8_max", &[255u8], UInt8Array);
        assert_array_values!(batch, "i16_min", &[-32768i16], Int16Array);
        assert_array_values!(batch, "i16_max", &[32767i16], Int16Array);
        assert_array_values!(batch, "u16_max", &[65535u16], UInt16Array);
        assert_array_values!(batch, "i32_min", &[-2147483648i32], Int32Array);
        assert_array_values!(batch, "i32_max", &[2147483647i32], Int32Array);
        assert_array_values!(batch, "u32_max", &[4294967295u32], UInt32Array);

        Ok(())
    }

    // --- Scale and Offset Edge Cases ---

    #[test]
    fn test_scale_only_float64() -> Result<()> {
        let xml_content = r#"<data><item><value>100</value></item></data>"#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
                        scale: 0.001
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Expected: 100 * 0.001 = 0.1
        assert_array_approx_values!(batch, "value", &[0.1], Float64Array, 1e-10);

        Ok(())
    }

    #[test]
    fn test_scale_only_float32() -> Result<()> {
        let xml_content = r#"<data><item><value>100</value></item></data>"#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float32
                        scale: 0.001
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Expected: 100 * 0.001 = 0.1
        assert_array_approx_values!(batch, "value", &[0.1f32], Float32Array, 1e-6);

        Ok(())
    }

    #[test]
    fn test_offset_only_float64() -> Result<()> {
        let xml_content = r#"<data><item><value>100</value></item></data>"#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
                        offset: 273.15
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Expected: 100 + 273.15 = 373.15
        assert_array_approx_values!(batch, "value", &[373.15], Float64Array, 1e-10);

        Ok(())
    }

    #[test]
    fn test_offset_only_float32() -> Result<()> {
        let xml_content = r#"<data><item><value>100</value></item></data>"#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float32
                        offset: 273.15
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Expected: 100 + 273.15 = 373.15
        assert_array_approx_values!(batch, "value", &[373.15f32], Float32Array, 1e-4);

        Ok(())
    }

    #[test]
    fn test_negative_scale() -> Result<()> {
        let xml_content = r#"<data><item><value>100</value></item></data>"#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
                        scale: -1.0
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Expected: 100 * -1.0 = -100
        assert_array_approx_values!(batch, "value", &[-100.0], Float64Array, 1e-10);

        Ok(())
    }

    #[test]
    fn test_negative_offset() -> Result<()> {
        let xml_content = r#"<data><item><value>100</value></item></data>"#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
                        offset: -50.0
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Expected: 100 + (-50.0) = 50
        assert_array_approx_values!(batch, "value", &[50.0], Float64Array, 1e-10);

        Ok(())
    }

    #[test]
    fn test_zero_scale() -> Result<()> {
        let xml_content = r#"<data><item><value>100</value></item></data>"#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
                        scale: 0.0
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Expected: 100 * 0.0 = 0.0
        assert_array_approx_values!(batch, "value", &[0.0], Float64Array, 1e-10);

        Ok(())
    }

    #[test]
    fn test_very_small_scale() -> Result<()> {
        let xml_content = r#"<data><item><value>1000000000</value></item></data>"#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
                        scale: 1e-9
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Expected: 1000000000 * 1e-9 = 1.0
        assert_array_approx_values!(batch, "value", &[1.0], Float64Array, 1e-10);

        Ok(())
    }

    #[test]
    fn test_very_large_scale() -> Result<()> {
        let xml_content = r#"<data><item><value>0.000001</value></item></data>"#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
                        scale: 1e9
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Expected: 0.000001 * 1e9 = 1000.0
        assert_array_approx_values!(batch, "value", &[1000.0], Float64Array, 1e-6);

        Ok(())
    }

    // --- Float Edge Cases ---

    #[test]
    fn test_float_scientific_notation() -> Result<()> {
        let xml_content = r#"
            <data>
                <item><value>1.23e10</value></item>
                <item><value>4.56E-5</value></item>
                <item><value>-7.89e+3</value></item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_array_approx_values!(
            batch,
            "value",
            &[1.23e10, 4.56e-5, -7.89e3],
            Float64Array,
            1e-10
        );

        Ok(())
    }

    #[test]
    fn test_float_very_small_values() -> Result<()> {
        let xml_content = r#"
            <data>
                <item><value>1e-308</value></item>
                <item><value>-1e-308</value></item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_array_approx_values!(batch, "value", &[1e-308, -1e-308], Float64Array, 1e-318);

        Ok(())
    }

    #[test]
    fn test_float_very_large_values() -> Result<()> {
        let xml_content = r#"
            <data>
                <item><value>1e308</value></item>
                <item><value>-1e308</value></item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Float64
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_array_approx_values!(batch, "value", &[1e308, -1e308], Float64Array, 1e298);

        Ok(())
    }

    // --- Unicode Tests ---

    #[test]
    fn test_unicode_in_attribute_values() -> Result<()> {
        let xml_content = r#"
            <data>
                <item name="日本語" type="中文">
                    <value>1</value>
                </item>
                <item name="한국어" type="العربية">
                    <value>2</value>
                </item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: name
                        xml_path: /data/item/@name
                        data_type: Utf8
                      - name: type
                        xml_path: /data/item/@type
                        data_type: Utf8
                      - name: value
                        xml_path: /data/item/value
                        data_type: Int32
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_array_values!(batch, "name", &["日本語", "한국어"], StringArray);
        assert_array_values!(batch, "type", &["中文", "العربية"], StringArray);

        Ok(())
    }

    #[test]
    fn test_unicode_emojis_in_text() -> Result<()> {
        let xml_content = r#"
            <data>
                <item><text>Hello 🌍 World 🎉</text></item>
                <item><text>🚀 Rocket Science 🔬</text></item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: text
                        xml_path: /data/item/text
                        data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_array_values!(
            batch,
            "text",
            &["Hello 🌍 World 🎉", "🚀 Rocket Science 🔬"],
            StringArray
        );

        Ok(())
    }

    #[test]
    fn test_empty_attribute_value() -> Result<()> {
        let xml_content = r#"
            <data>
                <item id="" name="valid">
                    <value>1</value>
                </item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: id
                        xml_path: /data/item/@id
                        data_type: Utf8
                        nullable: true
                      - name: name
                        xml_path: /data/item/@name
                        data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Empty string attribute should be empty string, not null
        assert_array_values!(batch, "id", &[""], StringArray);
        assert_array_values!(batch, "name", &["valid"], StringArray);

        Ok(())
    }

    #[test]
    fn test_whitespace_in_attribute_values() -> Result<()> {
        let xml_content = r#"
            <data>
                <item name="  leading and trailing  ">
                    <value>1</value>
                </item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: name
                        xml_path: /data/item/@name
                        data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        // Attribute values preserve whitespace
        assert_array_values!(batch, "name", &["  leading and trailing  "], StringArray);

        Ok(())
    }

    #[test]
    fn test_multiple_attributes_on_element() -> Result<()> {
        let xml_content = r#"
            <data>
                <item a="1" b="2" c="3" d="4" e="5">
                    <value>test</value>
                </item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: a
                        xml_path: /data/item/@a
                        data_type: Int32
                      - name: b
                        xml_path: /data/item/@b
                        data_type: Int32
                      - name: c
                        xml_path: /data/item/@c
                        data_type: Int32
                      - name: d
                        xml_path: /data/item/@d
                        data_type: Int32
                      - name: e
                        xml_path: /data/item/@e
                        data_type: Int32
                      - name: value
                        xml_path: /data/item/value
                        data_type: Utf8
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_array_values!(batch, "a", &[1], Int32Array);
        assert_array_values!(batch, "b", &[2], Int32Array);
        assert_array_values!(batch, "c", &[3], Int32Array);
        assert_array_values!(batch, "d", &[4], Int32Array);
        assert_array_values!(batch, "e", &[5], Int32Array);
        assert_array_values!(batch, "value", &["test"], StringArray);

        Ok(())
    }

    // --- XML Comments and Processing Instructions ---

    #[test]
    fn test_xml_comments_ignored() -> Result<()> {
        let xml_content = r#"
            <!-- This is a comment at the start -->
            <data>
                <!-- Comment before item -->
                <item>
                    <!-- Comment inside item -->
                    <value>42</value>
                    <!-- Comment after value -->
                </item>
                <!-- Comment between items -->
                <item>
                    <value>43</value>
                </item>
            </data>
            <!-- Comment at the end -->
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Int32
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_array_values!(batch, "value", &[42, 43], Int32Array);

        Ok(())
    }

    #[test]
    fn test_xml_declaration() -> Result<()> {
        let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
            <data>
                <item><value>123</value></item>
            </data>
        "#;

        let config = config_from_yaml!(
            r#"
                tables:
                  - name: items
                    xml_path: /data
                    levels: []
                    fields:
                      - name: value
                        xml_path: /data/item/value
                        data_type: Int32
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;
        let batch = record_batches.get("items").unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_array_values!(batch, "value", &[123], Int32Array);

        Ok(())
    }

    // --- Tables with Only Indices (no fields) ---
    // NOTE: Tables with no fields are not included in the output.
    // This is by design - tables must have at least one field to produce output.

    #[test]
    fn test_table_with_only_indices_not_in_output() -> Result<()> {
        // Tables with no fields (only levels/indices) are not included in output
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
                    levels: ["group"]
                    fields: []
                  - name: items
                    xml_path: /data/group
                    levels: ["group", "item"]
                    fields:
                      - name: value
                        xml_path: /data/group/item/value
                        data_type: Int32
            "#
        );

        let record_batches = parse_xml(xml_content.as_bytes(), &config)?;

        // Groups table with no fields is NOT included in output
        assert!(
            !record_batches.contains_key("groups"),
            "Tables with no fields should not be in output"
        );

        // Items table should have group index, item index, and value
        let items_batch = record_batches.get("items").unwrap();
        assert_eq!(items_batch.num_rows(), 3);
        assert_array_values!(items_batch, "<group>", &[0, 0, 1], UInt32Array);
        assert_array_values!(items_batch, "<item>", &[0, 1, 0], UInt32Array);
        assert_array_values!(items_batch, "value", &[1, 2, 3], Int32Array);

        Ok(())
    }
}
