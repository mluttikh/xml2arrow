use criterion::{Criterion, black_box, criterion_group, criterion_main};
use xml2arrow::{Config, parse_xml};

fn create_test_xml(num_rows: usize) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<data>
    <metadata>
        <title>Performance Test Data</title>
        <created_by>Benchmark Suite</created_by>
    </metadata>
    <items>
"#,
    );

    for i in 0..num_rows {
        xml.push_str(&format!(
            r#"        <item id="item_{}" category="test">
            <name>Item Number {}</name>
            <value>{}</value>
            <price>{}.99</price>
            <quantity>{}</quantity>
            <active>true</active>
            <description>This is a description for item {} with some text content to make it realistic.</description>
        </item>
"#,
            i, i, i * 100, i, i % 50 + 1, i
        ));
    }

    xml.push_str(
        r#"    </items>
</data>"#,
    );
    xml
}

fn create_test_config() -> Config {
    serde_yaml::from_str(
        r#"
parser_options:
  trim_text: false
tables:
  - name: root
    xml_path: /
    levels: []
    fields: []
  - name: items
    xml_path: /data/items
    levels:
      - item
    fields:
      - name: id
        xml_path: /data/items/item/@id
        data_type: Utf8
        nullable: false
      - name: category
        xml_path: /data/items/item/@category
        data_type: Utf8
        nullable: false
      - name: name
        xml_path: /data/items/item/name
        data_type: Utf8
        nullable: false
      - name: value
        xml_path: /data/items/item/value
        data_type: Int32
        nullable: false
      - name: price
        xml_path: /data/items/item/price
        data_type: Float64
        nullable: false
      - name: quantity
        xml_path: /data/items/item/quantity
        data_type: Int32
        nullable: false
      - name: active
        xml_path: /data/items/item/active
        data_type: Boolean
        nullable: false
      - name: description
        xml_path: /data/items/item/description
        data_type: Utf8
        nullable: false
"#,
    )
    .expect("Failed to parse config")
}

fn benchmark_small_xml(c: &mut Criterion) {
    let xml = create_test_xml(100);
    let config = create_test_config();

    c.bench_function("parse_100_rows", |b| {
        b.iter(|| {
            let result = parse_xml(black_box(xml.as_bytes()), black_box(&config));
            black_box(result.unwrap())
        });
    });
}

fn benchmark_medium_xml(c: &mut Criterion) {
    let xml = create_test_xml(1000);
    let config = create_test_config();

    c.bench_function("parse_1000_rows", |b| {
        b.iter(|| {
            let result = parse_xml(black_box(xml.as_bytes()), black_box(&config));
            black_box(result.unwrap())
        });
    });
}

fn benchmark_large_xml(c: &mut Criterion) {
    let xml = create_test_xml(10000);
    let config = create_test_config();

    c.bench_function("parse_10000_rows", |b| {
        b.iter(|| {
            let result = parse_xml(black_box(xml.as_bytes()), black_box(&config));
            black_box(result.unwrap())
        });
    });
}

fn benchmark_with_attributes(c: &mut Criterion) {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<products>
    <product id="1" sku="ABC123" category="electronics">
        <name>Laptop</name>
        <price currency="USD">999.99</price>
    </product>
    <product id="2" sku="DEF456" category="electronics">
        <name>Mouse</name>
        <price currency="USD">29.99</price>
    </product>
    <product id="3" sku="GHI789" category="accessories">
        <name>USB Cable</name>
        <price currency="USD">9.99</price>
    </product>
</products>"#;

    let config: Config = serde_yaml::from_str(
        r#"
tables:
  - name: root
    xml_path: /
    levels: []
    fields: []
  - name: products
    xml_path: /products
    levels:
      - product
    fields:
      - name: id
        xml_path: /products/product/@id
        data_type: Int32
        nullable: false
      - name: sku
        xml_path: /products/product/@sku
        data_type: Utf8
        nullable: false
      - name: category
        xml_path: /products/product/@category
        data_type: Utf8
        nullable: false
      - name: name
        xml_path: /products/product/name
        data_type: Utf8
        nullable: false
      - name: price
        xml_path: /products/product/price
        data_type: Float64
        nullable: false
      - name: currency
        xml_path: /products/product/price/@currency
        data_type: Utf8
        nullable: false
"#,
    )
    .expect("Failed to parse config");

    c.bench_function("parse_with_attributes", |b| {
        b.iter(|| {
            let result = parse_xml(black_box(xml.as_bytes()), black_box(&config));
            black_box(result.unwrap())
        });
    });
}

criterion_group!(
    benches,
    benchmark_small_xml,
    benchmark_medium_xml,
    benchmark_large_xml,
    benchmark_with_attributes
);
criterion_main!(benches);
