use std::time::Instant;
use xml2arrow::{Config, parse_xml};

fn create_test_xml(num_rows: usize) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<data>
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

fn main() {
    println!("XML to Arrow Parser Benchmark");
    println!("==============================\n");

    let test_cases = vec![
        ("Small (100 rows)", 100, 100),
        ("Medium (1,000 rows)", 1000, 50),
        ("Large (10,000 rows)", 10000, 10),
    ];

    for (name, num_rows, iterations) in test_cases {
        println!("Test case: {}", name);

        let xml = create_test_xml(num_rows);
        let config = create_test_config();

        // Warm-up
        for _ in 0..3 {
            let _ = parse_xml(xml.as_bytes(), &config).unwrap();
        }

        // Actual benchmark
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = parse_xml(xml.as_bytes(), &config).unwrap();
        }
        let duration = start.elapsed();

        let avg_ms = duration.as_secs_f64() * 1000.0 / iterations as f64;
        let throughput = (num_rows as f64 / avg_ms) * 1000.0;

        println!("  Average time: {:.2} ms", avg_ms);
        println!("  Throughput: {:.0} rows/sec", throughput);
        println!("  XML size: {} bytes", xml.len());
        println!();
    }
}
