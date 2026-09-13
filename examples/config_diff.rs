//! `config_diff` — show what changing a config does to a real document.
//!
//! Adoption tooling for declared row boundaries (`row:`).
//! Adding `row:` to a table is safe in the sense that it touches nothing else,
//! but it *can* change that table's row count — which is the whole point when
//! the inferred rule was producing half-filled rows. The question a user
//! actually has is "what happens to *my* data", and the honest answer is a diff
//! against their own document rather than a paragraph of prose.
//!
//! ```text
//! cargo run --example config_diff -- old.yaml new.yaml document.xml
//! ```
//!
//! Reports, per table: whether it appears in both configs, its row count under
//! each, which columns were added or removed, and — when the row counts match —
//! which shared columns hold different *values*. Exits non-zero when anything
//! differs, so it can gate a migration in CI.
//!
//! Values matter because the step most likely to change data changes nothing
//! else. Declaring `version: 2` switches two value defaults — `Utf8` values are
//! trimmed, and a missing non-nullable `Utf8` value becomes an error rather
//! than `""` — without adding a row or a column. A diff of shapes alone reports
//! that step as "No differences" while `"  padded  "` becomes `"padded"`.
//!
//! Deliberately a dev-only example rather than a shipped binary, so that it
//! costs users nothing: no dependency, no install surface, no compatibility
//! promise.

use std::collections::BTreeSet;
use std::process::ExitCode;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use xml2arrow::{Config, Parser};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [config_a, config_b, document] = args.as_slice() else {
        eprintln!("usage: config_diff <config-a.yaml> <config-b.yaml> <document.xml>");
        return ExitCode::from(2);
    };

    match run(config_a, config_b, document) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

/// Returns `Ok(true)` when the two configs produce identical output.
fn run(config_a: &str, config_b: &str, document: &str) -> Result<bool, Box<dyn std::error::Error>> {
    // Parsed separately rather than from one shared buffer: the point is to
    // model what each config does on its own, including its own failures.
    let a = parse_with(config_a, document)?;
    let b = parse_with(config_b, document)?;

    let table_names: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    let mut identical = true;

    println!("{config_a}  ->  {config_b}");
    println!("document: {document}\n");

    for name in table_names {
        match (a.get(name), b.get(name)) {
            (Some(before), Some(after)) => {
                let columns_before = column_names(before);
                let columns_after = column_names(after);
                let added: Vec<&str> = columns_after
                    .difference(&columns_before)
                    .map(String::as_str)
                    .collect();
                let removed: Vec<&str> = columns_before
                    .difference(&columns_after)
                    .map(String::as_str)
                    .collect();
                let rows_changed = before.num_rows() != after.num_rows();
                // Values are compared only when the row counts agree. Rows are
                // matched by position, and once a table gains or loses rows
                // every later position compares different rows — the row-count
                // line is then the whole story, and a column of cell
                // differences would only bury it.
                let value_changes = if rows_changed {
                    Vec::new()
                } else {
                    value_changes(before, after)
                };

                if !rows_changed
                    && added.is_empty()
                    && removed.is_empty()
                    && value_changes.is_empty()
                {
                    println!("  = {name}: unchanged ({} rows)", before.num_rows());
                    continue;
                }
                identical = false;
                println!("  ~ {name}:");
                if rows_changed {
                    println!("      rows: {} -> {}", before.num_rows(), after.num_rows());
                }
                if !added.is_empty() {
                    println!("      columns added:   {}", added.join(", "));
                }
                if !removed.is_empty() {
                    println!("      columns removed: {}", removed.join(", "));
                }
                for change in &value_changes {
                    println!("      values changed:  {change}");
                }
            }
            (Some(before), None) => {
                identical = false;
                println!("  - {name}: removed (was {} rows)", before.num_rows());
            }
            (None, Some(after)) => {
                identical = false;
                println!("  + {name}: added ({} rows)", after.num_rows());
            }
            (None, None) => unreachable!("name came from one of the two maps"),
        }
    }

    println!();
    if identical {
        println!("No differences.");
    } else {
        println!("Configs differ on this document.");
    }
    Ok(identical)
}

/// Parses `document` with the config at `config_path`, keeping whole batches:
/// comparing values needs the values.
fn parse_with(
    config_path: &str,
    document: &str,
) -> Result<indexmap::IndexMap<String, RecordBatch>, Box<dyn std::error::Error>> {
    let config = Config::from_yaml_file(config_path)?;
    let parser = Parser::new(&config)?;

    // Lints are the other half of the adoption story: they say what to change,
    // this says what changing it did.
    for lint in parser.warnings() {
        eprintln!("lint [{config_path}]: {lint}");
    }

    Ok(parser.parse_slice(&std::fs::read(document)?)?)
}

fn column_names(batch: &RecordBatch) -> BTreeSet<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// One line per shared column whose values differ: how many rows, and the
/// first one as an example.
///
/// A summary rather than a cell dump. The question being answered is "did this
/// change my data, and how" — one example of each kind of difference answers
/// it, and a thousand-line diff of the same trimmed whitespace would not.
fn value_changes(before: &RecordBatch, after: &RecordBatch) -> Vec<String> {
    let mut changes = Vec::new();
    for field in before.schema().fields() {
        let name = field.name();
        let (Some(a), Some(b)) = (before.column_by_name(name), after.column_by_name(name)) else {
            continue; // added or removed — already reported
        };
        if a.data_type() != b.data_type() {
            changes.push(format!(
                "{name}: type {} -> {}",
                a.data_type(),
                b.data_type()
            ));
            continue;
        }
        // Logical equality over the whole column first; walking rows is only
        // for columns that actually differ.
        if a.to_data() == b.to_data() {
            continue;
        }
        let mut differing = 0;
        let mut first = None;
        for row in 0..a.len() {
            let (va, vb) = (render(a.as_ref(), row), render(b.as_ref(), row));
            if va != vb {
                differing += 1;
                first.get_or_insert((row, va, vb));
            }
        }
        if let Some((row, va, vb)) = first {
            changes.push(format!(
                "{name}: {differing} of {} rows, e.g. row {row}: {va} -> {vb}",
                a.len()
            ));
        }
    }
    changes
}

/// A cell as the reader needs to see it. `Utf8` values are quoted, because the
/// difference a migration most often makes to text is whitespace, and
/// `  padded  -> padded` without quotes looks like no change at all.
fn render(column: &dyn Array, row: usize) -> String {
    if column.is_null(row) {
        return "null".to_string();
    }
    let text = array_value_to_string(column, row).unwrap_or_else(|e| format!("<{e}>"));
    if matches!(column.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        format!("{text:?}")
    } else {
        text
    }
}
