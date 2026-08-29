//! `config_diff` — show what changing a config does to a real document.
//!
//! Adoption tooling for declared row boundaries (`TRANSITION_PLAN.md` Phase C).
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
//! each, and which columns were added or removed. Exits non-zero when anything
//! differs, so it can gate a migration in CI.
//!
//! Deliberately a dev-only example rather than a shipped binary: it graduates
//! to the CLI crate in Phase G, and until then it should cost users nothing —
//! no dependency, no install surface, no compatibility promise.

use std::collections::BTreeSet;
use std::process::ExitCode;

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
                let rows_changed = before.0 != after.0;

                if !rows_changed && added.is_empty() && removed.is_empty() {
                    println!("  = {name}: unchanged ({} rows)", before.0);
                    continue;
                }
                identical = false;
                println!("  ~ {name}:");
                if rows_changed {
                    println!("      rows: {} -> {}", before.0, after.0);
                }
                if !added.is_empty() {
                    println!("      columns added:   {}", added.join(", "));
                }
                if !removed.is_empty() {
                    println!("      columns removed: {}", removed.join(", "));
                }
            }
            (Some(before), None) => {
                identical = false;
                println!("  - {name}: removed (was {} rows)", before.0);
            }
            (None, Some(after)) => {
                identical = false;
                println!("  + {name}: added ({} rows)", after.0);
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

/// `(row count, column names)` per table, which is the granularity the
/// migration question is actually asked at. Values are deliberately not
/// compared: a row-count or column change is the decision, and dumping cell
/// diffs would bury it.
type TableShape = (usize, Vec<String>);

fn parse_with(
    config_path: &str,
    document: &str,
) -> Result<indexmap::IndexMap<String, TableShape>, Box<dyn std::error::Error>> {
    let config = Config::from_yaml_file(config_path)?;
    let parser = Parser::new(&config)?;

    // Lints are the other half of the adoption story: they say what to change,
    // this says what changing it did.
    for lint in parser.warnings() {
        eprintln!("lint [{config_path}]: {lint}");
    }

    let batches = parser.parse_slice(&std::fs::read(document)?)?;
    Ok(batches
        .into_iter()
        .map(|(name, batch)| {
            let columns = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            (name, (batch.num_rows(), columns))
        })
        .collect())
}

fn column_names(shape: &TableShape) -> BTreeSet<String> {
    shape.1.iter().cloned().collect()
}
