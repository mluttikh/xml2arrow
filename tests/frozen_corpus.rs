//! The frozen corpus: a behavioral freeze over the whole 0.x line.
//!
//! Every case is a `(config, document)` pair whose output is snapshotted in
//! `tests/corpus/<case>/expected/`. The suite asserts that **nothing** changes
//! that output — the enforcement mechanism behind the transition plan's
//! compatibility contract C1 ("for any config that does not opt in, output is
//! byte-identical through the entire 0.x line"). Review cannot enforce that;
//! this can.
//!
//! Two things are frozen at once:
//!
//! - **Behavior**, per case: rows, columns, types, nullability, values — and
//!   for failing cases, the error's `Display`, which is itself a documented
//!   stability surface.
//! - **Driver parity**: every case runs through `Parser::parse` (buffered),
//!   `Parser::parse_slice` (zero-copy) and `Parser::parse_batches` (streaming,
//!   with a batch size small enough to force flushes), and all three must agree
//!   with the same snapshot. That pins the flush-transparency invariant and,
//!   more importantly, guards the upcoming unification of the three event pumps:
//!   a refactor that changes one path and not the others fails here.
//!
//! # Adding a case
//!
//! Create `tests/corpus/<name>/{config.yaml,input.xml}`, then bless it:
//!
//! ```text
//! XML2ARROW_FREEZE=1 cargo test --test frozen_corpus
//! ```
//!
//! # When a case fails
//!
//! A failure means output changed. That is a finding, not a chore: either the
//! change is a bug, or it is an intentional, *catalogued* semantic change (see
//! `DESIGN_V2.md` §10.1) that belongs to an opt-in. Re-blessing is a deliberate
//! act — read the diff first.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use indexmap::IndexMap;
use xml2arrow::{BatchOptions, Config, Error, Parser};

/// Set to re-record every snapshot instead of comparing against it.
const FREEZE_ENV: &str = "XML2ARROW_FREEZE";

/// Small enough that every multi-row case flushes at least once mid-parse, so
/// the streaming path is exercised as a *stream* rather than as one batch.
const STREAMING_BATCH_ROWS: usize = 2;

#[test]
fn frozen_corpus_output_is_unchanged() {
    let cases = discover_cases();
    assert!(
        !cases.is_empty(),
        "no corpus cases found under tests/corpus — the freeze would pass vacuously"
    );

    let freezing = std::env::var_os(FREEZE_ENV).is_some();
    let mut failures = Vec::new();
    for case in &cases {
        if let Err(message) = run_case(case, freezing) {
            failures.push(format!(
                "--- {} ---\n{message}",
                case.file_name().unwrap().to_string_lossy()
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} frozen corpus case(s) changed:\n\n{}\n\n\
         Output changed. Decide which it is:\n\
         - a bug: fix the code;\n\
         - an intentional, catalogued change behind an opt-in: update the \
           catalogue, then re-record with `{FREEZE_ENV}=1 cargo test --test \
           frozen_corpus`.",
        failures.len(),
        cases.len(),
        failures.join("\n\n")
    );

    if freezing {
        // Loud on purpose: a re-record that slips into a PR unnoticed defeats
        // the whole mechanism.
        println!(
            "RE-RECORDED {} corpus case(s) — review the diff",
            cases.len()
        );
    }
}

/// Case directories under `tests/corpus`, sorted so failures list in a stable
/// order.
fn discover_cases() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus");
    let mut cases: Vec<PathBuf> = fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", root.display()))
        .map(|entry| entry.expect("corpus dir entry").path())
        .filter(|path| path.is_dir())
        .collect();
    cases.sort();
    cases
}

/// Runs one case through all three entry points and compares (or records) its
/// snapshot. The `Err` payload is a human-readable explanation, collected by
/// the caller so one run reports every drift rather than only the first.
fn run_case(dir: &Path, freezing: bool) -> Result<(), String> {
    let config_yaml = read(&dir.join("config.yaml"))?;
    let xml = read(&dir.join("input.xml"))?;
    let config: Config =
        yaml_serde::from_str(&config_yaml).map_err(|e| format!("invalid config.yaml: {e}"))?;
    let parser = Parser::new(&config).map_err(|e| format!("Parser::new failed: {e}"))?;

    let buffered = parser.parse(xml.as_bytes());
    let zero_copy = parser.parse_slice(xml.as_bytes());
    let streamed = collect_streamed(&parser, xml.as_bytes());

    match buffered {
        Err(error) => {
            // A failing case freezes the message. The other two drivers must
            // fail identically — same class, same text.
            let rendered = error.to_string();
            expect_same_error(&zero_copy, &rendered, "zero-copy")?;
            expect_same_error(&streamed, &rendered, "streaming")?;
            compare_or_record(&dir.join("expected/error.txt"), &rendered, freezing)?;
            expect_no_table_snapshots(dir, freezing)
        }
        Ok(tables) => {
            let zero_copy = zero_copy.map_err(|e| format!("zero-copy path failed: {e}"))?;
            let streamed = streamed.map_err(|e| format!("streaming path failed: {e}"))?;

            let expected_dir = dir.join("expected");
            let mut recorded = BTreeSet::new();
            for (name, batch) in &tables {
                let snapshot = snapshot_batch(name, batch);
                compare_or_record(
                    &expected_dir.join(format!("{name}.txt")),
                    &snapshot,
                    freezing,
                )?;
                recorded.insert(format!("{name}.txt"));

                // Driver parity, checked against the same rendering the
                // snapshot uses, so a mismatch reports as a readable diff.
                let zc = zero_copy
                    .get(name)
                    .ok_or_else(|| format!("table '{name}' missing from the zero-copy result"))?;
                diff(&snapshot, &snapshot_batch(name, zc), "zero-copy")?;
                // A table with no rows yields no batch at all, so the streaming
                // result simply lacks it where the collecting result returns it
                // empty. That asymmetry is pinned here rather than hidden: the
                // substitution is only legal for a genuinely empty table, and a
                // missing *non-empty* table is a hard failure.
                let empty;
                let st = match streamed.get(name) {
                    Some(batch) => batch,
                    None if batch.num_rows() == 0 => {
                        empty = RecordBatch::new_empty(batch.schema());
                        &empty
                    }
                    None => {
                        return Err(format!(
                            "table '{name}' has {} row(s) when collected but never appeared in \
                             the stream",
                            batch.num_rows()
                        ));
                    }
                };
                diff(&snapshot, &snapshot_batch(name, st), "streaming")?;
            }

            if freezing {
                remove_stale(&expected_dir, &recorded)?;
            } else {
                expect_exactly(&expected_dir, &recorded)?;
            }
            Ok(())
        }
    }
}

/// Drives the streaming entry point to completion and concatenates each table's
/// batches, which by the flush-transparency invariant must reproduce the
/// collect-everything output exactly.
///
/// Returns only what the stream actually yielded — tables with no rows are
/// absent, which the caller handles explicitly.
fn collect_streamed(parser: &Parser, xml: &[u8]) -> Result<IndexMap<String, RecordBatch>, Error> {
    let options = BatchOptions::default().with_max_rows_per_batch(STREAMING_BATCH_ROWS);
    let mut grouped: IndexMap<String, Vec<RecordBatch>> = IndexMap::new();
    for item in parser.parse_batches_slice(xml, options) {
        let batch = item?;
        grouped
            .entry(batch.table.to_string())
            .or_default()
            .push(batch.batch);
    }

    let mut tables = IndexMap::new();
    for (name, batches) in grouped {
        let schema = batches[0].schema();
        tables.insert(name, concat_batches(&schema, &batches)?);
    }
    Ok(tables)
}

/// Renders a batch as a deterministic, diff-friendly text snapshot.
///
/// Deliberately not Arrow IPC: a binary blob makes a re-record unreviewable in
/// a pull request, and the point of the freeze is that a human reads the change
/// before accepting it. Values are quoted (so trailing whitespace and embedded
/// separators are visible) and nulls are a bare `null`, which is what makes the
/// "missing non-nullable Utf8 yields an empty string" asymmetry legible here.
fn snapshot_batch(name: &str, batch: &RecordBatch) -> String {
    let options = FormatOptions::default();
    let formatters: Vec<ArrayFormatter> = batch
        .columns()
        .iter()
        .map(|column| {
            ArrayFormatter::try_new(column.as_ref(), &options).expect("array is formattable")
        })
        .collect();

    let mut out = String::new();
    out.push_str("# frozen snapshot — see tests/frozen_corpus.rs\n");
    out.push_str(&format!("table: {name}\n"));
    out.push_str(&format!("rows: {}\n", batch.num_rows()));
    out.push_str("columns:\n");
    for (index, field) in batch.schema().fields().iter().enumerate() {
        out.push_str(&format!(
            "  {}. {}: {:?} ({})\n",
            index + 1,
            field.name(),
            field.data_type(),
            if field.is_nullable() {
                "nullable"
            } else {
                "not null"
            },
        ));
    }
    out.push_str("data:\n");
    for row in 0..batch.num_rows() {
        let cells: Vec<String> = batch
            .columns()
            .iter()
            .zip(&formatters)
            .map(|(column, formatter)| {
                if column.is_null(row) {
                    "null".to_string()
                } else {
                    format!("{:?}", formatter.value(row).to_string())
                }
            })
            .collect();
        out.push_str(&format!("  {}\n", cells.join(" | ")));
    }
    out
}

fn expect_same_error<T>(
    result: &Result<T, Error>,
    expected: &str,
    path: &str,
) -> Result<(), String> {
    match result {
        Err(error) if error.to_string() == expected => Ok(()),
        Err(error) => Err(format!(
            "the {path} path failed differently:\n  buffered: {expected}\n  {path}: {error}"
        )),
        Ok(_) => Err(format!(
            "the {path} path succeeded where the buffered path failed with: {expected}"
        )),
    }
}

fn compare_or_record(path: &Path, actual: &str, freezing: bool) -> Result<(), String> {
    if freezing {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("cannot create {parent:?}: {e}"))?;
        }
        return fs::write(path, actual).map_err(|e| format!("cannot write {path:?}: {e}"));
    }
    let expected = read(path).map_err(|_| {
        format!(
            "no snapshot at {}\n{actual}\nRecord it with `{FREEZE_ENV}=1 cargo test --test frozen_corpus`",
            path.display()
        )
    })?;
    diff(&expected, actual, "buffered")
}

/// Reports the first differing line with context — enough to see *what*
/// changed without printing two whole snapshots.
fn diff(expected: &str, actual: &str, path: &str) -> Result<(), String> {
    if expected == actual {
        return Ok(());
    }
    let mismatch = expected
        .lines()
        .zip(actual.lines())
        .enumerate()
        .find(|(_, (e, a))| e != a);
    let (expected_lines, actual_lines) = (expected.lines().count(), actual.lines().count());
    let detail = match mismatch {
        Some((line, (e, a))) => format!("line {}:\n  expected: {e}\n  actual:   {a}", line + 1),
        None if expected_lines != actual_lines => {
            format!("line count differs: expected {expected_lines} line(s), got {actual_lines}")
        }
        // Every line compares equal and there are as many of them, yet the
        // texts differ: the difference is in bytes `str::lines` discards — a
        // trailing `\r`, or a missing final newline. Reporting "line count
        // differs: 21 vs 21" here (the first version of this code) sends the
        // reader looking in exactly the wrong place.
        None => {
            let at = expected
                .chars()
                .zip(actual.chars())
                .position(|(e, a)| e != a)
                .unwrap_or_else(|| expected.chars().count().min(actual.chars().count()));
            format!(
                "identical lines but differing raw text at character {at} — line endings or a \
                 trailing newline:\n  expected: {}\n  actual:   {}",
                context(expected, at),
                context(actual, at)
            )
        }
    };
    Err(format!("{path} output changed — {detail}"))
}

/// A short window around `at`, escaped so control characters — the whole point
/// when the difference is a stray `\r` — are visible.
fn context(text: &str, at: usize) -> String {
    let start = at.saturating_sub(12);
    let window: String = text.chars().skip(start).take(28).collect();
    format!("…{}…", window.escape_debug())
}

/// A case that must fail may not carry table snapshots: a stale one would be a
/// silent claim that the case once succeeded.
fn expect_no_table_snapshots(dir: &Path, freezing: bool) -> Result<(), String> {
    let expected_dir = dir.join("expected");
    let mut only_error = BTreeSet::new();
    only_error.insert("error.txt".to_string());
    if freezing {
        remove_stale(&expected_dir, &only_error)
    } else {
        expect_exactly(&expected_dir, &only_error)
    }
}

fn expect_exactly(dir: &Path, expected: &BTreeSet<String>) -> Result<(), String> {
    let found = list_snapshots(dir)?;
    if &found == expected {
        return Ok(());
    }
    let stale: Vec<&String> = found.difference(expected).collect();
    let missing: Vec<&String> = expected.difference(&found).collect();
    Err(format!(
        "snapshot files do not match the tables produced (stale: {stale:?}, missing: {missing:?})"
    ))
}

fn remove_stale(dir: &Path, keep: &BTreeSet<String>) -> Result<(), String> {
    for name in list_snapshots(dir)?.difference(keep) {
        let path = dir.join(name);
        fs::remove_file(&path).map_err(|e| format!("cannot remove {path:?}: {e}"))?;
        println!("removed stale snapshot {}", path.display());
    }
    Ok(())
}

fn list_snapshots(dir: &Path) -> Result<BTreeSet<String>, String> {
    if !dir.exists() {
        return Ok(BTreeSet::new());
    }
    fs::read_dir(dir)
        .map_err(|e| format!("cannot read {dir:?}: {e}"))?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .map_err(|e| format!("cannot read {dir:?}: {e}"))
        })
        .collect()
}

/// Reads a corpus file with line endings normalized to LF.
///
/// The corpus is byte-exact — the expected errors carry byte offsets into
/// `input.xml`, so on a checkout that rewrote LF to CRLF every offset shifts by
/// the number of preceding lines, and every snapshot differs from what the
/// harness generates. `.gitattributes` pins these files to LF, and normalizing
/// here as well means an already-converted working tree, or an editor that
/// saved CRLF, still passes rather than failing 20 cases at once.
fn read(path: &Path) -> Result<String, String> {
    fs::read_to_string(path)
        .map(|text| text.replace("\r\n", "\n"))
        .map_err(|e| format!("cannot read {}: {e}", path.display()))
}
