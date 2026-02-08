# AI Agents Guide

This file helps AI assistants work effectively in this repository. Keep changes minimal, focus on correctness and performance, and verify behavior with tests and benchmarks when relevant.

## Project overview

- `xml2arrow` is a Rust crate that converts XML data into Apache Arrow tables.
- XML parsing uses `quick-xml`, and Arrow structures use the Rust Arrow crate (see `README.md`).
- Configuration is driven by YAML mappings that define tables, fields, XML paths, and Arrow data types.

## Public API surface

The crate root (`src/lib.rs`) re-exports everything that is public. Internal modules (`path_registry`, `xml_path`) are private.

- `parse_xml(reader: impl BufRead, config: &Config) -> Result<IndexMap<String, RecordBatch>>` — main entry point; streams XML events and returns named Arrow record batches.
- `Config` — top-level configuration; holds a list of `TableConfig` and `ParserOptions`. Can be loaded from a YAML file via `Config::from_yaml_file`.
- `TableConfig` — defines one output table: its name, XML path, optional nesting levels, and fields.
- `FieldConfig` — maps an XML path to a named Arrow column with a data type, nullability, and optional scale/offset.
- `FieldConfigBuilder` — builder for constructing `FieldConfig` programmatically.
- `DType` — enum of supported Arrow data types (Boolean, Float32, Float64, Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64, Utf8).
- `ParserOptions` — parser options such as `trim_text`.
- `Error`, `Result` — crate error type and result alias.

When modifying any of these types, treat them as public API: avoid breaking changes and update `README.md` accordingly.

## Architecture / data flow

The crate is a single-pass, streaming XML-to-Arrow converter. The modules are layered as follows:

1. **Configuration** (`config.rs`): The user provides a `Config` (from YAML or built programmatically) that declares tables, fields, XML paths, and Arrow data types.
2. **Path registry** (`path_registry.rs`): At startup, `PathRegistry::from_config` builds a trie from all configured XML paths and assigns each node an integer `PathNodeId`. This enables O(1) lookups during parsing instead of string-based hash matching.
3. **Path tracking** (`path_registry.rs` — `PathTracker`): During parsing, a `PathTracker` maintains a stack of `PathNodeId`s that mirrors the current XML nesting depth. As `quick-xml` emits `Start`/`End` events, the tracker pushes/pops IDs and resolves whether the current path is known to the registry.
4. **XML event loop** (`xml_parser.rs` — `process_xml_events`): Streams XML events from `quick-xml::Reader`. On each event:
   - `Start` → enters the path in the tracker; if the path is a table boundary, pushes a new `TableStackEntry`.
   - `Text` / `CData` → sets the current value on the active `FieldBuilder` via the `XmlToArrowConverter`.
   - `End` → pops the path; if leaving a table row, finalises the row (appends values or nulls to all field builders).
   - Attribute parsing is toggled at compile time via a const generic (`PARSE_ATTRIBUTES`) to avoid overhead when no fields use `@`-prefixed paths.
5. **Arrow conversion** (`xml_parser.rs` — `XmlToArrowConverter`, `TableBuilder`, `FieldBuilder`):
   - `XmlToArrowConverter` holds a `Vec<TableBuilder>` (one per configured table) and a `builder_stack` for nested table scoping.
   - Each `TableBuilder` owns `FieldBuilder`s that accumulate values into Arrow array builders (`StringBuilder`, `Float64Builder`, etc.).
   - On `finish()`, each `TableBuilder` drains its builders into a `RecordBatch`. The converter collects all batches into an `IndexMap<String, RecordBatch>` keyed by table name.
6. **XML path** (`xml_path.rs`): Legacy string-based path type retained for backward compatibility and tests. Not used in the main parsing hot path.

**Key performance details:**
- Parsing is single-pass and streaming — no DOM is built.
- Path lookups use integer IDs from the trie, not string comparisons.
- `string_cache::Atom` is used for interned element names; `fxhash` provides fast hashing in internal maps.
- `indexmap` preserves table insertion order in the output.

## Repository map

- `src/`: crate implementation
- `tests/`: test suite
- `benches/`: Criterion benchmarks
- `README.md`: usage, YAML mapping, and examples
- `Cargo.toml`: crate metadata and dependencies
- `target/`: build output (do not edit)

## Common commands

```/dev/null/commands.sh#L1-6
# Build
cargo build

# Tests
cargo test

# Benchmarks (Criterion)
cargo bench
```

### Benchmark baselines (Criterion)

```/dev/null/commands.sh#L1-6
# Save a baseline
cargo bench --bench parse_benchmark -- --save-baseline <name>

# Compare to a baseline
cargo bench --bench parse_benchmark -- --baseline <name>
```

## Feature flags

- `python` (optional): Enables PyO3 bindings for Python interop.
  - All Python-specific code is guarded by `#[cfg(feature = "python")]` and lives in `src/errors.rs`.
  - This includes custom Python exception types (`Xml2ArrowError`, `XmlParsingError`, `YamlParsingError`, etc.) and the `From<Error> for PyErr` conversion.
  - Build: `cargo build --features python`
  - Test: `cargo test --features python`
  - When adding or modifying variants in the `Error` enum, update the corresponding `From<Error> for PyErr` implementation and add a matching `create_exception!` call.

## Development guidance for AI agents

- Prefer small, targeted changes. Avoid refactors unless they are required.
- Preserve performance-sensitive behavior; benchmark changes that affect XML parsing or Arrow conversion.
- Update `README.md` if you change public behavior, configuration schema, or examples.
- Add or update tests when fixing bugs or adding features.
- Avoid touching generated artifacts under `target/`.

### Literate Programming

**Goal:** Code must explain reasoning and intent, not only implementation.

**Rules**

1. **Explain Why**

   * Comments describe purpose, assumptions, constraints, and tradeoffs.
   * Do not comment obvious syntax behavior.

2. **Top-Down Narrative**

   * Structure files as logical phases that read from high-level intent to detailed steps.
   * Use clear section headers for each conceptual step.

3. **Inline Context**

   * Place explanations immediately seeable above the code they describe.
   * Avoid distant or centralized explanations.

4. **Avoid Over-Abstraction**

   * Prefer readable inline logic with good documentation over splitting sequential logic into many small functions.
   * Introduce functions only for reuse, meaningful abstraction, or independent conceptual units.

5. **Self-Contained Logic**

   * Avoid introducing shared utilities for trivial operations.
   * Inline simple logic when doing so improves readability and reduces cross-file navigation.

**Apply When**

* Implementing algorithms, workflows, integrations, or business/domain logic
* Multi-step processes where reasoning is not obvious

**Avoid Over-Documenting**

* Trivial utilities
* Obvious wrappers
* Simple getters/setters
* Standard boilerplate patterns

**Expected Outcome**
Each file should read as a short narrative:
section intent → reasoning → implementation.

## YAML configuration notes

- `tables` define record batches and map XML paths to Arrow fields.
- Field definitions include `name`, `xml_path`, `data_type`, and optional `nullable`, `scale`, `offset`.
- `levels` can define nested index levels for hierarchical XML structures.

For details and full examples, see `README.md`.
