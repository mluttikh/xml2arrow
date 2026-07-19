# AI Contributor Guide: xml2arrow

> **This is the canonical AI/agent guide.** `CLAUDE.md`, `GEMINI.md` and
> `.github/copilot-instructions.md` are thin pointers to this file — edit
> **this file only**, never the pointers, so all AI tools stay in sync.

## Project Overview

- **Crate:** `xml2arrow` converts XML documents to Apache Arrow `RecordBatch`es
  in a single streaming pass.
- **Stack:** Rust (edition 2024), `quick-xml` (event parsing), `arrow`
  (columnar builders), optional `pyo3` (the Python bindings live in a separate
  repo, `xml2arrow-python`, but share the error mapping in this crate).
- **Configuration:** a YAML-driven `Config` maps XML paths to Arrow tables and
  fields.

**Core philosophy:**
- **Allocation-free hot path:** the event loop must not allocate.
- **O(1) lookups:** integer `PathNodeId` indexing — never string matching
  during parsing.
- **Correctness over cleverness:** silent data corruption is the worst
  failure mode; prefer a structured error over a guessed value.

## Architecture & Data Flow

The converter operates in phases:

1. **Setup (allocations allowed):** `Config` is validated (`config.rs`);
   `PathRegistry` compiles every configured path into a trie of integer
   `PathNodeId`s (`path_registry.rs`). `Parser` owns this compiled state so it
   can be reused across many documents ("compile once, parse many").
2. **Streaming (hot path — no allocations):** two event loops in
   `xml_parser.rs` — `process_xml_events` (buffered readers) and
   `process_xml_events_slice` (zero-copy over `&[u8]`) — both delegate to
   `handle_event`, which is written once. `PathTracker` mirrors XML nesting
   with a stack of node IDs; unknown subtrees push placeholder frames so no
   registry lookups happen inside them. Text/CDATA bytes are appended to the
   active `FieldBuilder` raw; UTF-8 validation happens once per row per Utf8
   field, not per event.
3. **Finalization:** `TableBuilder::finish` drains builders into one
   `RecordBatch` per table, returned as an ordered `IndexMap`.

**Row semantics (load-bearing):** a row of table T is finalized when a
*configured* (registry-known) direct child element of T's `xml_path` closes.
Unknown elements never delimit rows. Parent-link `levels` columns are labels
for the index columns; their values come from ancestor tables' row counters.

## Critical Rules (MUST follow)

### 1. Hot-path performance
Inside `process_xml_events`, `process_xml_events_slice`, `handle_event`,
`close_element`, `PathTracker::enter`, and `set_field_value_for_node`:
- **NEVER** clone a `String`/`Vec` or allocate, except appends into Arrow builders.
- **NEVER** look anything up by string; use `PathNodeId` indexing.
- Reuse buffers (`attr_name_buffer`, `parent_indices_buffer`, `current_value`).
- Keep `handle_event` small: rare arms and error construction go into
  `#[inline(never)]` / `#[cold]` helpers (see `handle_general_ref`,
  `duplicate_value_error`). Code-size growth here has caused >10% regressions
  from lost inlining even when the added work was trivial.

### 2. Error handling
- Use the structured `Error` / `ParseKind` / `ConfigIssue` enums in
  `errors.rs` — never stringly-typed errors. `Display` output is a stability
  surface; don't reword existing messages casually.
- Adding an `Error` variant? You **MUST** update `From<Error> for PyErr`
  (under `#[cfg(feature = "python")]`) and the `exhaustiveness_guard` +
  `sample_of_each_variant` tests in `errors.rs`. The compile-time guard is
  load-bearing: CI only *clippy-checks* the python feature, it does not run
  python-feature tests.

### 3. Behavioral contracts (pinned by tests — do not "fix" these)
- Missing non-nullable **Utf8** yields `""`; missing non-nullable
  numerics/booleans raise `MissingRequiredField`. Intentional asymmetry.
- A repeated value-bearing occurrence of a field's element in one row raises
  `ParseKind::DuplicateValue` (never concatenate). A value-less repeat
  (`<v/><v>2</v>`) is fine.
- Character references (`&#66;`) are resolved; an unresolvable entity in a
  captured field raises `ParseKind::UnresolvedEntity`; entities outside
  captured fields are ignored.
- Attribute values get XML 1.0 normalization (tab/CR/LF → space) and entity
  decoding; element text is taken raw (spec-required difference).
- With `validate_attributes: false`, duplicate attributes concatenate — a
  documented trusted-input trade-off.
- Boolean parsing trims whitespace and accepts `true/false/1/0/yes/no/on/off/t/f/y/n`
  case-insensitively; numeric parsing does not trim (that's `trim_text`'s job).
- Tables with an empty `fields` list are structural only and excluded from
  the output map; index columns are named `<level>` (angle brackets included).

## Public API & Compatibility

Treat `src/lib.rs` re-exports as the public API; avoid breaking changes:
`parse_xml`, `parse_xml_slice`, `Parser`, `Config`, `TableConfig`,
`FieldConfig`, `FieldConfigBuilder`, `ParserOptions`, `DType`, `Error`,
`Result`, and the `config_from_yaml!` macro.

Beware of hidden breakage: `DType` and `ParserOptions` are exhaustive pub
types — adding a `DType` variant breaks downstream exhaustive matches, and
adding a `ParserOptions` field breaks struct-literal construction. Call this
out in the PR when unavoidable.

## Testing Conventions

- Parsing behavior is tested in `src/xml_parser.rs` `mod tests` using the
  `parse(xml, yaml)` helper, `config_from_yaml!`, and the
  `assert_array_values!` family of macros. Use `rstest` for parameterized cases.
- `tests/integration_tests.rs` covers file-based concerns (encodings, BOM,
  YAML config files, `Parser` reuse) **plus a "bug reproductions" section —
  every bug fix adds an end-to-end repro there** in addition to unit tests.
- Config validation tests live in `src/config.rs`; error-mapping tests in
  `src/errors.rs`.
- The full suite runs in seconds — run it often.

## Performance Verification (required when touching the parser)

```bash
# 1. Save a baseline from the UNCHANGED code (git stash if needed):
cargo bench --bench parse_benchmark -- --save-baseline before 'parse_(tiny|small|wide_fanout)'
# 2. Apply changes, then compare:
cargo bench --bench parse_benchmark -- --baseline before 'parse_(tiny|small|wide_fanout)'
```

- `parse_tiny` isolates per-parse setup cost (validation + registry build);
  `parse_small`/`medium`/`large`/`xlarge` are realistic sensor documents;
  `parse_wide_fanout` stresses per-element-open work (24 sibling fields).
- **Beware machine drift:** baselines go stale within an hour on a laptop.
  If results look bad, re-bench the *unchanged* code against its own baseline
  first — a few percent of "regression" is often thermal noise. CI runs
  CodSpeed for authoritative numbers.

## Common Workflows

### Adding a new data type (e.g. `Date32`)
1. Add the variant to `DType` in `config.rs` and its `as_arrow_type` arm.
2. In `xml_parser.rs`: add a `TypedArrayBuilder` variant, plus arms in
   `from_dtype` and `finish`.
3. Add the byte-slice → value parse arm in `FieldBuilder::append_current_value`.
4. If scale/offset should (not) apply, update `FieldConfig::validate`.
5. Tests: parse + null/missing + overflow cases in `xml_parser.rs`, and the
   `as_arrow_type` conversion test in `config.rs`. Update the README's
   supported-types list.

### Modifying configuration logic
1. Change the struct in `config.rs`.
2. **Crucial:** extend `Config::validate()` (and add a `ConfigIssue` variant
   with a `Display` arm for new failure modes) — invalid configs must fail
   loudly at load, not silently at parse.
3. Add validation tests; check YAML round-trip still passes; update the
   README's config schema block.

## Repository Map

- `src/lib.rs` — public re-exports (the API surface).
- `src/config.rs` — YAML config structs; **all validation lives here**.
- `src/path_registry.rs` — integer path trie + `PathTracker`. Performance critical.
- `src/xml_parser.rs` — event loops, builders, row finalization. Performance critical.
- `src/errors.rs` — structured errors + Python exception mapping.
- `tests/` — integration tests (`common/` has shared macros/helpers).
- `benches/parse_benchmark.rs` — criterion/CodSpeed benchmarks.
- `examples/profile_large.rs` — profiling target for large documents.

## Commands & Pre-Finish Checklist

```bash
cargo test                                  # full suite incl. doctests
cargo fmt --check                           # CI-enforced
cargo clippy -- -D warnings                 # CI-enforced
cargo clippy --features python -- -D warnings   # CI-enforced
cargo check --features python               # python bindings still compile
```

Before declaring work done:
1. All of the above pass.
2. Parser touched? Benchmark comparison done (see above).
3. `Error`/`ConfigIssue` touched? PyErr mapping + guard tests updated.
4. Bug fix? Repro added to `tests/integration_tests.rs`.
5. Public API touched? Doc examples still compile (doctests run in `cargo test`).

## Code Style: Literate Programming

Code is a document for human logic verification:

1. **Document the "why", not the "how".** No syntax narration; explain intent,
   XML quirks, Arrow constraints, and performance reasoning at the site.
2. **Top-down narrative:** public API → coordination → helpers, with section
   headers (e.g. `// --- Event loop implementations ---`) in long files.
3. **Inline context over external docs** — reasoning lives next to the code.
4. **Strategic abstraction:** extract a function only when reused, a genuinely
   independent unit, or it simplifies the caller's narrative. No sprawl of
   single-use micro-functions — but *do* outline cold/error paths from hot
   functions (that's performance, not style).
5. **No generic `utils.rs`.** A helper used by one module stays private in it.

## Git & PR Conventions

- Conventional commit prefixes, matching history: `fix:`, `feat:`, `perf:`,
  `docs:`, `chore:`.
- Keep commits source-focused: no unrelated `Cargo.lock` churn, no editor or
  AI-tool artifacts, and no `Co-Authored-By` trailers.
- Never commit directly to `main`; work on a branch and let CI (fmt, clippy,
  tests on Linux/macOS/Windows, CodSpeed) pass before merge.
