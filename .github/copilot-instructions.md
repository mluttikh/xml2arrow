# Copilot Instructions: xml2arrow

**Canonical guide: [`AGENTS.md`](../AGENTS.md) at the repo root — read it
before making changes.** This file is a digest for Copilot surfaces that only
load `.github/copilot-instructions.md`; if anything here seems to conflict
with `AGENTS.md`, `AGENTS.md` wins.

## What this repo is

`xml2arrow` converts XML to Apache Arrow `RecordBatch`es in one streaming
pass. Rust, edition 2024; `quick-xml` events in, `arrow` builders out; YAML
`Config` maps XML paths to tables/fields via an integer-indexed path trie.

## Non-negotiable rules

1. **Hot path is allocation-free.** Inside `process_xml_events`,
   `process_xml_events_slice`, `handle_event`, `close_element`,
   `PathTracker::enter`, `set_field_value_for_node`: no cloning, no heap
   allocation (except Arrow builder appends), no string-keyed lookups — use
   `PathNodeId`. Outline rare/error paths with `#[cold]`/`#[inline(never)]`;
   code-size growth in `handle_event` has caused >10% regressions.
2. **Errors are structured.** Use `Error`/`ParseKind`/`ConfigIssue` in
   `errors.rs`. Any new `Error` variant MUST update `From<Error> for PyErr`
   and the `exhaustiveness_guard`/`sample_of_each_variant` tests — CI never
   runs python-feature tests, so the compile-time guard is load-bearing.
3. **Don't "fix" pinned behavior.** Missing non-nullable Utf8 → `""` (numerics
   error instead); repeated value-bearing field elements per row →
   `DuplicateValue` error (never concatenate); unresolvable entities in
   captured fields → error; attribute values are XML-1.0-normalized, element
   text is raw; rows are delimited only by *configured* child elements.
4. **Config changes require validation.** Extend `Config::validate()` (plus a
   `ConfigIssue` variant) so bad configs fail at load, not silently at parse.
5. **Public API** = `src/lib.rs` re-exports. `DType`/`ParserOptions` are
   exhaustive pub types — additions are breaking; flag them in the PR.

## Verification before finishing

```bash
cargo test
cargo fmt --check
cargo clippy -- -D warnings
cargo clippy --features python -- -D warnings
```

- Parser touched? Compare benchmarks:
  `cargo bench --bench parse_benchmark -- --save-baseline before` (on
  unchanged code) then `-- --baseline before` after changes. Re-check the
  baseline for machine drift before trusting a small regression.
- Bug fix? Add unit tests **and** an end-to-end repro in
  `tests/integration_tests.rs`.

## Style & git

- Literate programming: comment the *why* (XML quirks, Arrow constraints,
  perf reasoning), never the *how*; top-down narrative; no generic `utils.rs`.
- Conventional commits (`fix:`, `feat:`, `perf:`, `docs:`, `chore:`); no
  unrelated `Cargo.lock` churn; no `Co-Authored-By` trailers; branch + PR,
  never direct to `main`.
